use crate::{AttemptBudget, BoxFuture, ConfigError, Strategy, StrategyError, TimeBudget};
use std::{
    collections::VecDeque,
    sync::{Arc, Mutex, MutexGuard},
    time::Duration,
};
use tokio::time::Instant;

#[derive(Clone, Debug)]
pub struct BreakerConfig {
    pub window_size: usize,
    pub min_samples: usize,
    /// Opens only when the failure fraction is strictly greater than this value.
    pub failure_threshold: f64,
    pub cooldown: Duration,
    pub count_client_errors: bool,
}
impl Default for BreakerConfig {
    fn default() -> Self {
        Self {
            window_size: 20,
            min_samples: 10,
            failure_threshold: 0.5,
            cooldown: Duration::from_secs(5),
            count_client_errors: false,
        }
    }
}
impl BreakerConfig {
    pub fn for_assignment() -> Self {
        Self {
            count_client_errors: true,
            ..Self::default()
        }
    }
    fn validate(&self) -> Result<(), ConfigError> {
        if self.window_size == 0 || self.min_samples == 0 || self.min_samples > self.window_size {
            return Err(ConfigError(
                "breaker requires 1 <= min_samples <= window_size",
            ));
        }
        if !self.failure_threshold.is_finite() || !(0.0..=1.0).contains(&self.failure_threshold) {
            return Err(ConfigError(
                "breaker failure threshold must be between zero and one",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BreakerStatus {
    Closed,
    Open,
    HalfOpen,
}

/// Share one state per endpoint across request-scoped circuit-breaker wrappers.
/// Locks protect short state transitions and are never held across an await.
pub struct BreakerState {
    config: BreakerConfig,
    inner: Mutex<Inner>,
}
struct Inner {
    phase: Phase,
    epoch: u64,
    window: VecDeque<bool>,
}
enum Phase {
    Closed,
    Open { until: Option<Instant> },
    HalfOpen { probe_running: bool },
}

impl BreakerState {
    pub fn new(config: BreakerConfig) -> Result<Self, ConfigError> {
        config.validate()?;
        Ok(Self {
            config,
            inner: Mutex::new(Inner {
                phase: Phase::Closed,
                epoch: 0,
                window: VecDeque::new(),
            }),
        })
    }
    pub fn status(&self) -> BreakerStatus {
        match self.lock().phase {
            Phase::Closed => BreakerStatus::Closed,
            Phase::Open { .. } => BreakerStatus::Open,
            Phase::HalfOpen { .. } => BreakerStatus::HalfOpen,
        }
    }
    fn lock(&self) -> MutexGuard<'_, Inner> {
        self.inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn admit(self: &Arc<Self>) -> Result<Permit, StrategyError> {
        let mut inner = self.lock();
        if let Phase::Open { until } = inner.phase {
            if !until.is_some_and(|deadline| Instant::now() >= deadline) {
                return Err(StrategyError::Unavailable);
            }
            inner.phase = Phase::HalfOpen {
                probe_running: false,
            };
            inner.epoch = inner.epoch.wrapping_add(1);
        }
        let probe = match &mut inner.phase {
            Phase::Closed => false,
            Phase::HalfOpen { probe_running } if !*probe_running => {
                *probe_running = true;
                true
            }
            _ => return Err(StrategyError::Unavailable),
        };
        Ok(Permit {
            state: Arc::clone(self),
            epoch: inner.epoch,
            probe,
            active: true,
        })
    }

    fn failure<T>(&self, result: &Result<T, StrategyError>) -> bool {
        matches!(
            result,
            Err(StrategyError::ServerError(_) | StrategyError::Timeout(_))
        ) || (self.config.count_client_errors
            && matches!(result, Err(StrategyError::ClientError(_))))
    }
    fn open(&self, inner: &mut Inner) {
        inner.phase = Phase::Open {
            until: Instant::now().checked_add(self.config.cooldown),
        };
        inner.epoch = inner.epoch.wrapping_add(1);
    }
}

struct Permit {
    state: Arc<BreakerState>,
    epoch: u64,
    probe: bool,
    active: bool,
}
impl Permit {
    fn complete<T>(mut self, result: &Result<T, StrategyError>) {
        self.active = false;
        let mut inner = self.state.lock();
        if inner.epoch != self.epoch {
            return;
        }
        if self.probe {
            if matches!(result, Err(StrategyError::Unavailable)) {
                if let Phase::HalfOpen { probe_running } = &mut inner.phase {
                    *probe_running = false;
                }
            } else if !self.state.failure(result) {
                inner.phase = Phase::Closed;
                inner.window.clear();
                inner.epoch = inner.epoch.wrapping_add(1);
            } else {
                self.state.open(&mut inner);
            }
        } else if matches!(inner.phase, Phase::Closed) {
            // Unavailable is an absence of a sample, not an endpoint response.
            if matches!(result, Err(StrategyError::Unavailable)) {
                return;
            }
            inner.window.push_back(self.state.failure(result));
            if inner.window.len() > self.state.config.window_size {
                inner.window.pop_front();
            }
            let failures = inner.window.iter().filter(|failed| **failed).count();
            if inner.window.len() >= self.state.config.min_samples
                && failures as f64 / inner.window.len() as f64 > self.state.config.failure_threshold
            {
                self.state.open(&mut inner);
            }
        }
    }
}
impl Drop for Permit {
    fn drop(&mut self) {
        if self.active && self.probe {
            let mut inner = self.state.lock();
            if inner.epoch == self.epoch
                && let Phase::HalfOpen { probe_running } = &mut inner.phase
            {
                *probe_running = false;
            }
        }
    }
}

/// Put the timeout inside this wrapper so client timeouts become breaker samples.
pub struct CircuitBreaker<S> {
    child: S,
    state: Arc<BreakerState>,
}
impl<S> CircuitBreaker<S> {
    pub fn new(child: S, state: Arc<BreakerState>) -> Self {
        Self { child, state }
    }
}
impl<S: Strategy> Strategy for CircuitBreaker<S> {
    type Output = S::Output;
    fn execute(&mut self) -> BoxFuture<'_, Result<Self::Output, StrategyError>> {
        Box::pin(async {
            let permit = self.state.admit()?;
            let result = self.child.execute().await;
            permit.complete(&result);
            result
        })
    }
    fn max_requests(&self) -> AttemptBudget {
        self.child.max_requests()
    }
    fn max_time(&self) -> TimeBudget {
        self.child.max_time()
    }
}
