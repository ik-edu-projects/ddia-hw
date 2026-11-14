use crate::{
    AttemptBudget, BoxFuture, ConfigError, Strategy, StrategyError, TimeBudget,
    core::{sleep_for, within},
};
use std::{
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};
use tokio::time::Instant;

/// Policy for server-requested minimum waits between attempts or branches.
#[derive(Clone, Copy, Debug)]
pub struct RetryAfterConfig {
    pub respect: bool,
    /// A larger server wait stops execution with the original error; it is never clamped.
    pub max_wait: TimeBudget,
}
impl Default for RetryAfterConfig {
    fn default() -> Self {
        Self {
            respect: true,
            max_wait: TimeBudget::Limited(Duration::from_secs(30)),
        }
    }
}
impl RetryAfterConfig {
    pub(crate) fn remember(self, saved: &mut Option<Instant>, error: &StrategyError) {
        if self.respect
            && let Some(next) = error.not_before()
        {
            *saved = Some(saved.map_or(next, |old| old.max(next)));
        }
    }

    pub(crate) fn wait(self, saved: Option<Instant>) -> Option<Duration> {
        if !self.respect {
            return Some(Duration::ZERO);
        }
        let delay = saved.map_or(Duration::ZERO, |time| {
            time.saturating_duration_since(Instant::now())
        });
        match self.max_wait {
            TimeBudget::Limited(max) if delay > max => None,
            _ => Some(delay),
        }
    }

    pub(crate) fn bound(self) -> TimeBudget {
        if self.respect {
            self.max_wait
        } else {
            TimeBudget::Limited(Duration::ZERO)
        }
    }
}

/// Exponential backoff for server errors and client timeouts. Fast errors leave
/// the exponential sequence unchanged. Full jitter samples from zero to the cap.
#[derive(Clone, Copy, Debug)]
pub struct Backoff {
    pub initial: Duration,
    pub multiplier: f64,
    pub max_delay: Duration,
    pub jitter: bool,
    /// A fixed seed makes each execution reproducible; `None` varies the seed.
    pub seed: Option<u64>,
}
impl Default for Backoff {
    fn default() -> Self {
        Self {
            initial: Duration::from_millis(100),
            multiplier: 2.0,
            max_delay: Duration::from_secs(2),
            jitter: true,
            seed: None,
        }
    }
}
impl Backoff {
    fn validate(self) -> Result<(), ConfigError> {
        if !self.multiplier.is_finite() || self.multiplier < 1.0 {
            return Err(ConfigError(
                "backoff multiplier must be finite and at least one",
            ));
        }
        if self.initial > self.max_delay {
            return Err(ConfigError("initial backoff must not exceed maximum delay"));
        }
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub struct RetryConfig {
    /// Includes the first execution of the child.
    pub max_runs: AttemptBudget,
    pub fast_error_limit: AttemptBudget,
    pub failure_limit: AttemptBudget,
    pub timeout_limit: AttemptBudget,
    pub total_time: TimeBudget,
    pub backoff: Option<Backoff>,
    pub retry_after: RetryAfterConfig,
}
impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_runs: (..=3).into(),
            fast_error_limit: (..=3).into(),
            failure_limit: (..=3).into(),
            timeout_limit: (..=2).into(),
            total_time: (..=Duration::from_secs(5)).into(),
            backoff: None,
            retry_after: RetryAfterConfig::default(),
        }
    }
}
impl RetryConfig {
    fn validate(&self) -> Result<(), ConfigError> {
        if [
            self.max_runs,
            self.fast_error_limit,
            self.failure_limit,
            self.timeout_limit,
        ]
        .contains(&AttemptBudget::Limited(0))
        {
            return Err(ConfigError(
                "retry limits must be at least one or unlimited",
            ));
        }
        if let Some(backoff) = self.backoff {
            backoff.validate()?;
        }
        Ok(())
    }

    fn runs_bound(&self) -> AttemptBudget {
        let mut threshold = AttemptBudget::Limited(1);
        for limit in [
            self.fast_error_limit,
            self.failure_limit,
            self.timeout_limit,
        ] {
            threshold = threshold.add(match limit {
                AttemptBudget::Limited(n) => AttemptBudget::Limited(n - 1),
                AttemptBudget::Unlimited => AttemptBudget::Unlimited,
            });
        }
        self.max_runs.min(threshold)
    }
}

/// Repeats one child until success, a terminal error or a retry/latency limit.
/// Counters and backoff reset per execution; server cooldowns survive cancellation.
pub struct Retry<S> {
    child: S,
    config: RetryConfig,
    cooldown: Option<Instant>,
    cooldown_error: Option<StrategyError>,
}
impl<S> Retry<S> {
    pub fn new(child: S, config: RetryConfig) -> Result<Self, ConfigError> {
        config.validate()?;
        Ok(Self {
            child,
            config,
            cooldown: None,
            cooldown_error: None,
        })
    }
}
impl<S: Strategy> Strategy for Retry<S> {
    type Output = S::Output;
    fn execute(&mut self) -> BoxFuture<'_, Result<Self::Output, StrategyError>> {
        let limit = self.config.total_time;
        Box::pin(async move { within(limit, || self.run()).await })
    }
    fn max_requests(&self) -> AttemptBudget {
        self.child.max_requests().multiply(self.config.runs_bound())
    }
    fn max_time(&self) -> TimeBudget {
        let runs = self.config.runs_bound();
        if runs == AttemptBudget::Unlimited {
            return self.config.total_time;
        }
        let backoff =
            TimeBudget::Limited(self.config.backoff.map_or(Duration::ZERO, |x| x.max_delay));
        self.child
            .max_time()
            .add(backoff)
            .add(self.config.retry_after.bound())
            .multiply(runs)
            .min(self.config.total_time)
    }
}
impl<S: Strategy> Retry<S> {
    async fn run(&mut self) -> Result<S::Output, StrategyError> {
        let mut runs = 0_u64;
        let (mut fast, mut failures, mut timeouts) = (0_u64, 0_u64, 0_u64);
        let mut backoff = BackoffSequence::new(self.config.backoff);
        let mut local_delay = Duration::ZERO;
        loop {
            let server_delay = self.config.retry_after.wait(self.cooldown).ok_or_else(|| {
                self.cooldown_error
                    .clone()
                    .unwrap_or(StrategyError::Unavailable)
            })?;
            let delay = local_delay.max(server_delay);
            sleep_for(delay).await;
            let result = self.child.execute().await;
            runs = runs.saturating_add(1);
            let error = match result {
                Ok(value) => return Ok(value),
                Err(error) => error,
            };
            self.config.retry_after.remember(&mut self.cooldown, &error);
            if error.not_before().is_some() {
                self.cooldown_error = Some(error.clone());
            }
            if !error.is_retryable() {
                return Err(error);
            }
            let class_available = match error {
                StrategyError::ClientError(_) => {
                    fast = fast.saturating_add(1);
                    self.config.fast_error_limit.allows(fast)
                }
                StrategyError::ServerError(_) => {
                    failures = failures.saturating_add(1);
                    self.config.failure_limit.allows(failures)
                }
                StrategyError::Timeout(_) => {
                    timeouts = timeouts.saturating_add(1);
                    self.config.timeout_limit.allows(timeouts)
                }
                StrategyError::Unavailable => false,
            };
            if !class_available || !self.config.max_runs.allows(runs) {
                return Err(error);
            }
            local_delay = if matches!(
                error,
                StrategyError::ServerError(_) | StrategyError::Timeout(_)
            ) {
                backoff.next()
            } else {
                Duration::ZERO
            };
            if self.config.retry_after.wait(self.cooldown).is_none() {
                return Err(error);
            }
            tokio::task::yield_now().await;
        }
    }
}

struct BackoffSequence {
    config: Option<Backoff>,
    cap: Duration,
    random: u64,
}
impl BackoffSequence {
    fn new(config: Option<Backoff>) -> Self {
        static SEED: AtomicU64 = AtomicU64::new(0x6a09_e667_f3bc_c909);
        let seed = config.and_then(|x| x.seed).unwrap_or_else(|| {
            let counter = SEED.fetch_add(0x9e37_79b9_7f4a_7c15, Ordering::Relaxed);
            let wall = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos() as u64;
            counter ^ wall
        });
        Self {
            config,
            cap: config.map_or(Duration::ZERO, |x| x.initial),
            random: seed.max(1),
        }
    }
    fn next(&mut self) -> Duration {
        let Some(config) = self.config else {
            return Duration::ZERO;
        };
        let cap = self.cap;
        self.cap = Duration::try_from_secs_f64(cap.as_secs_f64() * config.multiplier)
            .unwrap_or(config.max_delay)
            .min(config.max_delay);
        if !config.jitter {
            return cap;
        }
        self.random ^= self.random << 13;
        self.random ^= self.random >> 7;
        self.random ^= self.random << 17;
        let fraction = (self.random >> 11) as f64 / ((1_u64 << 53) - 1) as f64;
        let nanos = ((cap.as_nanos() as f64 * fraction) as u128).min(cap.as_nanos());
        Duration::new(
            (nanos / 1_000_000_000) as u64,
            (nanos % 1_000_000_000) as u32,
        )
    }
}
