use crate::{
    AttemptBudget, BoxFuture, ConfigError, RetryAfterConfig, Strategy, StrategyError, TimeBudget,
    core::{sleep_for, within},
};
use futures_util::{StreamExt, stream::FuturesUnordered};
use std::time::Duration;
use tokio::time::Instant;

/// Rotates between children. A call skips `Unavailable` children for at most one
/// full rotation, so an entirely open circuit-breaker pool fails immediately.
pub struct RoundRobin<S> {
    children: Vec<S>,
    next: usize,
}
impl<S> RoundRobin<S> {
    pub fn new(children: Vec<S>) -> Result<Self, ConfigError> {
        if children.is_empty() {
            return Err(ConfigError("round-robin requires at least one child"));
        }
        Ok(Self { children, next: 0 })
    }
}
impl<S: Strategy> Strategy for RoundRobin<S> {
    type Output = S::Output;
    fn execute(&mut self) -> BoxFuture<'_, Result<Self::Output, StrategyError>> {
        Box::pin(async {
            for _ in 0..self.children.len() {
                let index = self.next;
                self.next = (index + 1) % self.children.len();
                match self.children[index].execute().await {
                    Err(StrategyError::Unavailable) => continue,
                    result => return result,
                }
            }
            Err(StrategyError::Unavailable)
        })
    }
    fn max_requests(&self) -> AttemptBudget {
        sends_bound(&self.children)
    }
    fn max_time(&self) -> TimeBudget {
        time_bound(&self.children)
    }
}

/// Tries children in order on retryable errors or unavailability. Terminal
/// errors stop the chain; every new execution starts with the first child.
pub struct Fallback<S> {
    children: Vec<S>,
    retry_after: RetryAfterConfig,
    cooldown: Option<Instant>,
    cooldown_error: Option<StrategyError>,
}
impl<S> Fallback<S> {
    pub fn new(children: Vec<S>) -> Result<Self, ConfigError> {
        if children.is_empty() {
            return Err(ConfigError("fallback requires at least one child"));
        }
        Ok(Self {
            children,
            retry_after: RetryAfterConfig::default(),
            cooldown: None,
            cooldown_error: None,
        })
    }
    pub fn with_retry_after(mut self, config: RetryAfterConfig) -> Self {
        self.retry_after = config;
        self
    }
}
impl<S: Strategy> Strategy for Fallback<S> {
    type Output = S::Output;
    fn execute(&mut self) -> BoxFuture<'_, Result<Self::Output, StrategyError>> {
        Box::pin(async {
            let mut last = StrategyError::Unavailable;
            for child in &mut self.children {
                let delay = self.retry_after.wait(self.cooldown).ok_or_else(|| {
                    self.cooldown_error
                        .clone()
                        .unwrap_or(StrategyError::Unavailable)
                })?;
                sleep_for(delay).await;
                match child.execute().await {
                    Ok(value) => return Ok(value),
                    Err(StrategyError::Unavailable) => {}
                    Err(error) => {
                        self.retry_after.remember(&mut self.cooldown, &error);
                        if error.not_before().is_some() {
                            self.cooldown_error = Some(error.clone());
                        }
                        if !error.is_retryable() {
                            return Err(error);
                        }
                        last = error;
                    }
                }
            }
            Err(last)
        })
    }
    fn max_requests(&self) -> AttemptBudget {
        sends_bound(&self.children)
    }
    fn max_time(&self) -> TimeBudget {
        time_bound(&self.children).add(
            self.retry_after
                .bound()
                .multiply(AttemptBudget::Limited(self.children.len() as u64)),
        )
    }
}

#[derive(Clone, Copy, Debug)]
pub struct HedgeConfig {
    pub delay: Duration,
    pub total_time: TimeBudget,
    /// Launch the remaining children as soon as the first child fails.
    pub fanout_on_error: bool,
    /// Otherwise all-failed hedges wait for the latency budget and return Timeout.
    pub return_when_all_failed: bool,
    pub retry_after: RetryAfterConfig,
}
impl Default for HedgeConfig {
    fn default() -> Self {
        Self {
            delay: Duration::from_millis(50),
            total_time: (..=Duration::from_secs(2)).into(),
            fanout_on_error: true,
            return_when_all_failed: false,
            retry_after: RetryAfterConfig::default(),
        }
    }
}

/// Races a first child against delayed parallel alternatives. The first success
/// wins; dropping the other futures cancels them without detached work.
pub struct Hedge<S> {
    children: Vec<S>,
    config: HedgeConfig,
    cooldown: Option<Instant>,
    cooldown_error: Option<StrategyError>,
}
impl<S> Hedge<S> {
    pub fn new(children: Vec<S>, config: HedgeConfig) -> Result<Self, ConfigError> {
        if children.is_empty() {
            return Err(ConfigError("hedging requires at least one child"));
        }
        Ok(Self {
            children,
            config,
            cooldown: None,
            cooldown_error: None,
        })
    }
}
impl<S: Strategy> Strategy for Hedge<S> {
    type Output = S::Output;
    fn execute(&mut self) -> BoxFuture<'_, Result<Self::Output, StrategyError>> {
        let limit = self.config.total_time;
        Box::pin(async move { within(limit, || self.run()).await })
    }
    fn max_requests(&self) -> AttemptBudget {
        sends_bound(&self.children)
    }
    fn max_time(&self) -> TimeBudget {
        // Default all-failed behavior deliberately waits for the total deadline.
        if !self.config.return_when_all_failed {
            return self.config.total_time;
        }
        let child_time =
            self.children
                .iter()
                .fold(TimeBudget::Limited(Duration::ZERO), |max, child| {
                    match (max, child.max_time()) {
                        (TimeBudget::Limited(a), TimeBudget::Limited(b)) => {
                            TimeBudget::Limited(a.max(b))
                        }
                        _ => TimeBudget::Unlimited,
                    }
                });
        child_time
            .add(TimeBudget::Limited(self.config.delay))
            .add(self.config.retry_after.bound().multiply((..=2).into()))
            .min(self.config.total_time)
    }
}
impl<S: Strategy> Hedge<S> {
    async fn run(&mut self) -> Result<S::Output, StrategyError> {
        let initial_wait = self.config.retry_after.wait(self.cooldown).ok_or_else(|| {
            self.cooldown_error
                .clone()
                .unwrap_or(StrategyError::Unavailable)
        })?;
        sleep_for(initial_wait).await;
        let (first, rest) = self
            .children
            .split_first_mut()
            .expect("validated nonempty children");
        let mut pending: FuturesUnordered<BoxFuture<'_, Result<S::Output, StrategyError>>> =
            FuturesUnordered::new();
        pending.push(first.execute());
        let mut rest = if rest.is_empty() { None } else { Some(rest) };
        let mut fanout_at = Instant::now().checked_add(self.config.delay);
        let mut last = StrategyError::Unavailable;
        loop {
            if rest.is_some() {
                if let Some(cooldown) = self.cooldown.filter(|_| self.config.retry_after.respect) {
                    fanout_at = fanout_at.map(|time| time.max(cooldown));
                }
                if self.config.retry_after.wait(self.cooldown).is_none() {
                    rest = None;
                }
            }
            tokio::select! {
                biased;
                result = pending.next(), if !pending.is_empty() => {
                    match result.expect("pending was nonempty") {
                        Ok(value) => return Ok(value),
                        Err(error) => {
                            self.config.retry_after.remember(&mut self.cooldown, &error);
                            if error.not_before().is_some() {
                                self.cooldown_error = Some(error.clone());
                            }
                            last = error;
                            if self.config.fanout_on_error && rest.is_some() {
                                fanout_at = Some(Instant::now());
                            }
                        }
                    }
                }
                () = wait_until(fanout_at), if rest.is_some() => {
                    for child in rest.take().expect("remaining children present") {
                        pending.push(child.execute());
                    }
                }
                else => {
                    if self.config.return_when_all_failed {
                        return Err(last);
                    }
                    return std::future::pending().await;
                }
            }
        }
    }
}

async fn wait_until(deadline: Option<Instant>) {
    match deadline {
        Some(time) => tokio::time::sleep_until(time).await,
        None => std::future::pending().await,
    }
}

fn sends_bound<S: Strategy>(children: &[S]) -> AttemptBudget {
    children
        .iter()
        .fold(AttemptBudget::Limited(0), |sum, child| {
            sum.add(child.max_requests())
        })
}
fn time_bound<S: Strategy>(children: &[S]) -> TimeBudget {
    children
        .iter()
        .fold(TimeBudget::Limited(Duration::ZERO), |sum, child| {
            sum.add(child.max_time())
        })
}
