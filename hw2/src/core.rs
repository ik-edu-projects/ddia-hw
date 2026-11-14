use crate::{AttemptBudget, StrategyError, TimeBudget};
use std::{future::Future, pin::Pin, time::Duration};

pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// A transport for one logical request, possibly sent more than once.
/// Dropping its future stops waiting; an already sent operation may still run remotely.
pub trait Sender: Send {
    type Response: Send;
    /// Performs one physical send, without retries or client timeout parameters.
    fn send(&mut self) -> BoxFuture<'_, Self::Response>;
}

/// An open, object-safe interface shared by clients and multi-clients.
pub trait Strategy: Send {
    type Output: Send;
    /// Executes the node again. Local retry budgets reset; routing and shared
    /// breaker state persist. Dropping this future cancels its pending children.
    fn execute(&mut self) -> BoxFuture<'_, Result<Self::Output, StrategyError>>;
    /// Conservative upper bound on physical sends, not child invocations.
    fn max_requests(&self) -> AttemptBudget;
    /// Conservative elapsed-time bound, including nested waits and cooldowns.
    fn max_time(&self) -> TimeBudget;
}

impl<S: Strategy + ?Sized> Strategy for Box<S> {
    type Output = S::Output;
    fn execute(&mut self) -> BoxFuture<'_, Result<Self::Output, StrategyError>> {
        (**self).execute()
    }
    fn max_requests(&self) -> AttemptBudget {
        (**self).max_requests()
    }
    fn max_time(&self) -> TimeBudget {
        (**self).max_time()
    }
}

/// One physical send, followed by an application-specific response classifier.
pub struct Call<S, F> {
    sender: S,
    classify: F,
}
impl<S, F> Call<S, F> {
    pub fn new(sender: S, classify: F) -> Self {
        Self { sender, classify }
    }
}
impl<S, F, T> Strategy for Call<S, F>
where
    S: Sender,
    F: FnMut(S::Response) -> Result<T, StrategyError> + Send,
    T: Send,
{
    type Output = T;
    fn execute(&mut self) -> BoxFuture<'_, Result<T, StrategyError>> {
        Box::pin(async { (self.classify)(self.sender.send().await) })
    }
    fn max_requests(&self) -> AttemptBudget {
        AttemptBudget::Limited(1)
    }
    fn max_time(&self) -> TimeBudget {
        TimeBudget::Unlimited
    }
}

/// Enforces a client timeout and cancels the child on expiry.
pub struct WithTimeout<S> {
    child: S,
    limit: TimeBudget,
}
impl<S> WithTimeout<S> {
    pub fn new(child: S, limit: impl Into<TimeBudget>) -> Self {
        Self {
            child,
            limit: limit.into(),
        }
    }
    pub fn default_timeout(child: S) -> Self {
        Self::new(child, ..=Duration::from_secs(1))
    }
}
impl<S: Strategy> Strategy for WithTimeout<S> {
    type Output = S::Output;
    fn execute(&mut self) -> BoxFuture<'_, Result<Self::Output, StrategyError>> {
        Box::pin(async { within(self.limit, || self.child.execute()).await })
    }
    fn max_requests(&self) -> AttemptBudget {
        self.child.max_requests()
    }
    fn max_time(&self) -> TimeBudget {
        self.child.max_time().min(self.limit)
    }
}

#[derive(Clone, Copy, Debug)]
pub struct RequestConfig {
    pub total_time: TimeBudget,
}
impl Default for RequestConfig {
    fn default() -> Self {
        Self {
            total_time: TimeBudget::Limited(Duration::from_secs(5)),
        }
    }
}

/// A single-use logical request. Strategy nodes themselves may be executed again.
///
/// ```compile_fail
/// # use resilience::{Request, Strategy};
/// async fn twice<S: Strategy>(request: Request<S>) {
///     let _ = request.execute().await;
///     let _ = request.execute().await; // the first execution consumed it
/// }
/// ```
pub struct Request<S> {
    strategy: S,
    config: RequestConfig,
}
impl<S: Strategy> Request<S> {
    pub fn new(strategy: S) -> Self {
        Self::with_config(strategy, RequestConfig::default())
    }
    pub fn with_config(strategy: S, config: RequestConfig) -> Self {
        Self { strategy, config }
    }
    pub fn max_requests(&self) -> AttemptBudget {
        self.strategy.max_requests()
    }
    pub fn max_time(&self) -> TimeBudget {
        self.strategy.max_time().min(self.config.total_time)
    }
    pub async fn execute(mut self) -> Result<S::Output, StrategyError> {
        within(self.config.total_time, || self.strategy.execute()).await
    }
}

pub(crate) async fn within<T, F, Fut>(limit: TimeBudget, make: F) -> Result<T, StrategyError>
where
    F: FnOnce() -> Fut,
    Fut: Future<Output = Result<T, StrategyError>>,
{
    match limit {
        TimeBudget::Limited(Duration::ZERO) => Err(StrategyError::timeout()),
        TimeBudget::Limited(time) => match tokio::time::Instant::now().checked_add(time) {
            Some(deadline) => tokio::time::timeout_at(deadline, make())
                .await
                .unwrap_or_else(|_| Err(StrategyError::timeout())),
            // Such a deadline is beyond the monotonic clock's representable range.
            None => make().await,
        },
        TimeBudget::Unlimited => make().await,
    }
}

pub(crate) async fn sleep_for(delay: Duration) {
    if delay.is_zero() {
        return;
    }
    match tokio::time::Instant::now().checked_add(delay) {
        Some(deadline) => tokio::time::sleep_until(deadline).await,
        None => std::future::pending().await,
    }
}
