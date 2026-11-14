use std::{
    collections::VecDeque,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use resilience::{
    AttemptBudget, Backoff, BoxFuture, BreakerConfig, BreakerState, BreakerStatus, CircuitBreaker,
    ErrorInfo, Request, RequestConfig, Retry, RetryAdvice, RetryConfig, RoundRobin, Strategy,
    StrategyError, TimeBudget, WithTimeout,
};
use tokio::time::{Instant, advance};

struct Script {
    steps: VecDeque<(Duration, Result<(), StrategyError>)>,
    calls: Arc<AtomicUsize>,
}

impl Script {
    fn new(steps: impl IntoIterator<Item = (Duration, Result<(), StrategyError>)>) -> Self {
        Self {
            steps: steps.into_iter().collect(),
            calls: Arc::default(),
        }
    }

    fn immediate(results: impl IntoIterator<Item = Result<(), StrategyError>>) -> Self {
        Self::new(results.into_iter().map(|result| (Duration::ZERO, result)))
    }
}

impl Strategy for Script {
    type Output = ();

    fn execute(&mut self) -> BoxFuture<'_, Result<(), StrategyError>> {
        Box::pin(async {
            let (delay, result) = self.steps.pop_front().expect("unexpected extra execution");
            self.calls.fetch_add(1, Ordering::SeqCst);
            if !delay.is_zero() {
                tokio::time::sleep(delay).await;
            }
            result
        })
    }

    fn max_requests(&self) -> AttemptBudget {
        AttemptBudget::Limited(1)
    }

    fn max_time(&self) -> TimeBudget {
        TimeBudget::Limited(
            self.steps
                .iter()
                .map(|(delay, _)| *delay)
                .max()
                .unwrap_or_default(),
        )
    }
}

fn retry_after(delay: Duration) -> StrategyError {
    StrategyError::ClientError(ErrorInfo {
        status: Some(429),
        retry: RetryAdvice::Allowed {
            not_before: Some(Instant::now() + delay),
        },
    })
}

fn retry_config() -> RetryConfig {
    RetryConfig {
        total_time: TimeBudget::Unlimited,
        ..RetryConfig::default()
    }
}

fn breaker_state() -> Arc<BreakerState> {
    Arc::new(
        BreakerState::new(BreakerConfig {
            window_size: 1,
            min_samples: 1,
            failure_threshold: 0.5,
            cooldown: Duration::from_secs(1),
            ..BreakerConfig::default()
        })
        .unwrap(),
    )
}

async fn open_breaker(state: &Arc<BreakerState>) {
    let child = Script::immediate([Err(StrategyError::server(503, true))]);
    let mut breaker = CircuitBreaker::new(child, Arc::clone(state));
    assert!(breaker.execute().await.is_err());
    assert_eq!(state.status(), BreakerStatus::Open);
}

#[tokio::test(start_paused = true)]
async fn retry_after_survives_cancellation_and_reexecution() {
    let started = Instant::now();
    let child = Script::immediate([Err(retry_after(Duration::from_secs(5))), Ok(())]);
    let calls = Arc::clone(&child.calls);
    let mut retry = Retry::new(child, retry_config()).unwrap();

    assert!(
        tokio::time::timeout(Duration::from_secs(1), retry.execute())
            .await
            .is_err()
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    retry.execute().await.unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    assert_eq!(started.elapsed(), Duration::from_secs(5));
}

#[tokio::test(start_paused = true)]
async fn rejected_server_wait_is_not_clamped_or_slept_on_reexecution() {
    let started = Instant::now();
    let error = retry_after(Duration::from_secs(60));
    let child = Script::immediate([Err(error.clone()), Ok(())]);
    let calls = Arc::clone(&child.calls);
    let mut retry = Retry::new(child, retry_config()).unwrap();

    assert_eq!(retry.execute().await, Err(error.clone()));
    assert_eq!(retry.execute().await, Err(error));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(started.elapsed(), Duration::ZERO);

    advance(Duration::from_secs(31)).await;
    retry.execute().await.unwrap();
    assert_eq!(started.elapsed(), Duration::from_secs(60));
    assert_eq!(calls.load(Ordering::SeqCst), 2);
}

#[tokio::test(start_paused = true)]
async fn nested_retry_observes_one_absolute_server_deadline() {
    let started = Instant::now();
    let child = Script::immediate([Err(retry_after(Duration::from_secs(5))), Ok(())]);
    let calls = Arc::clone(&child.calls);
    let inner = Retry::new(
        child,
        RetryConfig {
            max_runs: (..=1).into(),
            ..retry_config()
        },
    )
    .unwrap();
    let mut outer = Retry::new(
        inner,
        RetryConfig {
            max_runs: (..=2).into(),
            ..retry_config()
        },
    )
    .unwrap();

    outer.execute().await.unwrap();
    assert_eq!(started.elapsed(), Duration::from_secs(5));
    assert_eq!(calls.load(Ordering::SeqCst), 2);
}

#[tokio::test(start_paused = true)]
async fn round_robin_bound_includes_work_before_unavailability() {
    let children = vec![
        Script::new([(Duration::from_secs(1), Err(StrategyError::Unavailable))]),
        Script::new([(Duration::from_secs(2), Err(StrategyError::Unavailable))]),
        Script::new([(Duration::from_secs(3), Ok(()))]),
    ];
    let mut round_robin = RoundRobin::new(children).unwrap();
    assert_eq!(round_robin.max_requests(), AttemptBudget::Limited(3));
    assert_eq!(
        round_robin.max_time(),
        TimeBudget::Limited(Duration::from_secs(6))
    );
    let started = Instant::now();
    round_robin.execute().await.unwrap();
    assert_eq!(started.elapsed(), Duration::from_secs(6));
}

#[tokio::test(start_paused = true)]
async fn all_error_limits_bound_an_unlimited_retry() {
    let child = Script::immediate([
        Err(StrategyError::client(404, true)),
        Err(StrategyError::server(503, true)),
        Err(StrategyError::timeout()),
        Ok(()),
    ]);
    let calls = Arc::clone(&child.calls);
    let mut retry = Retry::new(
        child,
        RetryConfig {
            max_runs: AttemptBudget::Unlimited,
            fast_error_limit: (..=2).into(),
            failure_limit: (..=2).into(),
            timeout_limit: (..=2).into(),
            ..retry_config()
        },
    )
    .unwrap();
    assert_eq!(retry.max_requests(), AttemptBudget::Limited(4));
    retry.execute().await.unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 4);
}

#[tokio::test(start_paused = true)]
async fn unavailable_probe_does_not_report_an_endpoint_failure() {
    let state = breaker_state();
    open_breaker(&state).await;
    advance(Duration::from_secs(1)).await;

    let mut probe = CircuitBreaker::new(
        Script::immediate([Err(StrategyError::Unavailable)]),
        Arc::clone(&state),
    );
    assert_eq!(probe.execute().await, Err(StrategyError::Unavailable));
    assert_eq!(state.status(), BreakerStatus::HalfOpen);

    let mut healthy = CircuitBreaker::new(Script::immediate([Ok(())]), Arc::clone(&state));
    healthy.execute().await.unwrap();
    assert_eq!(state.status(), BreakerStatus::Closed);
}

#[tokio::test(start_paused = true)]
async fn probe_uses_the_same_failure_classification_as_closed_breaker() {
    let state = breaker_state();
    open_breaker(&state).await;
    advance(Duration::from_secs(1)).await;

    let mut probe = CircuitBreaker::new(
        Script::immediate([Err(StrategyError::client(404, false))]),
        Arc::clone(&state),
    );
    assert!(matches!(
        probe.execute().await,
        Err(StrategyError::ClientError(_))
    ));
    assert_eq!(state.status(), BreakerStatus::Closed);
}

#[tokio::test(start_paused = true)]
async fn late_failure_cannot_reopen_a_recovered_breaker() {
    let state = breaker_state();
    let mut slow = CircuitBreaker::new(
        Script::new([(
            Duration::from_secs(2),
            Err(StrategyError::server(503, true)),
        )]),
        Arc::clone(&state),
    );
    let mut old_call = slow.execute();
    assert!(futures_util::poll!(old_call.as_mut()).is_pending());
    open_breaker(&state).await;
    advance(Duration::from_secs(1)).await;

    let mut healthy = CircuitBreaker::new(Script::immediate([Ok(())]), Arc::clone(&state));
    healthy.execute().await.unwrap();
    assert!(old_call.await.is_err());
    assert_eq!(state.status(), BreakerStatus::Closed);
}

#[tokio::test(start_paused = true)]
async fn cancelled_probe_releases_the_single_probe_slot() {
    let state = breaker_state();
    open_breaker(&state).await;
    advance(Duration::from_secs(1)).await;

    let mut slow = CircuitBreaker::new(
        Script::new([(Duration::from_secs(100), Ok(()))]),
        Arc::clone(&state),
    );
    let mut probe = slow.execute();
    assert!(futures_util::poll!(probe.as_mut()).is_pending());
    let mut healthy = CircuitBreaker::new(Script::immediate([Ok(())]), Arc::clone(&state));
    assert_eq!(healthy.execute().await, Err(StrategyError::Unavailable));
    drop(probe);
    healthy.execute().await.unwrap();
    assert_eq!(state.status(), BreakerStatus::Closed);
}

#[tokio::test(start_paused = true)]
async fn zero_request_time_does_not_poll_the_strategy() {
    let child = Script::immediate([Ok(())]);
    let calls = Arc::clone(&child.calls);
    let request = Request::with_config(
        child,
        RequestConfig {
            total_time: (..=Duration::ZERO).into(),
        },
    );
    assert!(matches!(
        request.execute().await,
        Err(StrategyError::Timeout(_))
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}

#[tokio::test(start_paused = true)]
async fn very_large_timeout_does_not_panic() {
    let mut strategy = WithTimeout::new(Script::immediate([Ok(())]), ..=Duration::MAX);
    strategy.execute().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn very_large_backoff_remains_cancellable() {
    let child = Script::immediate([Err(StrategyError::server(503, true)), Ok(())]);
    let calls = Arc::clone(&child.calls);
    let mut retry = Retry::new(
        child,
        RetryConfig {
            total_time: (..=Duration::from_secs(1)).into(),
            backoff: Some(Backoff {
                initial: Duration::MAX,
                max_delay: Duration::MAX,
                multiplier: 1.0,
                jitter: false,
                seed: Some(1),
            }),
            ..retry_config()
        },
    )
    .unwrap();
    assert!(matches!(
        retry.execute().await,
        Err(StrategyError::Timeout(_))
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

struct AlwaysFail;

impl Strategy for AlwaysFail {
    type Output = ();

    fn execute(&mut self) -> BoxFuture<'_, Result<(), StrategyError>> {
        Box::pin(async { Err(StrategyError::server(503, true)) })
    }

    fn max_requests(&self) -> AttemptBudget {
        AttemptBudget::Limited(1)
    }
    fn max_time(&self) -> TimeBudget {
        TimeBudget::Limited(Duration::ZERO)
    }
}

struct HttpScript {
    responses: VecDeque<http::Response<&'static str>>,
    calls: Arc<AtomicUsize>,
}

impl resilience::Sender for HttpScript {
    type Response = http::Response<&'static str>;

    fn send(&mut self) -> BoxFuture<'_, Self::Response> {
        Box::pin(async {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.responses.pop_front().expect("unexpected HTTP send")
        })
    }
}

#[tokio::test(start_paused = true)]
async fn http_call_classifies_response_before_retry_scheduling() {
    let calls = Arc::new(AtomicUsize::new(0));
    let sender = HttpScript {
        responses: [
            http::Response::builder()
                .status(404)
                .header("retry-after", "2")
                .body("not ready")
                .unwrap(),
            http::Response::builder()
                .status(200)
                .body("created")
                .unwrap(),
        ]
        .into(),
        calls: Arc::clone(&calls),
    };
    let call = resilience::http_call(
        sender,
        resilience::HttpConfig::default().with_status(404, true),
    );
    let mut retry = Retry::new(call, retry_config()).unwrap();
    let started = Instant::now();
    assert_eq!(retry.execute().await.unwrap(), "created");
    assert_eq!(started.elapsed(), Duration::from_secs(2));
    assert_eq!(calls.load(Ordering::SeqCst), 2);
}

#[tokio::test(start_paused = true)]
async fn http_conflict_stays_terminal_through_nested_retry() {
    let calls = Arc::new(AtomicUsize::new(0));
    let sender = HttpScript {
        responses: [http::Response::builder()
            .status(409)
            .header("retry-after", "1")
            .body("conflict")
            .unwrap()]
        .into(),
        calls: Arc::clone(&calls),
    };
    let call = resilience::http_call(sender, resilience::HttpConfig::default());
    let inner = Retry::new(call, retry_config()).unwrap();
    let mut outer = Retry::new(inner, retry_config()).unwrap();
    let error = outer.execute().await.unwrap_err();
    assert_eq!(error.info().unwrap().status, Some(409));
    assert!(!error.is_retryable());
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[tokio::test(start_paused = true)]
async fn unlimited_immediate_retries_are_not_a_zero_time_execution() {
    let limit = Duration::from_secs(1);
    let mut retry = Retry::new(
        AlwaysFail,
        RetryConfig {
            max_runs: AttemptBudget::Unlimited,
            fast_error_limit: AttemptBudget::Unlimited,
            failure_limit: AttemptBudget::Unlimited,
            timeout_limit: AttemptBudget::Unlimited,
            total_time: (..=limit).into(),
            retry_after: resilience::RetryAfterConfig {
                respect: false,
                ..Default::default()
            },
            ..RetryConfig::default()
        },
    )
    .unwrap();
    assert_eq!(retry.max_time(), TimeBudget::Limited(limit));
    let running = tokio::spawn(async move { retry.execute().await });
    tokio::task::yield_now().await;
    advance(limit).await;
    assert!(matches!(
        running.await.unwrap(),
        Err(StrategyError::Timeout(_))
    ));
}
