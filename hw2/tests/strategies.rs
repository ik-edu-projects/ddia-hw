mod common;
use common::*;
use resilience::*;
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::time::Instant;

fn retry_config() -> RetryConfig {
    RetryConfig {
        total_time: TimeBudget::Unlimited,
        retry_after: RetryAfterConfig {
            respect: false,
            ..RetryAfterConfig::default()
        },
        ..RetryConfig::default()
    }
}

#[tokio::test(start_paused = true)]
async fn default_makes_one_send_and_returns_success() {
    let tracker = Tracker::default();
    assert_eq!(
        Request::new(default("a", vec![ok()], &tracker))
            .execute()
            .await,
        Ok("ok")
    );
    assert_eq!(tracker.calls(), 1);
}

#[tokio::test(start_paused = true)]
async fn default_timeout_cancels_send() {
    let tracker = Tracker::default();
    let start = Instant::now();
    assert!(matches!(
        Request::new(default("a", vec![hang()], &tracker))
            .execute()
            .await,
        Err(StrategyError::Timeout(_))
    ));
    assert_eq!(Instant::now() - start, Duration::from_secs(1));
    assert_eq!(
        (tracker.calls(), tracker.active(), tracker.canceled()),
        (1, 0, 1)
    );
}

#[tokio::test(start_paused = true)]
async fn zero_timeout_and_zero_request_budget_do_not_poll_child() {
    let tracker = Tracker::default();
    let mut timeout = WithTimeout::new(call("a", vec![], &tracker), ..=Duration::ZERO);
    assert!(matches!(
        timeout.execute().await,
        Err(StrategyError::Timeout(_))
    ));
    let request = Request::with_config(
        call("b", vec![], &tracker),
        RequestConfig {
            total_time: (..=Duration::ZERO).into(),
        },
    );
    assert!(matches!(
        request.execute().await,
        Err(StrategyError::Timeout(_))
    ));
    assert_eq!(tracker.calls(), 0);
}

#[tokio::test(start_paused = true)]
async fn retry_succeeds_and_stops_on_terminal_errors() {
    let tracker = Tracker::default();
    let mut retry = Retry::new(
        default("a", vec![fail(server()), ok()], &tracker),
        retry_config(),
    )
    .unwrap();
    assert_eq!(retry.execute().await, Ok("ok"));
    let terminal = StrategyError::client(404, false);
    let mut retry = Retry::new(
        default("b", vec![fail(terminal.clone()), ok()], &tracker),
        retry_config(),
    )
    .unwrap();
    assert_eq!(retry.execute().await, Err(terminal));
    assert_eq!(tracker.names(), vec!["a", "a", "b"]);
}

#[tokio::test(start_paused = true)]
async fn retry_stops_on_the_limitth_error_of_each_class() {
    for error in [
        StrategyError::client(429, true),
        server(),
        StrategyError::timeout(),
    ] {
        let tracker = Tracker::default();
        let config = RetryConfig {
            max_runs: (..=10).into(),
            fast_error_limit: (..=2).into(),
            failure_limit: (..=2).into(),
            timeout_limit: (..=2).into(),
            ..retry_config()
        };
        let mut retry = Retry::new(
            call(
                "a",
                vec![fail(error.clone()), fail(error.clone()), ok()],
                &tracker,
            ),
            config,
        )
        .unwrap();
        assert_eq!(retry.execute().await, Err(error));
        assert_eq!(tracker.calls(), 2);
    }
}

#[tokio::test(start_paused = true)]
async fn retry_class_budgets_are_independent_and_reset_per_execution() {
    let tracker = Tracker::default();
    let steps = vec![
        fail(server()),
        fail(StrategyError::client(429, true)),
        fail(StrategyError::timeout()),
        ok(),
        fail(server()),
        ok(),
    ];
    let config = RetryConfig {
        max_runs: (..=8).into(),
        fast_error_limit: (..=2).into(),
        failure_limit: (..=2).into(),
        timeout_limit: (..=2).into(),
        ..retry_config()
    };
    let mut retry = Retry::new(call("a", steps, &tracker), config).unwrap();
    assert_eq!(retry.max_requests(), AttemptBudget::Limited(4));
    assert_eq!(retry.execute().await, Ok("ok"));
    assert_eq!(retry.execute().await, Ok("ok"));
    assert_eq!(tracker.calls(), 6);
}

#[tokio::test(start_paused = true)]
async fn retry_total_deadline_cancels_active_subrequest() {
    let tracker = Tracker::default();
    let config = RetryConfig {
        total_time: (..=Duration::from_millis(500)).into(),
        ..retry_config()
    };
    let mut retry = Retry::new(call("a", vec![hang()], &tracker), config).unwrap();
    assert!(matches!(
        retry.execute().await,
        Err(StrategyError::Timeout(_))
    ));
    assert_eq!(
        (tracker.calls(), tracker.active(), tracker.canceled()),
        (1, 0, 1)
    );
}

#[tokio::test(start_paused = true)]
async fn exponential_backoff_skips_fast_errors_without_resetting_sequence() {
    let tracker = Tracker::default();
    let config = RetryConfig {
        max_runs: (..=5).into(),
        failure_limit: (..=5).into(),
        backoff: Some(Backoff {
            jitter: false,
            ..Backoff::default()
        }),
        ..retry_config()
    };
    let steps = vec![
        fail(server()),
        fail(StrategyError::client(429, true)),
        fail(StrategyError::timeout()),
        ok(),
    ];
    let mut retry = Retry::new(call("a", steps, &tracker), config).unwrap();
    let start = Instant::now();
    assert_eq!(retry.execute().await, Ok("ok"));
    assert_eq!(
        tracker
            .times()
            .into_iter()
            .map(|time| time - start)
            .collect::<Vec<_>>(),
        vec![
            Duration::ZERO,
            Duration::from_millis(100),
            Duration::from_millis(100),
            Duration::from_millis(300)
        ]
    );
}

#[tokio::test(start_paused = true)]
async fn exponential_backoff_caps_and_does_not_wait_after_last_attempt() {
    let tracker = Tracker::default();
    let config = RetryConfig {
        max_runs: (..=4).into(),
        failure_limit: AttemptBudget::Unlimited,
        backoff: Some(Backoff {
            initial: Duration::from_millis(100),
            max_delay: Duration::from_millis(150),
            jitter: false,
            ..Backoff::default()
        }),
        ..retry_config()
    };
    let mut retry = Retry::new(
        call("a", (0..4).map(|_| fail(server())).collect(), &tracker),
        config,
    )
    .unwrap();
    let start = Instant::now();
    assert_eq!(retry.execute().await, Err(server()));
    assert_eq!(Instant::now() - start, Duration::from_millis(400));
}

#[tokio::test(start_paused = true)]
async fn seeded_full_jitter_is_reproducible_and_within_cap() {
    let mut sequences = Vec::new();
    for _ in 0..2 {
        let tracker = Tracker::default();
        let config = RetryConfig {
            failure_limit: AttemptBudget::Unlimited,
            backoff: Some(Backoff {
                seed: Some(42),
                ..Backoff::default()
            }),
            ..retry_config()
        };
        let mut retry = Retry::new(
            call("a", vec![fail(server()), fail(server()), ok()], &tracker),
            config,
        )
        .unwrap();
        let start = Instant::now();
        assert_eq!(retry.execute().await, Ok("ok"));
        let times = tracker.times();
        let delays = [times[1] - start, times[2] - times[1]];
        assert!(delays[0] <= Duration::from_millis(101));
        assert!(delays[1] <= Duration::from_millis(201));
        sequences.push(delays);
    }
    assert_eq!(sequences[0], sequences[1]);
}

#[tokio::test(start_paused = true)]
async fn round_robin_rotates_every_retried_subrequest() {
    let tracker = Tracker::default();
    let rr = RoundRobin::new(vec![
        default("a", vec![fail(server()), ok()], &tracker),
        default("b", vec![fail(server())], &tracker),
    ])
    .unwrap();
    let mut retry = Retry::new(rr, retry_config()).unwrap();
    assert_eq!(retry.execute().await, Ok("ok"));
    assert_eq!(tracker.names(), vec!["a", "b", "a"]);
}

#[tokio::test(start_paused = true)]
async fn round_robin_keeps_cursor_between_executions() {
    let tracker = Tracker::default();
    let mut rr = RoundRobin::new(vec![
        call("a", vec![ok(), ok()], &tracker),
        call("b", vec![ok()], &tracker),
    ])
    .unwrap();
    for _ in 0..3 {
        assert_eq!(rr.execute().await, Ok("ok"));
    }
    assert_eq!(tracker.names(), vec!["a", "b", "a"]);
}

#[tokio::test(start_paused = true)]
async fn round_robin_skips_unavailable_but_scans_only_once() {
    let tracker = Tracker::default();
    let mut rr = RoundRobin::new(vec![
        call(
            "a",
            vec![
                fail(StrategyError::Unavailable),
                fail(StrategyError::Unavailable),
            ],
            &tracker,
        ),
        call("b", vec![ok(), fail(StrategyError::Unavailable)], &tracker),
    ])
    .unwrap();
    assert_eq!(rr.execute().await, Ok("ok"));
    assert_eq!(rr.execute().await, Err(StrategyError::Unavailable));
    assert_eq!(tracker.names(), vec!["a", "b", "a", "b"]);
}

#[tokio::test(start_paused = true)]
async fn fallback_supports_heterogeneous_boxed_strategies() {
    let tracker = Tracker::default();
    let children: Vec<Box<dyn Strategy<Output = &'static str>>> = vec![
        Box::new(call("a", vec![fail(server())], &tracker)),
        Box::new(default("b", vec![ok()], &tracker)),
    ];
    let mut fallback = Fallback::new(children).unwrap();
    assert_eq!(fallback.execute().await, Ok("ok"));
    assert_eq!(tracker.names(), vec!["a", "b"]);
}

#[tokio::test(start_paused = true)]
async fn fallback_stops_on_terminal_error_and_restarts_from_first() {
    let tracker = Tracker::default();
    let error = StrategyError::client(409, false);
    let mut fallback = Fallback::new(vec![
        call("a", vec![fail(error.clone()), ok()], &tracker),
        call("b", vec![], &tracker),
    ])
    .unwrap();
    assert_eq!(fallback.execute().await, Err(error));
    assert_eq!(fallback.execute().await, Ok("ok"));
    assert_eq!(tracker.names(), vec!["a", "a"]);
}

#[tokio::test(start_paused = true)]
async fn hedge_early_success_avoids_extra_sends() {
    let tracker = Tracker::default();
    let mut hedge = Hedge::new(
        vec![
            default("a", vec![ok()], &tracker),
            default("b", vec![], &tracker),
        ],
        HedgeConfig::default(),
    )
    .unwrap();
    assert_eq!(hedge.execute().await, Ok("ok"));
    assert_eq!(tracker.names(), vec!["a"]);
}

#[tokio::test(start_paused = true)]
async fn hedge_launches_all_alternatives_after_delay_and_cancels_losers() {
    let tracker = Tracker::default();
    let mut hedge = Hedge::new(
        vec![
            call("a", vec![hang()], &tracker),
            call("b", vec![slow(Duration::from_millis(20))], &tracker),
            call("c", vec![hang()], &tracker),
        ],
        HedgeConfig::default(),
    )
    .unwrap();
    let start = Instant::now();
    assert_eq!(hedge.execute().await, Ok("ok"));
    assert_eq!(tracker.names(), vec!["a", "b", "c"]);
    assert_eq!(
        tracker.times(),
        vec![
            start,
            start + Duration::from_millis(50),
            start + Duration::from_millis(50)
        ]
    );
    assert_eq!(Instant::now() - start, Duration::from_millis(70));
    assert_eq!((tracker.active(), tracker.canceled()), (0, 2));
}

#[tokio::test(start_paused = true)]
async fn hedge_fans_out_on_first_error_and_waits_deadline_if_all_fail() {
    let tracker = Tracker::default();
    let mut hedge = Hedge::new(
        vec![
            call("a", vec![fail(server())], &tracker),
            call("b", vec![fail(server())], &tracker),
        ],
        HedgeConfig::default(),
    )
    .unwrap();
    let start = Instant::now();
    assert!(matches!(
        hedge.execute().await,
        Err(StrategyError::Timeout(_))
    ));
    assert_eq!(tracker.times(), vec![start, start]);
    assert_eq!(Instant::now() - start, Duration::from_secs(2));
}

#[tokio::test(start_paused = true)]
async fn single_branch_hedge_can_return_its_last_error_immediately() {
    let tracker = Tracker::default();
    let config = HedgeConfig {
        return_when_all_failed: true,
        ..HedgeConfig::default()
    };
    let mut hedge = Hedge::new(vec![call("a", vec![fail(server())], &tracker)], config).unwrap();
    let start = Instant::now();
    assert_eq!(hedge.execute().await, Err(server()));
    assert_eq!(Instant::now(), start);
}

#[tokio::test(start_paused = true)]
async fn outer_request_deadline_cancels_all_hedge_branches() {
    let tracker = Tracker::default();
    let hedge = Hedge::new(
        vec![
            call("a", vec![hang()], &tracker),
            call("b", vec![hang()], &tracker),
        ],
        HedgeConfig::default(),
    )
    .unwrap();
    let request = Request::with_config(
        hedge,
        RequestConfig {
            total_time: (..=Duration::from_millis(100)).into(),
        },
    );
    assert!(matches!(
        request.execute().await,
        Err(StrategyError::Timeout(_))
    ));
    assert_eq!(
        (tracker.calls(), tracker.active(), tracker.canceled()),
        (2, 0, 2)
    );
}

#[tokio::test(start_paused = true)]
async fn nested_retry_restarts_inner_local_budgets() {
    let tracker = Tracker::default();
    let inner = Retry::new(
        call(
            "a",
            vec![fail(server()), fail(server()), fail(server()), ok()],
            &tracker,
        ),
        RetryConfig {
            max_runs: (..=2).into(),
            ..retry_config()
        },
    )
    .unwrap();
    let mut outer = Retry::new(inner, retry_config()).unwrap();
    assert_eq!(outer.max_requests(), AttemptBudget::Limited(6));
    assert_eq!(outer.execute().await, Ok("ok"));
    assert_eq!(tracker.calls(), 4);
}

#[test]
fn configuration_rejects_empty_pools_and_invalid_retry_limits() {
    type Node = Box<dyn Strategy<Output = ()>>;
    assert!(RoundRobin::<Node>::new(vec![]).is_err());
    assert!(Fallback::<Node>::new(vec![]).is_err());
    assert!(Hedge::<Node>::new(vec![], HedgeConfig::default()).is_err());
    let tracker = Tracker::default();
    assert!(
        Retry::new(
            call("a", vec![], &tracker),
            RetryConfig {
                max_runs: (..=0).into(),
                ..RetryConfig::default()
            }
        )
        .is_err()
    );
    assert!(
        Retry::new(
            call("a", vec![], &tracker),
            RetryConfig {
                backoff: Some(Backoff {
                    multiplier: f64::NAN,
                    ..Backoff::default()
                }),
                ..RetryConfig::default()
            }
        )
        .is_err()
    );
}

struct ImmediateErrors(Arc<AtomicUsize>);
impl Strategy for ImmediateErrors {
    type Output = ();
    fn execute(&mut self) -> BoxFuture<'_, Result<(), StrategyError>> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Box::pin(async { Err(server()) })
    }
    fn max_requests(&self) -> AttemptBudget {
        (..=1).into()
    }
    fn max_time(&self) -> TimeBudget {
        (..=Duration::ZERO).into()
    }
}

#[tokio::test(start_paused = true)]
async fn unlimited_ready_retry_still_allows_cancellation() {
    let calls = Arc::new(AtomicUsize::new(0));
    let retry = Retry::new(
        ImmediateErrors(Arc::clone(&calls)),
        RetryConfig {
            max_runs: AttemptBudget::Unlimited,
            failure_limit: AttemptBudget::Unlimited,
            ..retry_config()
        },
    )
    .unwrap();
    let task = tokio::spawn(Request::new(retry).execute());
    tokio::task::yield_now().await;
    tokio::task::yield_now().await;
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    assert!(calls.load(Ordering::SeqCst) > 0);
}
