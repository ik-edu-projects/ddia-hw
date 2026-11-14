mod common;
use common::*;
use resilience::*;
use std::{sync::Arc, time::Duration};

fn state() -> Arc<BreakerState> {
    Arc::new(
        BreakerState::new(BreakerConfig {
            window_size: 4,
            min_samples: 2,
            ..BreakerConfig::default()
        })
        .unwrap(),
    )
}

#[tokio::test(start_paused = true)]
async fn breaker_opens_only_above_threshold_and_shares_endpoint_state() {
    let tracker = Tracker::default();
    let state = state();
    let mut first = CircuitBreaker::new(
        call("a", vec![ok(), fail(server()), fail(server())], &tracker),
        Arc::clone(&state),
    );
    assert_eq!(first.execute().await, Ok("ok"));
    assert_eq!(first.execute().await, Err(server()));
    assert_eq!(state.status(), BreakerStatus::Closed);
    assert_eq!(first.execute().await, Err(server()));
    assert_eq!(state.status(), BreakerStatus::Open);
    let mut second = CircuitBreaker::new(call("a", vec![], &tracker), Arc::clone(&state));
    assert_eq!(second.execute().await, Err(StrategyError::Unavailable));
    assert_eq!(tracker.calls(), 3);
}

#[tokio::test(start_paused = true)]
async fn breaker_allows_one_probe_and_closes_after_success() {
    let tracker = Tracker::default();
    let state = state();
    let mut failures = CircuitBreaker::new(
        call("a", vec![fail(server()), fail(server())], &tracker),
        Arc::clone(&state),
    );
    for _ in 0..2 {
        assert_eq!(failures.execute().await, Err(server()));
    }
    tokio::time::advance(Duration::from_secs(5)).await;
    let mut probe = CircuitBreaker::new(
        call("a", vec![slow(Duration::from_secs(1))], &tracker),
        Arc::clone(&state),
    );
    let mut other = CircuitBreaker::new(call("a", vec![ok()], &tracker), Arc::clone(&state));
    let mut future = probe.execute();
    assert!(futures_util::poll!(&mut future).is_pending());
    assert_eq!(state.status(), BreakerStatus::HalfOpen);
    assert_eq!(other.execute().await, Err(StrategyError::Unavailable));
    assert_eq!(future.await, Ok("ok"));
    assert_eq!(state.status(), BreakerStatus::Closed);
    assert_eq!(other.execute().await, Ok("ok"));
}

#[tokio::test(start_paused = true)]
async fn canceled_probe_releases_its_slot_without_recording_failure() {
    let tracker = Tracker::default();
    let state = state();
    let mut breaker = CircuitBreaker::new(
        call("a", vec![fail(server()), fail(server()), hang()], &tracker),
        Arc::clone(&state),
    );
    for _ in 0..2 {
        assert_eq!(breaker.execute().await, Err(server()));
    }
    tokio::time::advance(Duration::from_secs(5)).await;
    let mut probe = breaker.execute();
    assert!(futures_util::poll!(&mut probe).is_pending());
    drop(probe);
    let mut replacement = CircuitBreaker::new(call("a", vec![ok()], &tracker), Arc::clone(&state));
    assert_eq!(replacement.execute().await, Ok("ok"));
    assert_eq!(state.status(), BreakerStatus::Closed);
    assert_eq!(tracker.canceled(), 1);
}

#[tokio::test(start_paused = true)]
async fn failing_probe_reopens_and_stale_success_does_not_close_it() {
    let tracker = Tracker::default();
    let state = state();
    let mut stale = CircuitBreaker::new(
        call("a", vec![slow(Duration::from_secs(10))], &tracker),
        Arc::clone(&state),
    );
    let mut stale_future = stale.execute();
    assert!(futures_util::poll!(&mut stale_future).is_pending());
    let mut failer = CircuitBreaker::new(
        call(
            "a",
            vec![fail(server()), fail(server()), fail(server())],
            &tracker,
        ),
        Arc::clone(&state),
    );
    for _ in 0..2 {
        assert_eq!(failer.execute().await, Err(server()));
    }
    tokio::time::advance(Duration::from_secs(5)).await;
    assert_eq!(failer.execute().await, Err(server()));
    assert_eq!(stale_future.await, Ok("ok"));
    assert_eq!(state.status(), BreakerStatus::Open);
}

#[tokio::test(start_paused = true)]
async fn breaker_counts_client_timeouts_and_assignment_fast_errors() {
    let tracker = Tracker::default();
    let state = state();
    let mut breaker = CircuitBreaker::new(
        default("a", vec![hang(), hang()], &tracker),
        Arc::clone(&state),
    );
    for _ in 0..2 {
        assert!(matches!(
            breaker.execute().await,
            Err(StrategyError::Timeout(_))
        ));
    }
    assert_eq!(state.status(), BreakerStatus::Open);
    let state = Arc::new(
        BreakerState::new(BreakerConfig {
            min_samples: 1,
            ..BreakerConfig::for_assignment()
        })
        .unwrap(),
    );
    let mut breaker = CircuitBreaker::new(
        call("b", vec![fail(StrategyError::client(404, false))], &tracker),
        Arc::clone(&state),
    );
    let _ = breaker.execute().await;
    assert_eq!(state.status(), BreakerStatus::Open);
}

#[tokio::test(start_paused = true)]
async fn round_robin_with_breakers_skips_open_replica_and_recovers() {
    let tracker = Tracker::default();
    let state_a = Arc::new(
        BreakerState::new(BreakerConfig {
            min_samples: 1,
            ..BreakerConfig::default()
        })
        .unwrap(),
    );
    let state_b = state();
    let mut rr = RoundRobin::new(vec![
        CircuitBreaker::new(
            default("a", vec![fail(server()), ok()], &tracker),
            Arc::clone(&state_a),
        ),
        CircuitBreaker::new(default("b", vec![ok(), ok()], &tracker), state_b),
    ])
    .unwrap();
    assert_eq!(rr.execute().await, Err(server()));
    assert_eq!(rr.execute().await, Ok("ok"));
    assert_eq!(rr.execute().await, Ok("ok"));
    tokio::time::advance(Duration::from_secs(5)).await;
    assert_eq!(rr.execute().await, Ok("ok"));
    assert_eq!(tracker.names(), vec!["a", "b", "b", "a"]);
    assert_eq!(state_a.status(), BreakerStatus::Closed);
}

#[test]
fn breaker_rejects_invalid_windows_and_thresholds() {
    assert!(
        BreakerState::new(BreakerConfig {
            min_samples: 0,
            ..BreakerConfig::default()
        })
        .is_err()
    );
    assert!(
        BreakerState::new(BreakerConfig {
            failure_threshold: f64::NAN,
            ..BreakerConfig::default()
        })
        .is_err()
    );
}
