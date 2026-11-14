mod common;
use common::*;
use http::{Response, header::RETRY_AFTER};
use resilience::*;
use std::time::{Duration, SystemTime};
use tokio::time::Instant;

fn response(status: u16, retry_after: Option<&str>) -> Response<&'static str> {
    let mut builder = Response::builder().status(status);
    if let Some(value) = retry_after {
        builder = builder.header(RETRY_AFTER, value);
    }
    builder.body("body").unwrap()
}

#[test]
fn http_policy_separates_status_category_from_retryability() {
    let config = HttpConfig::default();
    assert_eq!(classify_http(response(204, None), &config), Ok("body"));
    for status in [400, 401, 403, 404, 409, 501, 505, 302] {
        let error = classify_http(response(status, None), &config).unwrap_err();
        assert!(!error.is_retryable(), "{status}");
        assert_eq!(error.info().unwrap().status, Some(status));
    }
    for status in [408, 429, 500, 502, 503, 504] {
        let error = classify_http(response(status, None), &config).unwrap_err();
        assert!(error.is_retryable(), "{status}");
        assert!(!matches!(error, StrategyError::Timeout(_)));
    }
}

#[test]
fn status_overrides_and_assignment_policy_are_explicit() {
    let config = HttpConfig::default()
        .with_status(404, true)
        .with_status(503, false);
    assert!(
        classify_http(response(404, None), &config)
            .unwrap_err()
            .is_retryable()
    );
    assert!(
        !classify_http(response(503, None), &config)
            .unwrap_err()
            .is_retryable()
    );
    assert!(
        classify_http(response(409, None), &HttpConfig::for_assignment())
            .unwrap_err()
            .is_retryable()
    );
}

#[tokio::test(start_paused = true)]
async fn retry_after_parses_seconds_http_dates_and_past_dates() {
    let now = Instant::now();
    let wall = SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    assert_eq!(
        parse_retry_after("12", wall, now),
        Some(RetryAdvice::Allowed {
            not_before: Some(now + Duration::from_secs(12))
        })
    );
    let date = httpdate::fmt_http_date(wall + Duration::from_secs(20));
    assert_eq!(
        parse_retry_after(&date, wall, now),
        Some(RetryAdvice::Allowed {
            not_before: Some(now + Duration::from_secs(20))
        })
    );
    let date = httpdate::fmt_http_date(wall - Duration::from_secs(20));
    assert_eq!(
        parse_retry_after(&date, wall, now),
        Some(RetryAdvice::Allowed {
            not_before: Some(now)
        })
    );
    assert_eq!(parse_retry_after("tomorrow", wall, now), None);
    assert_eq!(parse_retry_after("-1", wall, now), None);
    assert_eq!(
        parse_retry_after("9999999999999999999999999999999", wall, now),
        Some(RetryAdvice::Never)
    );
    assert_eq!(
        parse_retry_after(&u64::MAX.to_string(), wall, now),
        Some(RetryAdvice::Never)
    );
}

#[test]
fn retry_after_never_turns_terminal_status_into_retryable() {
    assert!(
        !classify_http(response(404, Some("5")), &HttpConfig::default())
            .unwrap_err()
            .is_retryable()
    );
    assert!(
        !classify_http(
            response(503, Some("999999999999999999999999")),
            &HttpConfig::default()
        )
        .unwrap_err()
        .is_retryable()
    );
    assert!(
        classify_http(response(429, Some("invalid")), &HttpConfig::default())
            .unwrap_err()
            .is_retryable()
    );
}

fn cooldown(delay: Duration) -> StrategyError {
    StrategyError::ClientError(ErrorInfo {
        status: Some(429),
        retry: RetryAdvice::Allowed {
            not_before: Some(Instant::now() + delay),
        },
    })
}

#[tokio::test(start_paused = true)]
async fn retry_waits_maximum_of_backoff_and_server_advice() {
    let tracker = Tracker::default();
    let start = Instant::now();
    let error = StrategyError::ServerError(ErrorInfo {
        status: Some(503),
        retry: RetryAdvice::Allowed {
            not_before: Some(start + Duration::from_millis(300)),
        },
    });
    let mut retry = Retry::new(
        call("a", vec![fail(error), ok()], &tracker),
        RetryConfig {
            backoff: Some(Backoff {
                jitter: false,
                ..Backoff::default()
            }),
            ..RetryConfig::default()
        },
    )
    .unwrap();
    assert_eq!(retry.execute().await, Ok("ok"));
    assert_eq!(
        tracker.times(),
        vec![start, start + Duration::from_millis(300)]
    );
}

#[tokio::test(start_paused = true)]
async fn excessive_retry_after_preserves_the_error_without_early_retry() {
    let tracker = Tracker::default();
    let error = cooldown(Duration::from_secs(31));
    let mut retry = Retry::new(
        call("a", vec![fail(error.clone()), ok()], &tracker),
        RetryConfig::default(),
    )
    .unwrap();
    let start = Instant::now();
    assert_eq!(retry.execute().await, Err(error.clone()));
    assert_eq!(retry.execute().await, Err(error));
    assert_eq!(Instant::now(), start);
    assert_eq!(tracker.calls(), 1);
}

#[tokio::test(start_paused = true)]
async fn retry_cooldown_survives_cancellation_and_repeat_execution() {
    let tracker = Tracker::default();
    let start = Instant::now();
    let mut retry = Retry::new(
        call(
            "a",
            vec![fail(cooldown(Duration::from_secs(2))), ok()],
            &tracker,
        ),
        RetryConfig::default(),
    )
    .unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(100), retry.execute())
            .await
            .is_err()
    );
    assert_eq!(tracker.calls(), 1);
    assert_eq!(retry.execute().await, Ok("ok"));
    assert_eq!(tracker.times(), vec![start, start + Duration::from_secs(2)]);
}

#[tokio::test(start_paused = true)]
async fn fallback_and_hedge_respect_advice_before_starting_other_children() {
    let tracker = Tracker::default();
    let start = Instant::now();
    let mut fallback = Fallback::new(vec![
        call(
            "a",
            vec![fail(cooldown(Duration::from_millis(100)))],
            &tracker,
        ),
        call("b", vec![ok()], &tracker),
    ])
    .unwrap();
    assert_eq!(fallback.execute().await, Ok("ok"));
    assert_eq!(
        tracker.times(),
        vec![start, start + Duration::from_millis(100)]
    );
    let tracker = Tracker::default();
    let start = Instant::now();
    let mut hedge = Hedge::new(
        vec![
            call(
                "a",
                vec![fail(cooldown(Duration::from_millis(200)))],
                &tracker,
            ),
            call("b", vec![ok()], &tracker),
        ],
        HedgeConfig::default(),
    )
    .unwrap();
    assert_eq!(hedge.execute().await, Ok("ok"));
    assert_eq!(
        tracker.times(),
        vec![start, start + Duration::from_millis(200)]
    );
}

#[tokio::test(start_paused = true)]
async fn nested_retry_does_not_override_terminal_http_advice() {
    let tracker = Tracker::default();
    let error = StrategyError::client(409, false);
    let inner = Retry::new(
        call("a", vec![fail(error.clone())], &tracker),
        RetryConfig::default(),
    )
    .unwrap();
    let mut outer = Retry::new(inner, RetryConfig::default()).unwrap();
    assert_eq!(outer.execute().await, Err(error));
    assert_eq!(tracker.calls(), 1);
}
