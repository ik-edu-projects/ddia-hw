use crate::{Call, ErrorInfo, RetryAdvice, Sender, StrategyError, WithTimeout};
use http::{Response, header::RETRY_AFTER};
use std::{
    collections::BTreeMap,
    time::{Duration, SystemTime},
};
use tokio::time::Instant;

/// HTTP retry policy. Status overrides do not change the error's HTTP category.
#[derive(Clone, Debug, Default)]
pub struct HttpConfig {
    overrides: BTreeMap<u16, bool>,
}

impl HttpConfig {
    pub fn with_status(mut self, status: u16, retryable: bool) -> Self {
        self.overrides.insert(status, retryable);
        self
    }

    /// The assignment's simplified model treats every 4xx and 5xx as retryable.
    pub fn for_assignment() -> Self {
        Self {
            overrides: (400..600).map(|status| (status, true)).collect(),
        }
    }

    fn retryable(&self, status: u16) -> bool {
        self.overrides
            .get(&status)
            .copied()
            .unwrap_or(matches!(status, 408 | 429 | 500 | 502..=504 | 506..=599))
    }
}

/// Parses delta-seconds or HTTP-date once, translating wall time into monotonic
/// time. Malformed values are ignored; valid values too large to wait forbid retry.
pub fn parse_retry_after(value: &str, wall_now: SystemTime, now: Instant) -> Option<RetryAdvice> {
    let value = value.trim();
    let delay = if !value.is_empty() && value.bytes().all(|b| b.is_ascii_digit()) {
        match value.parse::<u64>() {
            Ok(seconds) => Duration::from_secs(seconds),
            Err(_) => return Some(RetryAdvice::Never),
        }
    } else {
        httpdate::parse_http_date(value)
            .ok()?
            .duration_since(wall_now)
            .unwrap_or(Duration::ZERO)
    };
    Some(match now.checked_add(delay) {
        Some(deadline) => RetryAdvice::Allowed {
            not_before: Some(deadline),
        },
        None => RetryAdvice::Never,
    })
}

/// Returns the body for 2xx. Other statuses retain their code; unexpected 1xx/3xx
/// are terminal client errors. HTTP 408/504 remain HTTP errors, not client timeouts.
pub fn classify_http<B>(response: Response<B>, config: &HttpConfig) -> Result<B, StrategyError> {
    let status = response.status().as_u16();
    if response.status().is_success() {
        return Ok(response.into_body());
    }
    let mut info = ErrorInfo::new(Some(status), config.retryable(status));
    if matches!(info.retry, RetryAdvice::Allowed { .. })
        && let Some(advice) = response
            .headers()
            .get(RETRY_AFTER)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| parse_retry_after(value, SystemTime::now(), Instant::now()))
    {
        info.retry = advice;
    }
    Err(if response.status().is_server_error() {
        StrategyError::ServerError(info)
    } else {
        StrategyError::ClientError(info)
    })
}

pub fn http_call<S, B>(
    sender: S,
    config: HttpConfig,
) -> Call<S, impl FnMut(Response<B>) -> Result<B, StrategyError> + Send>
where
    S: Sender<Response = Response<B>>,
    B: Send,
{
    Call::new(sender, move |response| classify_http(response, &config))
}

/// The default strategy: one HTTP send, at most one second waiting for it.
pub fn default_call<S, B>(sender: S) -> WithTimeout<impl crate::Strategy<Output = B>>
where
    S: Sender<Response = Response<B>>,
    B: Send,
{
    WithTimeout::default_timeout(http_call(sender, HttpConfig::default()))
}
