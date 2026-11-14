use std::{error::Error, fmt};
use tokio::time::Instant;

/// Retry permission and, optionally, an absolute server-requested cooldown.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RetryAdvice {
    Never,
    Allowed { not_before: Option<Instant> },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ErrorInfo {
    pub status: Option<u16>,
    pub retry: RetryAdvice,
}

impl ErrorInfo {
    pub fn new(status: Option<u16>, retryable: bool) -> Self {
        Self {
            status,
            retry: if retryable {
                RetryAdvice::Allowed { not_before: None }
            } else {
                RetryAdvice::Never
            },
        }
    }
}

/// Execution failures used by resilience policies. Exhaustion preserves the last error.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum StrategyError {
    ClientError(ErrorInfo),
    ServerError(ErrorInfo),
    Timeout(ErrorInfo),
    Unavailable,
}

impl StrategyError {
    pub fn client(status: u16, retryable: bool) -> Self {
        Self::ClientError(ErrorInfo::new(Some(status), retryable))
    }
    pub fn server(status: u16, retryable: bool) -> Self {
        Self::ServerError(ErrorInfo::new(Some(status), retryable))
    }
    pub fn timeout() -> Self {
        Self::Timeout(ErrorInfo::new(None, true))
    }
    pub fn info(&self) -> Option<&ErrorInfo> {
        match self {
            Self::ClientError(x) | Self::ServerError(x) | Self::Timeout(x) => Some(x),
            Self::Unavailable => None,
        }
    }
    pub fn is_retryable(&self) -> bool {
        self.info()
            .is_some_and(|x| matches!(x.retry, RetryAdvice::Allowed { .. }))
    }
    pub(crate) fn not_before(&self) -> Option<Instant> {
        match self.info()?.retry {
            RetryAdvice::Allowed { not_before } => not_before,
            RetryAdvice::Never => None,
        }
    }
}

impl fmt::Display for StrategyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let (name, info) = match self {
            Self::ClientError(info) => ("client error", Some(info)),
            Self::ServerError(info) => ("server error", Some(info)),
            Self::Timeout(info) => ("timeout", Some(info)),
            Self::Unavailable => ("no available strategy", None),
        };
        write!(f, "{name}")?;
        if let Some(status) = info.and_then(|x| x.status) {
            write!(f, " (HTTP {status})")?;
        }
        Ok(())
    }
}
impl Error for StrategyError {}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConfigError(pub &'static str);
impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.0)
    }
}
impl Error for ConfigError {}
