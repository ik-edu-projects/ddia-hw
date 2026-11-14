//! Small, composable resilience strategies. Each node implements [`Strategy`], so
//! retry, timeouts, balancing and circuit breaking can wrap one another.
//!
//! ```
//! use resilience::{Call, Request, Retry, RetryConfig};
//! # use resilience::{BoxFuture, Sender};
//! # struct Example;
//! # impl Sender for Example {
//! #     type Response = String;
//! #     fn send(&mut self) -> BoxFuture<'_, String> { Box::pin(async { "ok".into() }) }
//! # }
//! # async fn example() -> Result<(), resilience::StrategyError> {
//! let call = Call::new(Example, |body| Ok(body));
//! let retry = Retry::new(call, RetryConfig::default()).unwrap();
//! assert_eq!(Request::new(retry).execute().await?, "ok");
//! # Ok(()) }
//! ```

mod bounds;
mod breaker;
mod core;
mod error;
mod http;
mod multi;
mod retry;

pub use bounds::{AttemptBudget, TimeBudget};
pub use breaker::{BreakerConfig, BreakerState, BreakerStatus, CircuitBreaker};
pub use core::{BoxFuture, Call, Request, RequestConfig, Sender, Strategy, WithTimeout};
pub use error::{ConfigError, ErrorInfo, RetryAdvice, StrategyError};
pub use http::{HttpConfig, classify_http, default_call, http_call, parse_retry_after};
pub use multi::{Fallback, Hedge, HedgeConfig, RoundRobin};
pub use retry::{Backoff, Retry, RetryAfterConfig, RetryConfig};
