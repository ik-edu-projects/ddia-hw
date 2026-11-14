# HW2: resilience strategies

A small Rust/Tokio library for the imaginary stateless endpoint in the assignment.
There is no network dependency: `Sender` supplies raw responses, and tests provide
scripted senders. The implementation uses Rust 2024 and the pinned stable 1.100.0
toolchain. Install [Rust with rustup](https://rustup.rs/) and run from the repository:

```sh
./test.sh
```

The script checks formatting, Clippy with warnings denied, unit/integration tests,
and rustdoc examples (including a compile-fail check for consuming a request twice).
The first run downloads the pinned toolchain and Cargo dependencies. `Cargo.lock`
is committed. Tests use Tokio's paused clock, with no real sleeps or network calls.

## API and composition

`Sender::send(&mut self)` performs one physical send and returns an owned raw
response. It receives no scenario, retry limit or timeout. Its future must support
cancellation by being dropped. A sender belongs to one logical request and may
serve its repeated sends. The raw response may itself be a transport `Result`.

`Call::new(sender, classifier)` maps that response into a success value or one of
four error categories: `ClientError`, `ServerError`, `Timeout`, `Unavailable`.
An ordinary `FnMut` classifier supports non-HTTP protocols and transport errors.
`http_call(sender, HttpConfig)` classifies `http::Response<B>` and returns the body.
`default_call(sender)` adds the default one-second client timeout.

Every client and multi-client implements the same open `Strategy` trait.
Wrappers accept arbitrary strategies. Box forwarding supports heterogeneous
`Vec<Box<dyn Strategy<Output = T>>>`; implementations require `Send`, not `Sync`.
All futures borrow their strategies, so concurrent branches need separate children.

```rust
use resilience::{Request, Retry, RetryConfig, RoundRobin, default_call};

// sender_a and sender_b implement Sender<Response = http::Response<String>>.
let request = Request::new(Retry::new(
    RoundRobin::new(vec![default_call(sender_a), default_call(sender_b)])?,
    RetryConfig::default(),
)?);
let body = request.execute().await?;
```

Put Retry outside RoundRobin to rotate on every retried subrequest. Putting Retry
inside each branch is also legal, but finishes retries on that branch first.
Nested retries reset their local counters each time they are called:

```rust
let inner = Retry::new(default_call(sender), RetryConfig::default())?;
let outer = Retry::new(inner, RetryConfig {
    max_runs: (..=2).into(),
    ..RetryConfig::default()
})?;
let body = Request::new(outer).execute().await?;
```

`Request::execute(self)` consumes one logical request. The default outer latency
budget is five seconds; use `Request::with_config` to change it. Strategy nodes
can be executed again: Retry resets its counters and backoff, Fallback starts at
the first child, Hedge starts a fresh race, and RoundRobin keeps its cursor.
Known server cooldowns and shared circuit-breaker state persist.

## Strategies and budgets

| Assignment component | API | Default behavior |
| --- | --- | --- |
| Default | `WithTimeout<Call<...>>`, `default_call` | One send, one-second timeout |
| Retries | `Retry` | Three child runs including the first; fast/failure/timeout error limits 3/3/2; five-second total latency |
| Exponential backoff | `RetryConfig::backoff = Some(Backoff::default())` | 100 ms initial, multiplier 2, two-second cap, full jitter |
| Round-robin | `RoundRobin` | Persistent rotation; skips unavailable children for at most one full rotation |
| Hedging | `Hedge` | First immediately; all others after 50 ms, or immediately on first failure; first success wins; two-second total latency |
| Circuit breaking | `CircuitBreaker` + shared `BreakerState` | Count window 20, minimum 10 samples, strictly greater than 50% failures opens for five seconds; one half-open probe |
| Sequential fallback | `Fallback` | Next child on retryable error or unavailability; terminal errors stop |

Retry stops on the **Bth error** of a class with limit B, without one extra try.
Class counters are independent. `max_runs` limits child executions, including the
first; it is distinct from the physical-send bound when children themselves retry.
Exhaustion returns the last concrete error, total latency expiry returns Timeout,
and unavailable paths return Unavailable. Retry does not retry Unavailable.
Finite retry limits must be at least one. No sleep occurs after the final attempt.
Only server errors and client timeouts advance the exponential sequence; 4xx
neither advance nor reset it. Disable jitter for exact delays, or set a seed for
reproducible jitter. The generator is not cryptographic.

`AttemptBudget` and `TimeBudget` are separate enums. Use `(..=3).into()` and
`(..=Duration::from_secs(1)).into()` for finite limits, `(..).into()` for unlimited.
Zero time fails before polling the child. `max_requests()` conservatively bounds
all physical sends, and `max_time()` includes nested delays and remembered
cooldowns. Arithmetic overflow widens a bound to Unlimited. RoundRobin bounds
include a complete scan because a child may spend resources before returning
Unavailable. Timeouts are cooperative Tokio deadlines: blocking code cannot be
preempted. A deadline beyond the monotonic clock's representable range cannot
expire; oversized delays remain pending rather than panicking.

Hedge cancels every losing child by dropping its future, without detached tasks.
Its default all-failed behavior waits for the total latency budget and returns
Timeout, as required by the assignment. Set `return_when_all_failed` for an early
last-error result. With unlimited latency and all branches failed, the default
behavior waits indefinitely. Set `fanout_on_error = false` to retain the original
delay after an early error. Success before fanout avoids starting other children.

## HTTP policy and Retry-After

Default success is 2xx. HTTP 408/429 and 5xx except 501/505 allow retries. Other
4xx, including 404 and 409, are terminal: another identical 409 request may still
need application repair. Unexpected 1xx/3xx are terminal ClientError values.
HTTP 408 and 504 retain their HTTP error category; Timeout represents a client
deadline. Override individual statuses explicitly, or use
`HttpConfig::for_assignment()` to retry every model 4xx/5xx.

```rust
use resilience::{Call, HttpConfig, WithTimeout, http_call};

let config = HttpConfig::default().with_status(404, true).with_status(409, true);
let strategy = WithTimeout::default_timeout(http_call(sender, config));
// Or provide application-specific response handling:
let strategy = Call::new(sender, |response| classify_my_protocol(response));
```

Retry-After accepts delta-seconds and HTTP-date. Parsing translates received wall
time to an absolute Tokio instant once; malformed values are ignored and past
dates mean zero delay. A valid unrepresentably large value forbids retry.
The header never makes a terminal status retryable. Retry, Fallback and Hedge
honor cooldowns before starting future children; already running hedge branches
continue. A retry waits for the maximum of sampled local backoff and remaining
server cooldown. Default maximum accepted server wait is 30 seconds. Larger waits
return the original error or suppress pending hedge fanout; they are never clamped
to an earlier time. `RetryAfterConfig` can disable honoring the header or allow
unlimited waiting. Cancellation and repeated execution preserve known cooldowns,
so outer wrappers cannot erase a server's lower bound.

## Shared circuit breakers

Keep an `Arc<BreakerState>` per endpoint and reuse it across request-scoped sender
instances. No global endpoint registry is needed. Wrap the timeout with the breaker
so the breaker sees client timeouts:

```rust
use std::sync::Arc;
use resilience::{BreakerConfig, BreakerState, CircuitBreaker};

let state_a = Arc::new(BreakerState::new(BreakerConfig::default())?);
let state_b = Arc::new(BreakerState::new(BreakerConfig::default())?);
// Retain these Arcs for subsequent logical requests.
let pool = RoundRobin::new(vec![
    CircuitBreaker::new(default_call(sender_a), Arc::clone(&state_a)),
    CircuitBreaker::new(default_call(sender_b), Arc::clone(&state_b)),
])?;
let request = Request::new(Retry::new(pool, RetryConfig::default())?);
```

Server errors and client timeouts count as failures by default, independently of
retryability. Client errors count as healthy responses; `for_assignment()` also
counts 4xx as failures. Unavailable does not create a sample. After cooldown, one
concurrent half-open probe is admitted. A counted failure reopens the breaker; a
healthy response closes it and clears its window. Cancellation or Unavailable
releases the probe without recording a result. Generation checks ignore stale
completions from earlier circuit states. Shared locks never span an await.

Tests cover the six required strategies, composition/boxing, independent budgets,
time limits, actual send order and delays, cancellation cleanup, HTTP policies,
Retry-After, shared concurrent probes and conservative nested bounds.
