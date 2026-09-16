//! Circuit-breaker contract for RushWind: an algorithm-agnostic
//! [`CircuitBreaker`] interface. Concrete implementations (Vegas,
//! Hystrix, SRE, ...) live in `rushwind-circuitbreaker-*` crates and
//! implement this trait so business code depends only on the contract.
//!
//! # The Mark protocol
//!
//! [`CircuitBreaker::allow`] gates the request; when it succeeds the
//! caller **must** call exactly one of
//! [`CircuitBreaker::mark_success`] / [`CircuitBreaker::mark_failure`]
//! when the request completes — the engines count those outcomes.
//! [`execute`] wraps the protocol: it rejects with
//! [`ExecuteError::CircuitOpen`] before invoking the closure, then
//! marks success/failure from the closure's result.
//!
//! # Engines
//!
//! - `rushwind-circuitbreaker-vegas` — latency-inflation detection
//!   (TCP Vegas style).
//! - `rushwind-circuitbreaker-hystrix` — error-rate threshold with a
//!   half-open trial window (Netflix Hystrix style).
//! - `rushwind-circuitbreaker-sres` — Google-SRE probabilistic
//!   acceptance.
//!
//! Sentinel is deferred: no maintained Rust SDK exists to build on, while
//! build an engine on.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

use std::future::Future;
use std::pin::Pin;

/// Future type used across the circuit-breaker contract.
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// The internal state of a circuit breaker.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum State {
    /// The circuit is healthy; all requests flow. The default state.
    #[default]
    Closed,
    /// The circuit tripped; requests are rejected immediately.
    Open,
    /// The circuit is testing recovery with a limited number of trial
    /// requests.
    HalfOpen,
}

impl State {
    /// The readable string form of a state.
    pub fn as_str(&self) -> &'static str {
        match self {
            State::Closed => "closed",
            State::Open => "open",
            State::HalfOpen => "half-open",
        }
    }
}

/// Errors surfaced by circuit breakers.
#[derive(Debug)]
#[non_exhaustive]
pub enum CircuitError {
    /// The circuit is open — the request was rejected.
    Open,
    /// The engine could not complete the operation.
    Failed(String),
}

impl std::fmt::Display for CircuitError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Open => write!(f, "circuitbreaker: circuit is open"),
            Self::Failed(msg) => write!(f, "circuitbreaker operation failed: {msg}"),
        }
    }
}

impl std::error::Error for CircuitError {}

/// The core circuit-breaking contract.
/// Engines must be callable through shared references (`&self`) and
/// safe for concurrent use.
pub trait CircuitBreaker: Send + Sync {
    /// Checks whether a new request is permitted. `Err(Open)` when the
    /// circuit rejects — the caller must not then call the mark
    /// methods.
    fn allow(&self) -> std::result::Result<(), CircuitError>;

    /// Records a successful request outcome. Must be called exactly
    /// once after a successful [`CircuitBreaker::allow`].
    fn mark_success(&self);

    /// Records a failed request outcome. Must be called exactly once
    /// after a successful [`CircuitBreaker::allow`].
    fn mark_failure(&self);

    /// The current circuit-breaker state.
    fn state(&self) -> State;

    /// Releases any resources held by the breaker. A closed breaker
    /// rejects everything.
    fn close(&self);
}

/// The error of [`execute`]: the circuit rejected, or the wrapped
/// closure failed.
#[derive(Debug)]
pub enum ExecuteError<E> {
    /// The circuit rejected the request; the closure never ran.
    CircuitOpen,
    /// The closure ran and failed.
    Inner(E),
}

/// Convenience wrapper: allow, run the closure,
/// then mark success/failure from its result. When the circuit rejects,
/// the closure never runs and [`ExecuteError::CircuitOpen`] is
/// returned.
pub async fn execute<B, T, E, F, Fut>(breaker: &B, f: F) -> std::result::Result<T, ExecuteError<E>>
where
    B: CircuitBreaker + ?Sized,
    F: FnOnce() -> Fut,
    Fut: Future<Output = std::result::Result<T, E>>,
{
    breaker.allow().map_err(|_| ExecuteError::CircuitOpen)?;
    match f().await {
        Ok(value) => {
            breaker.mark_success();
            Ok(value)
        }
        Err(e) => {
            breaker.mark_failure();
            Err(ExecuteError::Inner(e))
        }
    }
}
