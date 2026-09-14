//! Rate-limiting contract for RushWind, extracted from the Go
//! predecessor `go-wind-plugins/ratelimit`: an algorithm-agnostic
//! [`Limiter`] interface. Concrete implementations (token bucket,
//! BBR, ...) live in `rushwind-ratelimit-*` crates and implement this
//! trait so business code depends only on the contract.
//!
//! # The Go shapes, translated
//!
//! Go's `Allow() (ok, err)` packs the rejection sentinel `ErrLimited`
//! into the error slot. Rust splits the outcomes: [`Limiter::allow`]
//! returns `Ok(false)` when the request is rejected and `Ok(true)`
//! when permitted — rejections are normal outcomes, not errors. The
//! closed-limiter distinction from Go's `(false, ErrLimited)` is not
//! carried: a closed limiter rejects like an exhausted one.
//!
//! Go's `Wait(ctx)` blocks until permitted or the context is
//! cancelled. Cancellation rides the future here — dropping the
//! [`Limiter::wait`] future is the cancellation. A permanently
//! exhausted limiter (e.g. a closed one) still returns
//! [`RateLimitError::Limited`].

#![forbid(unsafe_code)]
#![deny(missing_docs)]

use std::future::Future;
use std::pin::Pin;

/// Future type used across the ratelimit contract.
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Errors surfaced by rate limiters.
#[derive(Debug)]
#[non_exhaustive]
pub enum RateLimitError {
    /// The rate limit is permanently exhausted (e.g. the limiter was
    /// closed) — the Go `ErrLimited` from `Wait`.
    Limited,
    /// The engine could not complete the operation.
    Failed(String),
}

impl std::fmt::Display for RateLimitError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Limited => write!(f, "ratelimit: rate limit exceeded"),
            Self::Failed(msg) => write!(f, "ratelimit operation failed: {msg}"),
        }
    }
}

impl std::error::Error for RateLimitError {}

/// The core rate-limiting contract — the Go `Limiter`. Implementations
/// must be callable through shared references (`&self`) and safe for
/// concurrent use.
pub trait Limiter: Send + Sync {
    /// Attempts to consume one unit without blocking. `Ok(false)` —
    /// the rate limit is exceeded; `Ok(true)` — permitted.
    fn allow(&self) -> bool;

    /// Waits until a unit is available. `Err(Limited)` only when the
    /// limiter is permanently exhausted; cancellation rides the
    /// future.
    fn wait<'a>(&'a self) -> BoxFuture<'a, Result<(), RateLimitError>>;

    /// Releases engine resources. Waiters blocked in [`Limiter::wait`]
    /// resolve with [`RateLimitError::Limited`].
    fn close(&self);
}
