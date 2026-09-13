//! KV cache contract for RushWind, extracted from the Go predecessor
//! `go-wind-plugins/cache`: get/set/SetNX/delete/has plus batch
//! get/set, all TTL-driven.
//!
//! # The Go shapes, translated
//!
//! Go's `ErrNotFound` error signal becomes `Ok(None)`: a missing (or
//! expired) key is a normal outcome of a read, not an error, and
//! [`Cache::get_multi`] returns `None` entries aligned with the input
//! keys — no sentinel error when some are missing. Go's zero-TTL
//! means "use the backend's default" — [`Duration`] options here:
//! `None` is the backend's default (which may itself mean
//! never-expire).
//!
//! Values are raw bytes ([`Vec<u8>`]); serialization is the caller's
//! business. [`Cache::close`] releases engine resources (the local
//! engine clears, the redis engine is a no-op — the Go redis Close
//! explicitly does not close the client).
//!
//! # Engines
//!
//! Engines live in `rushwind-cache-*` crates (`local`, `redis`) and
//! implement [`Cache`] over their native storage.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

/// Future type used across the cache contract.
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Errors surfaced by cache engines.
#[derive(Debug)]
#[non_exhaustive]
pub enum CacheError {
    /// The engine could not complete the operation.
    Failed(String),
}

impl std::fmt::Display for CacheError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Failed(msg) => write!(f, "cache operation failed: {msg}"),
        }
    }
}

impl std::error::Error for CacheError {}

/// One key-value entry for batch writes — the Go `cache.Item`.
#[derive(Debug, Clone)]
pub struct Item {
    /// The cache key.
    pub key: String,
    /// The value bytes.
    pub value: Vec<u8>,
    /// The entry TTL; `None` uses the backend's default (which may
    /// itself mean never-expire).
    pub ttl: Option<Duration>,
}

/// The caching contract — the Go `cache.Cache`. Engines must be
/// callable through shared references (`&self`).
pub trait Cache: Send + Sync {
    /// Reads the value for the key; `Ok(None)` when the key is
    /// missing or expired — the Go `ErrNotFound` outcome.
    fn get<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<Option<Vec<u8>>, CacheError>>;

    /// Stores the value with a TTL; `None` uses the backend's
    /// default.
    fn set<'a>(
        &'a self,
        key: &'a str,
        value: &'a [u8],
        ttl: Option<Duration>,
    ) -> BoxFuture<'a, Result<(), CacheError>>;

    /// Sets the value only when the key does not exist — the
    /// distributed-lock / cache-stampede primitive. `Ok(true)` when
    /// the key was set.
    fn set_nx<'a>(
        &'a self,
        key: &'a str,
        value: &'a [u8],
        ttl: Option<Duration>,
    ) -> BoxFuture<'a, Result<bool, CacheError>>;

    /// Removes the value for the key; a no-op when missing.
    fn delete<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<(), CacheError>>;

    /// Reports whether the key exists and has not expired.
    fn has<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<bool, CacheError>>;

    /// Reads many keys in one call — one network round-trip for
    /// backends with native MGET/pipeline, sequential iteration
    /// otherwise. `None` entries mark missing keys, aligned with the
    /// input order.
    fn get_multi<'a>(
        &'a self,
        keys: &'a [String],
    ) -> BoxFuture<'a, Result<Vec<Option<Vec<u8>>>, CacheError>>;

    /// Writes many entries in one call — one network round-trip for
    /// backends with native MSET/pipeline, sequential iteration
    /// otherwise.
    fn set_multi<'a>(&'a self, items: &'a [Item]) -> BoxFuture<'a, Result<(), CacheError>>;

    /// Releases engine resources — the Go `Close`. The local engine
    /// clears its entries; the redis engine is a no-op (its Go Close
    /// explicitly does not close the client).
    fn close(&self) -> BoxFuture<'_, Result<(), CacheError>>;
}
