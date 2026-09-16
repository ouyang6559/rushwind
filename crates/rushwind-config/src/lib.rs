//! Configuration-source contract for RushWind.
//!
//! A config source serves **raw bytes by key** — decoding is deliberately
//! outside the contract: in Rust serde *is* the decoding
//! standard (`serde_json::from_slice`, `serde_yaml::from_slice`); a trait
//! re-wrapping it would add a layer with no seam to inject into.
//!
//! The contract is one trait with defaulted capability methods:
//!
//! - [`Source::load`] — one raw read by key. A missing
//!   key is `Ok(None)`, **not** an error — the signal a fallback source
//!   keyss off of.
//! - [`Source::watch`] — signal-mode change
//!   notifications ([`SignalStream`], a tick per change, value re-read by
//!   the caller). No engine implements it yet.
//! - [`Source::watch_value`] — push-mode change
//!   notifications ([`ValueStream`], the new raw value per change).
//!
//! Capabilities are discovered through the defaulted methods: the
//! defaults reject with [`ConfigError::NotWatchable`], the explicit
//! marker [`FallbackSource`] keys off to discover watchable sub-sources.
//!
//! # Composition
//!
//! [`FallbackSource`] walks sources in priority order — first successful
//! non-empty read wins — and merges its watchable sub-sources into one
//! stream that re-reads the effective value on every change, so a
//! lower-priority source's notification still surfaces the
//! higher-priority answer.
//!
//! # Design notes
//!
//! - One [`Source`] trait instead of separate reader/watcher interfaces;
//!   capability methods default to [`ConfigError::NotWatchable`] (an
//!   explicit capability check; trait upcasting would be the other route
//!   and is past the workspace MSRV).
//! - `Drop` replaces explicit close calls.
//! - serde replaces a decoder interface — decoding is the caller's
//!   `from_slice`.
//! - Watch streams end when all underlying watches end; dropping the
//!   stream object is the cancellation.
//! - [`FallbackSource`] races the sub-streams inside its own `next()` —
//!   no task boundaries, the orchestrator's boxed-future doctrine.
//!
//! # Engine matrix
//!
//! | Crate | Carrier |
//! |:---|:---|
//! | `rushwind-config-env` | environment variables, optional prefix |
//! | `rushwind-config-file` | one file, directory-watched for changes |
//!
//! Sources over external infrastructure — etcd, consul, nacos,
//! zookeeper, redis, vault, http, oss, kubernetes, apollo, polaris, and
//! embedded filesystems — are future work; each needs its network or
//! embedding contract decided first, the registry engines' trajectory.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

mod error;
mod fallback;

pub use error::ConfigError;
pub use fallback::FallbackSource;

/// Future type used across the config contract.
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// One configuration source: raw bytes by key, plus the watch
/// capabilities it can offer.
///
/// Implementations live in their own crates (`rushwind-config-*`), one
/// carrier per crate, the registry/storage pattern. A source is
/// [`Send`] + [`Sync`] and shareable behind an [`Arc`]; [`load`] takes
/// `&self` and never mutates state.
///
/// [`load`]: Source::load
pub trait Source: Send + Sync {
    /// Reads the raw configuration bytes for the key.
    ///
    /// A missing key is `Ok(None)` — the signal that
    /// tells a fallback source to keep walking. A source that *cannot*
    /// answer (I/O failure, backend down) returns `Err` instead; the
    /// distinction is the whole point of the fallback composition.
    fn load<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<Option<Vec<u8>>, ConfigError>>;

    /// Signal-mode change watch: a [`SignalStream`] delivering one tick
    /// per change of the key's value; the caller re-reads via
    /// [`Source::load`].
    ///
    /// The default rejects with [`ConfigError::NotWatchable`] — only
    /// watch-capable sources override it.
    fn watch<'a>(
        &'a self,
        key: &'a str,
    ) -> BoxFuture<'a, Result<Box<dyn SignalStream>, ConfigError>> {
        let _ = key;
        Box::pin(async { Err(ConfigError::NotWatchable) })
    }

    /// Push-mode change watch: a [`ValueStream`] delivering the new raw
    /// value on every change of the key.
    ///
    /// The default rejects with [`ConfigError::NotWatchable`].
    fn watch_value<'a>(
        &'a self,
        key: &'a str,
    ) -> BoxFuture<'a, Result<Box<dyn ValueStream>, ConfigError>> {
        let _ = key;
        Box::pin(async { Err(ConfigError::NotWatchable) })
    }
}

/// A stream of change ticks for one key. The value is *not* delivered —
/// re-read it with [`Source::load`], the signal only says "changed".
///
/// `next` resolves to `Some(())` per change and `None` once the stream
/// has ended. Dropping the stream is the cancellation: the underlying
/// watch stops with it. Streams must tolerate being re-`next`ed after a
/// dropped wait — a dropped wait cancels that wait, not the stream.
pub trait SignalStream: Send {
    /// Waits for the next change tick; `None` when the stream ended.
    fn next<'a>(&'a mut self) -> BoxFuture<'a, Option<()>>;
}

/// A stream of pushed values for one key.
///
/// `next` resolves to `Some(value)` per change and `None` once the
/// stream has ended. Dropping the stream is the cancellation; a dropped
/// wait cancels the wait, not the stream.
pub trait ValueStream: Send {
    /// Waits for the next pushed value; `None` when the stream ended.
    fn next<'a>(&'a mut self) -> BoxFuture<'a, Option<Vec<u8>>>;
}

/// Convenience for sharing a source across composition points.
pub type SharedSource = Arc<dyn Source>;
