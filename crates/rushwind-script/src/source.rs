//! The script-source contract.
//!
//! A script source serves script code by key; the key is whatever the
//! concrete implementation defines it to be (filesystem path, object
//! key, script id, ...). Loading is the only mandatory capability —
//! [`ScriptSource::watch`] is a defaulted capability method that
//! rejects with [`ScriptError::CapabilityNotSupported`], the runtime
//! form of the Go `Watcher` interface assertion, and compositions like
//! [`MultiSource`] skip sub-sources answering with it.
//!
//! The Go predecessor delivers change notifications through a channel
//! closed by context cancellation; the port follows the config
//! domain's stream doctrine — [`SignalStream`] yields one tick per
//! change, ends (`None`) when the underlying watch ends, and dropping
//! the stream is the cancellation. The `context.Context` parameter is
//! dropped throughout: callers needing deadlines wrap the call in
//! `tokio::time::timeout`, and dropping a load future cancels the
//! load.
//!
//! Unlike the config domain there is no "absent" answer — a missing
//! key is an [`ScriptError::Failed`], the Go shape, because script
//! sources have no priority-shadowing semantics riding on absence.
//!
//! The Go sources over external infrastructure — consul, etcd, redis,
//! http, s3, git, database — are unported and each is an engine crate
//! waiting on its client integration, the registry engines'
//! trajectory.

use crate::{BoxFuture, ScriptError};
use std::sync::Arc;

/// A script source: script code by key, plus the watch capability it
/// can offer.
///
/// Implementations live in their own crates (`rushwind-script-*`) for
/// remote carriers; the local carriers (memory, file, static tree) and
/// the compositions (multi-strategy, cached, transform) live in this
/// crate, the Go package's own layout. A source is [`Send`] + [`Sync`]
/// and shareable behind an [`Arc`].
pub trait ScriptSource: Send + Sync {
    /// Loads the script source code for `key`.
    ///
    /// A missing key is an error, not an absent signal.
    fn load<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<String, ScriptError>>;

    /// Signal-mode change watch: a [`SignalStream`] delivering one
    /// tick per change of the key's content; the caller re-reads via
    /// [`ScriptSource::load`].
    ///
    /// The default rejects with
    /// [`ScriptError::CapabilityNotSupported`] — only watch-capable
    /// sources override it.
    fn watch<'a>(
        &'a self,
        key: &'a str,
    ) -> BoxFuture<'a, Result<Box<dyn SignalStream>, ScriptError>> {
        let _ = key;
        Box::pin(async { Err(ScriptError::CapabilityNotSupported) })
    }
}

/// A stream of change ticks for one key. The new content is *not*
/// delivered — re-read it with [`ScriptSource::load`]; the tick only
/// says "changed".
///
/// `next` resolves to `Some(())` per change and `None` once the
/// stream has ended. Dropping the stream is the cancellation: the
/// underlying watch stops with it. Streams must tolerate being
/// re-`next`ed after a dropped wait — a dropped wait cancels that
/// wait, not the stream.
pub trait SignalStream: Send {
    /// Waits for the next change tick; `None` when the stream ended.
    fn next<'a>(&'a mut self) -> BoxFuture<'a, Option<()>>;
}

/// Convenience for sharing a source across composition points.
pub type SharedScriptSource = Arc<dyn ScriptSource>;

pub mod cached;
pub mod file;
pub mod fs;
pub mod memory;
pub mod multiple;
pub mod transform;
