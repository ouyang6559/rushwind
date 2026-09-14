//! The script-engine contract for RushWind, extracted from the Go
//! predecessor `go-scripts`.
//!
//! The domain is a capability-split engine surface — one mandatory
//! lifecycle trait, orthogonal optional capabilities, and the
//! [`FullEngine`] aggregate binding the embedded ones — plus the
//! machinery that makes engines deployable: a factory registry, a
//! fixed and an auto-growing pool, and a multi-engine manager. Script
//! *content* arrives through the source contract
//! ([`ScriptSource`]), whose local carriers and compositions
//! ([`MemSource`], [`FileSource`], [`FileSystemSource`],
//! [`MultiSource`], [`CachedSource`], [`TransformSource`]) live in
//! this crate; remote carriers are engine crates to come.
//!
//! Values crossing the engine boundary marshal through
//! [`ScriptValue`], the data-only bridge standing in for Go's `any`.
//!
//! # Layout
//!
//! | Module | Contents |
//! |:---|:---|
//! | [`engine`] | the lifecycle trait, the capability traits, the aggregate, the quota and host-hook types |
//! | [`factory`] | the name-keyed factory registry |
//! | [`manager`] | the named multi-engine lifecycle manager |
//! | [`pool`] | the fixed-size engine pool |
//! | [`pool_autogrow`] | the demand-growing engine pool |
//! | [`source`] | the source and stream contracts plus the local carriers and compositions |
//!
//! # Divergences from the Go predecessor
//!
//! | Go | Rust |
//! |:---|:---|
//! | `Type` constants + `map[Type]FactoryFunc` | `fn engine_type() -> &'static str` + the encoding domain's string-keyed registry |
//! | `As*` interface-assertion helpers over `any` | probe methods on [`ScriptEngine`] with `self: Arc<Self>` receivers and `None` defaults (trait upcasting is past the workspace MSRV) |
//! | `Engine` aggregate interface | [`FullEngine`] with a blanket impl over the supertrait bundle — implicit satisfaction preserved |
//! | `SandboxConfigurator` / `RuntimeHookRegistrar` / `SyncExecutor` / `QuotaController` | standalone traits, never in the aggregate, probes defaulting to `None` — the Go optionality contract |
//! | `context.Context` threading | dropped: cancellation is dropping the future, deadlines are the caller's `tokio::time::timeout` |
//! | `any` values across the boundary | [`ScriptValue`], the data-only bridge; host functions are the [`HostFunction`] closure shape |
//! | engine channel pools | queue + counting semaphore, with `Semaphore::close` as the Go `close(chan)` wake-up |
//! | `MultiSource` first-ok goroutine race | the sub-load futures race inside the call's own future, no task boundaries |
//! | `CachedSource` per-key invalidation goroutine | a lazy drain: each load polls the stored watch stream's already-pending signal, eviction lands by the next load |
//! | source channels closed by context cancellation | [`SignalStream`]: a tick per change, `None` when the watch ends, drop is the cancellation |
//! | `Close()` on sources | `Drop` |
//! | nil checks (`fs.FS`, cached remote, transforms) | dropped — `Arc` and `Box` cannot be null |
//! | `fs.FS` trees (embed, zip, dir) | [`StaticTree`], the caller-supplied immutable lookup; the source contributes the prefix joining |
//! | `Quota` zero-value "no bound" | [`Option`] fields, [`Default`] = no bound |
//! | the `mtimes` write-only map in the file source | dropped; the watch stream tracks its own baseline |
//!
//! # Engine matrix (ported)
//!
//! None yet. The engines with a real Rust runtime beneath them —
//! Starlark (`starlark-rust`), JavaScript (`boa_engine`), WebAssembly
//! (`wasmi`/`wasmtime`), CEL (`cel-interpreter`), Lua (`mlua`) — are
//! the next tranche, one engine crate each. The Go predecessor's
//! yaegi and TCL engines have no Rust counterpart and are not planned;
//! its gpython engine maps onto RustPython only at a cost and
//! stability tradeoff that keeps it deferred, and expr has no
//! semantic equal (`evalexpr` is a cousin, not a port).
//!
//! # Remote sources (unported)
//!
//! The predecessor's consul, etcd, redis, http, s3, git, and database
//! script sources each await their Rust client integration as an
//! engine crate — the registry engines' trajectory, and the consul
//! gap already has the raw-HTTP precedent from
//! `rushwind-registry-consul`.
#![forbid(unsafe_code)]
#![deny(missing_docs)]

use std::future::Future;
use std::pin::Pin;

mod engine;
mod error;
mod factory;
mod manager;
mod pool;
mod pool_autogrow;
mod source;
mod value;

#[cfg(test)]
mod testutil;

pub use error::ScriptError;
pub use value::ScriptValue;

pub use engine::{
    FullEngine, FunctionRegistrar, GlobalAccessor, HostFunction, ModuleRegistrar, Quota,
    QuotaController, RuntimeHook, RuntimeHookRegistrar, SandboxConfigurator, ScriptEngine,
    ScriptExecutor, ScriptLoader, ScriptWatcher, SharedEngine, SyncExecutor,
};
pub use factory::{
    get_factory, list_factories, new_script_engine, register_factory, unregister_factory,
    EngineFactory,
};
pub use manager::Manager;
pub use pool::EnginePool;
pub use pool_autogrow::{AutoGrowEnginePool, FullEngineFactory};
pub use source::cached::CachedSource;
pub use source::file::FileSource;
pub use source::fs::{FileSystemSource, StaticTree};
pub use source::memory::MemSource;
pub use source::multiple::{MultiSource, MultiStrategy};
pub use source::transform::{identity_transform, TransformFn, TransformSource};
pub use source::{ScriptSource, SharedScriptSource, SignalStream};

/// Future type used across the script contract.
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;
