//! The engine contract: one core lifecycle trait, a set of orthogonal
//! capability traits, and the `FullEngine` aggregate that full-featured
//! engines satisfy.
//!
//! The surface splits across [`ScriptEngine`]
//! (lifecycle, mandatory), the optional capability traits
//! ([`ScriptLoader`], [`ScriptExecutor`], [`GlobalAccessor`],
//! [`FunctionRegistrar`], [`ModuleRegistrar`], [`ScriptWatcher`],
//! [`SandboxConfigurator`], [`RuntimeHookRegistrar`], [`SyncExecutor`],
//! [`QuotaController`]), and the [`FullEngine`] aggregate binding the first
//! seven together. The split is the point: lightweight engines (CEL,
//! Expr) implement the core plus one capability and nothing else, and
//! *callers* degrade gracefully around the gaps.
//!
//! Capability discovery uses probe methods
//! on [`ScriptEngine`] — `fn as_loader(self: Arc<Self>) ->
//! Option<Arc<dyn ScriptLoader>>` and its siblings. A capability an
//! engine does not implement simply keeps the default `None` probe;
//! the `self: Arc<Self>` receiver is the object-safe form of handing
//! the engine back as the capability object. Trait upcasting would be
//! the other route and is past the workspace MSRV (1.81 < 1.86).
//!
//! `SandboxConfigurator`, `RuntimeHookRegistrar`, `SyncExecutor`, and
//! `QuotaController` are deliberately **standalone** capabilities — the
//! aggregate does not embed them, engines without a standard-library
//! concept or a hot path satisfy [`FullEngine`] without them, and their
//! probes default to `None` there. See the standalone-capability test
//! for the guarded shape.

use std::sync::Arc;
use std::time::Duration;

use crate::{BoxFuture, ScriptError, ScriptValue, SharedScriptSource};

/// A host function exposed to scripts, in the uniform
/// [`ScriptValue`] marshalling.
///
/// Per-engine native callback registrations accept per-engine
/// signatures; the contract's uniform shape is a
/// [`ScriptValue`]-marshalled async closure, which every engine crate
/// wraps onto its own native callback registration (Boa
/// `NativeFunction`, mlua `create_function`, Starlark natives, Wasm
/// linker imports). Engines whose runtime cannot host plain closures
/// expose their own native API besides this one.
pub type HostFunction = Arc<
    dyn Fn(&[ScriptValue]) -> BoxFuture<'static, Result<ScriptValue, ScriptError>> + Send + Sync,
>;

/// A runtime-initialization hook: run on the engine's runtime after it
/// is created and ready, before any load or execute.
///
/// There is no context parameter (the workspace doctrine: cancellation
/// is dropping the future, deadlines wrap the call in `tokio::time::
/// timeout`), so the hook is a plain `'static` boxed future.
pub type RuntimeHook = Arc<dyn Fn() -> BoxFuture<'static, Result<(), ScriptError>> + Send + Sync>;

/// An execution budget for synchronous hot-path runs.
///
/// An unset budget means "no bound"; the [`Option`] shape makes that
/// explicit with [`Option`]. At least one field should be set for the
/// quota to take effect — engines wire the fields onto their VM's
/// cancellation primitive (instruction counters, epoch deadlines, fuel)
/// where the runtime offers one, and reject [`Self::default`] budgets
/// with [`ScriptError::Failed`] where it does not.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Quota {
    /// The wall-clock budget for one synchronous run.
    pub timeout: Option<Duration>,
    /// The instruction-count budget for one synchronous run.
    pub max_instructions: Option<u64>,
}

/// The core lifecycle interface every engine satisfies, regardless of
/// its feature set.
///
/// Implementations are shared behind [`Arc`] (the pools and the
/// [`crate::Manager`] hold them that way), so every method takes
/// `&self` with interior mutability — the per-engine
/// locking discipline lives in the engine's own state.
pub trait ScriptEngine: Send + Sync {
    /// The engine's registry name (e.g. `"javascript"`). The
    /// [`crate`] factory registry
    /// keys on it.
    fn engine_type(&self) -> &'static str;

    /// Initializes the engine. Must be called before any load or
    /// execute; returns an error if initialization fails or the engine
    /// is already initialized.
    fn init(&self) -> BoxFuture<'_, Result<(), ScriptError>>;

    /// Releases the resources the engine holds (runtime, VM, handles).
    /// After this the engine must be re-initialized before reuse.
    fn close(&self) -> Result<(), ScriptError>;

    /// Reports whether the engine is initialized and not yet closed.
    fn is_initialized(&self) -> bool;

    /// Returns the last error the engine recorded, if any.
    fn last_error(&self) -> Option<ScriptError>;

    /// Clears the engine's last-error state.
    fn clear_error(&self);

    // ------------------------------------------------------------------
    // Capability probes.
    //
    // Every probe defaults to `None`; an engine implementing the
    // corresponding capability overrides the probe to hand itself back
    // as that capability object. Callers use the probes to degrade
    // gracefully around engines that lack a capability.
    // ------------------------------------------------------------------

    /// Probe for the [`ScriptLoader`] capability.
    fn as_loader(self: Arc<Self>) -> Option<Arc<dyn ScriptLoader>> {
        None
    }

    /// Probe for the [`ScriptExecutor`] capability.
    fn as_executor(self: Arc<Self>) -> Option<Arc<dyn ScriptExecutor>> {
        None
    }

    /// Probe for the [`GlobalAccessor`] capability.
    fn as_global_accessor(self: Arc<Self>) -> Option<Arc<dyn GlobalAccessor>> {
        None
    }

    /// Probe for the [`FunctionRegistrar`] capability.
    fn as_function_registrar(self: Arc<Self>) -> Option<Arc<dyn FunctionRegistrar>> {
        None
    }

    /// Probe for the [`ModuleRegistrar`] capability.
    fn as_module_registrar(self: Arc<Self>) -> Option<Arc<dyn ModuleRegistrar>> {
        None
    }

    /// Probe for the [`ScriptWatcher`] capability.
    fn as_watcher(self: Arc<Self>) -> Option<Arc<dyn ScriptWatcher>> {
        None
    }

    /// Probe for the [`SandboxConfigurator`] capability.
    fn as_sandbox_configurator(self: Arc<Self>) -> Option<Arc<dyn SandboxConfigurator>> {
        None
    }

    /// Probe for the [`RuntimeHookRegistrar`] capability.
    fn as_runtime_hook_registrar(self: Arc<Self>) -> Option<Arc<dyn RuntimeHookRegistrar>> {
        None
    }

    /// Probe for the [`SyncExecutor`] capability.
    fn as_sync_executor(self: Arc<Self>) -> Option<Arc<dyn SyncExecutor>> {
        None
    }

    /// Probe for the [`QuotaController`] capability.
    fn as_quota_controller(self: Arc<Self>) -> Option<Arc<dyn QuotaController>> {
        None
    }
}

/// Source-driven script loading.
///
/// Loading is uniformly driven by the bound [`SharedScriptSource`] so
/// the engine stays decoupled from concrete IO mechanisms (filesystem,
/// object storage, memory, ...).
pub trait ScriptLoader: Send + Sync {
    /// Binds (or, with `None`, unbinds) a script source; subsequent
    /// loads read through it. Binding is synchronous.
    fn set_source(&self, source: Option<SharedScriptSource>);

    /// Returns the currently bound source, if any.
    fn get_source(&self) -> Option<SharedScriptSource>;

    /// Loads the script identified by `key` from the bound source and
    /// keeps it for a later [`ScriptExecutor::execute`].
    fn load<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<(), ScriptError>>;

    /// Loads multiple scripts from the bound source, in order, aborting
    /// on the first error.
    fn load_multi<'a>(&'a self, keys: &'a [String]) -> BoxFuture<'a, Result<(), ScriptError>>;

    /// Compiles an inline script given directly as a string, bypassing
    /// the bound source. `name` is used for diagnostics (stack traces,
    /// error messages).
    fn load_string<'a>(
        &'a self,
        name: &'a str,
        code: &'a str,
    ) -> BoxFuture<'a, Result<(), ScriptError>>;
}

/// Script execution.
pub trait ScriptExecutor: Send + Sync {
    /// Runs every script previously loaded into the engine and returns
    /// the combined result.
    fn execute(&self) -> BoxFuture<'_, Result<ScriptValue, ScriptError>>;

    /// Loads the script identified by `key` from the bound source and
    /// immediately runs it, all in one step.
    fn execute_from_key<'a>(
        &'a self,
        key: &'a str,
    ) -> BoxFuture<'a, Result<ScriptValue, ScriptError>>;

    /// The multi-key variant of [`ScriptExecutor::execute_from_key`];
    /// results come back in the same order as `keys`.
    fn execute_from_keys<'a>(
        &'a self,
        keys: &'a [String],
    ) -> BoxFuture<'a, Result<Vec<ScriptValue>, ScriptError>>;

    /// Compiles and immediately runs an inline string script, bypassing
    /// the bound source. `name` is used for diagnostics.
    fn execute_string<'a>(
        &'a self,
        name: &'a str,
        code: &'a str,
    ) -> BoxFuture<'a, Result<ScriptValue, ScriptError>>;
}

/// Read/write access to global variables visible to scripts.
pub trait GlobalAccessor: Send + Sync {
    /// Registers or overwrites a global variable visible to scripts.
    /// Values marshal through
    /// [`ScriptValue`], the data-only bridge.
    fn register_global(&self, name: &str, value: ScriptValue) -> Result<(), ScriptError>;

    /// Reads the value of a global variable; an undefined name is an
    /// error.
    fn get_global(&self, name: &str) -> Result<ScriptValue, ScriptError>;
}

/// Host-function registration and script-function invocation.
pub trait FunctionRegistrar: Send + Sync {
    /// Registers a host function scripts can call by `name`.
    ///
    /// The uniform shape is the [`HostFunction`]
    /// closure, marshalled through [`ScriptValue`]. Engines wrap it
    /// onto their own native callback machinery.
    fn register_function(&self, name: &str, function: HostFunction) -> Result<(), ScriptError>;

    /// Invokes the script-side function registered as `name` with the
    /// given arguments and returns its result.
    fn call_function<'a>(
        &'a self,
        name: &'a str,
        args: &'a [ScriptValue],
    ) -> BoxFuture<'a, Result<ScriptValue, ScriptError>>;
}

/// Module registration for engines with a module system (Lua's
/// `require`, JavaScript's `import`).
///
/// Lightweight expression engines (CEL, Expr) do not implement this;
/// their probe stays `None`.
pub trait ModuleRegistrar: Send + Sync {
    /// Registers a module under `name` so scripts can require or import
    /// it.
    ///
    /// Modules come in two shapes: data tables carried as
    /// [`ScriptValue::Map`], and native loaders, which are
    /// engine-specific
    /// and live on the engine crate's own API.
    fn register_module(&self, name: &str, module: ScriptValue) -> Result<(), ScriptError>;
}

/// Hot-reload: re-watching scripts bound through a source that offers
/// [`ScriptSource::watch`](crate::ScriptSource::watch).
///
/// Engines without a hot-reload story (CEL, Expr, ...) do not implement
/// this; their probe stays `None`.
pub trait ScriptWatcher: Send + Sync {
    /// Starts watching the script identified by `key` through the bound
    /// source's watch capability; on change the script is automatically
    /// reloaded.
    fn start_watch<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<(), ScriptError>>;

    /// Stops watching the script identified by `key`.
    fn stop_watch(&self, key: &str) -> Result<(), ScriptError>;
}

/// An optional, **standalone** capability restricting the standard
/// libraries a script can touch.
///
/// Engines whose runtime has a notion of selectable standard libraries
/// (Lua) implement it; engines without that concept (CEL, Expr,
/// Starlark, Wasm) do not, and their probe stays `None`. It is not part
/// of the [`FullEngine`] aggregate.
///
/// [`set_open_libs`](SandboxConfigurator::set_open_libs) must be called
/// before [`ScriptEngine::init`]; the engine consumes the allow-list
/// when it creates the runtime inside init, and libraries not listed
/// are never opened (and, where the engine recycles pooled runtimes,
/// are actively removed), preventing scripts from escaping the host
/// through `os`/`io`/`debug`-style libraries. An empty list reverts to
/// the engine default of the full standard set.
pub trait SandboxConfigurator: Send + Sync {
    /// Configures the allow-list of standard libraries to open. Library
    /// names are engine-specific (the future Lua engine will mirror the
    /// mlua crate's library namespaces).
    fn set_open_libs(&self, libs: &[&str]);
}

/// An optional, **standalone** capability for injecting host modules,
/// host functions, and reverse callbacks — a `hook.register` surface
/// exposed to scripts — into the runtime after init and before any load
/// or execute.
///
/// Engines that pool and recycle runtimes replay every registered hook
/// on each (re)acquired runtime, clearing whatever the previous owner
/// injected first, so engine instances stay isolated. Lightweight
/// engines (CEL, Expr, Starlark, Wasm) typically do not implement
/// this; their probe stays `None`.
pub trait RuntimeHookRegistrar: Send + Sync {
    /// Registers a hook to run on the runtime. Before init the
    /// registration defers to init; after init it runs immediately on
    /// the live runtime.
    fn add_runtime_hook(&self, hook: RuntimeHook) -> Result<(), ScriptError>;
}

/// The hot-path executor: synchronous runs with no per-call task or
/// channel machinery, for per-frame callback loads (game main loops).
///
/// A timed-out or over-budget run answers [`ScriptError::QuotaExceeded`]
/// rather than a generic failure. Combine with
/// [`QuotaController`] to bound the run.
pub trait SyncExecutor: Send + Sync {
    /// Runs the last script loaded into the engine synchronously.
    /// Returns [`ScriptError::QuotaExceeded`] if a configured quota was
    /// hit.
    fn execute_sync(&self) -> BoxFuture<'_, Result<ScriptValue, ScriptError>>;
}

/// An optional, **standalone** capability setting the execution budget
/// applied to subsequent [`SyncExecutor::execute_sync`] runs.
///
/// Engines wire this onto their VM's cancellation primitive (epoch
/// deadlines plus fuel in Wasmtime/Wasmi, instruction budgets
/// elsewhere); where the runtime offers no such primitive the engine
/// does not implement the capability and the probe stays `None`.
pub trait QuotaController: Send + Sync {
    /// Configures the budget for subsequent synchronous runs. A
    /// default (all-`None`) quota removes any bound.
    fn set_quota(&self, quota: Quota);
}

/// The aggregate interface full-featured engines satisfy: the core
/// lifecycle plus every embedded capability.
///
/// The aggregate is satisfied implicitly: a blanket impl over the
/// supertrait bundle means an engine struct
/// that implements the seven traits *is* a `dyn FullEngine` without
/// naming this trait. Lightweight engines implement a subset — callers
/// discover which through the probes on [`ScriptEngine`].
pub trait FullEngine:
    ScriptEngine
    + ScriptLoader
    + ScriptExecutor
    + GlobalAccessor
    + FunctionRegistrar
    + ModuleRegistrar
    + ScriptWatcher
{
}

impl<T> FullEngine for T where
    T: ScriptEngine
        + ScriptLoader
        + ScriptExecutor
        + GlobalAccessor
        + FunctionRegistrar
        + ModuleRegistrar
        + ScriptWatcher
{
}

/// Convenience for sharing an engine across the pools and the
/// [`crate::Manager`].
pub type SharedEngine = Arc<dyn FullEngine>;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testutil::{LifecycleOnly, MockEngine, MockNoSandbox};

    /// The capabilityless engine: an engine implementing only the
    /// lifecycle trait probes `None` for every capability, and the
    /// standalone capabilities probe `None` even on a full engine.
    #[test]
    fn unsupported_capabilities_probe_none() {
        let lifecycle: Arc<dyn ScriptEngine> = Arc::new(LifecycleOnly::new("lifecycle"));
        assert!(Arc::clone(&lifecycle).as_loader().is_none());
        assert!(Arc::clone(&lifecycle).as_executor().is_none());
        assert!(Arc::clone(&lifecycle).as_global_accessor().is_none());
        assert!(Arc::clone(&lifecycle).as_function_registrar().is_none());
        assert!(Arc::clone(&lifecycle).as_module_registrar().is_none());
        assert!(Arc::clone(&lifecycle).as_watcher().is_none());
        assert!(Arc::clone(&lifecycle).as_sandbox_configurator().is_none());
        assert!(Arc::clone(&lifecycle).as_runtime_hook_registrar().is_none());
        assert!(Arc::clone(&lifecycle).as_sync_executor().is_none());
        assert!(Arc::clone(&lifecycle).as_quota_controller().is_none());
        drop(lifecycle);

        // The standalone capabilities are not part of the aggregate:
        // a full engine probes `None` for each of them.
        let full: Arc<dyn FullEngine> = Arc::new(MockEngine::new("mock"));
        assert!(Arc::clone(&full).as_runtime_hook_registrar().is_none());
        assert!(Arc::clone(&full).as_sync_executor().is_none());
        assert!(Arc::clone(&full).as_quota_controller().is_none());
    }

    /// The sandbox
    /// capability is discovered through the aggregate object, and the
    /// capability call flows into the same underlying engine.
    #[test]
    fn the_sandbox_capability_flows_through_a_full_engine() {
        let mock = Arc::new(MockEngine::new("mock"));
        let full: Arc<dyn FullEngine> = mock.clone();
        let sandbox = Arc::clone(&full)
            .as_sandbox_configurator()
            .expect("full engine offers the sandbox capability");
        sandbox.set_open_libs(&["base", "string"]);
        assert_eq!(
            mock.open_libs(),
            vec!["base".to_string(), "string".to_string()],
            "the capability writes the same engine's recorded state"
        );
    }

    /// Standalone-capability check: an
    /// engine without `set_open_libs` still satisfies the aggregate,
    /// implements the embedded watcher capability, and probes `None`
    /// for the sandbox — while an engine with it probes `Some`.
    #[test]
    fn the_sandbox_capability_is_standalone() {
        fn requires_full<T: FullEngine>() {}
        requires_full::<MockNoSandbox>();

        let no_sandbox: Arc<dyn ScriptEngine> = Arc::new(MockNoSandbox::new("nosandbox"));
        assert!(
            Arc::clone(&no_sandbox).as_watcher().is_some(),
            "the watcher capability is embedded in the aggregate"
        );
        assert!(Arc::clone(&no_sandbox).as_sandbox_configurator().is_none());

        let with_sandbox = Arc::new(MockEngine::new("mock"));
        let with_sandbox: Arc<dyn ScriptEngine> = with_sandbox;
        assert!(Arc::clone(&with_sandbox)
            .as_sandbox_configurator()
            .is_some());
    }
}
