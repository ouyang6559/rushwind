//! The WebAssembly engine for the Rust script contract — the Go
//! predecessor's wazero engine, rebuilt over [`wasmi`], the
//! pure-Rust interpreter, as the pure-interpreter counterpart of
//! wazero's pure-Go one.
//!
//! Semantics preserved from the predecessor:
//!
//! - `load` compiles a module and keeps it; `execute` instantiates
//!   the **last** loaded module and invokes its `_start` export.
//! - A module without a `_start` export instantiates silently and the
//!   run answers `Null` — the module stays instantiated, the Go
//!   "returns nil if `_start` is not exported" shape.
//! - The engine implements only the lifecycle, loader, and executor
//!   capabilities — no globals, no host functions, no modules, no
//!   watch, the Go wazero capability set; every other probe answers
//!   `None`.
//!
//! Divergences from the Go predecessor:
//!
//! - Wasm bytes ride the contract's [`ScriptSource`](rushwind_script)
//!   `String` carrier, which the port made UTF-8; modules must
//!   therefore be UTF-8-clean byte strings (the predecessor's Go
//!   `string` carried arbitrary bytes). Hand-encoded minimal sections
//!   are; compiler output generally is not.
//! - The predecessor's wazero `Runtime` handle and its close
//!   bookkeeping collapse into the engine dropping its
//!   [`wasmi::Engine`] on close.
//!
//! # Engine matrix
//!
//! | Capability | Status |
//! |:---|:---|
//! | loader / executor / lifecycle | implemented |
//! | globals, functions, modules, watch, sandbox, hooks, sync+quota | not offered by the predecessor's wazero engine either |

#![forbid(unsafe_code)]
#![deny(missing_docs)]

use std::sync::{Arc, Mutex};

use wasmi::{Engine, Instance, Module, Store};

use rushwind_script::{
    register_factory, BoxFuture, ScriptEngine, ScriptError, ScriptExecutor, ScriptLoader,
    ScriptValue, SharedEngine, SharedScriptSource,
};

/// The `wasm` engine: [`wasmi`] module compilation and `_start`
/// invocation.
pub struct WasmEngine {
    engine: Mutex<Option<Engine>>,
    source: Mutex<Option<SharedScriptSource>>,
    modules: Mutex<Vec<Module>>,
    initialized: Mutex<bool>,
    last_error: Mutex<Option<ScriptError>>,
}

impl WasmEngine {
    /// Builds the engine uninitialized; call
    /// [`ScriptEngine::init`] before use.
    pub fn new() -> Self {
        Self {
            engine: Mutex::new(None),
            source: Mutex::new(None),
            modules: Mutex::new(Vec::new()),
            initialized: Mutex::new(false),
            last_error: Mutex::new(None),
        }
    }

    fn set_last_error(&self, error: ScriptError) {
        *self.last_error.lock().expect("wasm engine last-error lock") = Some(error);
    }

    fn clear_last_error(&self) {
        *self.last_error.lock().expect("wasm engine last-error lock") = None;
    }

    /// A missing `_start` export answers `Null`; the run proceeds only
    /// for an exported zero-argument zero-result function.
    fn run_last(&self) -> Result<ScriptValue, ScriptError> {
        if !self.is_initialized() {
            let err = ScriptError::Failed("wasm engine: not initialized".to_string());
            self.set_last_error(err.clone());
            return Err(err);
        }
        let (engine, module) = {
            let engine = self.engine.lock().expect("wasm engine handle lock").clone();
            let module = self
                .modules
                .lock()
                .expect("wasm engine module lock")
                .last()
                .cloned();
            (engine, module)
        };
        let Some(engine) = engine else {
            let err = ScriptError::Failed("wasm engine: not initialized".to_string());
            self.set_last_error(err.clone());
            return Err(err);
        };
        let Some(module) = module else {
            let err = ScriptError::Failed("wasm engine: no module loaded".to_string());
            self.set_last_error(err.clone());
            return Err(err);
        };
        let mut store = Store::new(&engine, ());
        let instance = match Instance::new(&mut store, &module, &[]) {
            Ok(instance) => instance,
            Err(err) => {
                let wrapped =
                    ScriptError::Failed(format!("wasm engine: instantiation failed: {err}"));
                self.set_last_error(wrapped.clone());
                return Err(wrapped);
            }
        };
        let Some(func_extern) = instance.get_export(&store, "_start") else {
            self.clear_last_error();
            return Ok(ScriptValue::Null);
        };
        let Some(func) = func_extern.into_func() else {
            self.clear_last_error();
            return Ok(ScriptValue::Null);
        };
        let Ok(start) = func.typed::<(), ()>(&store) else {
            self.clear_last_error();
            return Ok(ScriptValue::Null);
        };
        if let Err(err) = start.call(&mut store, ()) {
            let wrapped = ScriptError::Failed(format!("wasm engine: _start trapped: {err}"));
            self.set_last_error(wrapped.clone());
            return Err(wrapped);
        }
        self.clear_last_error();
        Ok(ScriptValue::Null)
    }
}

impl Default for WasmEngine {
    fn default() -> Self {
        Self::new()
    }
}

impl ScriptEngine for WasmEngine {
    fn engine_type(&self) -> &'static str {
        "wasm"
    }

    fn init(&self) -> BoxFuture<'_, Result<(), ScriptError>> {
        let outcome = {
            let mut initialized = self.initialized.lock().expect("wasm engine init lock");
            if *initialized {
                Err(ScriptError::Failed(
                    "wasm engine: already initialized".to_string(),
                ))
            } else {
                *initialized = true;
                *self.engine.lock().expect("wasm engine handle lock") = Some(Engine::default());
                Ok(())
            }
        };
        match &outcome {
            Err(err) => self.set_last_error(err.clone()),
            Ok(()) => self.clear_last_error(),
        }
        Box::pin(async move { outcome })
    }

    fn close(&self) -> Result<(), ScriptError> {
        {
            let mut initialized = self.initialized.lock().expect("wasm engine init lock");
            *initialized = false;
            *self.engine.lock().expect("wasm engine handle lock") = None;
            self.modules
                .lock()
                .expect("wasm engine module lock")
                .clear();
            *self.source.lock().expect("wasm engine source lock") = None;
        }
        self.clear_last_error();
        Ok(())
    }

    fn is_initialized(&self) -> bool {
        *self.initialized.lock().expect("wasm engine init lock")
    }

    fn last_error(&self) -> Option<ScriptError> {
        self.last_error
            .lock()
            .expect("wasm engine last-error lock")
            .clone()
    }

    fn clear_error(&self) {
        self.clear_last_error();
    }

    fn as_loader(self: Arc<Self>) -> Option<Arc<dyn ScriptLoader>> {
        Some(self)
    }

    fn as_executor(self: Arc<Self>) -> Option<Arc<dyn ScriptExecutor>> {
        Some(self)
    }
}

impl WasmEngine {
    fn compile(&self, code: &str) -> Result<(), ScriptError> {
        if !self.is_initialized() {
            let err = ScriptError::Failed("wasm engine: not initialized".to_string());
            self.set_last_error(err.clone());
            return Err(err);
        }
        let engine = self.engine.lock().expect("wasm engine handle lock").clone();
        let Some(engine) = engine else {
            let err = ScriptError::Failed("wasm engine: not initialized".to_string());
            self.set_last_error(err.clone());
            return Err(err);
        };
        match Module::new(&engine, code.as_bytes()) {
            Ok(module) => {
                self.modules
                    .lock()
                    .expect("wasm engine module lock")
                    .push(module);
                self.clear_last_error();
                Ok(())
            }
            Err(err) => {
                let wrapped = ScriptError::Failed(format!("wasm engine: compile failed: {err}"));
                self.set_last_error(wrapped.clone());
                Err(wrapped)
            }
        }
    }

    /// The sync half of a source-driven load: the bound-source check.
    fn bound_source(&self) -> Result<SharedScriptSource, ScriptError> {
        if !self.is_initialized() {
            let err = ScriptError::Failed("wasm engine: not initialized".to_string());
            self.set_last_error(err.clone());
            return Err(err);
        }
        let source = self.source.lock().expect("wasm engine source lock").clone();
        let Some(source) = source else {
            let err = ScriptError::Failed("wasm engine: no source bound".to_string());
            self.set_last_error(err.clone());
            return Err(err);
        };
        Ok(source)
    }
}

impl ScriptLoader for WasmEngine {
    fn set_source(&self, source: Option<SharedScriptSource>) {
        *self.source.lock().expect("wasm engine source lock") = source;
    }

    fn get_source(&self) -> Option<SharedScriptSource> {
        self.source.lock().expect("wasm engine source lock").clone()
    }

    fn load<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<(), ScriptError>> {
        Box::pin(async move {
            let source = self.bound_source()?;
            let code = match source.load(key).await {
                Ok(code) => code,
                Err(err) => {
                    self.set_last_error(err.clone());
                    return Err(err);
                }
            };
            self.compile(&code)
        })
    }

    fn load_multi<'a>(&'a self, keys: &'a [String]) -> BoxFuture<'a, Result<(), ScriptError>> {
        Box::pin(async move {
            for key in keys {
                self.load(key).await?;
            }
            Ok(())
        })
    }

    fn load_string<'a>(
        &'a self,
        _name: &'a str,
        code: &'a str,
    ) -> BoxFuture<'a, Result<(), ScriptError>> {
        Box::pin(async move { self.compile(code) })
    }
}

impl ScriptExecutor for WasmEngine {
    fn execute(&self) -> BoxFuture<'_, Result<ScriptValue, ScriptError>> {
        Box::pin(async move { self.run_last() })
    }

    fn execute_from_key<'a>(
        &'a self,
        key: &'a str,
    ) -> BoxFuture<'a, Result<ScriptValue, ScriptError>> {
        Box::pin(async move {
            self.load(key).await?;
            self.run_last()
        })
    }

    fn execute_from_keys<'a>(
        &'a self,
        keys: &'a [String],
    ) -> BoxFuture<'a, Result<Vec<ScriptValue>, ScriptError>> {
        Box::pin(async move {
            let mut results = Vec::with_capacity(keys.len());
            for key in keys {
                let value = self.execute_from_key(key).await?;
                results.push(value);
            }
            Ok(results)
        })
    }

    fn execute_string<'a>(
        &'a self,
        _name: &'a str,
        code: &'a str,
    ) -> BoxFuture<'a, Result<ScriptValue, ScriptError>> {
        Box::pin(async move {
            self.compile(code)?;
            self.run_last()
        })
    }
}

/// The factory hook the Go predecessor's `init()` registered under
/// the wazero type: a fresh uninitialized engine.
pub fn factory() -> Result<SharedEngine, ScriptError> {
    Ok(Arc::new(WasmEngine::new()))
}

/// Installs the engine factory in the registry under the `wasm`
/// type. Call once at startup.
pub fn register() {
    let factory_fn: rushwind_script::EngineFactory = Arc::new(factory);
    let _ = register_factory("wasm", factory_fn);
}

// ---------------------------------------------------------------------
// The aggregate's remaining faces as absent-capability stubs. The Go
// predecessor satisfied the Engine interface for wazero by embedding
// nil interfaces — a structural satisfier whose every use panicked.
// The port keeps the trait set satisfied (the blanket FullEngine
// requires it) but answers every call with CapabilityNotSupported,
// and the probes on the core trait stay `None`, so well-behaved
// callers never reach these stubs at all.
// ---------------------------------------------------------------------

impl rushwind_script::GlobalAccessor for WasmEngine {
    fn register_global(&self, _name: &str, _value: ScriptValue) -> Result<(), ScriptError> {
        Err(ScriptError::CapabilityNotSupported)
    }

    fn get_global(&self, _name: &str) -> Result<ScriptValue, ScriptError> {
        Err(ScriptError::CapabilityNotSupported)
    }
}

impl rushwind_script::FunctionRegistrar for WasmEngine {
    fn register_function(
        &self,
        _name: &str,
        _function: rushwind_script::HostFunction,
    ) -> Result<(), ScriptError> {
        Err(ScriptError::CapabilityNotSupported)
    }

    fn call_function<'a>(
        &'a self,
        _name: &'a str,
        _args: &'a [ScriptValue],
    ) -> BoxFuture<'a, Result<ScriptValue, ScriptError>> {
        Box::pin(async { Err(ScriptError::CapabilityNotSupported) })
    }
}

impl rushwind_script::ModuleRegistrar for WasmEngine {
    fn register_module(&self, _name: &str, _module: ScriptValue) -> Result<(), ScriptError> {
        Err(ScriptError::CapabilityNotSupported)
    }
}

impl rushwind_script::ScriptWatcher for WasmEngine {
    fn start_watch<'a>(&'a self, _key: &'a str) -> BoxFuture<'a, Result<(), ScriptError>> {
        Box::pin(async { Err(ScriptError::CapabilityNotSupported) })
    }

    fn stop_watch(&self, _key: &str) -> Result<(), ScriptError> {
        Err(ScriptError::CapabilityNotSupported)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rushwind_script::MemSource;
    use std::sync::Arc;

    /// A minimal UTF-8-clean module exporting `()->()` as `_start`.
    const START_MODULE: &str = concat!(
        "\u{0}asm\u{1}\u{0}\u{0}\u{0}",
        "\u{1}\u{4}\u{1}`\u{0}\u{0}",
        "\u{3}\u{2}\u{1}\u{0}",
        "\u{7}\u{a}\u{1}\u{6}_start\u{0}\u{0}",
        "\u{a}\u{4}\u{1}\u{2}\u{0}\u{b}",
    );

    /// The same module with the export section dropped: valid, with a
    /// function body but no exports, so instantiation is silent.
    const SILENT_MODULE: &str = concat!(
        "\u{0}asm\u{1}\u{0}\u{0}\u{0}",
        "\u{1}\u{4}\u{1}`\u{0}\u{0}",
        "\u{3}\u{2}\u{1}\u{0}",
        "\u{a}\u{4}\u{1}\u{2}\u{0}\u{b}",
    );

    async fn engine() -> Arc<WasmEngine> {
        let engine = Arc::new(WasmEngine::new());
        engine.init().await.expect("init");
        engine
    }

    #[tokio::test]
    async fn a_start_module_runs_and_answers_null() {
        let engine = engine().await;
        engine
            .load_string("m", START_MODULE)
            .await
            .expect("compile");
        assert_eq!(engine.execute().await.expect("run"), ScriptValue::Null);
    }

    #[tokio::test]
    async fn a_module_without_start_instantiates_silently() {
        let engine = engine().await;
        engine
            .load_string("m", SILENT_MODULE)
            .await
            .expect("compile");
        assert_eq!(
            engine.execute().await.expect("silent run"),
            ScriptValue::Null
        );
    }

    #[tokio::test]
    async fn garbage_fails_to_compile() {
        let engine = engine().await;
        assert!(matches!(
            engine.load_string("m", "not wasm at all").await,
            Err(ScriptError::Failed(msg)) if msg.contains("compile failed")
        ));
        assert!(engine.last_error().is_some());
        engine.clear_error();
        assert!(engine.last_error().is_none());
    }

    #[tokio::test]
    async fn nothing_loaded_fails() {
        let engine = engine().await;
        assert!(matches!(
            engine.execute().await,
            Err(ScriptError::Failed(msg)) if msg.contains("no module loaded")
        ));
    }

    #[tokio::test]
    async fn without_init_everything_fails() {
        let engine = Arc::new(WasmEngine::new());
        assert!(!engine.is_initialized());
        assert!(engine.load_string("m", START_MODULE).await.is_err());
        assert!(engine.execute().await.is_err());
        engine.init().await.expect("init");
        engine.init().await.expect_err("second init fails");
        assert!(engine.is_initialized());
        engine.close().expect("close");
        assert!(!engine.is_initialized());
    }

    #[tokio::test]
    async fn sources_drive_the_load() {
        let engine = engine().await;
        let source = Arc::new(MemSource::new());
        source.set("mod", START_MODULE);
        engine.set_source(Some(source));
        assert!(engine.get_source().is_some());
        engine.load("mod").await.expect("source-driven compile");
        assert_eq!(engine.execute().await.expect("run"), ScriptValue::Null);
    }

    #[tokio::test]
    async fn probes_offer_loader_and_executor_only() {
        let engine: Arc<dyn ScriptEngine> = Arc::new(WasmEngine::new());
        assert!(engine.clone().as_loader().is_some());
        assert!(engine.clone().as_executor().is_some());
        assert!(engine.clone().as_global_accessor().is_none());
        assert!(engine.clone().as_function_registrar().is_none());
        assert!(engine.clone().as_module_registrar().is_none());
        assert!(engine.clone().as_watcher().is_none());
        assert!(engine.clone().as_sandbox_configurator().is_none());
        assert!(engine.clone().as_runtime_hook_registrar().is_none());
        assert!(engine.clone().as_sync_executor().is_none());
        assert!(engine.as_quota_controller().is_none());
    }
}
