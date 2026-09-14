//! The Starlark engine for the Rust script contract — the Go
//! predecessor's starlark-go engine, rebuilt over Meta's [`starlark`]
//! crate (starlark-rust).
//!
//! Semantics preserved from the predecessor:
//!
//! - `load`/`load_multi`/`load_string` queue (name, source) pairs;
//!   `execute` runs every queued script in order over one shared
//!   environment, later scripts seeing earlier scripts' globals;
//!   `execute_from_key`/`execute_string` run one fetched or inline
//!   script the same way. Results are discarded — every execution
//!   answers `Null`, the predecessor's `(nil, nil)`.
//! - Globals accumulate across runs. The predecessor kept a
//!   persistent `scriptGlobals` dict and merged it, under
//!   `hostPredeclared`, into each run's predeclared environment —
//!   script values overriding host ones on collision, its merge
//!   order. starlark-rust cannot carry values between its
//!   per-evaluation heaps without `unsafe` conversions, so the port
//!   accumulates every successfully run script — queued and ad-hoc
//!   alike — and re-runs the whole set on each evaluation,
//!   reproducing the accumulated environment observably. The frozen
//!   successor of the latest run backs `get_global`'s readback.
//! - `register_global` populates the host environment;
//!   `register_module` flattens its entries into `name_key` globals,
//!   the predecessor's gpython-style shape. `get_global` reads the
//!   frozen environment first, then the host map — the predecessor's
//!   scriptGlobals-then-hostPredeclared lookup order.
//! - `call_function` invokes a script-defined function — the
//!   accumulated script set defines it again in the run's module —
//!   with bridged arguments and a bridged result.
//! - `start_watch`/`stop_watch` requeue the watched key on change
//!   ticks — reload only, never execution, the predecessor's shape.
//!
//! Divergences from the Go predecessor:
//!
//! - `register_function` **always fails**: a runtime-registered host
//!   callable needs `NativeFunction`, which starlark-rust keeps
//!   `pub(crate)` — no public constructor for a native function from
//!   a closure exists. `call_function` on script-defined functions
//!   works as in the predecessor.
//! - Every evaluation re-runs the accumulated script set rather than
//!   carrying a globals dict forward: for deterministic scripts the
//!   resulting environment matches the predecessor exactly, but a
//!   script with side effects (output, mutation of host-injected
//!   data) re-runs those effects on every evaluation, where the
//!   predecessor ran it once. The Go engine's origin split —
//!   `scriptGlobals` versus `hostPredeclared`, each with its own
//!   precedence — is likewise unrecoverable; fresh host
//!   registrations land on top of the merged state each run.
//! - Integer globals clamp to Starlark's 32-bit range — the
//!   predecessor's arbitrary-precision integers have no counterpart —
//!   and byte strings bridge as lossy UTF-8, the predecessor's
//!   `string(byte-slice)` conversion.
//! - The bridge-out rides the value JSON serializer: data values
//!   cross exactly as they do inbound, and anything the serializer
//!   rejects (functions, types) bridges as `Null`, where the
//!   predecessor returned a description string.
//! - The predecessor's standard-library selection is fixed: the port
//!   evaluates against `Globals::standard()`, the crate's Starlark
//!   standard environment, for every run.
//!
//! # Engine matrix
//!
//! | Capability | Status |
//! |:---|:---|
//! | loader / executor / globals / modules / watch / lifecycle | implemented |
//! | host functions (`register_function`) | rejected — starlark-rust's native-function constructor is crate-private |
//! | sandbox / runtime hooks / sync executor / quota | not offered by the predecessor's engine either |

#![forbid(unsafe_code)]
#![deny(missing_docs)]

use std::collections::HashMap;
use std::sync::{Arc, Mutex, Weak};

use starlark::environment::{FrozenModule, Globals, Module};
use starlark::eval::Evaluator;
use starlark::syntax::{AstModule, Dialect};
use starlark::values::dict::AllocDict;
use starlark::values::list::AllocList;
use starlark::values::Value;
use tokio::task::AbortHandle;

use rushwind_script::{
    register_factory, BoxFuture, GlobalAccessor, ScriptEngine, ScriptError, ScriptExecutor,
    ScriptLoader, ScriptValue, ScriptWatcher, SharedEngine, SharedScriptSource,
};

/// The registry name — the Go `scriptEngine.StarlarkType` constant.
pub const NAME: &str = "starlark";

/// The `starlark` engine: [`starlark`] — starlark-rust — behind a
/// frozen-module accumulation of the script globals.
pub struct StarlarkEngine {
    initialized: Mutex<bool>,
    source: Mutex<Option<SharedScriptSource>>,
    scripts: Mutex<Vec<(String, String)>>,
    host_globals: Mutex<HashMap<String, ScriptValue>>,
    frozen_globals: Mutex<Option<FrozenModule>>,
    watchers: Mutex<HashMap<String, AbortHandle>>,
    weak: Mutex<Weak<Self>>,
    last_error: Mutex<Option<ScriptError>>,
}

impl StarlarkEngine {
    /// Builds the engine uninitialized; call [`ScriptEngine::init`]
    /// before use. Prefer [`factory`], which also arms the weak
    /// self-reference the watch tasks need.
    pub fn new() -> Self {
        Self {
            initialized: Mutex::new(false),
            source: Mutex::new(None),
            scripts: Mutex::new(Vec::new()),
            host_globals: Mutex::new(HashMap::new()),
            frozen_globals: Mutex::new(None),
            watchers: Mutex::new(HashMap::new()),
            weak: Mutex::new(Weak::new()),
            last_error: Mutex::new(None),
        }
    }

    fn set_last_error(&self, error: ScriptError) {
        *self
            .last_error
            .lock()
            .expect("starlark engine last-error lock") = Some(error);
    }

    fn clear_last_error(&self) {
        *self
            .last_error
            .lock()
            .expect("starlark engine last-error lock") = None;
    }

    fn guard_initialized(&self) -> Result<(), ScriptError> {
        if !*self.initialized.lock().expect("starlark engine init lock") {
            let err = ScriptError::Failed("starlark engine: not initialized".to_string());
            self.set_last_error(err.clone());
            return Err(err);
        }
        Ok(())
    }

    fn stop_all_watchers(&self) {
        let watchers: Vec<AbortHandle> = {
            let mut watchers = self.watchers.lock().expect("starlark engine watcher lock");
            watchers.drain().map(|(_, handle)| handle).collect()
        };
        for handle in watchers {
            handle.abort();
        }
    }

    /// The predecessor's source-fetch step — shared by
    /// [`ScriptLoader::load`] and
    /// [`ScriptExecutor::execute_from_key`], the Go
    /// `Load`/`ExecuteFromKey` fetch duplication.
    async fn load_code(&self, key: &str) -> Result<String, ScriptError> {
        self.guard_initialized()?;
        let source = self
            .source
            .lock()
            .expect("starlark engine source lock")
            .clone();
        let Some(source) = source else {
            let err = ScriptError::Failed("starlark engine: no source bound".to_string());
            self.set_last_error(err.clone());
            return Err(err);
        };
        match source.load(key).await {
            Ok(code) => Ok(code),
            Err(err) => {
                self.set_last_error(err.clone());
                Err(err)
            }
        }
    }

    /// The predecessor's predeclared merge, host side: the host
    /// registrations into the run's module. The script side — the Go
    /// engine's persistent `scriptGlobals` injection — is covered by
    /// the accumulated script set re-running; see the crate docs for
    /// that divergence.
    fn inject_globals<'v>(module: &Module<'v>, host: &HashMap<String, ScriptValue>) {
        for (name, value) in host {
            module.set(name, bridge::to_starlark(module, value));
        }
    }

    /// The predecessor's execution loop: every script runs in order
    /// over one module — later scripts seeing earlier scripts'
    /// globals — and the module's frozen successor, the readback
    /// snapshot, comes back. A failing script aborts the loop with
    /// nothing accumulated — the predecessor's abort-on-error shape.
    fn run_scripts(
        host: &HashMap<String, ScriptValue>,
        scripts: &[(String, String)],
    ) -> Result<FrozenModule, ScriptError> {
        Module::with_temp_heap(|module| {
            Self::inject_globals(&module, host);
            let globals = Globals::standard();
            {
                let mut eval = Evaluator::new(&module);
                for (name, src) in scripts {
                    let ast =
                        AstModule::parse(name, src.clone(), &Dialect::Standard).map_err(|err| {
                            ScriptError::Failed(format!(
                                "starlark engine: exec failed: {name}: {err}"
                            ))
                        })?;
                    eval.eval_module(ast, &globals).map_err(|err| {
                        ScriptError::Failed(format!("starlark engine: exec failed: {name}: {err}"))
                    })?;
                }
            }
            module
                .freeze()
                .map_err(|_| ScriptError::Failed("starlark engine: freeze failed".to_string()))
        })
    }

    /// Runs the accumulated script set plus `extra`, stores the
    /// frozen successor for readback, and — on success — folds
    /// `extra` into the accumulated set: the predecessor's
    /// accumulate-then-clear-errors tail, answering `Null` for the
    /// run itself.
    fn run_and_store(&self, extra: &[(String, String)]) -> Result<ScriptValue, ScriptError> {
        let mut scripts = self
            .scripts
            .lock()
            .expect("starlark engine script lock")
            .clone();
        scripts.extend_from_slice(extra);
        let host = self
            .host_globals
            .lock()
            .expect("starlark engine host-global lock")
            .clone();
        let outcome = Self::run_scripts(&host, &scripts);
        let frozen = match outcome {
            Ok(frozen) => frozen,
            Err(err) => {
                self.set_last_error(err.clone());
                return Err(err);
            }
        };
        if !extra.is_empty() {
            self.scripts
                .lock()
                .expect("starlark engine script lock")
                .extend_from_slice(extra);
        }
        *self
            .frozen_globals
            .lock()
            .expect("starlark engine frozen-global lock") = Some(frozen);
        self.clear_last_error();
        Ok(ScriptValue::Null)
    }
}

impl Default for StarlarkEngine {
    fn default() -> Self {
        Self::new()
    }
}

impl ScriptEngine for StarlarkEngine {
    fn engine_type(&self) -> &'static str {
        NAME
    }

    fn init(&self) -> BoxFuture<'_, Result<(), ScriptError>> {
        Box::pin(async move {
            {
                let mut initialized = self.initialized.lock().expect("starlark engine init lock");
                if *initialized {
                    let err =
                        ScriptError::Failed("starlark engine: already initialized".to_string());
                    self.set_last_error(err.clone());
                    return Err(err);
                }
                *initialized = true;
            }
            // The predecessor's Init resets the whole engine state.
            self.scripts
                .lock()
                .expect("starlark engine script lock")
                .clear();
            self.host_globals
                .lock()
                .expect("starlark engine host-global lock")
                .clear();
            *self
                .frozen_globals
                .lock()
                .expect("starlark engine frozen-global lock") = None;
            self.clear_last_error();
            Ok(())
        })
    }

    fn close(&self) -> Result<(), ScriptError> {
        {
            let mut initialized = self.initialized.lock().expect("starlark engine init lock");
            if !*initialized {
                let err = ScriptError::Failed("starlark engine: not initialized".to_string());
                self.set_last_error(err.clone());
                return Err(err);
            }
            *initialized = false;
        }
        self.stop_all_watchers();
        *self.source.lock().expect("starlark engine source lock") = None;
        self.scripts
            .lock()
            .expect("starlark engine script lock")
            .clear();
        self.host_globals
            .lock()
            .expect("starlark engine host-global lock")
            .clear();
        *self
            .frozen_globals
            .lock()
            .expect("starlark engine frozen-global lock") = None;
        self.clear_last_error();
        Ok(())
    }

    fn is_initialized(&self) -> bool {
        *self.initialized.lock().expect("starlark engine init lock")
    }

    fn last_error(&self) -> Option<ScriptError> {
        self.last_error
            .lock()
            .expect("starlark engine last-error lock")
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

    fn as_global_accessor(self: Arc<Self>) -> Option<Arc<dyn GlobalAccessor>> {
        Some(self)
    }

    fn as_function_registrar(
        self: Arc<Self>,
    ) -> Option<Arc<dyn rushwind_script::FunctionRegistrar>> {
        Some(self)
    }

    fn as_module_registrar(self: Arc<Self>) -> Option<Arc<dyn rushwind_script::ModuleRegistrar>> {
        Some(self)
    }

    fn as_watcher(self: Arc<Self>) -> Option<Arc<dyn ScriptWatcher>> {
        Some(self)
    }
}

/// The value bridge: contract values and Starlark values meeting over
/// the crate's allocation and JSON-serialization surfaces — the Go
/// predecessor's `goToStarlark`/`starlarkToGo` over the data model.
mod bridge {
    use super::*;

    /// Converts a JSON value back into a contract value.
    pub fn from_json(value: serde_json::Value) -> ScriptValue {
        match value {
            serde_json::Value::Null => ScriptValue::Null,
            serde_json::Value::Bool(v) => ScriptValue::Bool(v),
            serde_json::Value::Number(v) => {
                if let Some(i) = v.as_i64() {
                    ScriptValue::Int(i)
                } else if let Some(u) = v.as_u64() {
                    ScriptValue::UInt(u)
                } else if let Some(f) = v.as_f64() {
                    ScriptValue::Float(f)
                } else {
                    ScriptValue::Null
                }
            }
            serde_json::Value::String(v) => ScriptValue::String(v),
            serde_json::Value::Array(items) => {
                ScriptValue::Array(items.into_iter().map(from_json).collect())
            }
            serde_json::Value::Object(map) => {
                ScriptValue::Map(map.into_iter().map(|(k, v)| (k, from_json(v))).collect())
            }
        }
    }

    /// Clamps a contract integer into Starlark's 32-bit integer
    /// range — the documented divergence from the predecessor's
    /// arbitrary-precision integers.
    fn clamp_int(value: i128) -> i32 {
        value.clamp(i32::MIN as i128, i32::MAX as i128) as i32
    }

    /// Bridges a contract value into a Starlark value on the module's
    /// heap. Integers clamp to 32 bits; byte strings bridge as lossy
    /// UTF-8, the predecessor's `string(byte-slice)` conversion.
    pub fn to_starlark<'v>(module: &Module<'v>, value: &ScriptValue) -> Value<'v> {
        match value {
            ScriptValue::Null => Value::new_none(),
            ScriptValue::Bool(v) => module.heap().alloc(*v),
            ScriptValue::Int(v) => module.heap().alloc(clamp_int(*v as i128)),
            ScriptValue::UInt(v) => module.heap().alloc(clamp_int(*v as i128)),
            ScriptValue::Float(v) => module.heap().alloc(*v),
            ScriptValue::String(v) => module.heap().alloc(v.as_str()),
            ScriptValue::Bytes(v) => {
                let lossy = String::from_utf8_lossy(v);
                module.heap().alloc(lossy.as_ref())
            }
            ScriptValue::Array(items) => module.heap().alloc(AllocList(
                items
                    .iter()
                    .map(|item| to_starlark(module, item))
                    .collect::<Vec<Value>>(),
            )),
            ScriptValue::Map(map) => module.heap().alloc(AllocDict(
                map.iter()
                    .map(|(k, v)| {
                        (
                            to_starlark(module, &ScriptValue::String(k.clone())),
                            to_starlark(module, v),
                        )
                    })
                    .collect::<Vec<(Value, Value)>>(),
            )),
            _ => Value::new_none(),
        }
    }

    /// Bridges a Starlark value back through the value JSON
    /// serializer — the predecessor's `starlarkToGo` over the data
    /// model. Whatever the serializer rejects — functions, types —
    /// bridges as `Null`, where the predecessor returned a
    /// description string.
    pub fn from_starlark(value: Value<'_>) -> ScriptValue {
        match value.to_json_value() {
            Ok(json) => from_json(json),
            Err(_) => ScriptValue::Null,
        }
    }
}

impl ScriptLoader for StarlarkEngine {
    fn set_source(&self, source: Option<SharedScriptSource>) {
        *self.source.lock().expect("starlark engine source lock") = source;
    }

    fn get_source(&self) -> Option<SharedScriptSource> {
        self.source
            .lock()
            .expect("starlark engine source lock")
            .clone()
    }

    fn load<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<(), ScriptError>> {
        Box::pin(async move {
            // The predecessor's Load: fetch, then queue — LoadString's
            // append step.
            let code = self.load_code(key).await?;
            self.scripts
                .lock()
                .expect("starlark engine script lock")
                .push((key.to_string(), code));
            self.clear_last_error();
            Ok(())
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
        name: &'a str,
        code: &'a str,
    ) -> BoxFuture<'a, Result<(), ScriptError>> {
        let outcome = (|| {
            self.guard_initialized()?;
            self.scripts
                .lock()
                .expect("starlark engine script lock")
                .push((name.to_string(), code.to_string()));
            self.clear_last_error();
            Ok(())
        })();
        Box::pin(async move { outcome })
    }
}

impl ScriptExecutor for StarlarkEngine {
    fn execute(&self) -> BoxFuture<'_, Result<ScriptValue, ScriptError>> {
        Box::pin(async move {
            self.guard_initialized()?;
            let scripts = self
                .scripts
                .lock()
                .expect("starlark engine script lock")
                .clone();
            if scripts.is_empty() {
                let err = ScriptError::Failed("starlark engine: no script loaded".to_string());
                self.set_last_error(err.clone());
                return Err(err);
            }
            self.run_and_store(&[])
        })
    }

    /// The predecessor's from-key shape: the fetched script runs
    /// alone, its globals accumulating like any other run.
    fn execute_from_key<'a>(
        &'a self,
        key: &'a str,
    ) -> BoxFuture<'a, Result<ScriptValue, ScriptError>> {
        Box::pin(async move {
            self.guard_initialized()?;
            let code = self.load_code(key).await?;
            let adhoc = [(key.to_string(), code)];
            self.run_and_store(&adhoc)
        })
    }

    fn execute_from_keys<'a>(
        &'a self,
        keys: &'a [String],
    ) -> BoxFuture<'a, Result<Vec<ScriptValue>, ScriptError>> {
        Box::pin(async move {
            let mut results = Vec::with_capacity(keys.len());
            for key in keys {
                results.push(self.execute_from_key(key).await?);
            }
            Ok(results)
        })
    }

    fn execute_string<'a>(
        &'a self,
        name: &'a str,
        code: &'a str,
    ) -> BoxFuture<'a, Result<ScriptValue, ScriptError>> {
        Box::pin(async move {
            self.guard_initialized()?;
            let adhoc = [(name.to_string(), code.to_string())];
            self.run_and_store(&adhoc)
        })
    }
}

impl GlobalAccessor for StarlarkEngine {
    fn register_global(&self, name: &str, value: ScriptValue) -> Result<(), ScriptError> {
        self.guard_initialized()?;
        self.host_globals
            .lock()
            .expect("starlark engine host-global lock")
            .insert(name.to_string(), value);
        self.clear_last_error();
        Ok(())
    }

    fn get_global(&self, name: &str) -> Result<ScriptValue, ScriptError> {
        self.guard_initialized()?;
        // The frozen environment first, then the host map — the
        // predecessor's scriptGlobals-then-hostPredeclared order.
        let frozen = self
            .frozen_globals
            .lock()
            .expect("starlark engine frozen-global lock")
            .clone();
        if let Some(frozen) = frozen {
            if let Ok(Some(value)) = frozen.get_option(name) {
                let bridged = bridge::from_starlark(value.value());
                self.clear_last_error();
                return Ok(bridged);
            }
        }
        let host = self
            .host_globals
            .lock()
            .expect("starlark engine host-global lock")
            .get(name)
            .cloned();
        if let Some(value) = host {
            self.clear_last_error();
            return Ok(value);
        }
        let err = ScriptError::Failed(format!("starlark engine: global not found: {name}"));
        self.set_last_error(err.clone());
        Err(err)
    }
}

impl rushwind_script::FunctionRegistrar for StarlarkEngine {
    /// Always fails — see the crate docs: starlark-rust keeps its
    /// native-function constructor crate-private, so a runtime host
    /// callable cannot be built.
    fn register_function(
        &self,
        _name: &str,
        _function: rushwind_script::HostFunction,
    ) -> Result<(), ScriptError> {
        let err = ScriptError::Failed(
            "starlark engine: host functions are unavailable — starlark-rust's native-function constructor is crate-private"
                .to_string(),
        );
        self.set_last_error(err.clone());
        Err(err)
    }

    fn call_function<'a>(
        &'a self,
        name: &'a str,
        args: &'a [ScriptValue],
    ) -> BoxFuture<'a, Result<ScriptValue, ScriptError>> {
        Box::pin(async move {
            self.guard_initialized()?;
            let scripts = self
                .scripts
                .lock()
                .expect("starlark engine script lock")
                .clone();
            let host = self
                .host_globals
                .lock()
                .expect("starlark engine host-global lock")
                .clone();
            let outcome = Module::with_temp_heap(|module| {
                Self::inject_globals(&module, &host);
                // The accumulated script set defines the function
                // again in this run's module.
                {
                    let globals = Globals::standard();
                    let mut eval = Evaluator::new(&module);
                    for (script_name, src) in &scripts {
                        let ast = AstModule::parse(script_name, src.clone(), &Dialect::Standard)
                            .map_err(|err| {
                                ScriptError::Failed(format!(
                                    "starlark engine: exec failed: {script_name}: {err}"
                                ))
                            })?;
                        eval.eval_module(ast, &globals).map_err(|err| {
                            ScriptError::Failed(format!(
                                "starlark engine: exec failed: {script_name}: {err}"
                            ))
                        })?;
                    }
                }
                let Some(function) = module.get(name) else {
                    return Err(ScriptError::Failed(format!(
                        "starlark engine: function not found: {name}"
                    )));
                };
                let starlark_args: Vec<_> = args
                    .iter()
                    .map(|arg| bridge::to_starlark(&module, arg))
                    .collect();
                let mut eval = Evaluator::new(&module);
                let result = eval
                    .eval_function(function, &starlark_args, &[])
                    .map_err(|err| {
                        ScriptError::Failed(format!("starlark engine: call failed: {name}: {err}"))
                    })?;
                Ok(bridge::from_starlark(result))
            });
            match outcome {
                Ok(value) => {
                    self.clear_last_error();
                    Ok(value)
                }
                Err(err) => {
                    self.set_last_error(err.clone());
                    Err(err)
                }
            }
        })
    }
}

impl rushwind_script::ModuleRegistrar for StarlarkEngine {
    /// The predecessor's gpython-style flattening: every module entry
    /// becomes a `name_key` host global.
    fn register_module(&self, name: &str, module: ScriptValue) -> Result<(), ScriptError> {
        self.guard_initialized()?;
        let ScriptValue::Map(map) = module else {
            let err = ScriptError::Failed("starlark engine: module must be a map".to_string());
            self.set_last_error(err.clone());
            return Err(err);
        };
        {
            let mut host = self
                .host_globals
                .lock()
                .expect("starlark engine host-global lock");
            for (key, value) in map {
                host.insert(format!("{name}_{key}"), value);
            }
        }
        self.clear_last_error();
        Ok(())
    }
}

impl ScriptWatcher for StarlarkEngine {
    fn start_watch<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<(), ScriptError>> {
        Box::pin(async move {
            self.guard_initialized()?;
            let source = self
                .source
                .lock()
                .expect("starlark engine source lock")
                .clone();
            let Some(source) = source else {
                let err = ScriptError::Failed("starlark engine: no source bound".to_string());
                self.set_last_error(err.clone());
                return Err(err);
            };
            let stream = match source.watch(key).await {
                Ok(stream) => stream,
                Err(err) => {
                    self.set_last_error(err.clone());
                    return Err(err);
                }
            };
            let weak = self.weak.lock().expect("starlark engine weak lock").clone();
            if weak.upgrade().is_none() {
                let err =
                    ScriptError::Failed("starlark engine: engine handle unavailable".to_string());
                self.set_last_error(err.clone());
                return Err(err);
            }
            let key_owned = key.to_string();
            let handle = tokio::spawn(async move {
                let mut stream = stream;
                loop {
                    if stream.next().await.is_none() {
                        break;
                    }
                    let Some(engine) = weak.upgrade() else {
                        break;
                    };
                    let _ = engine.load(&key_owned).await;
                }
            })
            .abort_handle();
            self.watchers
                .lock()
                .expect("starlark engine watcher lock")
                .insert(key.to_string(), handle);
            self.clear_last_error();
            Ok(())
        })
    }

    fn stop_watch(&self, key: &str) -> Result<(), ScriptError> {
        let handle = self
            .watchers
            .lock()
            .expect("starlark engine watcher lock")
            .remove(key);
        if let Some(handle) = handle {
            handle.abort();
        }
        Ok(())
    }
}

/// Builds an engine, arming the weak self-reference the watch tasks
/// need. The Go predecessor registered this under its Starlark type
/// through package `init()`; [`register`] is the explicit Rust form.
pub fn factory() -> Result<SharedEngine, ScriptError> {
    let engine = Arc::new(StarlarkEngine::new());
    *engine.weak.lock().expect("starlark engine weak lock") = Arc::downgrade(&engine);
    Ok(engine)
}

/// Installs the engine factory in the registry under [`NAME`]. Call
/// once at startup.
pub fn register() {
    let factory_fn: rushwind_script::EngineFactory = Arc::new(factory);
    let _ = register_factory(NAME, factory_fn);
}

#[cfg(test)]
mod tests {
    use super::*;
    use rushwind_script::MemSource;
    use std::sync::Arc;

    async fn engine() -> SharedEngine {
        let engine = factory().expect("engine");
        engine.init().await.expect("init");
        engine
    }

    #[tokio::test]
    async fn the_engine_type_is_starlark() {
        let engine = engine().await;
        assert_eq!(engine.engine_type(), NAME);
    }

    #[tokio::test]
    async fn init_and_close_lifecycle() {
        let engine = factory().expect("engine");
        assert!(!engine.is_initialized());
        assert!(engine.execute().await.is_err());
        engine.init().await.expect("init");
        assert!(engine.is_initialized());
        assert!(engine.init().await.is_err());
        engine.close().expect("close");
        assert!(!engine.is_initialized());
        assert!(engine.close().is_err());
    }

    #[tokio::test]
    async fn operations_before_init_fail() {
        let engine = factory().expect("engine");
        assert!(engine.load("k").await.is_err());
        assert!(engine.load_string("k", "x = 1\n").await.is_err());
        assert!(engine.execute().await.is_err());
        assert!(engine.execute_from_key("k").await.is_err());
        assert!(engine.execute_string("k", "x = 1\n").await.is_err());
        assert!(engine.register_global("x", ScriptValue::Int(1)).is_err());
        assert!(engine.get_global("x").is_err());
        assert!(engine
            .register_module("m", ScriptValue::Map(HashMap::new()))
            .is_err());
        assert!(engine.call_function("f", &[]).await.is_err());
        assert!(engine.start_watch("k").await.is_err());
    }

    #[tokio::test]
    async fn sources_bind_and_unbind() {
        let engine = engine().await;
        let source = Arc::new(MemSource::new());
        engine.set_source(Some(source.clone()));
        assert!(engine.get_source().is_some());
        engine.set_source(None);
        assert!(engine.get_source().is_none());
    }

    #[tokio::test]
    async fn load_without_a_bound_source_fails() {
        let engine = engine().await;
        assert!(engine.load("script.star").await.is_err());
    }

    #[tokio::test]
    async fn execute_from_key_without_a_bound_source_fails() {
        let engine = engine().await;
        assert!(engine.execute_from_key("script.star").await.is_err());
    }

    #[tokio::test]
    async fn arithmetic_assignments_reach_the_frozen_environment() {
        let engine = engine().await;
        engine
            .execute_string("test", "x = 1 + 2\n")
            .await
            .expect("exec");
        assert_eq!(engine.get_global("x").expect("x"), ScriptValue::Int(3));
    }

    #[tokio::test]
    async fn string_concatenation_assignments_read_back() {
        let engine = engine().await;
        engine
            .execute_string("test", "s = \"hello\" + \" \" + \"world\"\n")
            .await
            .expect("exec");
        assert_eq!(
            engine.get_global("s").expect("s"),
            ScriptValue::String("hello world".to_string())
        );
    }

    #[tokio::test]
    async fn boolean_assignments_read_back() {
        let engine = engine().await;
        engine
            .execute_string("test", "b = True and False\n")
            .await
            .expect("exec");
        assert_eq!(engine.get_global("b").expect("b"), ScriptValue::Bool(false));
    }

    #[tokio::test]
    async fn script_functions_call_with_bridged_args() {
        let engine = engine().await;
        engine
            .execute_string("test", "def add(a, b):\n    return a + b\n")
            .await
            .expect("exec");
        assert_eq!(
            engine
                .call_function("add", &[ScriptValue::Int(3), ScriptValue::Int(4)])
                .await
                .expect("call"),
            ScriptValue::Int(7)
        );
    }

    #[tokio::test]
    async fn script_functions_of_two_args_multiply() {
        let engine = engine().await;
        engine
            .execute_string("test", "def multiply(a, b):\n    return a * b\n")
            .await
            .expect("exec");
        assert_eq!(
            engine
                .call_function("multiply", &[ScriptValue::Int(6), ScriptValue::Int(7)])
                .await
                .expect("call"),
            ScriptValue::Int(42)
        );
    }

    #[tokio::test]
    async fn broken_syntax_fails() {
        let engine = engine().await;
        assert!(matches!(
            engine.execute_string("test", "def broken(\n").await,
            Err(ScriptError::Failed(msg)) if msg.contains("starlark engine")
        ));
    }

    #[tokio::test]
    async fn nothing_loaded_fails() {
        let engine = engine().await;
        assert!(matches!(
            engine.execute().await,
            Err(ScriptError::Failed(msg)) if msg.contains("no script loaded")
        ));
    }

    #[tokio::test]
    async fn queued_scripts_share_globals_in_one_run() {
        let engine = engine().await;
        engine.load_string("a", "a = 10\n").await.expect("load a");
        engine
            .load_string("b", "b = a + 20\n")
            .await
            .expect("load b");
        engine.execute().await.expect("execute");
        assert_eq!(engine.get_global("b").expect("b"), ScriptValue::Int(30));
    }

    #[tokio::test]
    async fn runtime_errors_fail() {
        let engine = engine().await;
        assert!(matches!(
            engine
                .execute_string("test", "x = [1, 2, 3]\ny = x[10]\n")
                .await,
            Err(ScriptError::Failed(msg)) if msg.contains("starlark engine")
        ));
    }

    #[tokio::test]
    async fn multi_statement_scripts_assign() {
        let engine = engine().await;
        engine
            .execute_string("test", "x = 10\ny = 20\nz = x + y\n")
            .await
            .expect("exec");
        assert_eq!(engine.get_global("z").expect("z"), ScriptValue::Int(30));
    }

    #[tokio::test]
    async fn host_integer_globals_feed_scripts() {
        let engine = engine().await;
        engine
            .register_global("x", ScriptValue::Int(42))
            .expect("register");
        engine
            .execute_string("test", "y = x + 8\n")
            .await
            .expect("exec");
        assert_eq!(engine.get_global("y").expect("y"), ScriptValue::Int(50));
    }

    #[tokio::test]
    async fn host_string_globals_feed_scripts() {
        let engine = engine().await;
        engine
            .register_global("name", ScriptValue::String("Alice".to_string()))
            .expect("register");
        engine
            .execute_string("test", "greeting = \"Hello, \" + name\n")
            .await
            .expect("exec");
        assert_eq!(
            engine.get_global("greeting").expect("greeting"),
            ScriptValue::String("Hello, Alice".to_string())
        );
    }

    #[tokio::test]
    async fn host_boolean_globals_feed_scripts() {
        let engine = engine().await;
        engine
            .register_global("flag", ScriptValue::Bool(true))
            .expect("register");
        engine
            .execute_string("test", "result = flag and True\n")
            .await
            .expect("exec");
        assert_eq!(
            engine.get_global("result").expect("result"),
            ScriptValue::Bool(true)
        );
    }

    #[tokio::test]
    async fn host_global_registration_overwrites() {
        let engine = engine().await;
        engine
            .register_global("x", ScriptValue::Int(10))
            .expect("register");
        engine
            .register_global("x", ScriptValue::Int(99))
            .expect("re-register");
        assert_eq!(engine.get_global("x").expect("x"), ScriptValue::Int(99));
    }

    #[tokio::test]
    async fn host_floats_round_trip_through_scripts() {
        let engine = engine().await;
        engine
            .register_global("pi", ScriptValue::Float(2.5))
            .expect("register");
        engine
            .execute_string("test", "twice = pi * 2.0\n")
            .await
            .expect("exec");
        assert_eq!(
            engine.get_global("twice").expect("twice"),
            ScriptValue::Float(5.0)
        );
    }

    #[tokio::test]
    async fn host_globals_read_back_directly() {
        let engine = engine().await;
        engine
            .register_global("x", ScriptValue::Int(42))
            .expect("register");
        assert_eq!(engine.get_global("x").expect("x"), ScriptValue::Int(42));
    }

    #[tokio::test]
    async fn missing_globals_fail() {
        let engine = engine().await;
        assert!(matches!(
            engine.get_global("nonexistent"),
            Err(ScriptError::Failed(msg)) if msg.contains("starlark engine")
        ));
    }

    #[tokio::test]
    async fn script_defined_globals_read_back() {
        let engine = engine().await;
        engine
            .execute_string("test", "counter = 100\nname = \"test\"\n")
            .await
            .expect("exec");
        assert_eq!(
            engine.get_global("counter").expect("counter"),
            ScriptValue::Int(100)
        );
        assert_eq!(
            engine.get_global("name").expect("name"),
            ScriptValue::String("test".to_string())
        );
    }

    #[tokio::test]
    async fn host_functions_are_rejected() {
        let engine = engine().await;
        let host: rushwind_script::HostFunction = Arc::new(
            |_args: &[ScriptValue]| -> BoxFuture<'static, Result<ScriptValue, ScriptError>> {
                Box::pin(async { Ok(ScriptValue::Null) })
            },
        );
        assert!(matches!(
            engine.register_function("h", host),
            Err(ScriptError::Failed(msg)) if msg.contains("starlark engine")
        ));
    }

    #[tokio::test]
    async fn call_function_on_missing_names_fails() {
        let engine = engine().await;
        assert!(matches!(
            engine.call_function("nonexistent", &[]).await,
            Err(ScriptError::Failed(msg)) if msg.contains("starlark engine")
        ));
    }

    #[tokio::test]
    async fn module_registration_flattens_entries() {
        let engine = engine().await;
        let mut map = HashMap::new();
        map.insert("timeout".to_string(), ScriptValue::Int(30));
        map.insert("name".to_string(), ScriptValue::String("myapp".to_string()));
        engine
            .register_module("config", ScriptValue::Map(map))
            .expect("register module");
        assert_eq!(
            engine.get_global("config_timeout").expect("timeout"),
            ScriptValue::Int(30)
        );
        assert_eq!(
            engine.get_global("config_name").expect("name"),
            ScriptValue::String("myapp".to_string())
        );
    }

    #[tokio::test]
    async fn module_registration_rejects_non_maps() {
        let engine = engine().await;
        assert!(matches!(
            engine.register_module("bad", ScriptValue::Int(1)),
            Err(ScriptError::Failed(msg)) if msg.contains("starlark engine")
        ));
    }

    #[tokio::test]
    async fn source_driven_loads_execute() {
        let engine = engine().await;
        let source = Arc::new(MemSource::new());
        source.set("script1.star", "x = 1 + 2\n");
        engine.set_source(Some(source));
        engine.load("script1.star").await.expect("load");
        engine.execute().await.expect("execute");
        assert_eq!(engine.get_global("x").expect("x"), ScriptValue::Int(3));
    }

    #[tokio::test]
    async fn load_multi_runs_in_order() {
        let engine = engine().await;
        let source = Arc::new(MemSource::new());
        source.set("a.star", "a = 10\n");
        source.set("b.star", "b = a + 20\n");
        engine.set_source(Some(source));
        engine
            .load_multi(&["a.star".to_string(), "b.star".to_string()])
            .await
            .expect("load multi");
        engine.execute().await.expect("execute");
        assert_eq!(engine.get_global("b").expect("b"), ScriptValue::Int(30));
    }

    #[tokio::test]
    async fn load_multi_aborts_on_missing_keys() {
        let engine = engine().await;
        let source = Arc::new(MemSource::new());
        source.set("a.star", "x = 1\n");
        engine.set_source(Some(source));
        assert!(engine
            .load_multi(&["a.star".to_string(), "missing.star".to_string()])
            .await
            .is_err());
    }

    #[tokio::test]
    async fn execute_from_key_runs_the_fetched_script() {
        let engine = engine().await;
        let source = Arc::new(MemSource::new());
        source.set("expr.star", "x = 5 * 6\n");
        engine.set_source(Some(source));
        engine.execute_from_key("expr.star").await.expect("exec");
        assert_eq!(engine.get_global("x").expect("x"), ScriptValue::Int(30));
    }

    #[tokio::test]
    async fn execute_from_keys_answers_null_per_key() {
        let engine = engine().await;
        let source = Arc::new(MemSource::new());
        source.set("a.star", "a = 10\n");
        source.set("b.star", "b = 20\n");
        engine.set_source(Some(source));
        let results = engine
            .execute_from_keys(&["a.star".to_string(), "b.star".to_string()])
            .await
            .expect("execute from keys");
        assert_eq!(results, vec![ScriptValue::Null, ScriptValue::Null]);
    }

    #[tokio::test]
    async fn list_iteration_language_features() {
        let engine = engine().await;
        engine
            .execute_string(
                "test",
                "def calc_total():\n    items = [1, 2, 3, 4, 5]\n    total = 0\n    for i in items:\n        total = total + i\n    return total\n\ntotal = calc_total()\n",
            )
            .await
            .expect("exec");
        assert_eq!(
            engine.get_global("total").expect("total"),
            ScriptValue::Int(15)
        );
    }

    #[tokio::test]
    async fn dict_indexing_language_features() {
        let engine = engine().await;
        engine
            .execute_string(
                "test",
                "d = {\"a\": 1, \"b\": 2}\nresult = d[\"a\"] + d[\"b\"]\n",
            )
            .await
            .expect("exec");
        assert_eq!(
            engine.get_global("result").expect("result"),
            ScriptValue::Int(3)
        );
    }

    #[tokio::test]
    async fn if_else_language_features() {
        let engine = engine().await;
        engine
            .execute_string(
                "test",
                "def check(x):\n    if x > 5:\n        return \"big\"\n    else:\n        return \"small\"\n\nresult = check(10)\n",
            )
            .await
            .expect("exec");
        assert_eq!(
            engine.get_global("result").expect("result"),
            ScriptValue::String("big".to_string())
        );
    }

    #[tokio::test]
    async fn for_loop_language_features() {
        let engine = engine().await;
        engine
            .execute_string(
                "test",
                "def calc_sum():\n    total = 0\n    for i in range(10):\n        total = total + i\n    return total\n\ntotal = calc_sum()\n",
            )
            .await
            .expect("exec");
        assert_eq!(
            engine.get_global("total").expect("total"),
            ScriptValue::Int(45)
        );
    }

    #[tokio::test]
    async fn comprehensions_read_back_as_arrays() {
        let engine = engine().await;
        engine
            .execute_string("test", "squares = [x * x for x in range(5)]\n")
            .await
            .expect("exec");
        assert_eq!(
            engine.get_global("squares").expect("squares"),
            ScriptValue::Array(vec![
                ScriptValue::Int(0),
                ScriptValue::Int(1),
                ScriptValue::Int(4),
                ScriptValue::Int(9),
                ScriptValue::Int(16)
            ])
        );
    }

    #[tokio::test]
    async fn nested_functions_close_over_their_environment() {
        let engine = engine().await;
        engine
            .execute_string(
                "test",
                "def make_multiplier(n):\n    def multiply(x):\n        return x * n\n    return multiply\n\ntriple = make_multiplier(3)\nresult = triple(14)\n",
            )
            .await
            .expect("exec");
        assert_eq!(
            engine.get_global("result").expect("result"),
            ScriptValue::Int(42)
        );
    }

    #[tokio::test]
    async fn last_error_records_and_clears() {
        let engine = engine().await;
        assert!(engine.last_error().is_none());
        let _ = engine.execute_string("bad.star", "x = 1 +\n").await;
        assert!(engine.last_error().is_some());
        engine.clear_error();
        assert!(engine.last_error().is_none());
    }

    #[tokio::test]
    async fn watch_requeues_the_changed_script() {
        let engine = engine().await;
        let source = Arc::new(MemSource::new());
        source.set("watch_script.star", "x = 1\n");
        engine.set_source(Some(source.clone()));
        engine.load("watch_script.star").await.expect("load");
        engine
            .start_watch("watch_script.star")
            .await
            .expect("watch");
        source.set("watch_script.star", "x = 2\n");
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        // The watch task requeued the changed script; execution now
        // runs both copies and the later assignment wins.
        engine.execute().await.expect("execute");
        assert_eq!(engine.get_global("x").expect("x"), ScriptValue::Int(2));
        engine.stop_watch("watch_script.star").expect("stop watch");
    }

    #[tokio::test]
    async fn watch_without_a_bound_source_fails() {
        let engine = engine().await;
        assert!(engine.start_watch("script.star").await.is_err());
    }

    #[tokio::test]
    async fn watch_on_non_watcher_sources_fails() {
        struct BareSource;
        impl rushwind_script::ScriptSource for BareSource {
            fn load<'a>(&'a self, _key: &'a str) -> BoxFuture<'a, Result<String, ScriptError>> {
                Box::pin(async { Err(ScriptError::Failed("bare source".to_string())) })
            }
        }
        let engine = engine().await;
        engine.set_source(Some(Arc::new(BareSource)));
        assert!(engine.start_watch("script.star").await.is_err());
    }

    #[tokio::test]
    async fn stop_watch_without_registration_is_a_no_op() {
        let engine = engine().await;
        engine.stop_watch("nonexistent").expect("stop watch");
    }

    #[tokio::test]
    async fn concurrent_executions_serialize() {
        let engine = engine().await;
        engine
            .load_string("expr", "x = 1 + 2\n")
            .await
            .expect("load");
        let mut tasks = tokio::task::JoinSet::new();
        for _ in 0..5 {
            let engine = engine.clone();
            tasks.spawn(async move { engine.execute().await });
        }
        let mut outcomes = Vec::new();
        while let Some(joined) = tasks.join_next().await {
            outcomes.push(joined.expect("joined").expect("execution"));
        }
        assert_eq!(outcomes.len(), 5);
        for outcome in outcomes {
            assert_eq!(outcome, ScriptValue::Null);
        }
    }

    #[tokio::test]
    async fn probes_offer_the_predecessor_capability_set() {
        let engine: Arc<dyn ScriptEngine> = factory().expect("engine");
        assert!(engine.clone().as_loader().is_some());
        assert!(engine.clone().as_executor().is_some());
        assert!(engine.clone().as_global_accessor().is_some());
        assert!(engine.clone().as_function_registrar().is_some());
        assert!(engine.clone().as_module_registrar().is_some());
        assert!(engine.as_watcher().is_some());
    }

    #[tokio::test]
    async fn probes_refuse_the_predecessor_absent_capabilities() {
        let engine: Arc<dyn ScriptEngine> = factory().expect("engine");
        assert!(engine.clone().as_sandbox_configurator().is_none());
        assert!(engine.clone().as_runtime_hook_registrar().is_none());
        assert!(engine.clone().as_sync_executor().is_none());
        assert!(engine.as_quota_controller().is_none());
    }
}
