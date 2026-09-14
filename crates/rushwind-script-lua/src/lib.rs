//! The Lua engine for the Rust script contract — the Go predecessor's
//! gopher-lua engine, rebuilt over [`mlua`] with its vendored Lua 5.4.
//!
//! Semantics preserved from the predecessor:
//!
//! - The sandbox allow-list: [`set_open_libs`] records library names
//!   and [`init`] builds the [`mlua::Lua`] instance over exactly that
//!   set; an empty list is the predecessor's "full standard set"
//!   default, rendered as [`StdLib::ALL_SAFE`] because mlua's safe
//!   constructor refuses the `debug` and `ffi` libraries the
//!   predecessor's full set included. The gopher-lua names map onto
//!   [`mlua::StdLib`] bits (`base` has no mlua counterpart — the base
//!   library is always present — and `channel` does not exist in Lua
//!   5.4; both are skipped).
//! - `execute` and `execute_string` run every loaded chunk and
//!   **discard the results** — the predecessor returned nil for both;
//!   only `call_function` bridges a value back.
//! - `register_global`/`get_global` bridge values both ways.
//! - `register_function` wraps the [`HostFunction`] closure into a Lua
//!   global callable; `call_function` invokes script-side functions
//!   with bridged arguments and results.
//! - The quota: [`QuotaController::set_quota`] arms an instruction
//!   budget through mlua's `every_nth_instruction` hook, which is the
//!   same debug-hook mechanism the predecessor armed; a tripped budget
//!   aborts the run mid-instruction and answers
//!   [`ScriptError::QuotaExceeded`]. A wall-clock budget is checked
//!   post-run: mlua offers no mid-run wall-clock interruption, so the
//!   run completes and the exceed is reported after the fact.
//! - Runtime hooks registered before init replay during init; the
//!   predecessor also stripped "business globals" from recycled
//!   pooled LStates — this engine holds one Lua instance per engine,
//!   so there is no recycling to isolate.
//! - `start_watch` reloads the key on source change ticks, the
//!   weak-task + abort-handle shape.
//!
//! Divergences from the Go predecessor:
//!
//! - The predecessor's `RegisterModule` accepted only native
//!   `Lua.LGFunction` modules and rejected value tables; the contract
//!   carries data tables only, so the Lua engine **always rejects**
//!   module registration — the native-module surface has no contract
//!   representation.
//! - The LState pool with pre-warmed states collapses into one Lua
//!   instance per engine behind a mutex; the predecessor's exec mutex
//!   becomes the same serialization.
//! - Host functions run on a minimal inline executor (they must not
//!   park on runtime resources), and the wall-clock budget is
//!   post-hoc (see above).
//!
//! # Engine matrix
//!
//! | Capability | Status |
//! |:---|:---|
//! | loader / executor / globals / functions / watch / sandbox / runtime hooks / sync+quota / lifecycle | implemented |
//! | modules | implemented as an always-rejecting stub (the predecessor rejected value tables too) |

#![forbid(unsafe_code)]
#![deny(missing_docs)]

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::Instant;

use mlua::{
    Function as LuaFunction, HookTriggers, Lua, LuaOptions, MultiValue, StdLib, Value as LuaValue,
    VmState,
};
use tokio::task::AbortHandle;

use rushwind_script::{
    register_factory, BoxFuture, GlobalAccessor, HostFunction, Quota, QuotaController, RuntimeHook,
    RuntimeHookRegistrar, SandboxConfigurator, ScriptEngine, ScriptError, ScriptExecutor,
    ScriptLoader, ScriptValue, ScriptWatcher, SharedEngine, SharedScriptSource, SyncExecutor,
};

/// The registry name — the Go `scriptEngine.LuaType` constant.
pub const NAME: &str = "lua";

/// The `lua` engine: [`mlua`] over vendored Lua 5.4.
pub struct LuaEngine {
    lua: Mutex<Option<Lua>>,
    initialized: Mutex<bool>,
    source: Mutex<Option<SharedScriptSource>>,
    open_libs: Mutex<Vec<String>>,
    chunks: Mutex<Vec<LuaFunction>>,
    hooks: Mutex<Vec<RuntimeHook>>,
    quota: Mutex<Quota>,
    tripped: Arc<AtomicBool>,
    watchers: Mutex<HashMap<String, AbortHandle>>,
    weak: Mutex<Weak<Self>>,
    last_error: Mutex<Option<ScriptError>>,
}

impl LuaEngine {
    /// Builds the engine uninitialized; call [`ScriptEngine::init`]
    /// before use. Prefer [`factory`], which also arms the weak
    /// self-reference the watch tasks need.
    pub fn new() -> Self {
        Self {
            lua: Mutex::new(None),
            initialized: Mutex::new(false),
            source: Mutex::new(None),
            open_libs: Mutex::new(Vec::new()),
            chunks: Mutex::new(Vec::new()),
            hooks: Mutex::new(Vec::new()),
            quota: Mutex::new(Quota::default()),
            tripped: Arc::new(AtomicBool::new(false)),
            watchers: Mutex::new(HashMap::new()),
            weak: Mutex::new(Weak::new()),
            last_error: Mutex::new(None),
        }
    }

    fn set_last_error(&self, error: ScriptError) {
        *self.last_error.lock().expect("lua engine last-error lock") = Some(error);
    }

    fn clear_last_error(&self) {
        *self.last_error.lock().expect("lua engine last-error lock") = None;
    }

    fn lua_handle(&self) -> Option<Lua> {
        self.lua.lock().expect("lua engine handle lock").clone()
    }

    /// The gopher-lua library names mapped onto [`StdLib`] bits. The
    /// default — no `set_open_libs` call, or an empty list — is the
    /// predecessor's full standard set, rendered here as
    /// [`StdLib::ALL_SAFE`]: mlua's safe constructor refuses the
    /// `debug` and `ffi` libraries that the full set would include,
    /// so both the default and any explicit `debug` entry open
    /// without them. `base` is always present in mlua and `channel`
    /// does not exist in Lua 5.4; a list naming only those opens
    /// base alone, exactly as the predecessor's base-only list did.
    fn std_lib_for(names: &[String]) -> StdLib {
        if names.is_empty() {
            return StdLib::ALL_SAFE;
        }
        let mut libs = StdLib::NONE;
        for name in names {
            libs |= match name.as_str() {
                "coroutine" => StdLib::COROUTINE,
                "table" => StdLib::TABLE,
                "io" => StdLib::IO,
                "os" => StdLib::OS,
                "string" => StdLib::STRING,
                "math" => StdLib::MATH,
                "package" => StdLib::PACKAGE,
                // Skipped: `base` is always present; `channel` does
                // not exist in Lua 5.4; `debug` cannot be opened
                // through mlua's safe constructor.
                _ => StdLib::NONE,
            };
        }
        libs
    }

    /// Bridges a contract value into a Lua value.
    fn to_lua(lua: &Lua, value: &ScriptValue) -> LuaValue {
        match value {
            ScriptValue::Null => LuaValue::Nil,
            ScriptValue::Bool(v) => LuaValue::Boolean(*v),
            ScriptValue::Int(v) => LuaValue::Integer(*v),
            ScriptValue::UInt(v) => LuaValue::Integer(*v as i64),
            ScriptValue::Float(v) => LuaValue::Number(*v),
            ScriptValue::String(v) => match lua.create_string(v.as_bytes()) {
                Ok(s) => LuaValue::String(s),
                Err(_) => LuaValue::Nil,
            },
            ScriptValue::Bytes(v) => match lua.create_string(v.as_slice()) {
                Ok(s) => LuaValue::String(s),
                Err(_) => LuaValue::Nil,
            },
            ScriptValue::Array(items) => {
                match (|| -> Result<LuaValue, mlua::Error> {
                    let table = lua.create_table()?;
                    for (index, item) in items.iter().enumerate() {
                        table.set(index as i64 + 1, Self::to_lua(lua, item))?;
                    }
                    Ok(LuaValue::Table(table))
                })() {
                    Ok(v) => v,
                    Err(_) => LuaValue::Nil,
                }
            }
            ScriptValue::Map(map) => match (|| -> Result<LuaValue, mlua::Error> {
                let table = lua.create_table()?;
                for (key, item) in map {
                    table.set(key.as_str(), Self::to_lua(lua, item))?;
                }
                Ok(LuaValue::Table(table))
            })() {
                Ok(v) => v,
                Err(_) => LuaValue::Nil,
            },
            _ => LuaValue::Nil,
        }
    }

    /// Bridges a Lua value back into a contract value. Functions,
    /// threads, userdata, and tables with non-string keys bridge as
    /// Null; arrays bridge back as maps (Lua arrays are tables with
    /// integer keys and the bridge keeps only string keys).
    fn from_lua(value: LuaValue) -> ScriptValue {
        match value {
            LuaValue::Nil => ScriptValue::Null,
            LuaValue::Boolean(v) => ScriptValue::Bool(v),
            LuaValue::Integer(v) => ScriptValue::Int(v),
            LuaValue::Number(v) => ScriptValue::Float(v),
            LuaValue::String(v) => match v.to_str() {
                Ok(s) => ScriptValue::String(s.to_string()),
                Err(_) => ScriptValue::Bytes(v.as_bytes().to_vec()),
            },
            LuaValue::Table(table) => {
                let mut out = HashMap::new();
                for pair in table.pairs::<String, LuaValue>() {
                    let Ok((key, value)) = pair else {
                        break;
                    };
                    out.insert(key, Self::from_lua(value));
                }
                ScriptValue::Map(out)
            }
            _ => ScriptValue::Null,
        }
    }

    fn compile_chunk(&self, code: &str) -> Result<(), ScriptError> {
        self.guard_initialized()?;
        let Some(lua) = self.lua_handle() else {
            let err = ScriptError::Failed("lua engine: not initialized".to_string());
            self.set_last_error(err.clone());
            return Err(err);
        };
        match lua.load(code).into_function() {
            Ok(func) => {
                self.chunks
                    .lock()
                    .expect("lua engine chunk lock")
                    .push(func);
                self.clear_last_error();
                Ok(())
            }
            Err(err) => {
                let wrapped = ScriptError::Failed(format!("lua engine: compile failed: {err}"));
                self.set_last_error(wrapped.clone());
                Err(wrapped)
            }
        }
    }

    fn guard_initialized(&self) -> Result<(), ScriptError> {
        if !*self.initialized.lock().expect("lua engine init lock") {
            let err = ScriptError::Failed("lua engine: not initialized".to_string());
            self.set_last_error(err.clone());
            return Err(err);
        }
        Ok(())
    }

    fn stop_all_watchers(&self) {
        let watchers: Vec<AbortHandle> = {
            let mut watchers = self.watchers.lock().expect("lua engine watcher lock");
            watchers.drain().map(|(_, handle)| handle).collect()
        };
        for handle in watchers {
            handle.abort();
        }
    }

    /// The guarded run wrapper: the instruction-budget hook may trip
    /// mid-run (answering QuotaExceeded), and a wall-clock budget is
    /// checked post-run — mlua cannot interrupt a run on the clock,
    /// so an over-budget run completes and is reported after the
    /// fact.
    fn guard_run<T>(&self, run: impl FnOnce() -> Result<T, mlua::Error>) -> Result<T, ScriptError> {
        self.guard_initialized()?;
        let quota = *self.quota.lock().expect("lua engine quota lock");
        self.tripped.store(false, Ordering::SeqCst);
        let start = Instant::now();
        let outcome = run();
        let tripped = self.tripped.load(Ordering::SeqCst);
        let elapsed = start.elapsed();
        if tripped {
            let err = ScriptError::QuotaExceeded;
            self.set_last_error(err.clone());
            return Err(err);
        }
        if let Some(budget) = quota.timeout {
            if elapsed >= budget {
                let err = ScriptError::QuotaExceeded;
                self.set_last_error(err.clone());
                return Err(err);
            }
        }
        match outcome {
            Ok(value) => {
                self.clear_last_error();
                Ok(value)
            }
            Err(err) => {
                let wrapped = ScriptError::Failed(format!("lua engine: {err}"));
                self.set_last_error(wrapped.clone());
                Err(wrapped)
            }
        }
    }

    fn arm_quota(&self) {
        let quota = *self.quota.lock().expect("lua engine quota lock");
        let Some(lua) = self.lua_handle() else {
            return;
        };
        if let Some(budget) = quota.max_instructions {
            let remaining = Arc::new(AtomicI64::new(budget.min(i64::MAX as u64) as i64));
            let tripped = self.tripped.clone();
            let _ = lua.set_global_hook(
                HookTriggers {
                    every_nth_instruction: Some(1),
                    ..Default::default()
                },
                move |_lua, _debug| {
                    let left = remaining.fetch_sub(1, Ordering::SeqCst) - 1;
                    if left <= 0 {
                        tripped.store(true, Ordering::SeqCst);
                        return Err(mlua::Error::RuntimeError(
                            "instruction quota exceeded".to_string(),
                        ));
                    }
                    Ok(VmState::Continue)
                },
            );
        } else {
            lua.remove_global_hook();
        }
    }
}

impl Default for LuaEngine {
    fn default() -> Self {
        Self::new()
    }
}

impl SandboxConfigurator for LuaEngine {
    fn set_open_libs(&self, libs: &[&str]) {
        let mut open_libs = self.open_libs.lock().expect("lua engine open-libs lock");
        *open_libs = libs.iter().map(|lib| lib.to_string()).collect();
    }
}

impl RuntimeHookRegistrar for LuaEngine {
    /// Hooks registered before init replay during init; on an
    /// initialized engine they run immediately. The immediate run
    /// drives the hook's boxed future on a minimal inline executor —
    /// hooks must not park on runtime resources.
    fn add_runtime_hook(&self, hook: RuntimeHook) -> Result<(), ScriptError> {
        if !*self.initialized.lock().expect("lua engine init lock") {
            self.hooks.lock().expect("lua engine hook lock").push(hook);
            return Ok(());
        }
        let _ = futures::executor::block_on(hook());
        Ok(())
    }
}

impl QuotaController for LuaEngine {
    fn set_quota(&self, quota: Quota) {
        *self.quota.lock().expect("lua engine quota lock") = quota;
        self.arm_quota();
    }
}

impl ScriptEngine for LuaEngine {
    fn engine_type(&self) -> &'static str {
        NAME
    }

    fn init(&self) -> BoxFuture<'_, Result<(), ScriptError>> {
        Box::pin(async move {
            let lua = {
                let mut initialized = self.initialized.lock().expect("lua engine init lock");
                if *initialized {
                    self.set_last_error(ScriptError::Failed(
                        "lua engine: already initialized".to_string(),
                    ));
                    return Err(ScriptError::Failed(
                        "lua engine: already initialized".to_string(),
                    ));
                }
                *initialized = true;
                let libs = Self::std_lib_for(
                    &self
                        .open_libs
                        .lock()
                        .expect("lua engine open-libs lock")
                        .clone(),
                );
                let lua = match Lua::new_with(libs, LuaOptions::default()) {
                    Ok(lua) => lua,
                    Err(err) => {
                        *initialized = false;
                        let wrapped =
                            ScriptError::Failed(format!("lua engine: init failed: {err}"));
                        self.set_last_error(wrapped.clone());
                        return Err(wrapped);
                    }
                };
                *self.lua.lock().expect("lua engine handle lock") = Some(lua);
                self
            };
            // Replay hooks registered before init, outside the init
            // lock — the Go replay order. Hooks may call back into
            // the engine.
            let hooks: Vec<RuntimeHook> = lua
                .hooks
                .lock()
                .expect("lua engine hook lock")
                .drain(..)
                .collect();
            for hook in hooks {
                if hook().await.is_err() {
                    return Err(ScriptError::Failed(
                        "lua engine: runtime hook failed".to_string(),
                    ));
                }
            }
            // A quota configured before init arms now that the Lua
            // instance exists.
            self.arm_quota();
            self.clear_last_error();
            Ok(())
        })
    }

    fn close(&self) -> Result<(), ScriptError> {
        self.stop_all_watchers();
        {
            let mut initialized = self.initialized.lock().expect("lua engine init lock");
            *initialized = false;
            if let Some(lua) = self.lua.lock().expect("lua engine handle lock").take() {
                lua.remove_global_hook();
            }
            *self.source.lock().expect("lua engine source lock") = None;
            self.chunks.lock().expect("lua engine chunk lock").clear();
            self.hooks.lock().expect("lua engine hook lock").clear();
        }
        self.clear_last_error();
        Ok(())
    }

    fn is_initialized(&self) -> bool {
        *self.initialized.lock().expect("lua engine init lock")
    }

    fn last_error(&self) -> Option<ScriptError> {
        self.last_error
            .lock()
            .expect("lua engine last-error lock")
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

    fn as_sandbox_configurator(self: Arc<Self>) -> Option<Arc<dyn SandboxConfigurator>> {
        Some(self)
    }

    fn as_runtime_hook_registrar(self: Arc<Self>) -> Option<Arc<dyn RuntimeHookRegistrar>> {
        Some(self)
    }

    fn as_sync_executor(self: Arc<Self>) -> Option<Arc<dyn SyncExecutor>> {
        Some(self)
    }

    fn as_quota_controller(self: Arc<Self>) -> Option<Arc<dyn QuotaController>> {
        Some(self)
    }
}

impl ScriptLoader for LuaEngine {
    fn set_source(&self, source: Option<SharedScriptSource>) {
        *self.source.lock().expect("lua engine source lock") = source;
    }

    fn get_source(&self) -> Option<SharedScriptSource> {
        self.source.lock().expect("lua engine source lock").clone()
    }

    fn load<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<(), ScriptError>> {
        Box::pin(async move {
            self.guard_initialized()?;
            let source = self.source.lock().expect("lua engine source lock").clone();
            let Some(source) = source else {
                let err = ScriptError::Failed("lua engine: no source bound".to_string());
                self.set_last_error(err.clone());
                return Err(err);
            };
            let code = match source.load(key).await {
                Ok(code) => code,
                Err(err) => {
                    self.set_last_error(err.clone());
                    return Err(err);
                }
            };
            self.compile_chunk(&code)
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
        let outcome = self.compile_chunk(code);
        Box::pin(async move { outcome })
    }
}

impl LuaEngine {
    fn exec_chunks(&self) -> Result<ScriptValue, ScriptError> {
        let chunks: Vec<LuaFunction> = self.chunks.lock().expect("lua engine chunk lock").clone();
        if chunks.is_empty() {
            let err = ScriptError::Failed("lua engine: no chunk loaded".to_string());
            self.set_last_error(err.clone());
            return Err(err);
        }
        self.guard_run(|| {
            for chunk in &chunks {
                chunk.call::<()>(())?;
            }
            Ok(())
        })?;
        Ok(ScriptValue::Null)
    }
}

impl ScriptExecutor for LuaEngine {
    fn execute(&self) -> BoxFuture<'_, Result<ScriptValue, ScriptError>> {
        let outcome = self.exec_chunks();
        Box::pin(async move { outcome })
    }

    fn execute_from_key<'a>(
        &'a self,
        key: &'a str,
    ) -> BoxFuture<'a, Result<ScriptValue, ScriptError>> {
        Box::pin(async move {
            self.load(key).await?;
            self.exec_chunks()
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
        _name: &'a str,
        code: &'a str,
    ) -> BoxFuture<'a, Result<ScriptValue, ScriptError>> {
        let outcome = (|| {
            self.compile_chunk(code)?;
            self.exec_chunks()
        })();
        Box::pin(async move { outcome })
    }
}

impl SyncExecutor for LuaEngine {
    fn execute_sync(&self) -> BoxFuture<'_, Result<ScriptValue, ScriptError>> {
        let outcome = self.exec_chunks();
        Box::pin(async move { outcome })
    }
}

impl GlobalAccessor for LuaEngine {
    fn register_global(&self, name: &str, value: ScriptValue) -> Result<(), ScriptError> {
        self.guard_initialized()?;
        let Some(lua) = self.lua_handle() else {
            let err = ScriptError::Failed("lua engine: not initialized".to_string());
            self.set_last_error(err.clone());
            return Err(err);
        };
        let bridged = Self::to_lua(&lua, &value);
        if let Err(err) = lua.globals().set(name.to_string(), bridged) {
            let wrapped = ScriptError::Failed(format!("lua engine: {err}"));
            self.set_last_error(wrapped.clone());
            return Err(wrapped);
        }
        self.clear_last_error();
        Ok(())
    }

    fn get_global(&self, name: &str) -> Result<ScriptValue, ScriptError> {
        self.guard_initialized()?;
        let Some(lua) = self.lua_handle() else {
            let err = ScriptError::Failed("lua engine: not initialized".to_string());
            self.set_last_error(err.clone());
            return Err(err);
        };
        match lua.globals().get::<LuaValue>(name.to_string()) {
            Ok(value) => {
                self.clear_last_error();
                Ok(Self::from_lua(value))
            }
            Err(err) => {
                let wrapped = ScriptError::Failed(format!("lua engine: {err}"));
                self.set_last_error(wrapped.clone());
                Err(wrapped)
            }
        }
    }
}

impl rushwind_script::FunctionRegistrar for LuaEngine {
    fn register_function(&self, name: &str, function: HostFunction) -> Result<(), ScriptError> {
        self.guard_initialized()?;
        let Some(lua) = self.lua_handle() else {
            let err = ScriptError::Failed("lua engine: not initialized".to_string());
            self.set_last_error(err.clone());
            return Err(err);
        };
        // The host closure runs on a minimal inline executor: the
        // callback shape mlua offers is synchronous, and host
        // functions must therefore complete without parking on
        // runtime resources.
        let host = function;
        let bound = move |_lua: &Lua, args: MultiValue| -> Result<LuaValue, mlua::Error> {
            let bridged: Vec<ScriptValue> = args.into_iter().map(Self::from_lua).collect();
            let host = host.clone();
            let result = futures::executor::block_on(host(bridged.as_slice()))
                .map_err(|err| mlua::Error::RuntimeError(err.to_string()))?;
            Ok(Self::to_lua(_lua, &result))
        };
        let lua_function = match lua.create_function(bound) {
            Ok(f) => f,
            Err(err) => {
                let wrapped = ScriptError::Failed(format!("lua engine: {err}"));
                self.set_last_error(wrapped.clone());
                return Err(wrapped);
            }
        };
        if let Err(err) = lua.globals().set(name.to_string(), lua_function) {
            let wrapped = ScriptError::Failed(format!("lua engine: {err}"));
            self.set_last_error(wrapped.clone());
            return Err(wrapped);
        }
        self.clear_last_error();
        Ok(())
    }

    fn call_function<'a>(
        &'a self,
        name: &'a str,
        args: &'a [ScriptValue],
    ) -> BoxFuture<'a, Result<ScriptValue, ScriptError>> {
        let outcome = self.call_script_function(name, args);
        Box::pin(async move { outcome })
    }
}

impl LuaEngine {
    fn call_script_function(
        &self,
        name: &str,
        args: &[ScriptValue],
    ) -> Result<ScriptValue, ScriptError> {
        self.guard_initialized()?;
        let Some(lua) = self.lua_handle() else {
            let err = ScriptError::Failed("lua engine: not initialized".to_string());
            self.set_last_error(err.clone());
            return Err(err);
        };
        let function = match lua.globals().get::<LuaValue>(name.to_string()) {
            Ok(LuaValue::Function(f)) => f,
            Ok(_) => {
                let err = ScriptError::Failed(format!("lua engine: not callable: {name}"));
                self.set_last_error(err.clone());
                return Err(err);
            }
            Err(err) => {
                let wrapped = ScriptError::Failed(format!("lua engine: {err}"));
                self.set_last_error(wrapped.clone());
                return Err(wrapped);
            }
        };
        let bridged: Vec<LuaValue> = args.iter().map(|arg| Self::to_lua(&lua, arg)).collect();
        let multi = MultiValue::from(bridged);
        self.guard_run(|| function.call::<LuaValue>(multi))
            .map(Self::from_lua)
    }
}

impl rushwind_script::ModuleRegistrar for LuaEngine {
    /// The predecessor accepted only native `Lua.LGFunction` modules
    /// and rejected value tables; the contract carries value tables
    /// only, so registration always fails here.
    fn register_module(&self, _name: &str, _module: ScriptValue) -> Result<(), ScriptError> {
        let err = ScriptError::Failed("lua engine: module must be a native module".to_string());
        self.set_last_error(err.clone());
        Err(err)
    }
}

impl ScriptWatcher for LuaEngine {
    fn start_watch<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<(), ScriptError>> {
        Box::pin(async move {
            self.guard_initialized()?;
            let source = self.source.lock().expect("lua engine source lock").clone();
            let Some(source) = source else {
                let err = ScriptError::Failed("lua engine: no source bound".to_string());
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
            let weak = self.weak.lock().expect("lua engine weak lock").clone();
            if weak.upgrade().is_none() {
                let err = ScriptError::Failed("lua engine: engine handle unavailable".to_string());
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
                .expect("lua engine watcher lock")
                .insert(key.to_string(), handle);
            self.clear_last_error();
            Ok(())
        })
    }

    fn stop_watch(&self, key: &str) -> Result<(), ScriptError> {
        let handle = self
            .watchers
            .lock()
            .expect("lua engine watcher lock")
            .remove(key);
        if let Some(handle) = handle {
            handle.abort();
        }
        Ok(())
    }
}

/// Builds an engine, arming the weak self-reference the watch tasks
/// need. The Go predecessor registered this under its Lua type
/// through package `init()`; [`register`] is the explicit Rust form.
pub fn factory() -> Result<SharedEngine, ScriptError> {
    let engine = Arc::new(LuaEngine::new());
    *engine.weak.lock().expect("lua engine weak lock") = Arc::downgrade(&engine);
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
    async fn execution_discards_results_but_call_function_bridges() {
        let engine = engine().await;
        engine
            .load_string("s", "function add(a, b) return a + b end")
            .await
            .expect("load script");
        assert_eq!(engine.execute().await.expect("execute"), ScriptValue::Null);
        let args = [ScriptValue::Int(1), ScriptValue::Int(2)];
        assert_eq!(
            engine
                .call_function("add", &args)
                .await
                .expect("call function"),
            ScriptValue::Int(3)
        );
    }

    #[tokio::test]
    async fn a_broken_chunk_fails_to_compile() {
        let engine = engine().await;
        assert!(matches!(
            engine.load_string("s", "function (").await,
            Err(ScriptError::Failed(msg)) if msg.contains("compile failed")
        ));
    }

    #[tokio::test]
    async fn a_runtime_error_fails_the_run() {
        let engine = engine().await;
        engine
            .load_string("s", "error('boom')")
            .await
            .expect("load");
        assert!(matches!(
            engine.execute().await,
            Err(ScriptError::Failed(msg)) if msg.contains("lua engine")
        ));
    }

    #[tokio::test]
    async fn globals_bridge_both_ways() {
        let engine = engine().await;
        engine
            .register_global("x", ScriptValue::Int(5))
            .expect("register global");
        assert_eq!(
            engine.get_global("x").expect("get global"),
            ScriptValue::Int(5)
        );
        engine
            .load_string("s", "y = x * 2")
            .await
            .expect("load script");
        engine.execute().await.expect("execute");
        assert_eq!(
            engine.get_global("y").expect("script global read back"),
            ScriptValue::Int(10)
        );
    }

    #[tokio::test]
    async fn host_functions_reach_scripts_and_callers() {
        let engine = engine().await;
        let host: HostFunction = Arc::new(
            |args: &[ScriptValue]| -> BoxFuture<'static, Result<ScriptValue, ScriptError>> {
                let v = args.first().cloned().unwrap_or(ScriptValue::Null);
                Box::pin(async move { Ok(v) })
            },
        );
        engine
            .register_function("echo", host)
            .expect("register function");
        engine
            .load_string("s", "function f() return echo(42) end")
            .await
            .expect("load script");
        engine.execute().await.expect("execute");
        assert_eq!(
            engine.call_function("f", &[]).await.expect("call"),
            ScriptValue::Int(42)
        );
    }

    #[tokio::test]
    async fn modules_are_rejected() {
        let engine = engine().await;
        assert!(matches!(
            engine.register_module("m", ScriptValue::Map(HashMap::new())),
            Err(ScriptError::Failed(msg)) if msg.contains("native module")
        ));
    }

    #[tokio::test]
    async fn the_sandbox_allow_list_shrinks_the_libraries() {
        let engine = factory().expect("engine");
        {
            let sandbox: Arc<dyn SandboxConfigurator> = engine
                .clone()
                .as_sandbox_configurator()
                .expect("sandbox capability");
            sandbox.set_open_libs(&["base", "string", "math"]);
        }
        engine.init().await.expect("init");
        engine
            .load_string("s", "function hasio() return io ~= nil end")
            .await
            .expect("load script");
        engine.execute().await.expect("execute");
        assert_eq!(
            engine
                .call_function("hasio", &[])
                .await
                .expect("io is absent"),
            ScriptValue::Bool(false)
        );
    }

    #[tokio::test]
    async fn the_default_library_set_is_full() {
        let engine = engine().await;
        engine
            .load_string("s", "function hasio() return io ~= nil end")
            .await
            .expect("load script");
        engine.execute().await.expect("execute");
        assert_eq!(
            engine
                .call_function("hasio", &[])
                .await
                .expect("io is present"),
            ScriptValue::Bool(true)
        );
    }

    #[tokio::test]
    async fn an_instruction_budget_interrupts_a_runaway_script() {
        let engine = engine().await;
        {
            let quota: Arc<dyn QuotaController> = engine
                .clone()
                .as_quota_controller()
                .expect("quota capability");
            quota.set_quota(Quota {
                timeout: None,
                max_instructions: Some(64),
            });
        }
        engine
            .load_string("s", "while true do end")
            .await
            .expect("load script");
        let sync: Arc<dyn SyncExecutor> =
            engine.clone().as_sync_executor().expect("sync capability");
        assert_eq!(
            sync.execute_sync().await.unwrap_err(),
            ScriptError::QuotaExceeded
        );
    }

    #[tokio::test]
    async fn a_wall_clock_budget_is_reported_post_run() {
        let engine = engine().await;
        {
            let quota: Arc<dyn QuotaController> = engine
                .clone()
                .as_quota_controller()
                .expect("quota capability");
            quota.set_quota(Quota {
                timeout: Some(std::time::Duration::from_nanos(1)),
                max_instructions: None,
            });
        }
        engine
            .load_string("s", "local t = 0 for i = 1, 100 do t = t + i end")
            .await
            .expect("load script");
        let sync: Arc<dyn SyncExecutor> =
            engine.clone().as_sync_executor().expect("sync capability");
        assert_eq!(
            sync.execute_sync().await.unwrap_err(),
            ScriptError::QuotaExceeded
        );
    }

    #[tokio::test]
    async fn a_watch_signal_reloads_the_chunk() {
        let engine = engine().await;
        let source = Arc::new(MemSource::new());
        source.set("chunk", "function f() return 1 end");
        engine.set_source(Some(source.clone()));
        engine.load("chunk").await.expect("initial load");
        engine.execute().await.expect("initial execute");
        assert_eq!(
            engine.call_function("f", &[]).await.expect("v1"),
            ScriptValue::Int(1)
        );
        engine.start_watch("chunk").await.expect("start watch");
        source.set("chunk", "function f() return 2 end");
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        // The watch task reloaded the chunk into the engine; running
        // the loaded set defines the reloaded function over the old
        // one, the predecessor's reload-then-execute flow.
        engine.execute().await.expect("post-reload execute");
        assert_eq!(
            engine.call_function("f", &[]).await.expect("reloaded"),
            ScriptValue::Int(2)
        );
        engine.stop_watch("chunk").expect("stop watch");
    }

    #[tokio::test]
    async fn sources_drive_loads() {
        let engine = engine().await;
        let source = Arc::new(MemSource::new());
        source.set("chunk", "x = 40 + 2");
        engine.set_source(Some(source));
        engine.load("chunk").await.expect("load");
        engine.execute().await.expect("execute");
        assert_eq!(
            engine.get_global("x").expect("source-driven global"),
            ScriptValue::Int(42)
        );
    }

    #[tokio::test]
    async fn uninitialized_and_lifecycle_guards_hold() {
        let engine = factory().expect("engine");
        assert!(!engine.is_initialized());
        assert!(engine.execute().await.is_err());
        assert!(engine.register_global("x", ScriptValue::Int(1)).is_err());
        engine.init().await.expect("init");
        engine.init().await.expect_err("second init refused");
        engine.close().expect("close");
        assert!(!engine.is_initialized());
        assert!(engine.execute().await.is_err());
    }

    #[tokio::test]
    async fn probes_offer_the_predecessor_capability_set() {
        let engine: Arc<dyn ScriptEngine> = factory().expect("engine");
        assert!(engine.clone().as_loader().is_some());
        assert!(engine.clone().as_executor().is_some());
        assert!(engine.clone().as_global_accessor().is_some());
        assert!(engine.clone().as_function_registrar().is_some());
        assert!(engine.clone().as_module_registrar().is_some());
        assert!(engine.clone().as_watcher().is_some());
        assert!(engine.clone().as_sandbox_configurator().is_some());
        assert!(engine.clone().as_runtime_hook_registrar().is_some());
        assert!(engine.clone().as_sync_executor().is_some());
        assert!(engine.as_quota_controller().is_some());
    }
}
