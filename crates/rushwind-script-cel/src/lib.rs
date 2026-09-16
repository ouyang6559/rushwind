//! The CEL engine for the Rust script contract, built over
//! [`cel_interpreter`].
//!
//! Semantics:
//!
//! - `load` compiles an expression and keeps it; `execute` evaluates
//!   every loaded expression in order against one variable context
//!   built from the registered globals and answers with the **last**
//!   result.
//! - `register_global` binds a variable; `get_global` reads it back.
//! - `register_module` flattens a value map into `name_key`-prefixed
//!   variables.
//! - `register_function` stores a host function and
//!   `call_function` invokes it host-side with the marshalled
//!   arguments.
//! - `start_watch` opens the bound source's watch stream and reloads
//!   the key on every change tick; `stop_watch` aborts it, and close
//!   aborts all of them.
//!
//! Divergences from a type-checked CEL environment:
//!
//! - Host functions are
//!   reachable through `call_function` only, never from an
//!   expression: cel-rust functions take statically-typed closures,
//!   which the dynamic
//!   [`HostFunction`] shape cannot become.
//! - There is no CEL type-inference pass — values stay the dynamic
//!   [`cel_interpreter`] values.
//! - The watch loop is a tokio task holding the engine
//!   weakly; the engine dropping ends the task, and `stop_watch` maps
//!   to task abortion.
//!
//! # Engine matrix
//!
//! | Capability | Status |
//! |:---|:---|
//! | loader / executor / globals / functions / modules / watch / lifecycle | implemented |
//! | sandbox, runtime hooks, sync+quota | not offered |

#![forbid(unsafe_code)]
#![deny(missing_docs)]

use std::collections::HashMap;
use std::sync::{Arc, Mutex, Weak};

use cel_interpreter::objects::Key as CelKey;
use cel_interpreter::{Context as CelContext, Program, Value as CelValue};
use tokio::task::AbortHandle;

use rushwind_script::{
    register_factory, BoxFuture, GlobalAccessor, HostFunction, ScriptEngine, ScriptError,
    ScriptExecutor, ScriptLoader, ScriptValue, ScriptWatcher, SharedEngine, SharedScriptSource,
};

/// The registry name.
pub const NAME: &str = "cel";

/// The `cel` engine: expression compilation and evaluation over
/// [`cel_interpreter`].
pub struct CelEngine {
    initialized: Mutex<bool>,
    source: Mutex<Option<SharedScriptSource>>,
    programs: Mutex<Vec<(String, Program)>>,
    globals: Mutex<HashMap<String, ScriptValue>>,
    functions: Mutex<HashMap<String, HostFunction>>,
    watchers: Mutex<HashMap<String, AbortHandle>>,
    weak: Mutex<Weak<Self>>,
    last_error: Mutex<Option<ScriptError>>,
}

impl CelEngine {
    /// Builds the engine uninitialized; call
    /// [`ScriptEngine::init`] before use. Prefer [`factory`], which
    /// also arms the weak self-reference the watch tasks need.
    pub fn new() -> Self {
        Self {
            initialized: Mutex::new(false),
            source: Mutex::new(None),
            programs: Mutex::new(Vec::new()),
            globals: Mutex::new(HashMap::new()),
            functions: Mutex::new(HashMap::new()),
            watchers: Mutex::new(HashMap::new()),
            weak: Mutex::new(Weak::new()),
            last_error: Mutex::new(None),
        }
    }

    fn set_last_error(&self, error: ScriptError) {
        *self.last_error.lock().expect("cel engine last-error lock") = Some(error);
    }

    fn clear_last_error(&self) {
        *self.last_error.lock().expect("cel engine last-error lock") = None;
    }

    fn guard_initialized(&self) -> Result<(), ScriptError> {
        if !*self.initialized.lock().expect("cel engine init lock") {
            let err = ScriptError::Failed("cel engine: not initialized".to_string());
            self.set_last_error(err.clone());
            return Err(err);
        }
        Ok(())
    }

    fn stop_all_watchers(&self) {
        let watchers: Vec<AbortHandle> = {
            let mut watchers = self.watchers.lock().expect("cel engine watcher lock");
            watchers.drain().map(|(_, handle)| handle).collect()
        };
        for handle in watchers {
            handle.abort();
        }
    }

    /// Bridges a contract value into a CEL value. Null has no CEL
    /// rendering and fails the bridge.
    fn to_cel(value: &ScriptValue) -> Result<CelValue, ScriptError> {
        match value {
            ScriptValue::Null => Err(ScriptError::Failed(
                "cel engine: null has no CEL rendering".to_string(),
            )),
            ScriptValue::Bool(v) => Ok(CelValue::Bool(*v)),
            ScriptValue::Int(v) => Ok(CelValue::Int(*v)),
            ScriptValue::UInt(v) => Ok(CelValue::UInt(*v)),
            ScriptValue::Float(v) => Ok(CelValue::Float(*v)),
            ScriptValue::String(v) => Ok(CelValue::String(Arc::new(v.clone()))),
            ScriptValue::Bytes(v) => Ok(CelValue::Bytes(Arc::new(v.clone()))),
            ScriptValue::Array(items) => {
                let mut converted = Vec::with_capacity(items.len());
                for item in items {
                    converted.push(Self::to_cel(item)?);
                }
                Ok(CelValue::List(Arc::new(converted)))
            }
            ScriptValue::Map(map) => {
                let mut converted: HashMap<String, CelValue> = HashMap::new();
                for (key, item) in map {
                    converted.insert(key.clone(), Self::to_cel(item)?);
                }
                Ok(CelValue::Map(converted.into()))
            }
            _ => Err(ScriptError::Failed(
                "cel engine: value has no CEL rendering".to_string(),
            )),
        }
    }

    /// Bridges a CEL value back into a contract value. Functions carry
    /// no data and bridge as Null.
    fn from_cel(value: &CelValue) -> ScriptValue {
        match value {
            CelValue::Bool(v) => ScriptValue::Bool(*v),
            CelValue::Int(v) => ScriptValue::Int(*v),
            CelValue::UInt(v) => ScriptValue::UInt(*v),
            CelValue::Float(v) => ScriptValue::Float(*v),
            CelValue::String(v) => ScriptValue::String(v.to_string()),
            CelValue::Bytes(v) => ScriptValue::Bytes(v.to_vec()),
            CelValue::List(items) => ScriptValue::Array(items.iter().map(Self::from_cel).collect()),
            CelValue::Map(map) => {
                let mut out = HashMap::new();
                for (key, item) in map.map.iter() {
                    if let CelKey::String(key) = key {
                        out.insert(key.to_string(), Self::from_cel(item));
                    }
                }
                ScriptValue::Map(out)
            }
            CelValue::Function(_, _) => ScriptValue::Null,
            _ => ScriptValue::Null,
        }
    }

    fn compile(&self, name: &str, code: &str) -> Result<(), ScriptError> {
        self.guard_initialized()?;
        // The antlr4rust-backed parser panics outright on some
        // malformed inputs (empty strings among them) instead of
        // answering Err; the catch turns those panics into compile
        // failures so a script author's typo cannot take the host
        // down.
        let outcome = std::panic::catch_unwind(|| Program::compile(code));
        match outcome {
            Ok(Ok(program)) => {
                self.programs
                    .lock()
                    .expect("cel engine program lock")
                    .push((name.to_string(), program));
                self.clear_last_error();
                Ok(())
            }
            Ok(Err(err)) => {
                let wrapped =
                    ScriptError::Failed(format!("cel engine: compile failed: {name}: {err}"));
                self.set_last_error(wrapped.clone());
                Err(wrapped)
            }
            Err(_) => {
                let wrapped = ScriptError::Failed(format!(
                    "cel engine: compile failed: {name}: parser panicked"
                ));
                self.set_last_error(wrapped.clone());
                Err(wrapped)
            }
        }
    }
}

impl Default for CelEngine {
    fn default() -> Self {
        Self::new()
    }
}

impl ScriptEngine for CelEngine {
    fn engine_type(&self) -> &'static str {
        NAME
    }

    fn init(&self) -> BoxFuture<'_, Result<(), ScriptError>> {
        let outcome = {
            let mut initialized = self.initialized.lock().expect("cel engine init lock");
            if *initialized {
                Err(ScriptError::Failed(
                    "cel engine: already initialized".to_string(),
                ))
            } else {
                *initialized = true;
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
        self.stop_all_watchers();
        {
            let mut initialized = self.initialized.lock().expect("cel engine init lock");
            *initialized = false;
            *self.source.lock().expect("cel engine source lock") = None;
            self.programs
                .lock()
                .expect("cel engine program lock")
                .clear();
            self.globals.lock().expect("cel engine global lock").clear();
            self.functions
                .lock()
                .expect("cel engine function lock")
                .clear();
        }
        self.clear_last_error();
        Ok(())
    }

    fn is_initialized(&self) -> bool {
        *self.initialized.lock().expect("cel engine init lock")
    }

    fn last_error(&self) -> Option<ScriptError> {
        self.last_error
            .lock()
            .expect("cel engine last-error lock")
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

impl ScriptLoader for CelEngine {
    fn set_source(&self, source: Option<SharedScriptSource>) {
        *self.source.lock().expect("cel engine source lock") = source;
    }

    fn get_source(&self) -> Option<SharedScriptSource> {
        self.source.lock().expect("cel engine source lock").clone()
    }

    fn load<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<(), ScriptError>> {
        Box::pin(async move {
            self.guard_initialized()?;
            let source = self.source.lock().expect("cel engine source lock").clone();
            let Some(source) = source else {
                let err = ScriptError::Failed("cel engine: no source bound".to_string());
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
            self.compile(key, &code)
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
        let outcome = self.compile(name, code);
        Box::pin(async move { outcome })
    }
}

impl ScriptExecutor for CelEngine {
    fn execute(&self) -> BoxFuture<'_, Result<ScriptValue, ScriptError>> {
        Box::pin(async move {
            self.guard_initialized()?;
            let globals = self.globals.lock().expect("cel engine global lock").clone();
            let mut context = CelContext::default();
            for (name, value) in &globals {
                if let Ok(cel_value) = Self::to_cel(value) {
                    context.add_variable_from_value(name.clone(), cel_value);
                }
            }
            // Execution is serialized behind the program-list lock
            // and answers with the last program's result; the
            // program list stays locked for the duration here, which
            // is the serialization.
            let programs = self.programs.lock().expect("cel engine program lock");
            if programs.is_empty() {
                drop(programs);
                let err = ScriptError::Failed("cel engine: no expression loaded".to_string());
                self.set_last_error(err.clone());
                return Err(err);
            }
            let mut last = ScriptValue::Null;
            for (name, program) in programs.iter() {
                match program.execute(&context) {
                    Ok(value) => last = Self::from_cel(&value),
                    Err(err) => {
                        let wrapped = ScriptError::Failed(format!(
                            "cel engine: evaluation failed: {name}: {err}"
                        ));
                        self.set_last_error(wrapped.clone());
                        return Err(wrapped);
                    }
                }
            }
            drop(programs);
            self.clear_last_error();
            Ok(last)
        })
    }

    fn execute_from_key<'a>(
        &'a self,
        key: &'a str,
    ) -> BoxFuture<'a, Result<ScriptValue, ScriptError>> {
        Box::pin(async move {
            self.load(key).await?;
            self.execute().await
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
            self.compile(name, code)?;
            self.execute().await
        })
    }
}

impl GlobalAccessor for CelEngine {
    fn register_global(&self, name: &str, value: ScriptValue) -> Result<(), ScriptError> {
        self.guard_initialized()?;
        self.globals
            .lock()
            .expect("cel engine global lock")
            .insert(name.to_string(), value);
        self.clear_last_error();
        Ok(())
    }

    fn get_global(&self, name: &str) -> Result<ScriptValue, ScriptError> {
        self.guard_initialized()?;
        let value = self
            .globals
            .lock()
            .expect("cel engine global lock")
            .get(name)
            .cloned();
        let Some(value) = value else {
            let err = ScriptError::Failed(format!("cel engine: global not found: {name}"));
            self.set_last_error(err.clone());
            return Err(err);
        };
        self.clear_last_error();
        Ok(value)
    }
}

impl rushwind_script::FunctionRegistrar for CelEngine {
    fn register_function(&self, name: &str, function: HostFunction) -> Result<(), ScriptError> {
        self.guard_initialized()?;
        self.functions
            .lock()
            .expect("cel engine function lock")
            .insert(name.to_string(), function);
        self.clear_last_error();
        Ok(())
    }

    fn call_function<'a>(
        &'a self,
        name: &'a str,
        args: &'a [ScriptValue],
    ) -> BoxFuture<'a, Result<ScriptValue, ScriptError>> {
        Box::pin(async move {
            self.guard_initialized()?;
            let function = self
                .functions
                .lock()
                .expect("cel engine function lock")
                .get(name)
                .cloned();
            let Some(function) = function else {
                let err = ScriptError::Failed(format!("cel engine: function not found: {name}"));
                self.set_last_error(err.clone());
                return Err(err);
            };
            let result = function(args).await;
            self.clear_last_error();
            result
        })
    }
}

impl rushwind_script::ModuleRegistrar for CelEngine {
    /// Flattening: every map entry becomes a
    /// `name_key`-prefixed global variable.
    fn register_module(&self, name: &str, module: ScriptValue) -> Result<(), ScriptError> {
        self.guard_initialized()?;
        let ScriptValue::Map(map) = module else {
            let err = ScriptError::Failed("cel engine: register_module expects a map".to_string());
            self.set_last_error(err.clone());
            return Err(err);
        };
        let mut globals = self.globals.lock().expect("cel engine global lock");
        for (key, value) in map {
            globals.insert(format!("{name}_{key}"), value);
        }
        self.clear_last_error();
        Ok(())
    }
}

impl ScriptWatcher for CelEngine {
    fn start_watch<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<(), ScriptError>> {
        Box::pin(async move {
            self.guard_initialized()?;
            let source = self.source.lock().expect("cel engine source lock").clone();
            let Some(source) = source else {
                let err = ScriptError::Failed("cel engine: no source bound".to_string());
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
            let weak = self.weak.lock().expect("cel engine weak lock").clone();
            if weak.upgrade().is_none() {
                let err = ScriptError::Failed("cel engine: engine handle unavailable".to_string());
                self.set_last_error(err.clone());
                return Err(err);
            }
            // The reload task holds the engine weakly: dropping the
            // engine ends it, and stop_watch aborts it by handle.
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
                .expect("cel engine watcher lock")
                .insert(key.to_string(), handle);
            self.clear_last_error();
            Ok(())
        })
    }

    fn stop_watch(&self, key: &str) -> Result<(), ScriptError> {
        let handle = self
            .watchers
            .lock()
            .expect("cel engine watcher lock")
            .remove(key);
        if let Some(handle) = handle {
            handle.abort();
        }
        Ok(())
    }
}

/// Builds an initialized engine, arming the weak self-reference the
/// watch tasks need. [`register`] installs it in the
/// factory registry under [`NAME`].
pub fn factory() -> Result<SharedEngine, ScriptError> {
    let engine = Arc::new(CelEngine::new());
    *engine.weak.lock().expect("cel engine weak lock") = Arc::downgrade(&engine);
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
    async fn arithmetic_evaluates() {
        let engine = engine().await;
        assert_eq!(
            engine.execute_string("t", "1 + 2").await.expect("eval"),
            ScriptValue::Int(3)
        );
    }

    #[tokio::test]
    async fn strings_evaluate() {
        let engine = engine().await;
        assert_eq!(
            engine
                .execute_string("t", r#""hello" + " " + "world""#)
                .await
                .expect("eval"),
            ScriptValue::String("hello world".to_string())
        );
    }

    #[tokio::test]
    async fn a_broken_expression_fails_to_compile() {
        let engine = engine().await;
        assert!(matches!(
            engine.execute_string("t", "a b c").await,
            Err(ScriptError::Failed(msg)) if msg.contains("compile failed")
        ));
        // The upstream parser panics on some malformed inputs —
        // including the empty expression — and the compile wrapper
        // converts the panic into the same failure.
        assert!(matches!(
            engine.execute_string("t", "").await,
            Err(ScriptError::Failed(msg)) if msg.contains("compile failed")
        ));
        assert!(engine.last_error().is_some());
    }

    #[tokio::test]
    async fn an_unknown_variable_fails_the_evaluation() {
        let engine = engine().await;
        assert!(matches!(
            engine.execute_string("t", "missing_var").await,
            Err(ScriptError::Failed(msg)) if msg.contains("evaluation failed")
        ));
    }

    #[tokio::test]
    async fn globals_reach_the_expressions() {
        let engine = engine().await;
        engine
            .register_global("x", ScriptValue::Int(10))
            .expect("register global");
        assert_eq!(
            engine.get_global("x").expect("get global"),
            ScriptValue::Int(10)
        );
        assert_eq!(
            engine.execute_string("t", "x * 2").await.expect("eval"),
            ScriptValue::Int(20)
        );
    }

    #[tokio::test]
    async fn an_unknown_global_errors() {
        let engine = engine().await;
        assert!(matches!(
            engine.get_global("missing"),
            Err(ScriptError::Failed(msg)) if msg.contains("global not found")
        ));
    }

    #[tokio::test]
    async fn modules_flatten_into_prefixed_globals() {
        let engine = engine().await;
        let mut map = HashMap::new();
        map.insert("a".to_string(), ScriptValue::Int(1));
        map.insert("b".to_string(), ScriptValue::Int(2));
        engine
            .register_module("m", ScriptValue::Map(map))
            .expect("register module");
        assert_eq!(
            engine.execute_string("t", "m_a + m_b").await.expect("eval"),
            ScriptValue::Int(3)
        );
    }

    #[tokio::test]
    async fn a_non_map_module_is_rejected() {
        let engine = engine().await;
        assert!(matches!(
            engine.register_module("m", ScriptValue::Int(1)),
            Err(ScriptError::Failed(msg)) if msg.contains("expects a map")
        ));
    }

    #[tokio::test]
    async fn host_functions_call_through_the_engine() {
        let engine = engine().await;
        let host: HostFunction = Arc::new(
            |args: &[ScriptValue]| -> BoxFuture<'static, Result<ScriptValue, ScriptError>> {
                let sum = args
                    .iter()
                    .map(|v| match v {
                        ScriptValue::Int(i) => *i,
                        _ => 0,
                    })
                    .sum::<i64>();
                Box::pin(async move { Ok(ScriptValue::Int(sum)) })
            },
        );
        engine
            .register_function("sum", host)
            .expect("register function");
        let args = [ScriptValue::Int(1), ScriptValue::Int(2)];
        assert_eq!(
            engine
                .call_function("sum", &args)
                .await
                .expect("call function"),
            ScriptValue::Int(3)
        );
    }

    #[tokio::test]
    async fn an_unknown_function_errors() {
        let engine = engine().await;
        assert!(matches!(
            engine.call_function("missing", &[]).await,
            Err(ScriptError::Failed(msg)) if msg.contains("function not found")
        ));
    }

    #[tokio::test]
    async fn sources_drive_loads_and_evaluations() {
        let engine = engine().await;
        let source = Arc::new(MemSource::new());
        source.set("expr", "6 * 7");
        engine.set_source(Some(source));
        assert_eq!(
            engine.execute_from_key("expr").await.expect("eval"),
            ScriptValue::Int(42)
        );
    }

    #[tokio::test]
    async fn without_a_source_loads_fail() {
        let engine = engine().await;
        assert!(matches!(
            engine.load("k").await,
            Err(ScriptError::Failed(msg)) if msg.contains("no source bound")
        ));
    }

    #[tokio::test]
    async fn nothing_loaded_fails() {
        let engine = engine().await;
        assert!(matches!(
            engine.execute().await,
            Err(ScriptError::Failed(msg)) if msg.contains("no expression loaded")
        ));
    }

    #[tokio::test]
    async fn a_watch_signal_reloads_the_expression() {
        let engine = engine().await;
        let source = Arc::new(MemSource::new());
        source.set("expr", "1 + 1");
        engine.set_source(Some(source.clone()));
        assert_eq!(
            engine.execute_from_key("expr").await.expect("initial eval"),
            ScriptValue::Int(2)
        );
        engine.start_watch("expr").await.expect("start watch");
        source.set("expr", "2 + 3");
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        assert_eq!(
            engine
                .execute_from_key("expr")
                .await
                .expect("reloaded eval"),
            ScriptValue::Int(5)
        );
        engine.stop_watch("expr").expect("stop watch");
    }

    #[tokio::test]
    async fn uninitialized_engines_refuse_everything() {
        let engine = factory().expect("engine");
        assert!(!engine.is_initialized());
        assert!(engine.execute().await.is_err());
        assert!(engine.register_global("x", ScriptValue::Int(1)).is_err());
        engine.init().await.expect("init");
        engine.init().await.expect_err("second init refused");
        engine.close().expect("close");
        assert!(!engine.is_initialized());
    }

    #[tokio::test]
    async fn probes_offer_the_full_capability_set() {
        let engine: Arc<dyn ScriptEngine> = factory().expect("engine");
        assert!(engine.clone().as_loader().is_some());
        assert!(engine.clone().as_executor().is_some());
        assert!(engine.clone().as_global_accessor().is_some());
        assert!(engine.clone().as_function_registrar().is_some());
        assert!(engine.clone().as_module_registrar().is_some());
        assert!(engine.clone().as_watcher().is_some());
        assert!(engine.clone().as_sandbox_configurator().is_none());
        assert!(engine.clone().as_runtime_hook_registrar().is_none());
        assert!(engine.clone().as_sync_executor().is_none());
        assert!(engine.as_quota_controller().is_none());
    }
}
