//! Test doubles shared by the root-module test suites — a recording
//! mock engine, its recording factory, and a temp-dir guard.
//!
//! `MockEngine` implements the full aggregate plus the sandbox
//! capability; `MockNoSandbox` is the same
//! engine minus sandbox, guarding that `FullEngine` does not embed
//! it; `LifecycleOnly` implements just the core trait, the
//! lightweight-engine shape whose every probe answers `None`.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use crate::{
    factory, BoxFuture, EngineFactory, FullEngineFactory, FunctionRegistrar, GlobalAccessor,
    ModuleRegistrar, SandboxConfigurator, ScriptEngine, ScriptError, ScriptExecutor, ScriptLoader,
    ScriptValue, ScriptWatcher, SharedEngine, SharedScriptSource,
};

// ---------------------------------------------------------------------
// The recording state every mock shares.
// ---------------------------------------------------------------------

#[derive(Default)]
struct MockState {
    initialized: bool,
    source: Option<SharedScriptSource>,
    open_libs: Vec<String>,
    last_key: Option<String>,
    last_code: Option<String>,
    loaded: u32,
    executed: u32,
    registered_globals: HashMap<String, ScriptValue>,
    registered_functions: HashSet<String>,
    registered_modules: HashSet<String>,
    last_error: Option<ScriptError>,
    init_count: u32,
    close_count: u32,
}

macro_rules! mock_struct {
    ($t:ident) => {
        /// A recording implementation of the
        /// aggregate capability set (this variant's trait set is
        /// chosen by the impl macros below).
        ///
        /// The fixture generates the full accessor surface for every
        /// variant; variants that exercise a subset leave the rest
        /// dead, which is fine for a test double.
        #[allow(dead_code)]
        pub struct $t {
            /// The engine name this mock reports.
            pub typ: &'static str,
            /// Injected init failure.
            pub init_err: Option<ScriptError>,
            /// Injected close failure.
            pub close_err: Option<ScriptError>,
            /// Injected load failure.
            pub load_err: Option<ScriptError>,
            /// Injected execute failure.
            pub exec_err: Option<ScriptError>,
            /// Scripted CallFunction result.
            pub call_result: Option<ScriptValue>,
            /// Injected CallFunction failure.
            pub call_err: Option<ScriptError>,
            state: Mutex<MockState>,
        }

        #[allow(dead_code)]
        impl $t {
            /// Builds the mock with no injected failures.
            pub fn new(typ: &'static str) -> Self {
                Self {
                    typ,
                    init_err: None,
                    close_err: None,
                    load_err: None,
                    exec_err: None,
                    call_result: None,
                    call_err: None,
                    state: Mutex::new(MockState::default()),
                }
            }

            /// Recorded load count.
            pub fn loaded(&self) -> u32 {
                self.state.lock().expect("mock state lock").loaded
            }

            /// Recorded execute count.
            pub fn executed(&self) -> u32 {
                self.state.lock().expect("mock state lock").executed
            }

            /// Recorded init count.
            pub fn init_count(&self) -> u32 {
                self.state.lock().expect("mock state lock").init_count
            }

            /// Recorded close count.
            pub fn close_count(&self) -> u32 {
                self.state.lock().expect("mock state lock").close_count
            }

            /// Recorded sandbox allow-list.
            pub fn open_libs(&self) -> Vec<String> {
                self.state
                    .lock()
                    .expect("mock state lock")
                    .open_libs
                    .clone()
            }

            /// Injects the engine's recorded last error (the pool
            /// wrappers' `get_last_error` reads it back).
            pub fn set_last_error(&self, err: Option<ScriptError>) {
                self.state.lock().expect("mock state lock").last_error = err;
            }

            /// The last key handed to a load.
            pub fn last_key(&self) -> Option<String> {
                self.state.lock().expect("mock state lock").last_key.clone()
            }

            /// The last script content loaded.
            pub fn last_code(&self) -> Option<String> {
                self.state
                    .lock()
                    .expect("mock state lock")
                    .last_code
                    .clone()
            }

            /// The names registered as globals.
            pub fn registered_globals(&self) -> Vec<String> {
                self.state
                    .lock()
                    .expect("mock state lock")
                    .registered_globals
                    .keys()
                    .cloned()
                    .collect()
            }

            /// The names registered as host functions.
            pub fn registered_functions(&self) -> Vec<String> {
                self.state
                    .lock()
                    .expect("mock state lock")
                    .registered_functions
                    .iter()
                    .cloned()
                    .collect()
            }

            /// The names registered as modules.
            pub fn registered_modules(&self) -> Vec<String> {
                self.state
                    .lock()
                    .expect("mock state lock")
                    .registered_modules
                    .iter()
                    .cloned()
                    .collect()
            }
        }
    };
}

mock_struct!(MockEngine);
mock_struct!(MockNoSandbox);
mock_struct!(LifecycleOnly);

// ---------------------------------------------------------------------
// The core trait. `full` overrides the six aggregate-capability
// probes; `full_with_sandbox` also overrides the sandbox probe;
// `core` keeps every probe at its default.
// ---------------------------------------------------------------------

macro_rules! mock_core_body {
    () => {
        fn engine_type(&self) -> &'static str {
            self.typ
        }
        fn init(&self) -> BoxFuture<'_, Result<(), ScriptError>> {
            let outcome = {
                let mut state = self.state.lock().expect("mock state lock");
                state.init_count += 1;
                if let Some(err) = self.init_err.clone() {
                    Err(err)
                } else {
                    state.initialized = true;
                    Ok(())
                }
            };
            Box::pin(async move { outcome })
        }
        fn close(&self) -> Result<(), ScriptError> {
            let mut state = self.state.lock().expect("mock state lock");
            state.close_count += 1;
            if let Some(err) = self.close_err.clone() {
                return Err(err);
            }
            state.initialized = false;
            Ok(())
        }
        fn is_initialized(&self) -> bool {
            self.state.lock().expect("mock state lock").initialized
        }
        fn last_error(&self) -> Option<ScriptError> {
            self.state
                .lock()
                .expect("mock state lock")
                .last_error
                .clone()
        }
        fn clear_error(&self) {
            self.state.lock().expect("mock state lock").last_error = None;
        }
    };
}

macro_rules! mock_engine_impl {
    ($t:ty, core) => {
        impl ScriptEngine for $t {
            mock_core_body!();
        }
    };
    ($t:ty, full) => {
        impl ScriptEngine for $t {
            mock_core_body!();

            fn as_loader(self: Arc<Self>) -> Option<Arc<dyn ScriptLoader>> {
                Some(self)
            }
            fn as_executor(self: Arc<Self>) -> Option<Arc<dyn ScriptExecutor>> {
                Some(self)
            }
            fn as_global_accessor(self: Arc<Self>) -> Option<Arc<dyn GlobalAccessor>> {
                Some(self)
            }
            fn as_function_registrar(self: Arc<Self>) -> Option<Arc<dyn FunctionRegistrar>> {
                Some(self)
            }
            fn as_module_registrar(self: Arc<Self>) -> Option<Arc<dyn ModuleRegistrar>> {
                Some(self)
            }
            fn as_watcher(self: Arc<Self>) -> Option<Arc<dyn ScriptWatcher>> {
                Some(self)
            }
        }
    };
    ($t:ty, full_with_sandbox) => {
        impl ScriptEngine for $t {
            mock_core_body!();

            fn as_loader(self: Arc<Self>) -> Option<Arc<dyn ScriptLoader>> {
                Some(self)
            }
            fn as_executor(self: Arc<Self>) -> Option<Arc<dyn ScriptExecutor>> {
                Some(self)
            }
            fn as_global_accessor(self: Arc<Self>) -> Option<Arc<dyn GlobalAccessor>> {
                Some(self)
            }
            fn as_function_registrar(self: Arc<Self>) -> Option<Arc<dyn FunctionRegistrar>> {
                Some(self)
            }
            fn as_module_registrar(self: Arc<Self>) -> Option<Arc<dyn ModuleRegistrar>> {
                Some(self)
            }
            fn as_watcher(self: Arc<Self>) -> Option<Arc<dyn ScriptWatcher>> {
                Some(self)
            }
            fn as_sandbox_configurator(self: Arc<Self>) -> Option<Arc<dyn SandboxConfigurator>> {
                Some(self)
            }
        }
    };
}

mock_engine_impl!(MockEngine, full_with_sandbox);
mock_engine_impl!(MockNoSandbox, full);
mock_engine_impl!(LifecycleOnly, core);

// ---------------------------------------------------------------------
// The aggregate capability traits, shared by both full mocks.
// ---------------------------------------------------------------------

macro_rules! mock_loader_impl {
    ($t:ty) => {
        impl ScriptLoader for $t {
            fn set_source(&self, source: Option<SharedScriptSource>) {
                self.state.lock().expect("mock state lock").source = source;
            }
            fn get_source(&self) -> Option<SharedScriptSource> {
                self.state.lock().expect("mock state lock").source.clone()
            }
            fn load<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<(), ScriptError>> {
                // Record first, then read through the bound source —
                // the two phases never hold the state lock across the
                // source's future.
                let source = {
                    let mut state = self.state.lock().expect("mock state lock");
                    state.last_key = Some(key.to_string());
                    state.loaded += 1;
                    state.source.clone()
                };
                let injected = self.load_err.clone();
                Box::pin(async move {
                    if let Some(err) = injected {
                        return Err(err);
                    }
                    if let Some(source) = source {
                        let code = source.load(key).await?;
                        self.state.lock().expect("mock state lock").last_code = Some(code);
                    }
                    Ok(())
                })
            }
            fn load_multi<'a>(
                &'a self,
                keys: &'a [String],
            ) -> BoxFuture<'a, Result<(), ScriptError>> {
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
                {
                    let mut state = self.state.lock().expect("mock state lock");
                    state.last_code = Some(code.to_string());
                    state.loaded += 1;
                }
                Box::pin(async { Ok(()) })
            }
        }
    };
}

macro_rules! mock_executor_impl {
    ($t:ty) => {
        impl ScriptExecutor for $t {
            fn execute(&self) -> BoxFuture<'_, Result<ScriptValue, ScriptError>> {
                {
                    let mut state = self.state.lock().expect("mock state lock");
                    state.executed += 1;
                }
                let injected = self.exec_err.clone();
                Box::pin(async move {
                    if let Some(err) = injected {
                        return Err(err);
                    }
                    Ok(ScriptValue::String("exec-result".to_string()))
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
                _name: &'a str,
                code: &'a str,
            ) -> BoxFuture<'a, Result<ScriptValue, ScriptError>> {
                {
                    let mut state = self.state.lock().expect("mock state lock");
                    state.last_code = Some(code.to_string());
                    state.executed += 1;
                }
                let injected = self.exec_err.clone();
                Box::pin(async move {
                    if let Some(err) = injected {
                        return Err(err);
                    }
                    Ok(ScriptValue::String(code.to_string()))
                })
            }
        }
    };
}

macro_rules! mock_globals_impl {
    ($t:ty) => {
        impl GlobalAccessor for $t {
            fn register_global(&self, name: &str, value: ScriptValue) -> Result<(), ScriptError> {
                self.state
                    .lock()
                    .expect("mock state lock")
                    .registered_globals
                    .insert(name.to_string(), value);
                Ok(())
            }
            fn get_global(&self, name: &str) -> Result<ScriptValue, ScriptError> {
                match self
                    .state
                    .lock()
                    .expect("mock state lock")
                    .registered_globals
                    .get(name)
                {
                    Some(value) => Ok(value.clone()),
                    None => Err(ScriptError::Failed("not found".to_string())),
                }
            }
        }
    };
}

macro_rules! mock_functions_impl {
    ($t:ty) => {
        impl FunctionRegistrar for $t {
            fn register_function(
                &self,
                name: &str,
                _function: crate::HostFunction,
            ) -> Result<(), ScriptError> {
                self.state
                    .lock()
                    .expect("mock state lock")
                    .registered_functions
                    .insert(name.to_string());
                Ok(())
            }
            fn call_function<'a>(
                &'a self,
                _name: &'a str,
                _args: &'a [ScriptValue],
            ) -> BoxFuture<'a, Result<ScriptValue, ScriptError>> {
                let outcome = match (self.call_err.clone(), self.call_result.clone()) {
                    (Some(err), _) => Err(err),
                    (None, Some(value)) => Ok(value),
                    (None, None) => Ok(ScriptValue::Null),
                };
                Box::pin(async move { outcome })
            }
        }
    };
}

macro_rules! mock_modules_impl {
    ($t:ty) => {
        impl ModuleRegistrar for $t {
            fn register_module(&self, name: &str, _module: ScriptValue) -> Result<(), ScriptError> {
                self.state
                    .lock()
                    .expect("mock state lock")
                    .registered_modules
                    .insert(name.to_string());
                Ok(())
            }
        }
    };
}

macro_rules! mock_watcher_impl {
    ($t:ty) => {
        impl ScriptWatcher for $t {
            fn start_watch<'a>(&'a self, _key: &'a str) -> BoxFuture<'a, Result<(), ScriptError>> {
                Box::pin(async { Ok(()) })
            }
            fn stop_watch(&self, _key: &str) -> Result<(), ScriptError> {
                Ok(())
            }
        }
    };
}

mock_loader_impl!(MockEngine);
mock_executor_impl!(MockEngine);
mock_globals_impl!(MockEngine);
mock_functions_impl!(MockEngine);
mock_modules_impl!(MockEngine);
mock_watcher_impl!(MockEngine);
mock_loader_impl!(MockNoSandbox);
mock_executor_impl!(MockNoSandbox);
mock_globals_impl!(MockNoSandbox);
mock_functions_impl!(MockNoSandbox);
mock_modules_impl!(MockNoSandbox);
mock_watcher_impl!(MockNoSandbox);

impl SandboxConfigurator for MockEngine {
    fn set_open_libs(&self, libs: &[&str]) {
        let mut state = self.state.lock().expect("mock state lock");
        state.open_libs = libs.iter().map(|lib| lib.to_string()).collect();
    }
}

// ---------------------------------------------------------------------
// The recording factories.
// ---------------------------------------------------------------------

/// A factory handle recording every engine it produced and every
/// production attempt, with injected failures.
pub struct MockEngineFactory {
    typ: &'static str,
    init_err: Option<ScriptError>,
    close_err: Option<ScriptError>,
    created: Mutex<Vec<Arc<MockEngine>>>,
    attempts: AtomicUsize,
}

impl MockEngineFactory {
    /// Number of engines the factory produced.
    pub fn created_count(&self) -> usize {
        self.created.lock().expect("factory created lock").len()
    }

    /// Number of production attempts, successful or not.
    pub fn attempts(&self) -> usize {
        self.attempts.load(Ordering::SeqCst)
    }

    /// One of the produced engines.
    pub fn created(&self, index: usize) -> Arc<MockEngine> {
        self.created
            .lock()
            .expect("factory created lock")
            .get(index)
            .expect("engine exists")
            .clone()
    }
}

/// A registry registration guard: unregistering on drop.
pub struct RegisteredFactory {
    /// The recording factory handle.
    pub factory: Arc<MockEngineFactory>,
    typ: &'static str,
}

impl Drop for RegisteredFactory {
    fn drop(&mut self) {
        factory::unregister_factory(self.typ);
    }
}

/// Registers a construction-only [`EngineFactory`] under `typ` that
/// produces [`MockEngine`]s with the injected failures, recording
/// every instance; the guard unregisters on drop.
pub fn with_factory_type(
    typ: &'static str,
    init_err: Option<ScriptError>,
    close_err: Option<ScriptError>,
) -> RegisteredFactory {
    let handle = Arc::new(MockEngineFactory {
        typ,
        init_err,
        close_err,
        created: Mutex::new(Vec::new()),
        attempts: AtomicUsize::new(0),
    });
    let closure_handle = handle.clone();
    let registered: EngineFactory = Arc::new(move || -> Result<SharedEngine, ScriptError> {
        closure_handle.attempts.fetch_add(1, Ordering::SeqCst);
        let mut mock = MockEngine::new(closure_handle.typ);
        mock.init_err = closure_handle.init_err.clone();
        mock.close_err = closure_handle.close_err.clone();
        let mock = Arc::new(mock);
        closure_handle
            .created
            .lock()
            .expect("factory created lock")
            .push(mock.clone());
        Ok(mock)
    });
    factory::register_factory(typ, registered).expect("register test factory");
    RegisteredFactory {
        factory: handle,
        typ,
    }
}

/// Builds a [`FullEngineFactory`] that constructs, optionally
/// pre-configures (the sandbox allow-list — the per-instance pre-init
/// configuration the shape exists for), and initializes a
/// [`MockEngine`], recording every instance. With `fail_init` the
/// produced engine's initialization fails, making the factory itself
/// fail from the pool's perspective. The factory is handed to the
/// caller, not the registry — [`crate::AutoGrowEnginePool::
/// with_factory`] takes it directly.
pub fn with_full_factory(
    typ: &'static str,
    preconfigure: Option<&'static [&'static str]>,
    fail_init: bool,
) -> (Arc<MockEngineFactory>, FullEngineFactory) {
    let handle = Arc::new(MockEngineFactory {
        typ,
        init_err: None,
        close_err: None,
        created: Mutex::new(Vec::new()),
        attempts: AtomicUsize::new(0),
    });
    let closure_handle = handle.clone();
    let factory: FullEngineFactory = Arc::new(
        move || -> BoxFuture<'static, Result<SharedEngine, ScriptError>> {
            let closure_handle = closure_handle.clone();
            Box::pin(async move {
                closure_handle.attempts.fetch_add(1, Ordering::SeqCst);
                let mut mock = MockEngine::new(closure_handle.typ);
                if fail_init {
                    mock.init_err = Some(ScriptError::Failed("factory init refused".to_string()));
                }
                let mock = Arc::new(mock);
                if let Some(libs) = preconfigure {
                    mock.set_open_libs(libs);
                }
                mock.init().await?;
                closure_handle
                    .created
                    .lock()
                    .expect("factory created lock")
                    .push(mock.clone());
                let shared: SharedEngine = mock;
                Ok(shared)
            })
        },
    );
    (handle, factory)
}

// ---------------------------------------------------------------------
// The temp-dir guard.
// ---------------------------------------------------------------------

static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

/// A uniquely-named temp directory removed on drop.
pub struct TempDir {
    dir: std::path::PathBuf,
}

impl TempDir {
    /// Creates the directory.
    pub fn new(tag: &str) -> Self {
        let unique = TEMP_COUNTER.fetch_add(1, Ordering::SeqCst);
        let dir = std::env::temp_dir().join(format!(
            "rushwind-script-{tag}-{}-{unique}",
            std::process::id()
        ));
        std::fs::create_dir_all(&dir).expect("temp dir");
        Self { dir }
    }

    /// A path inside the directory. No file is created.
    pub fn path(&self, name: &str) -> std::path::PathBuf {
        self.dir.join(name)
    }

    /// Writes a file inside the directory (creating parent
    /// directories) and returns its path.
    pub fn write(&self, name: &str, content: &str) -> std::path::PathBuf {
        let path = self.dir.join(name);
        if let Some(parent) = path.parent() {
            let _ = std::fs::create_dir_all(parent);
        }
        std::fs::write(&path, content).expect("write temp file");
        path
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}
