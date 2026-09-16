//! The fixed-size engine pool: `size` engines built up front through
//! the factory registry, lent out one at a time.
//!
//! The pool is the standard
//! queue-plus-counting-semaphore rendering of a lent-out engine set — every
//! queued engine has exactly one permit, acquiring consumes a permit
//! and dequeues, releasing enqueues and re-adds one — with
//! [`Semaphore::close`] waking every
//! blocked acquirer as a failure.
//!
//! The per-call wrapper methods follow an acquire-invoke-
//! release pattern so callers avoid the boilerplate for one-shot use.
//! The binding they leave behind (source, globals, functions, modules)
//! is **local to the engine instance they happened to acquire** — for
//! pool-wide setup, acquire and configure each engine yourself.
//!
//! A null-engine release is unnecessary (`SharedEngine` cannot be
//! null), and the closed flag plus the queue bound
//! together make an overflowing queue unreachable.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use tokio::sync::Semaphore;

use crate::factory;
use crate::{BoxFuture, HostFunction, ScriptError, ScriptValue, SharedEngine, SharedScriptSource};

/// A fixed-size pool of independently-initialized engines.
pub struct EnginePool {
    queue: Mutex<VecDeque<SharedEngine>>,
    semaphore: Arc<Semaphore>,
    closed: AtomicBool,
    size: usize,
}

impl EnginePool {
    /// Creates and initializes a pool of `size` engines produced by
    /// the factory registered under `typ`.
    ///
    /// On any construction or initialization failure every engine
    /// created so far — including the failing one — is closed and the
    /// pool is not built.
    pub async fn new(size: usize, typ: &str) -> Result<Self, ScriptError> {
        if size < 1 {
            return Err(ScriptError::Failed(
                "engine pool: pool size must be >= 1".to_string(),
            ));
        }
        let factory = factory::get_factory(typ).ok_or_else(|| {
            ScriptError::Failed(format!("script engine factory {typ:?} not registered"))
        })?;

        let mut created: Vec<SharedEngine> = Vec::with_capacity(size);
        for _ in 0..size {
            let engine = match factory() {
                Ok(engine) => engine,
                Err(err) => {
                    for engine in &created {
                        let _ = engine.close();
                    }
                    return Err(ScriptError::Failed(format!(
                        "engine pool: factory failed: {err}"
                    )));
                }
            };
            if let Err(err) = engine.init().await {
                let _ = engine.close();
                for engine in &created {
                    let _ = engine.close();
                }
                return Err(ScriptError::Failed(format!(
                    "engine pool: init failed: {err}"
                )));
            }
            created.push(engine);
        }

        let queue = VecDeque::from(created);
        let permits = queue.len();
        Ok(Self {
            queue: Mutex::new(queue),
            semaphore: Arc::new(Semaphore::new(permits)),
            closed: AtomicBool::new(false),
            size,
        })
    }

    /// Takes an engine out of the pool, blocking until one is
    /// available. Fails once the pool is closed.
    pub async fn acquire(&self) -> Result<SharedEngine, ScriptError> {
        if self.closed.load(Ordering::Acquire) {
            return Err(ScriptError::Failed("engine pool closed".to_string()));
        }
        // The permit is held through the dequeue so a permit can never
        // be re-acquired between its release and the engine leaving
        // the queue.
        let permit = match Arc::clone(&self.semaphore).acquire_owned().await {
            Ok(permit) => permit,
            Err(_) => return Err(ScriptError::Failed("engine pool closed".to_string())),
        };
        if self.closed.load(Ordering::Acquire) {
            return Err(ScriptError::Failed("engine pool closed".to_string()));
        }
        let engine = self
            .queue
            .lock()
            .expect("engine pool queue lock")
            .pop_front();
        // The permit stood for this queued engine; the engine leaving
        // the queue takes its permit out of circulation entirely —
        // dropping (rather than forgetting) it would hand a second
        // caller a permit with nothing behind it.
        permit.forget();
        engine.ok_or_else(|| ScriptError::Failed("engine pool closed".to_string()))
    }

    /// Returns an engine to the pool. If the pool is closed — or the
    /// queue is already full, which the permit accounting makes
    /// unreachable — the engine is closed instead.
    pub fn release(&self, engine: SharedEngine) {
        if self.closed.load(Ordering::Acquire) {
            let _ = engine.close();
            return;
        }
        let accepted = {
            let queue = self.queue.lock().expect("engine pool queue lock");
            queue.len() < self.size
        };
        if accepted {
            let mut queue = self.queue.lock().expect("engine pool queue lock");
            queue.push_back(engine);
            drop(queue);
            self.semaphore.add_permits(1);
        } else {
            let _ = engine.close();
        }
    }

    /// Closes the pool and destroys every engine left in it. A second
    /// close is a no-op. Engines still lent out are closed by their
    /// borrowers' releases.
    pub fn close(&self) -> Result<(), ScriptError> {
        if self.closed.swap(true, Ordering::AcqRel) {
            return Ok(());
        }
        self.semaphore.close();
        let drained: Vec<SharedEngine> = {
            let mut queue = self.queue.lock().expect("engine pool queue lock");
            queue.drain(..).collect()
        };
        let mut last: Result<(), ScriptError> = Ok(());
        for engine in drained {
            if let Err(err) = engine.close() {
                last = Err(err);
            }
        }
        last
    }

    /// Reports whether the pool is closed.
    pub fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }

    /// Re-initializes every engine in the pool: acquires them all,
    /// initializes each, and releases them back. On any failure every
    /// acquired engine is closed.
    pub async fn init_all(&self) -> Result<(), ScriptError> {
        let mut engines: Vec<SharedEngine> = Vec::with_capacity(self.size);
        for _ in 0..self.size {
            match self.acquire().await {
                Ok(engine) => engines.push(engine),
                Err(err) => {
                    for engine in engines {
                        let _ = engine.close();
                    }
                    return Err(err);
                }
            }
        }
        let mut failure: Option<ScriptError> = None;
        for engine in &engines {
            if let Err(err) = engine.init().await {
                failure = Some(ScriptError::Failed(format!(
                    "engine pool: init failed: {err}"
                )));
                break;
            }
        }
        if let Some(err) = failure {
            for engine in engines {
                let _ = engine.close();
            }
            return Err(err);
        }
        for engine in engines {
            self.release(engine);
        }
        Ok(())
    }

    // ------------------------------------------------------------------
    // Per-call wrappers: acquire one engine, invoke, release.
    // ------------------------------------------------------------------

    /// Binds a source on an acquired engine (local to that instance).
    pub fn set_source<'a>(&'a self, source: Option<SharedScriptSource>) -> BoxFuture<'a, ()> {
        Box::pin(async move {
            let Ok(engine) = self.acquire().await else {
                return;
            };
            engine.set_source(source);
            self.release(engine);
        })
    }

    /// Returns the source bound on an acquired engine, if any.
    pub fn get_source(&self) -> BoxFuture<'_, Option<SharedScriptSource>> {
        Box::pin(async move {
            let engine = self.acquire().await.ok()?;
            let source = engine.get_source();
            self.release(engine);
            source
        })
    }

    /// Loads one script from the bound source on an acquired engine.
    pub fn load<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<(), ScriptError>> {
        Box::pin(async move {
            let engine = self.acquire().await?;
            let result = engine.load(key).await;
            self.release(engine);
            result
        })
    }

    /// Loads several scripts from the bound source on an acquired
    /// engine, in order.
    pub fn load_multi<'a>(&'a self, keys: &'a [String]) -> BoxFuture<'a, Result<(), ScriptError>> {
        Box::pin(async move {
            let engine = self.acquire().await?;
            let result = engine.load_multi(keys).await;
            self.release(engine);
            result
        })
    }

    /// Compiles an inline script (bypassing any bound source) on an
    /// acquired engine.
    pub fn load_string<'a>(
        &'a self,
        name: &'a str,
        code: &'a str,
    ) -> BoxFuture<'a, Result<(), ScriptError>> {
        Box::pin(async move {
            let engine = self.acquire().await?;
            let result = engine.load_string(name, code).await;
            self.release(engine);
            result
        })
    }

    /// Runs every script loaded on an acquired engine.
    pub fn execute(&self) -> BoxFuture<'_, Result<ScriptValue, ScriptError>> {
        Box::pin(async move {
            let engine = self.acquire().await?;
            let result = engine.execute().await;
            self.release(engine);
            result
        })
    }

    /// Loads and immediately runs one script from the bound source on
    /// an acquired engine.
    pub fn execute_from_key<'a>(
        &'a self,
        key: &'a str,
    ) -> BoxFuture<'a, Result<ScriptValue, ScriptError>> {
        Box::pin(async move {
            let engine = self.acquire().await?;
            let result = engine.execute_from_key(key).await;
            self.release(engine);
            result
        })
    }

    /// The multi-key variant of [`EnginePool::execute_from_key`].
    pub fn execute_from_keys<'a>(
        &'a self,
        keys: &'a [String],
    ) -> BoxFuture<'a, Result<Vec<ScriptValue>, ScriptError>> {
        Box::pin(async move {
            let engine = self.acquire().await?;
            let result = engine.execute_from_keys(keys).await;
            self.release(engine);
            result
        })
    }

    /// Compiles and immediately runs an inline script on an acquired
    /// engine.
    pub fn execute_string<'a>(
        &'a self,
        name: &'a str,
        code: &'a str,
    ) -> BoxFuture<'a, Result<ScriptValue, ScriptError>> {
        Box::pin(async move {
            let engine = self.acquire().await?;
            let result = engine.execute_string(name, code).await;
            self.release(engine);
            result
        })
    }

    /// Registers a global variable on an acquired engine (local to
    /// that instance).
    pub fn register_global<'a>(
        &'a self,
        name: &'a str,
        value: ScriptValue,
    ) -> BoxFuture<'a, Result<(), ScriptError>> {
        Box::pin(async move {
            let engine = self.acquire().await?;
            let result = engine.register_global(name, value);
            self.release(engine);
            result
        })
    }

    /// Reads a global variable from an acquired engine.
    pub fn get_global<'a>(
        &'a self,
        name: &'a str,
    ) -> BoxFuture<'a, Result<ScriptValue, ScriptError>> {
        Box::pin(async move {
            let engine = self.acquire().await?;
            let result = engine.get_global(name);
            self.release(engine);
            result
        })
    }

    /// Registers a host function on an acquired engine (local to that
    /// instance).
    pub fn register_function<'a>(
        &'a self,
        name: &'a str,
        function: HostFunction,
    ) -> BoxFuture<'a, Result<(), ScriptError>> {
        Box::pin(async move {
            let engine = self.acquire().await?;
            let result = engine.register_function(name, function);
            self.release(engine);
            result
        })
    }

    /// Invokes a script-side function on an acquired engine.
    pub fn call_function<'a>(
        &'a self,
        name: &'a str,
        args: &'a [ScriptValue],
    ) -> BoxFuture<'a, Result<ScriptValue, ScriptError>> {
        Box::pin(async move {
            let engine = self.acquire().await?;
            let result = engine.call_function(name, args).await;
            self.release(engine);
            result
        })
    }

    /// Registers a module on an acquired engine (local to that
    /// instance).
    pub fn register_module<'a>(
        &'a self,
        name: &'a str,
        module: ScriptValue,
    ) -> BoxFuture<'a, Result<(), ScriptError>> {
        Box::pin(async move {
            let engine = self.acquire().await?;
            let result = engine.register_module(name, module);
            self.release(engine);
            result
        })
    }

    /// Starts watching a script key on an acquired engine (local to
    /// that instance).
    pub fn start_watch<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<(), ScriptError>> {
        Box::pin(async move {
            let engine = self.acquire().await?;
            let result = engine.start_watch(key).await;
            self.release(engine);
            result
        })
    }

    /// Stops watching a script key on an acquired engine.
    pub fn stop_watch<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<(), ScriptError>> {
        Box::pin(async move {
            let engine = self.acquire().await?;
            let result = engine.stop_watch(key);
            self.release(engine);
            result
        })
    }

    /// Returns the last error recorded on an acquired engine.
    pub fn get_last_error(&self) -> BoxFuture<'_, Option<ScriptError>> {
        Box::pin(async move {
            let engine = self.acquire().await.ok()?;
            let error = engine.last_error();
            self.release(engine);
            error
        })
    }

    /// Clears the last-error state on an acquired engine.
    pub fn clear_error(&self) -> BoxFuture<'_, ()> {
        Box::pin(async move {
            let Ok(engine) = self.acquire().await else {
                return;
            };
            engine.clear_error();
            self.release(engine);
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testutil::with_factory_type;
    use crate::{MemSource, ScriptError, ScriptValue};
    use std::collections::HashMap;
    use std::time::Duration;

    /// A size-1 pool over a healthy registered mock factory.
    async fn one_engine(
        typ: &'static str,
    ) -> (
        std::sync::Arc<EnginePool>,
        crate::testutil::RegisteredFactory,
    ) {
        let registered = with_factory_type(typ, None, None);
        let pool = EnginePool::new(1, typ).await.expect("pool comes up");
        assert_eq!(registered.factory.created_count(), 1);
        (std::sync::Arc::new(pool), registered)
    }

    #[tokio::test]
    async fn a_zero_size_pool_is_rejected() {
        let registered = with_factory_type("mock-pool-zerosize", None, None);
        assert!(matches!(
            EnginePool::new(0, "mock-pool-zerosize").await,
            Err(ScriptError::Failed(msg)) if msg.contains("size must be >= 1")
        ));
        assert_eq!(registered.factory.attempts(), 0);
    }

    #[tokio::test]
    async fn an_unregistered_type_is_rejected() {
        assert!(matches!(
            EnginePool::new(1, "mock-pool-missing").await,
            Err(ScriptError::Failed(msg)) if msg.contains("not registered")
        ));
    }

    #[tokio::test]
    async fn an_init_failure_closes_the_engine_and_aborts_the_pool() {
        let registered = with_factory_type(
            "mock-pool-initfail",
            Some(ScriptError::Failed("init refused".to_string())),
            None,
        );
        let Err(err) = EnginePool::new(2, "mock-pool-initfail").await else {
            panic!("the pool must fail when init fails");
        };
        assert!(err.to_string().contains("init failed"));
        assert_eq!(registered.factory.attempts(), 1);
        // The factory recorded the engine it built; the pool then
        // closed it as part of the failure cleanup.
        assert_eq!(registered.factory.created_count(), 1);
        assert_eq!(registered.factory.created(0).close_count(), 1);
    }

    #[tokio::test]
    async fn acquire_and_release_round_trip() {
        let (pool, _registered) = one_engine("mock-pool-roundtrip").await;
        let engine = pool.acquire().await.expect("acquire");
        assert_eq!(engine.engine_type(), "mock-pool-roundtrip");
        pool.release(engine);
        assert!(!pool.is_closed());
    }

    #[tokio::test]
    async fn acquire_blocks_until_a_release() {
        let (pool, _registered) = one_engine("mock-pool-blocks").await;
        let engine = pool.acquire().await.expect("first acquire");
        let releaser = {
            let pool = pool.clone();
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(300)).await;
                pool.release(engine);
            })
        };
        let blocked = tokio::time::timeout(Duration::from_millis(150), pool.acquire()).await;
        assert!(blocked.is_err(), "the lone engine is checked out");
        let _ = releaser.await;
        let second = tokio::time::timeout(Duration::from_secs(2), pool.acquire())
            .await
            .expect("unblocks after release")
            .expect("acquire");
        pool.release(second);
    }

    #[tokio::test]
    async fn a_closed_pool_fails_acquire_and_closes_releases() {
        let (pool, registered) = one_engine("mock-pool-closed").await;
        let engine = pool.acquire().await.expect("acquire");
        pool.close().expect("close");
        assert!(pool.is_closed());
        assert!(matches!(
            pool.acquire().await,
            Err(ScriptError::Failed(msg)) if msg.contains("closed")
        ));
        pool.release(engine);
        assert_eq!(registered.factory.created(0).close_count(), 1);
    }

    #[tokio::test]
    async fn close_is_idempotent() {
        let (pool, _registered) = one_engine("mock-pool-closetwice").await;
        pool.close().expect("first close");
        pool.close().expect("second close is a no-op");
    }

    #[tokio::test]
    async fn close_destroys_the_idle_engines() {
        let registered = with_factory_type("mock-pool-closeidle", None, None);
        let pool = EnginePool::new(2, "mock-pool-closeidle")
            .await
            .expect("pool");
        pool.close().expect("close destroys the idle engines");
        assert_eq!(registered.factory.created(0).close_count(), 1);
        assert_eq!(registered.factory.created(1).close_count(), 1);
    }

    #[tokio::test]
    async fn init_all_reinitializes_every_engine() {
        let registered = with_factory_type("mock-pool-initall", None, None);
        let pool = EnginePool::new(2, "mock-pool-initall").await.expect("pool");
        pool.init_all().await.expect("init all");
        assert_eq!(registered.factory.created(0).init_count(), 2);
        assert_eq!(registered.factory.created(1).init_count(), 2);
        pool.close().expect("close");
    }

    #[tokio::test]
    async fn sources_bind_and_load_through_the_pool() {
        let (pool, registered) = one_engine("mock-pool-source").await;
        let mem = std::sync::Arc::new(MemSource::new());
        mem.set("k", "mem-code");
        pool.set_source(Some(mem.clone())).await;
        let bound = pool.get_source().await.expect("source bound");
        assert_eq!(bound.load("k").await.expect("bound load"), "mem-code");
        pool.load("k").await.expect("pool load");
        let mock = registered.factory.created(0);
        assert_eq!(mock.loaded(), 1);
        assert_eq!(mock.last_key().as_deref(), Some("k"));
        assert_eq!(mock.last_code().as_deref(), Some("mem-code"));
    }

    #[tokio::test]
    async fn load_multi_loads_each_key() {
        let (pool, registered) = one_engine("mock-pool-loadmulti").await;
        let keys = vec!["k1".to_string(), "k2".to_string()];
        pool.load_multi(&keys).await.expect("load multi");
        assert_eq!(registered.factory.created(0).loaded(), 2);
    }

    #[tokio::test]
    async fn execute_runs_the_loaded_scripts() {
        let (pool, registered) = one_engine("mock-pool-execute").await;
        let value = pool.execute().await.expect("execute");
        assert_eq!(value, ScriptValue::String("exec-result".to_string()));
        assert_eq!(registered.factory.created(0).executed(), 1);
    }

    #[tokio::test]
    async fn execute_from_key_loads_then_runs() {
        let (pool, registered) = one_engine("mock-pool-execfromkey").await;
        let mem = std::sync::Arc::new(MemSource::new());
        mem.set("k", "mem-code");
        pool.set_source(Some(mem.clone())).await;
        let value = pool.execute_from_key("k").await.expect("execute from key");
        assert_eq!(value, ScriptValue::String("exec-result".to_string()));
        let mock = registered.factory.created(0);
        assert_eq!(mock.loaded(), 1);
        assert_eq!(mock.executed(), 1);
    }

    #[tokio::test]
    async fn execute_from_keys_runs_each_in_order() {
        let (pool, _registered) = one_engine("mock-pool-execfromkeys").await;
        let keys = vec!["k1".to_string(), "k2".to_string()];
        let results = pool.execute_from_keys(&keys).await.expect("results");
        assert_eq!(results.len(), 2);
        assert_eq!(results[0], ScriptValue::String("exec-result".to_string()));
        assert_eq!(results[1], ScriptValue::String("exec-result".to_string()));
    }

    #[tokio::test]
    async fn inline_strings_load_and_run() {
        let (pool, registered) = one_engine("mock-pool-inlinestrings").await;
        pool.load_string("inline", "code-1")
            .await
            .expect("load string");
        let value = pool
            .execute_string("inline", "code-2")
            .await
            .expect("execute string");
        assert_eq!(value, ScriptValue::String("code-2".to_string()));
        let mock = registered.factory.created(0);
        assert_eq!(mock.loaded(), 1);
        assert_eq!(mock.executed(), 1);
        assert_eq!(mock.last_code().as_deref(), Some("code-2"));
    }

    #[tokio::test]
    async fn globals_round_trip_through_the_pool() {
        let (pool, registered) = one_engine("mock-pool-globals").await;
        pool.register_global("g", ScriptValue::Int(7))
            .await
            .expect("register global");
        assert_eq!(
            pool.get_global("g").await.expect("get global"),
            ScriptValue::Int(7)
        );
        assert_eq!(
            registered.factory.created(0).registered_globals(),
            vec!["g".to_string()]
        );
    }

    #[tokio::test]
    async fn functions_register_and_call_through_the_pool() {
        let (pool, registered) = one_engine("mock-pool-functions").await;
        let host: crate::HostFunction = std::sync::Arc::new(
            |_args: &[ScriptValue]| -> BoxFuture<'static, Result<ScriptValue, ScriptError>> {
                Box::pin(async { Ok(ScriptValue::Null) })
            },
        );
        pool.register_function("hostfn", host)
            .await
            .expect("register function");
        assert_eq!(
            pool.call_function("hostfn", &[]).await.expect("call"),
            ScriptValue::Null
        );
        assert_eq!(
            registered.factory.created(0).registered_functions(),
            vec!["hostfn".to_string()]
        );
    }

    #[tokio::test]
    async fn modules_register_through_the_pool() {
        let (pool, registered) = one_engine("mock-pool-modules").await;
        pool.register_module("m", ScriptValue::Map(HashMap::new()))
            .await
            .expect("register module");
        assert_eq!(
            registered.factory.created(0).registered_modules(),
            vec!["m".to_string()]
        );
    }

    #[tokio::test]
    async fn watch_controls_pass_through() {
        let (pool, _registered) = one_engine("mock-pool-watch").await;
        pool.start_watch("k").await.expect("start watch");
        pool.stop_watch("k").await.expect("stop watch");
    }

    #[tokio::test]
    async fn last_error_round_trips_through_the_pool() {
        let (pool, registered) = one_engine("mock-pool-lasterror").await;
        registered
            .factory
            .created(0)
            .set_last_error(Some(ScriptError::Failed("recorded".to_string())));
        assert!(pool.get_last_error().await.is_some());
        pool.clear_error().await;
        assert!(pool.get_last_error().await.is_none());
    }

    #[tokio::test]
    async fn concurrent_acquire_execute_release() {
        let registered = with_factory_type("mock-pool-concurrent", None, None);
        let pool = std::sync::Arc::new(
            EnginePool::new(2, "mock-pool-concurrent")
                .await
                .expect("pool"),
        );
        let mut tasks = Vec::new();
        for _ in 0..8 {
            let pool = pool.clone();
            tasks.push(tokio::spawn(async move {
                for _ in 0..10 {
                    let engine = pool.acquire().await.expect("acquire");
                    let value = engine.execute().await.expect("execute");
                    assert_eq!(value, ScriptValue::String("exec-result".to_string()));
                    pool.release(engine);
                }
            }));
        }
        for task in tasks {
            task.await.expect("task");
        }
        drop(pool);
        assert_eq!(registered.factory.created_count(), 2);
    }
}
