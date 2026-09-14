//! The auto-growing engine pool: idle instances are reused, and when
//! none is idle but the configured cap has not been reached a new
//! instance is produced on the fly.
//!
//! Instances come from a factory that owns **both** construction and
//! initialization — the Go `EngineFactoryFunc` shape — so the pool
//! builder can apply per-instance pre-init configuration (a Lua
//! engine's sandbox allow-list, pre-registered globals) before the
//! runtime exists. [`AutoGrowEnginePool::new`], the type-based
//! convenience constructor, wraps the factory registered under the
//! given name plus [`ScriptEngine::init`](crate::ScriptEngine::init)
//! into that shape, the historical behavior.
//!
//! The growth bookkeeping follows the Go structure: a live-instance
//! counter checked and incremented under its lock before the factory
//! runs (rolled back on factory failure), the lending queue and its
//! counting semaphore for the blocking path, and [`Semaphore::close`]
//! as the Go `close(chan)` wake-up for blocked acquirers.
//!
//! Divergence from the Go predecessor: the `typ` field kept "for
//! diagnostics" is dropped (never read), the nil-factory check is
//! dropped (`FullEngineFactory` cannot be null), and `Release` into a
//! full queue — unreachable under the accounting — closes the engine
//! without the Go channel-panic guard.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use tokio::sync::Semaphore;

use crate::factory;
use crate::{BoxFuture, HostFunction, ScriptError, ScriptValue, SharedEngine, SharedScriptSource};

/// The factory shape owning construction *and* initialization: every
/// engine it produces must come back ready to use, including any
/// per-instance pre-init configuration.
pub type FullEngineFactory =
    Arc<dyn Fn() -> BoxFuture<'static, Result<SharedEngine, ScriptError>> + Send + Sync>;

/// A pool of engines that grows on demand up to a configured cap.
pub struct AutoGrowEnginePool {
    queue: Mutex<VecDeque<SharedEngine>>,
    semaphore: Arc<Semaphore>,
    factory: FullEngineFactory,
    total: Mutex<usize>,
    max: usize,
    closed: AtomicBool,
}

impl AutoGrowEnginePool {
    /// Creates a pool whose instances are all produced by `factory`,
    /// with `initial` built eagerly and growth capped at `max`.
    ///
    /// On any eager-construction failure the created instances are
    /// closed and the pool is not built.
    pub async fn with_factory(
        initial: usize,
        max: usize,
        factory: FullEngineFactory,
    ) -> Result<Self, ScriptError> {
        if max < 1 || initial > max {
            return Err(ScriptError::Failed(format!(
                "script engine: invalid sizes: initial={initial} max={max}"
            )));
        }

        let mut created: Vec<SharedEngine> = Vec::new();
        for _ in 0..initial {
            let engine = match (factory)().await {
                Ok(engine) => engine,
                Err(err) => {
                    for engine in &created {
                        let _ = engine.close();
                    }
                    return Err(ScriptError::Failed(format!(
                        "script engine: factory failed: {err}"
                    )));
                }
            };
            created.push(engine);
        }

        let queue = VecDeque::from(created);
        let total = queue.len();
        Ok(Self {
            queue: Mutex::new(queue),
            semaphore: Arc::new(Semaphore::new(total)),
            factory,
            total: Mutex::new(total),
            max,
            closed: AtomicBool::new(false),
        })
    }

    /// The type-based convenience constructor: each instance is built
    /// by the factory registered under `typ` and then initialized, the
    /// historical shape wrapped into [`FullEngineFactory`].
    pub async fn new(initial: usize, max: usize, typ: &str) -> Result<Self, ScriptError> {
        if typ.is_empty() {
            return Err(ScriptError::Failed(
                "engine type cannot be empty".to_string(),
            ));
        }
        let typ = typ.to_string();
        let factory: FullEngineFactory = Arc::new(
            move || -> BoxFuture<'static, Result<SharedEngine, ScriptError>> {
                let typ = typ.clone();
                Box::pin(async move {
                    let engine = factory::new_script_engine(&typ)?;
                    if let Err(err) = engine.init().await {
                        let _ = engine.close();
                        return Err(err);
                    }
                    Ok(engine)
                })
            },
        );
        Self::with_factory(initial, max, factory).await
    }

    /// Takes an engine out of the pool.
    ///
    /// If an idle instance is available it is returned immediately.
    /// Otherwise, if the live count is below the cap, a new instance
    /// is produced by the factory and returned (never queued). At the
    /// cap the call blocks until an instance is released.
    pub async fn acquire(&self) -> Result<SharedEngine, ScriptError> {
        if self.closed.load(Ordering::Acquire) {
            return Err(ScriptError::Failed(
                "script engine: engine pool closed".to_string(),
            ));
        }
        // Fast path: an idle instance.
        if let Ok(permit) = Arc::clone(&self.semaphore).try_acquire_owned() {
            if self.closed.load(Ordering::Acquire) {
                return Err(ScriptError::Failed(
                    "script engine: engine pool closed".to_string(),
                ));
            }
            let engine = self
                .queue
                .lock()
                .expect("engine pool queue lock")
                .pop_front();
            // The permit stood for this queued engine and leaves
            // circulation with it — see the slow path.
            permit.forget();
            return engine.ok_or_else(|| {
                ScriptError::Failed("script engine: engine pool closed".to_string())
            });
        }

        // Growth path: under the cap, produce a fresh instance. The
        // counter rolls back on factory failure so the capacity stays
        // usable.
        let grown = {
            let mut total = self.total.lock().expect("engine pool total lock");
            if *total < self.max {
                *total += 1;
                true
            } else {
                false
            }
        };
        if grown {
            let engine = match (self.factory)().await {
                Ok(engine) => engine,
                Err(err) => {
                    let mut total = self.total.lock().expect("engine pool total lock");
                    *total = (*total).saturating_sub(1);
                    return Err(err);
                }
            };
            return Ok(engine);
        }

        // Cap reached: block for a released instance.
        let permit = match Arc::clone(&self.semaphore).acquire_owned().await {
            Ok(permit) => permit,
            Err(_) => {
                return Err(ScriptError::Failed(
                    "script engine: engine pool closed".to_string(),
                ))
            }
        };
        if self.closed.load(Ordering::Acquire) {
            return Err(ScriptError::Failed(
                "script engine: engine pool closed".to_string(),
            ));
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
        engine.ok_or_else(|| ScriptError::Failed("script engine: engine pool closed".to_string()))
    }

    /// Returns an engine to the pool. If the pool is closed — or the
    /// queue is full, which the accounting makes unreachable — the
    /// engine is closed instead and the live count drops with it.
    pub fn release(&self, engine: SharedEngine) {
        if self.closed.load(Ordering::Acquire) {
            let _ = engine.close();
            return;
        }
        let accepted = {
            let queue = self.queue.lock().expect("engine pool queue lock");
            queue.len() < self.max
        };
        if accepted {
            let mut queue = self.queue.lock().expect("engine pool queue lock");
            queue.push_back(engine);
            drop(queue);
            self.semaphore.add_permits(1);
        } else {
            let _ = engine.close();
            let mut total = self.total.lock().expect("engine pool total lock");
            *total = (*total).saturating_sub(1);
        }
    }

    /// Closes the pool and destroys every idle instance. A second close
    /// is a no-op. Instances still lent out are closed by their
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

    /// The multi-key variant of [`AutoGrowEnginePool::execute_from_key`].
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
    use crate::testutil::{with_factory_type, with_full_factory};
    use crate::{MemSource, ScriptEngine, ScriptError, ScriptValue};
    use std::time::Duration;

    #[tokio::test]
    async fn invalid_sizes_are_rejected() {
        let registered = with_factory_type("mock-autogrow-sizes", None, None);
        for (initial, max) in [(0usize, 0usize), (2, 1), (3, 1)] {
            assert!(matches!(
                AutoGrowEnginePool::new(initial, max, "mock-autogrow-sizes").await,
                Err(ScriptError::Failed(msg)) if msg.contains("invalid sizes")
            ));
        }
        let (_, failing) = with_full_factory("mock-autogrow-sizes-full", None, false);
        assert!(matches!(
            AutoGrowEnginePool::with_factory(2, 1, failing).await,
            Err(ScriptError::Failed(msg)) if msg.contains("invalid sizes")
        ));
        assert_eq!(registered.factory.attempts(), 0);
    }

    #[tokio::test]
    async fn an_empty_type_is_rejected() {
        assert!(matches!(
            AutoGrowEnginePool::new(1, 1, "").await,
            Err(ScriptError::Failed(msg)) if msg.contains("cannot be empty")
        ));
    }

    #[tokio::test]
    async fn an_eager_factory_failure_rolls_back() {
        let (handle, failing) = with_full_factory("mock-autogrow-eagerfail", None, true);
        assert!(matches!(
            AutoGrowEnginePool::with_factory(2, 2, failing).await,
            Err(ScriptError::Failed(msg)) if msg.contains("factory failed")
        ));
        // The eager loop aborts on the first failure, so exactly one
        // attempt was made and nothing survived it.
        assert_eq!(handle.attempts(), 1);
        assert_eq!(handle.created_count(), 0);
    }

    #[tokio::test]
    async fn a_registered_factory_failure_rolls_back() {
        let registered = with_factory_type(
            "mock-autogrow-regfail",
            Some(ScriptError::Failed("init refused".to_string())),
            None,
        );
        assert!(matches!(
            AutoGrowEnginePool::new(2, 2, "mock-autogrow-regfail").await,
            Err(ScriptError::Failed(msg)) if msg.contains("factory failed")
        ));
        // One attempt, one recorded engine, closed by the failure
        // cleanup.
        assert_eq!(registered.factory.attempts(), 1);
        assert_eq!(registered.factory.created_count(), 1);
        assert_eq!(registered.factory.created(0).close_count(), 1);
    }

    #[tokio::test]
    async fn a_growing_factory_failure_leaves_the_capacity_usable() {
        let (handle, failing) = with_full_factory("mock-autogrow-growfail", None, true);
        let pool = AutoGrowEnginePool::with_factory(0, 1, failing)
            .await
            .expect("pool comes up empty");
        for _ in 0..2 {
            assert!(matches!(
                pool.acquire().await,
                Err(ScriptError::Failed(msg)) if msg.contains("init refused")
            ));
        }
        assert_eq!(handle.attempts(), 2);
        assert_eq!(handle.created_count(), 0);
        pool.close().expect("close");
    }

    #[tokio::test]
    async fn a_zero_initial_pool_grows_on_demand() {
        let (handle, factory) = with_full_factory("mock-autogrow-zeroinit", None, false);
        let pool = AutoGrowEnginePool::with_factory(0, 2, factory)
            .await
            .expect("pool comes up empty");
        assert_eq!(handle.created_count(), 0);
        let engine = pool.acquire().await.expect("growth on demand");
        assert!(engine.is_initialized());
        assert_eq!(handle.created_count(), 1);
        pool.release(engine);
        // The released instance is reused: no second growth.
        let reused = pool.acquire().await.expect("reuse");
        assert_eq!(handle.created_count(), 1);
        pool.release(reused);
        pool.close().expect("close");
    }

    #[tokio::test]
    async fn growth_under_the_cap_produces_distinct_engines() {
        let (handle, factory) = with_full_factory("mock-autogrow-undercap", None, false);
        let pool = AutoGrowEnginePool::with_factory(1, 3, factory)
            .await
            .expect("pool comes up");
        assert_eq!(handle.created_count(), 1);
        let a = pool.acquire().await.expect("idle instance");
        let b = pool.acquire().await.expect("grown instance");
        assert_eq!(handle.created_count(), 2);
        assert_ne!(
            std::sync::Arc::as_ptr(&a) as *const u8,
            std::sync::Arc::as_ptr(&b) as *const u8,
            "the grown engine is a distinct instance"
        );
        pool.release(a);
        pool.release(b);
        pool.close().expect("close");
    }

    #[tokio::test]
    async fn the_cap_blocks_until_a_release() {
        let (handle, factory) = with_full_factory("mock-autogrow-cap", None, false);
        let pool = std::sync::Arc::new(
            AutoGrowEnginePool::with_factory(0, 1, factory)
                .await
                .expect("pool comes up empty"),
        );
        let engine = pool.acquire().await.expect("growth to the cap");
        assert_eq!(handle.created_count(), 1);
        let releaser = {
            let pool = pool.clone();
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(300)).await;
                pool.release(engine);
            })
        };
        let blocked = tokio::time::timeout(Duration::from_millis(150), pool.acquire()).await;
        assert!(
            blocked.is_err(),
            "the cap is reached and the one engine is out"
        );
        let _ = releaser.await;
        let second = tokio::time::timeout(Duration::from_secs(2), pool.acquire())
            .await
            .expect("unblocks after release")
            .expect("acquire");
        pool.release(second);
        pool.close().expect("close");
    }

    #[tokio::test]
    async fn a_release_into_a_closed_pool_closes_the_engine() {
        let (handle, factory) = with_full_factory("mock-autogrow-releaseclosed", None, false);
        let pool = AutoGrowEnginePool::with_factory(1, 1, factory)
            .await
            .expect("pool");
        let engine = pool.acquire().await.expect("idle instance");
        pool.close().expect("close");
        pool.release(engine);
        assert_eq!(handle.created(0).close_count(), 1);
    }

    #[tokio::test]
    async fn close_destroys_the_idle_instances() {
        let (handle, factory) = with_full_factory("mock-autogrow-closeidle", None, false);
        let pool = AutoGrowEnginePool::with_factory(2, 2, factory)
            .await
            .expect("pool");
        pool.close().expect("close");
        assert_eq!(handle.created(0).close_count(), 1);
        assert_eq!(handle.created(1).close_count(), 1);
    }

    #[tokio::test]
    async fn close_is_idempotent() {
        let (_, factory) = with_full_factory("mock-autogrow-closetwice", None, false);
        let pool = AutoGrowEnginePool::with_factory(1, 1, factory)
            .await
            .expect("pool");
        pool.close().expect("first close");
        pool.close().expect("second close is a no-op");
    }

    #[tokio::test]
    async fn a_preconfiguring_factory_preconfigures_the_eager_instances() {
        let (handle, factory) =
            with_full_factory("mock-autogrow-preconfig-eager", Some(&["base"]), false);
        let pool = AutoGrowEnginePool::with_factory(2, 2, factory)
            .await
            .expect("pool");
        assert_eq!(handle.created(0).open_libs(), vec!["base".to_string()]);
        assert_eq!(handle.created(1).open_libs(), vec!["base".to_string()]);
        pool.close().expect("close");
    }

    #[tokio::test]
    async fn a_preconfiguring_factory_preconfigures_the_grown_instances() {
        let (handle, factory) =
            with_full_factory("mock-autogrow-preconfig-grown", Some(&["base"]), false);
        let pool = AutoGrowEnginePool::with_factory(0, 2, factory)
            .await
            .expect("pool comes up empty");
        let a = pool.acquire().await.expect("grown a");
        let b = pool.acquire().await.expect("grown b");
        assert_eq!(handle.created_count(), 2);
        assert_eq!(handle.created(0).open_libs(), vec!["base".to_string()]);
        assert_eq!(handle.created(1).open_libs(), vec!["base".to_string()]);
        pool.release(a);
        pool.release(b);
        pool.close().expect("close");
    }

    #[tokio::test]
    async fn the_type_based_wrapper_initializes_its_engines() {
        let registered = with_factory_type("mock-autogrow-typed", None, None);
        let pool = AutoGrowEnginePool::new(1, 1, "mock-autogrow-typed")
            .await
            .expect("pool");
        assert!(registered.factory.created(0).is_initialized());
        assert_eq!(registered.factory.created(0).init_count(), 1);
        pool.close().expect("close");
    }

    #[tokio::test]
    async fn sources_bind_and_load_through_the_pool() {
        let registered = with_factory_type("mock-autogrow-source", None, None);
        let pool = AutoGrowEnginePool::new(1, 1, "mock-autogrow-source")
            .await
            .expect("pool");
        let mem = std::sync::Arc::new(MemSource::new());
        mem.set("k", "mem-code");
        pool.set_source(Some(mem.clone())).await;
        let bound = pool.get_source().await.expect("source bound");
        assert_eq!(bound.load("k").await.expect("bound load"), "mem-code");
        pool.load("k").await.expect("pool load");
        let mock = registered.factory.created(0);
        assert_eq!(mock.loaded(), 1);
        assert_eq!(mock.last_code().as_deref(), Some("mem-code"));
        pool.close().expect("close");
    }

    #[tokio::test]
    async fn execute_from_key_runs_through_the_pool() {
        let registered = with_factory_type("mock-autogrow-exec", None, None);
        let pool = AutoGrowEnginePool::new(1, 1, "mock-autogrow-exec")
            .await
            .expect("pool");
        let mem = std::sync::Arc::new(MemSource::new());
        mem.set("k", "mem-code");
        pool.set_source(Some(mem.clone())).await;
        let value = pool.execute_from_key("k").await.expect("execute from key");
        assert_eq!(value, ScriptValue::String("exec-result".to_string()));
        assert_eq!(registered.factory.created(0).executed(), 1);
        pool.close().expect("close");
    }

    #[tokio::test]
    async fn concurrent_acquire_execute_release() {
        let registered = with_factory_type("mock-autogrow-concurrent", None, None);
        let pool = std::sync::Arc::new(
            AutoGrowEnginePool::new(1, 4, "mock-autogrow-concurrent")
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
        // Growth anywhere between the eager one and the cap, decided
        // purely by task interleaving.
        let created = registered.factory.created_count();
        assert!((1..=4).contains(&created), "created {created} engines");
    }
}
