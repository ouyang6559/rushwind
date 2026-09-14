//! The multi-engine lifecycle manager.
//!
//! Namespaces named [`SharedEngine`] instances behind one lock with
//! uniform init/close sweeps — the Go `Manager` shape. When the
//! application needs a single engine the pools alone are enough; the
//! manager earns its keep when several engines of several types must
//! come up and go down together.
//!
//! Divergence from the Go predecessor: the Go `Register` rejects a nil
//! engine, which `Arc` rules out here; the empty-name rejection is
//! ported, the nil-engine one is dropped.

use std::collections::HashMap;
use std::sync::RwLock;

use crate::{ScriptError, SharedEngine};

struct Inner {
    engines: HashMap<String, SharedEngine>,
    default_name: String,
}

/// A name-keyed registry of engine instances with lifecycle sweeps.
pub struct Manager {
    inner: RwLock<Inner>,
}

impl Manager {
    /// Creates an empty manager.
    pub fn new() -> Self {
        Self {
            inner: RwLock::new(Inner {
                engines: HashMap::new(),
                default_name: String::new(),
            }),
        }
    }

    /// Registers `engine` under `name` without initializing it. Fails
    /// on an empty name or a taken one.
    pub fn register(&self, name: &str, engine: SharedEngine) -> Result<(), ScriptError> {
        if name.is_empty() {
            return Err(ScriptError::Failed("manager: invalid name".to_string()));
        }
        let mut inner = self.inner.write().expect("manager lock");
        if inner.engines.contains_key(name) {
            return Err(ScriptError::Failed(
                "manager: engine already registered".to_string(),
            ));
        }
        inner.engines.insert(name.to_string(), engine);
        Ok(())
    }

    /// Returns the engine registered under `name`, if any.
    pub fn get(&self, name: &str) -> Option<SharedEngine> {
        self.inner
            .read()
            .expect("manager lock")
            .engines
            .get(name)
            .cloned()
    }

    /// Calls [`ScriptEngine::init`](crate::ScriptEngine::init) on every
    /// registered engine, aborting on the first failure. The sweep
    /// takes a snapshot under the read lock and initializes outside
    /// it, the Go shape.
    pub async fn init_all(&self) -> Result<(), ScriptError> {
        let engines: Vec<SharedEngine> = {
            let inner = self.inner.read().expect("manager lock");
            inner.engines.values().cloned().collect()
        };
        for engine in &engines {
            engine.init().await?;
        }
        Ok(())
    }

    /// Closes every registered engine and clears the registry.
    /// Individual close failures are collected and the last one is
    /// returned, the Go `CloseAll` shape.
    pub fn close_all(&self) -> Result<(), ScriptError> {
        let engines: Vec<SharedEngine> = {
            let mut inner = self.inner.write().expect("manager lock");
            std::mem::take(&mut inner.engines).into_values().collect()
        };
        let mut last: Result<(), ScriptError> = Ok(());
        for engine in engines {
            if let Err(err) = engine.close() {
                last = Err(err);
            }
        }
        last
    }

    /// Unregisters the engine named `name`; when `close_if_exists` is
    /// set and the engine exists, it is closed as well.
    pub fn remove(&self, name: &str, close_if_exists: bool) {
        let engine = {
            let mut inner = self.inner.write().expect("manager lock");
            inner.engines.remove(name)
        };
        if let (true, Some(engine)) = (close_if_exists, engine) {
            let _ = engine.close();
        }
    }

    /// Records the name [`Manager::get_default`] resolves to.
    pub fn set_default(&self, name: &str) {
        self.inner.write().expect("manager lock").default_name = name.to_string();
    }

    /// Returns the engine registered under the default name, if any.
    pub fn get_default(&self) -> Option<SharedEngine> {
        let name = {
            let inner = self.inner.read().expect("manager lock");
            inner.default_name.clone()
        };
        self.get(&name)
    }
}

impl Default for Manager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testutil::MockEngine;
    use crate::ScriptEngine;
    use std::sync::Arc;

    fn mock(typ: &'static str) -> Arc<MockEngine> {
        Arc::new(MockEngine::new(typ))
    }

    #[tokio::test]
    async fn engines_register_and_fetch_by_name() {
        let manager = Manager::new();
        manager
            .register("alpha", mock("mock-manager-alpha"))
            .expect("register alpha");
        let engine = manager.get("alpha").expect("fetch alpha");
        assert_eq!(engine.engine_type(), "mock-manager-alpha");
        assert!(manager.get("missing").is_none());
    }

    #[test]
    fn duplicate_names_are_rejected() {
        let manager = Manager::new();
        manager
            .register("dup", mock("mock-manager-dup"))
            .expect("register");
        assert!(matches!(
            manager.register("dup", mock("mock-manager-dup")),
            Err(ScriptError::Failed(_))
        ));
    }

    #[test]
    fn empty_names_are_rejected() {
        let manager = Manager::new();
        assert!(matches!(
            manager.register("", mock("mock-manager-empty")),
            Err(ScriptError::Failed(_))
        ));
    }

    #[tokio::test]
    async fn init_all_initializes_every_engine() {
        let manager = Manager::new();
        let a = mock("mock-manager-init-a");
        let b = mock("mock-manager-init-b");
        manager.register("a", a.clone()).expect("register a");
        manager.register("b", b.clone()).expect("register b");
        manager.init_all().await.expect("init all");
        assert!(a.is_initialized());
        assert!(b.is_initialized());
    }

    #[tokio::test]
    async fn init_all_aborts_on_the_first_failure() {
        let manager = Manager::new();
        let ok = mock("mock-manager-init-c");
        let mut failing = MockEngine::new("mock-manager-init-d");
        failing.init_err = Some(ScriptError::Failed("init refused".to_string()));
        let failing = Arc::new(failing);
        manager.register("ok", ok.clone()).expect("register ok");
        manager
            .register("failing", failing.clone())
            .expect("register failing");
        assert!(manager.init_all().await.is_err());
        // The failing engine never comes up, whichever order the sweep
        // visited it in; the healthy engine's outcome is decided by
        // that arbitrary order and carries no assertion.
        assert!(!failing.is_initialized());
    }

    #[test]
    fn close_all_closes_and_clears() {
        let manager = Manager::new();
        let a = mock("mock-manager-close-a");
        let b = mock("mock-manager-close-b");
        manager.register("a", a.clone()).expect("register a");
        manager.register("b", b.clone()).expect("register b");
        manager.close_all().expect("close all");
        assert_eq!(a.close_count(), 1);
        assert_eq!(b.close_count(), 1);
        assert!(manager.get("a").is_none());
        assert!(manager.get("b").is_none());
    }

    #[test]
    fn remove_with_close_closes_the_engine() {
        let manager = Manager::new();
        let engine = mock("mock-manager-remove-a");
        manager.register("x", engine.clone()).expect("register");
        manager.remove("x", true);
        assert_eq!(engine.close_count(), 1);
        assert!(manager.get("x").is_none());
    }

    #[test]
    fn remove_without_close_leaves_the_engine_alive() {
        let manager = Manager::new();
        let engine = mock("mock-manager-remove-b");
        manager.register("x", engine.clone()).expect("register");
        manager.remove("x", false);
        assert_eq!(engine.close_count(), 0);
        assert!(manager.get("x").is_none());
    }

    #[test]
    fn the_default_name_resolves_through_get() {
        let manager = Manager::new();
        let engine = mock("mock-manager-default");
        manager.register("dflt", engine.clone()).expect("register");
        manager.set_default("dflt");
        let resolved = manager.get_default().expect("default resolves");
        assert_eq!(
            resolved.engine_type(),
            engine.engine_type(),
            "the default resolves the same engine"
        );
    }
}
