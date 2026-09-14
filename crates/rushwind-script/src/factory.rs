//! The process-wide engine factory registry, keyed by engine name.
//!
//! The Go predecessor keeps a package-level `map[Type]FactoryFunc`
//! guarded by an `RWMutex`; the port follows the encoding domain's
//! `OnceLock<RwLock<HashMap<…>>>` registry shape with the same
//! semantics: registration under a taken name fails, lookup and
//! listing are lock-shared, and the type-based constructors consult
//! the registry through [`new_script_engine`].
//!
//! The factory does construction only — [`crate::EnginePool`]
//! initializes what it produces, and the autogrow pool's type-based
//! constructor wraps the pair; [`crate::FullEngineFactory`] is the
//! caller-owned construction-plus-initialization shape used by
//! [`crate::AutoGrowEnginePool::with_factory`] for per-instance
//! pre-init configuration (sandbox allow-lists, pre-registered
//! globals).

use std::collections::HashMap;
use std::sync::{Arc, OnceLock, RwLock};

use crate::{ScriptError, SharedEngine};

/// The construction-only factory shape: builds an engine instance
/// without initializing it.
pub type EngineFactory = Arc<dyn Fn() -> Result<SharedEngine, ScriptError> + Send + Sync>;

fn factories() -> &'static RwLock<HashMap<String, EngineFactory>> {
    static FACTORIES: OnceLock<RwLock<HashMap<String, EngineFactory>>> = OnceLock::new();
    FACTORIES.get_or_init(|| RwLock::new(HashMap::new()))
}

/// Creates an engine instance via the factory registered under `typ`.
///
/// The returned engine is uninitialized — call
/// [`ScriptEngine::init`](crate::ScriptEngine::init) before use. An
/// unregistered name is an error.
pub fn new_script_engine(typ: &str) -> Result<SharedEngine, ScriptError> {
    let factory = get_factory(typ).ok_or_else(|| {
        ScriptError::Failed(format!("script engine factory {typ:?} not registered"))
    })?;
    factory()
}

/// Registers `factory` under `typ`. Fails if the name is taken.
pub fn register_factory(typ: &str, factory: EngineFactory) -> Result<(), ScriptError> {
    let mut factories = factories()
        .write()
        .expect("script engine factory registry lock");
    if factories.contains_key(typ) {
        return Err(ScriptError::Failed(format!(
            "script engine factory {typ:?} already registered"
        )));
    }
    factories.insert(typ.to_string(), factory);
    Ok(())
}

/// Returns the factory registered under `typ`, if any.
pub fn get_factory(typ: &str) -> Option<EngineFactory> {
    factories()
        .read()
        .expect("script engine factory registry lock")
        .get(typ)
        .cloned()
}

/// Returns a snapshot of the registered engine names.
pub fn list_factories() -> Vec<String> {
    factories()
        .read()
        .expect("script engine factory registry lock")
        .keys()
        .cloned()
        .collect()
}

/// Removes the factory registered under `typ`; returns whether one was
/// removed.
pub fn unregister_factory(typ: &str) -> bool {
    factories()
        .write()
        .expect("script engine factory registry lock")
        .remove(typ)
        .is_some()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testutil::MockEngine;

    fn mock_factory() -> EngineFactory {
        Arc::new(|| Ok(Arc::new(MockEngine::new("mock-factory"))))
    }

    #[test]
    fn registered_factories_are_listed_and_fetched() {
        register_factory("mock-factory-a", mock_factory()).expect("register a");
        register_factory("mock-factory-b", mock_factory()).expect("register b");
        assert!(get_factory("mock-factory-a").is_some());
        let names = list_factories();
        assert!(names.contains(&"mock-factory-a".to_string()));
        assert!(names.contains(&"mock-factory-b".to_string()));
        unregister_factory("mock-factory-a");
        unregister_factory("mock-factory-b");
        assert!(get_factory("mock-factory-a").is_none());
        assert!(!list_factories().contains(&"mock-factory-a".to_string()));
    }

    #[test]
    fn duplicate_registration_fails() {
        register_factory("mock-factory-dup", mock_factory()).expect("register");
        assert!(matches!(
            register_factory("mock-factory-dup", mock_factory()),
            Err(ScriptError::Failed(_))
        ));
        unregister_factory("mock-factory-dup");
    }

    #[test]
    fn unregistered_types_are_rejected() {
        assert!(matches!(
            new_script_engine("mock-factory-missing"),
            Err(ScriptError::Failed(msg)) if msg.contains("not registered")
        ));
    }

    #[test]
    fn registered_types_produce_engines() {
        register_factory("mock-factory-ok", mock_factory()).expect("register");
        let engine = new_script_engine("mock-factory-ok").expect("engine from factory");
        assert_eq!(engine.engine_type(), "mock-factory");
        assert!(!engine.is_initialized());
        unregister_factory("mock-factory-ok");
    }
}
