//! Contract transparency: the cache decorator must pass the full storage
//! conformance suite over an in-memory engine.

use std::sync::Arc;

use rushwind_storage::Repository;
use rushwind_storage_cache::CacheRepo;
use rushwind_storage_memory::MemoryRepo;

/// A fresh, empty cached table for every suite test.
async fn fresh_repo() -> Arc<dyn Repository> {
    let inner = MemoryRepo::new(rushwind_testkit::storage_conformance::suite_schema())
        .expect("suite schema is valid");
    CacheRepo::new(Arc::new(inner))
}

rushwind_testkit::rushwind_storage_conformance_suite!(crate::fresh_repo);
