//! Contract transparency: the observability decorator must pass the full
//! storage conformance suite — instrumentation changes nothing observable.

use std::sync::Arc;

use rushwind_storage::Repository;
use rushwind_storage_memory::MemoryRepo;
use rushwind_storage_observe::ObservedRepo;

/// A fresh, empty observed table for every suite test.
async fn fresh_repo() -> Arc<dyn Repository> {
    let inner = MemoryRepo::new(rushwind_testkit::storage_conformance::suite_schema())
        .expect("suite schema is valid");
    ObservedRepo::new(Arc::new(inner))
}

rushwind_testkit::rushwind_storage_conformance_suite!(crate::fresh_repo);
