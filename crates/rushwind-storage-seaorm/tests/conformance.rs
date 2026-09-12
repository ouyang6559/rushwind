//! Storage conformance for the SeaORM-backed engine (SQLite in memory).

use std::sync::Arc;

use rushwind_storage::Repository;
use rushwind_storage_seaorm::SeaRepo;

/// A fresh, empty in-memory SQLite database for every suite test.
async fn fresh_repo() -> Arc<dyn Repository> {
    let repo = SeaRepo::sqlite_memory(rushwind_testkit::storage_conformance::suite_schema())
        .await
        .expect("in-memory sqlite opens");
    repo.migrate_create().await.expect("suite table creates");
    Arc::new(repo)
}

rushwind_testkit::rushwind_storage_conformance_suite!(crate::fresh_repo);
