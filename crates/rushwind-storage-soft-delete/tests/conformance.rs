//! Contract transparency: the soft-delete decorator must pass the full
//! storage conformance suite over an in-memory engine. The suite schema
//! gains the tombstone column the decorator requires.

use std::sync::Arc;

use rushwind_storage::{Column, ColumnKind, Repository, Schema};
use rushwind_storage_memory::MemoryRepo;
use rushwind_storage_soft_delete::{SoftDeleteRepo, DELETED_AT_COLUMN};

/// The suite schema plus the tombstone column.
fn soft_delete_schema() -> Schema {
    let mut schema = rushwind_testkit::storage_conformance::suite_schema();
    schema.columns.push(Column {
        name: DELETED_AT_COLUMN.into(),
        kind: ColumnKind::Int,
    });
    schema
}

/// A fresh, empty soft-deleting table for every suite test.
async fn fresh_repo() -> Arc<dyn Repository> {
    let inner = MemoryRepo::new(soft_delete_schema()).expect("suite schema is valid");
    SoftDeleteRepo::new(Arc::new(inner)).expect("schema carries the tombstone column")
}

rushwind_testkit::rushwind_storage_conformance_suite!(crate::fresh_repo);
