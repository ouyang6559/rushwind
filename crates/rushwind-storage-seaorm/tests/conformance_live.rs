//! Live conformance against PostgreSQL and MySQL, driven by CI service
//! containers. Compile-gated behind the `live` feature; the target comes
//! from `STORAGE_DATABASE_URL`. Every test gets its own uniquely-named
//! table, so the suite's parallel tests never fight over one table.

#![cfg(feature = "live")]

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use rushwind_storage::{ColumnKind, Repository, Schema};
use rushwind_storage_seaorm::SeaRepo;

static SEQ: AtomicUsize = AtomicUsize::new(0);

async fn fresh_repo() -> Arc<dyn Repository> {
    let url = std::env::var("STORAGE_DATABASE_URL")
        .expect("STORAGE_DATABASE_URL must point at a live PostgreSQL or MySQL instance");
    let mut schema = Schema::builder("widgets", "id")
        .column("name", ColumnKind::Text)
        .column("age", ColumnKind::Int)
        .column("score", ColumnKind::Real)
        .column("owner_id", ColumnKind::Int)
        .column("unit_id", ColumnKind::Int)
        .build()
        .expect("suite schema is valid");
    schema.table = format!("widgets_live_{}", SEQ.fetch_add(1, Ordering::Relaxed));

    let repo = SeaRepo::connect(url, schema)
        .await
        .expect("connects to the live database");
    repo.migrate_create()
        .await
        .expect("creates the suite table");
    Arc::new(repo)
}

rushwind_testkit::rushwind_storage_conformance_suite!(crate::fresh_repo);
