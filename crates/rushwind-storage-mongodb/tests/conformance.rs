//! Live conformance against a running MongoDB, driven by CI's
//! storage-integration job. Compile-gated behind the `live` feature; the
//! endpoint comes from `MONGODB_URI`. Every test gets its own uniquely-named
//! collection, so the suite's parallel tests never collide.

#![cfg(feature = "live")]

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use rushwind_storage::{ColumnKind, Repository, Schema};
use rushwind_storage_mongodb::MongoRepo;

static SEQ: AtomicUsize = AtomicUsize::new(0);

async fn fresh_repo() -> Arc<dyn Repository> {
    let uri =
        std::env::var("MONGODB_URI").expect("MONGODB_URI must point at a running MongoDB instance");
    let mut schema = Schema::builder("widgets", "id")
        .column("name", ColumnKind::Text)
        .column("age", ColumnKind::Int)
        .column("score", ColumnKind::Real)
        .column("owner_id", ColumnKind::Int)
        .column("unit_id", ColumnKind::Int)
        .build()
        .expect("suite schema is valid");
    schema.table = format!("widgets_live_{}", SEQ.fetch_add(1, Ordering::Relaxed));

    let repo = MongoRepo::connect(&uri, "rushwind_suite", schema)
        .await
        .expect("connects to MongoDB");
    Arc::new(repo)
}

rushwind_testkit::rushwind_storage_conformance_suite!(crate::fresh_repo);
