//! Live conformance against a running InfluxDB 1.8, driven by CI's
//! storage-integration job. Compile-gated behind the `live` feature; the
//! database name comes from `INFLUXDB_DB` (created externally) and the
//! endpoint from `INFLUXDB_URL`. Each test drops and recreates the suite
//! measurement via `DROP MEASUREMENT`.

#![cfg(feature = "live")]

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use rushwind_storage::{ColumnKind, Repository, Schema};
use rushwind_storage_influxdb::InfluxRepo;

static SEQ: AtomicUsize = AtomicUsize::new(0);

/// A fresh, empty measurement for every suite test.
async fn fresh_repo() -> Arc<dyn Repository> {
    let url = std::env::var("INFLUXDB_URL").expect("INFLUXDB_URL must point at a running InfluxDB");
    let database = std::env::var("INFLUXDB_DB").unwrap_or_else(|_| "rushwind".to_owned());
    let mut schema = Schema::builder("widgets", "id")
        .column("name", ColumnKind::Text)
        .column("age", ColumnKind::Int)
        .column("score", ColumnKind::Real)
        .column("owner_id", ColumnKind::Int)
        .column("unit_id", ColumnKind::Int)
        .build()
        .expect("suite schema is valid");
    schema.table = format!("widgets_live_{}", SEQ.fetch_add(1, Ordering::Relaxed));

    let repo = InfluxRepo::new(url.clone(), database.clone(), schema);
    // Reset the measurement: a plain InfluxQL drop is 200-tolerant.
    let client = reqwest::Client::new();
    let _ = client
        .post(&format!("{}/query?db={}", url, database))
        .form(&[("q", format!("DROP MEASUREMENT \"{}\"", repo.schema().table))])
        .send()
        .await;
    Arc::new(repo)
}

rushwind_testkit::rushwind_storage_conformance_suite!(crate::fresh_repo);
