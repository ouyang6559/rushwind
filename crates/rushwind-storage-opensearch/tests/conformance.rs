//! Live conformance against a running OpenSearch 2.x, driven by CI's
//! storage-integration job. Compile-gated behind the `live` feature.

#![cfg(feature = "live")]

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use rushwind_storage::{ColumnKind, Repository, Schema};
use rushwind_storage_opensearch::OpenSearchRepo;

static SEQ: AtomicUsize = AtomicUsize::new(0);

/// A fresh, empty index for every suite test.
async fn fresh_repo() -> Arc<dyn Repository> {
    let url =
        std::env::var("OPENSEARCH_URL").expect("OPENSEARCH_URL must point at a running OpenSearch");
    let mut schema = Schema::builder("widgets", "id")
        .column("name", ColumnKind::Text)
        .column("age", ColumnKind::Int)
        .column("score", ColumnKind::Real)
        .column("owner_id", ColumnKind::Int)
        .column("unit_id", ColumnKind::Int)
        .build()
        .expect("suite schema is valid");
    schema.table = format!("widgets_live_{}", SEQ.fetch_add(1, Ordering::Relaxed));

    let repo = OpenSearchRepo::new(url, schema);
    let base = format!("http://localhost:9200/{}", repo.schema().table);
    let client = reqwest::Client::new();
    let _ = client.delete(&base).send().await;
    for _ in 0..50 {
        let gone = client
            .get(&base)
            .send()
            .await
            .map(|r| r.status().as_u16() == 404)
            .unwrap_or(true);
        if gone {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    repo.ensure_index()
        .await
        .expect("creates the suite index with mappings");
    Arc::new(repo)
}

rushwind_testkit::rushwind_storage_conformance_suite!(crate::fresh_repo);
