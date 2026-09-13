//! Admin/API surface demo: the full RushWind stack in one process.
//!
//! The listener binds eagerly on an OS-assigned port (reported via
//! `endpoint()`), serves a health route plus a live CRUD API — an
//! in-memory repository behind the cache, soft-delete and observe
//! decorators, mounted through `rushwind-storage-axum` — and shuts down
//! on Ctrl+C (SIGTERM on Unix): axum stops accepting and drains, then
//! the lifecycle completes.
//!
//! Try it: `cargo run -p axum-admin`, then
//!
//! ```text
//! curl http://<addr>/health
//! curl -X POST http://<addr>/api/widgets -H "content-type: application/json" //!      -d '{"name": "bolt", "age": 3, "owner_id": 1, "unit_id": 1}'
//! curl "http://<addr>/api/widgets?filter=age >= 1&sort=name:asc"
//! curl "http://<addr>/api/widgets?q=%7B%22paginationType%22%3A%7B%22pageBased%22%3A%7B%22page%22%3A1%2C%22pageSize%22%3A20%7D%7D%7D"
//! ```
//!
//! then Ctrl+C.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use axum::routing::get;
use axum::Router;
use rushwind_core::App;
use rushwind_storage::{ColumnKind, Repository, Schema};
use rushwind_storage_axum::CrudApi;
use rushwind_storage_memory::MemoryRepo;
use rushwind_transport::{Server, StopSignal};

async fn health() -> &'static str {
    "ok"
}

/// The CRUD surface over the decorated in-memory engine. Production would
/// swap the engine for `SeaRepo::connect(...)` or `MongoRepo::connect(...)`
/// — every layer above is unchanged.
fn crud_router() -> Router {
    let schema = Schema::builder("widgets", "id")
        .column("name", ColumnKind::Text)
        .column("age", ColumnKind::Int)
        .column("score", ColumnKind::Real)
        .column("owner_id", ColumnKind::Int)
        .column("unit_id", ColumnKind::Int)
        .column("deleted_at", ColumnKind::Int)
        .build()
        .expect("demo schema is valid");
    let repo = MemoryRepo::new(schema).expect("schema is valid");
    let repo =
        rushwind_storage_soft_delete::SoftDeleteRepo::new(Arc::new(repo) as Arc<dyn Repository>)
            .expect("schema carries the tombstone column");
    let repo = rushwind_storage_cache::CacheRepo::new(repo);
    let repo = rushwind_storage_observe::ObservedRepo::new(repo);

    CrudApi::new(repo)
        .with_viewer(Arc::new(|headers: &axum::http::HeaderMap| {
            match headers
                .get("x-owner")
                .and_then(|v| v.to_str().ok())
                .and_then(|v| v.parse::<i64>().ok())
            {
                // Scoped callers see only their own rows; scopes close by
                // default.
                Some(owner) => rushwind_storage::Viewer::own(owner),
                None => rushwind_storage::Viewer::default(),
            }
        }))
        .router()
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let router = Router::new()
        .route("/health", get(health))
        .nest("/api/widgets", crud_router());
    let server =
        rushwind_transport_axum::AxumServer::new(SocketAddr::from(([127, 0, 0, 1], 0)), router)?;
    println!("[admin] bound on {}", server.endpoint()?);

    let app = App::builder()
        .name("admin-demo")
        .version("0.0.1")
        .erased_server(Arc::new(server))
        .stop_timeout(Duration::from_secs(10))
        .build();

    let result = app.run(StopSignal::new()).await;
    println!("[admin] lifecycle finished: {result:?}");
    Ok(())
}
