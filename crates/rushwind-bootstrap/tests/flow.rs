//! End-to-end assembly tests: YAML → storage engine + HTTP server with
//! route packs, under one lifecycle — plus the unknown-name error paths.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use axum::routing::get;
use axum::Router;
use rushwind_bootstrap::{Bootstrap, BootstrapError, RouteInput};
use rushwind_storage::{ColumnKind, Repository, Schema};
use rushwind_storage_memory::MemoryRepo;
use rushwind_transport::StopSignal;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::time::{sleep, timeout};

const FLOW_YAML: &str = r#"
app:
  name: flow-test
  version: v0.1.0
  stop_timeout_secs: 1
storage:
  engine: memory
  settings: {}
servers:
  - kind: http
    bind: 127.0.0.1:0
    route_packs: [health, wired]
"#;

fn schema() -> Schema {
    Schema::builder("widgets", "id")
        .column("name", ColumnKind::Text)
        .build()
        .expect("schema is valid")
}

/// The application-side glue: the storage factory captures the schema
/// (application knowledge) and reads engine knobs from settings.
fn register_memory_engine(bootstrap: Bootstrap) -> Bootstrap {
    bootstrap.storage_factory("memory", |_settings| {
        Box::pin(async move {
            let repo =
                MemoryRepo::new(schema()).map_err(|e| BootstrapError::Failed(e.to_string()))?;
            Ok(Arc::new(repo) as Arc<dyn Repository>)
        })
    })
}

fn register_packs(bootstrap: Bootstrap) -> Bootstrap {
    bootstrap
        .route_pack("health", |_input| {
            Ok(Router::new().route("/health", get(|| async { "ok" })))
        })
        .route_pack("wired", |input: RouteInput| {
            // Proves the storage engine reaches the pack: the route
            // reports whether the repository was injected.
            let repository = input.repository.clone();
            Ok(Router::new().route(
                "/wired",
                get(move || {
                    let repository = repository.clone();
                    async move { format!("repo={}", repository.is_some()) }
                }),
            ))
        })
}

/// A minimal raw HTTP/1.1 GET; `Connection: close` makes the server end
/// the stream so `read_to_end` completes.
async fn http_get(addr: SocketAddr, path: &str) -> String {
    let mut stream = tokio::net::TcpStream::connect(addr)
        .await
        .expect("connect must succeed");
    stream
        .write_all(
            format!("GET {path} HTTP/1.1\r\nHost: test\r\nConnection: close\r\n\r\n").as_bytes(),
        )
        .await
        .expect("write must succeed");
    let mut buf = Vec::new();
    stream
        .read_to_end(&mut buf)
        .await
        .expect("read must complete");
    String::from_utf8(buf).expect("response must be utf-8")
}

#[tokio::test]
async fn assembles_storage_and_http_under_one_lifecycle() {
    let bootstrapped = register_packs(register_memory_engine(
        Bootstrap::from_yaml_str(FLOW_YAML).expect("yaml must parse"),
    ))
    .build()
    .await
    .expect("assembly must succeed");

    // The configured engine was constructed and shared into the packs.
    assert!(bootstrapped.repository.is_some());
    assert_eq!(bootstrapped.endpoints.len(), 1);
    assert!(bootstrapped.endpoints[0].starts_with("http://127.0.0.1:"));

    let addr: SocketAddr = bootstrapped.endpoints[0]
        .trim_start_matches("http://")
        .parse()
        .expect("endpoint must be host:port");

    let app = Arc::new(bootstrapped.app);
    let trigger = StopSignal::new();
    let run_trigger = trigger.clone();
    let run = tokio::spawn(async move { app.run(run_trigger).await });
    // Wait for the listener to come up.
    sleep(Duration::from_millis(200)).await;

    let health = timeout(Duration::from_secs(5), http_get(addr, "/health"))
        .await
        .expect("health must respond");
    assert!(health.contains("200"), "health must return 200: {health}");
    assert!(health.contains("ok"), "health body: {health}");

    let wired = timeout(Duration::from_secs(5), http_get(addr, "/wired"))
        .await
        .expect("wired must respond");
    assert!(
        wired.contains("repo=true"),
        "route packs must receive the configured storage: {wired}"
    );

    // Lifecycle shutdown returns a clean outcome. (The listener stopping
    // to accept is axum-adapter behavior, covered by its own suite; here
    // the contract is assembly + clean run.)
    trigger.signal();
    let outcome = timeout(Duration::from_secs(5), run)
        .await
        .expect("run must finish")
        .expect("run task must not panic");
    assert!(outcome.is_ok());
}

#[tokio::test]
async fn unknown_server_kind_is_an_error() {
    let yaml = r#"
servers:
  - kind: websocket
    bind: 127.0.0.1:0
"#;
    let error = Bootstrap::from_yaml_str(yaml)
        .expect("yaml must parse")
        .build()
        .await
        .expect_err("unknown kind must fail");
    assert!(matches!(error, BootstrapError::UnknownServerKind(kind) if kind == "websocket"));
}

#[tokio::test]
async fn unknown_route_pack_is_an_error() {
    let yaml = r#"
servers:
  - kind: http
    bind: 127.0.0.1:0
    route_packs: [does-not-exist]
"#;
    let error = Bootstrap::from_yaml_str(yaml)
        .expect("yaml must parse")
        .build()
        .await
        .expect_err("unknown pack must fail");
    assert!(matches!(error, BootstrapError::UnknownRoutePack(name) if name == "does-not-exist"));
}

#[tokio::test]
async fn unknown_storage_engine_is_an_error() {
    let yaml = r#"
storage:
  engine: postgres
servers: []
"#;
    let error = Bootstrap::from_yaml_str(yaml)
        .expect("yaml must parse")
        .build()
        .await
        .expect_err("unknown engine must fail");
    assert!(matches!(error, BootstrapError::UnknownStorageEngine(engine) if engine == "postgres"));
}

#[tokio::test]
async fn malformed_yaml_is_a_config_error() {
    let error =
        Bootstrap::from_yaml_str("app: [unclosed").expect_err("malformed yaml must fail at parse");
    assert!(matches!(error, BootstrapError::Config(_)));
}
