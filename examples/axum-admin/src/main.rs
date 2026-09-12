//! Admin/API surface demo: an axum router served under the RushWind
//! lifecycle.
//!
//! The listener binds eagerly on an OS-assigned port (reported via
//! `endpoint()`), serves a single health route, and shuts down on Ctrl+C
//! (SIGTERM on Unix): axum stops accepting and drains, then the lifecycle
//! completes.
//!
//! Try it: `cargo run -p axum-admin`, then
//! `curl http://<printed-address>/health`, then Ctrl+C.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use axum::routing::get;
use axum::Router;
use rushwind_core::App;
use rushwind_transport::{Server, StopSignal};

async fn health() -> &'static str {
    "ok"
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let router = Router::new().route("/health", get(health));
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
