//! Lifecycle conformance for the axum adapter.
//!
//! The suite's orchestrator-semantics half pins the core; its
//! adapter-behavior half pins this adapter's obligations: real endpoint
//! resolution for `:0` binds, and cooperative shutdown of a real axum
//! serve loop on signal.

use std::net::SocketAddr;
use std::sync::Arc;

use axum::Router;
use rushwind_transport::Server;
use rushwind_transport_axum::AxumServer;

fn axum_server() -> Arc<dyn Server> {
    let bind = SocketAddr::from(([127, 0, 0, 1], 0));
    let server = AxumServer::new(bind, Router::new()).expect("ephemeral bind must succeed");
    Arc::new(server)
}

rushwind_testkit::rushwind_conformance_suite!(crate::axum_server);
