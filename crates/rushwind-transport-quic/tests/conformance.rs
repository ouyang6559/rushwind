//! Lifecycle conformance for the QUIC adapter.
//!
//! The suite's orchestrator-semantics half pins the core; its
//! adapter-behavior half pins this adapter's obligations: real endpoint
//! resolution for `:0` binds, and cooperative shutdown of a real quinn
//! accept loop on signal.

use std::sync::Arc;

use rushwind_transport::Server;
use rushwind_transport_quic::QuicServer;

mod common;

fn quic_server() -> Arc<dyn Server> {
    let server = QuicServer::builder(common::server_config())
        .session_handler(common::echo_session)
        .build(common::bind_addr())
        .expect("ephemeral bind must succeed");
    Arc::new(server)
}

rushwind_testkit::rushwind_conformance_suite!(crate::quic_server);
