//! Lifecycle conformance for the HTTP/3 adapter.
//!
//! The suite's orchestrator-semantics half pins the core; its
//! adapter-behavior half pins this adapter's obligations: real endpoint
//! resolution for `:0` binds, and cooperative shutdown of a real h3
//! accept loop on signal.
//!
//! The server configuration comes from the shared fixture in
//! [`common`], which pins the rustls provider — the workspace build
//! unions more than one provider feature, and the implicit
//! builder-provider resolution is then ambiguous.

use std::sync::Arc;

use rushwind_transport::Server;
use rushwind_transport_h3::H3Server;

mod common;

fn h3_server() -> Arc<dyn Server> {
    let server = H3Server::builder(common::server_config())
        .request_handler(|_request, _stream| async {})
        .build(common::bind_addr())
        .expect("ephemeral bind must succeed");
    Arc::new(server)
}

rushwind_testkit::rushwind_conformance_suite!(crate::h3_server);
