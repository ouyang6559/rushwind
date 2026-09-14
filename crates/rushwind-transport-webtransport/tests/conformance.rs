//! Lifecycle conformance for the WebTransport adapter.

use std::sync::Arc;

use rushwind_transport::Server;
use rushwind_transport_webtransport::WebTransportServer;

fn webtransport_server() -> Arc<dyn Server> {
    let identity = wtransport::Identity::self_signed(["localhost"]).expect("identity");
    let server = WebTransportServer::builder(identity)
        .session_handler(|_session| async {})
        .build("127.0.0.1:0".parse().unwrap())
        .expect("server must build");
    Arc::new(server)
}

rushwind_testkit::rushwind_conformance_suite!(crate::webtransport_server);
