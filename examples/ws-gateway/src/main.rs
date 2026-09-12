//! Gated WebSocket gateway demo.
//!
//! An echo session behind a demo handshake gate and a session cap, served
//! under the RushWind lifecycle. The route's shutdown bus is registered
//! with the server, so Ctrl+C tears the sessions down together with the
//! listener instead of leaving them to die with the process.
//!
//! The demo gate refuses handshakes lacking the `X-Demo-Token` header —
//! deliberately a header browsers cannot send, so a browser's
//! `new WebSocket(...)` is rejected with 403. A scripting client that
//! sends the header gets an echo session.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use axum::extract::ws::WebSocket;
use axum::Router;
use rushwind_core::App;
use rushwind_transport::{GateVerdict, Handshake, HandshakeGate, Rejection, Server, StopSignal};
use rushwind_transport_ws::WsRoute;

struct DemoTokenGate;

impl HandshakeGate for DemoTokenGate {
    fn name(&self) -> String {
        "demo-token".to_string()
    }
    fn inspect(&self, handshake: &Handshake) -> GateVerdict {
        let token_ok = handshake.headers.iter().any(|(name, value)| {
            name.eq_ignore_ascii_case("x-demo-token") && value == "demo-token"
        });
        if token_ok {
            GateVerdict::Continue
        } else {
            GateVerdict::Reject(Rejection {
                status: 403,
                reason: "missing demo token".to_string(),
            })
        }
    }
}

async fn echo_session(mut socket: WebSocket) {
    while let Some(Ok(msg)) = socket.recv().await {
        if socket.send(msg).await.is_err() {
            break;
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let (route, bus) = WsRoute::new()
        .gate(Arc::new(DemoTokenGate))
        .max_concurrent_sessions(16)
        .session_handler(echo_session)
        .build()?;
    let server = rushwind_transport_axum::AxumServer::new(
        SocketAddr::from(([127, 0, 0, 1], 0)),
        Router::new().route("/ws", route),
    )?
    .with_aux_shutdown(bus);
    println!("[gateway] bound on {}", server.endpoint()?);

    let app = App::builder()
        .name("ws-gateway-demo")
        .version("0.0.1")
        .erased_server(Arc::new(server))
        .stop_timeout(Duration::from_secs(10))
        .build();
    let result = app.run(StopSignal::new()).await;
    println!("[gateway] lifecycle finished: {result:?}");
    Ok(())
}
