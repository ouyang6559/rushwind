//! Gated QUIC gateway demo.
//!
//! An echo session behind a loopback-only gate, a session cap, and a
//! handshake deadline, served under the RushWind lifecycle. This is the
//! raw-transport shape of the session middleware chain: the gate sees the
//! peer address — the only evidence a raw transport offers — and refuses
//! every non-loopback peer. Lifecycle shutdown (Ctrl+C / SIGTERM) closes
//! the endpoint and every connection it owns.
//!
//! The certificate is a throwaway self-signed pair generated at startup —
//! real deployments provision certificates out-of-band and pass the quinn
//! server configuration of their choosing.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use quinn::Connection;
use rcgen::generate_simple_self_signed;
use rushwind_core::App;
use rushwind_transport::{GateVerdict, Handshake, HandshakeGate, Rejection, Server, StopSignal};
use rushwind_transport_quic::QuicServer;

/// A gate that admits loopback peers only — the raw-transport analog of a
/// header check: a single piece of local evidence, one sync verdict.
struct LoopbackOnlyGate;

impl HandshakeGate for LoopbackOnlyGate {
    fn name(&self) -> String {
        "loopback-only".to_string()
    }
    fn inspect(&self, handshake: &Handshake) -> GateVerdict {
        match handshake.remote {
            Some(addr) if addr.ip().is_loopback() => GateVerdict::Continue,
            _ => GateVerdict::Reject(Rejection {
                status: 403,
                reason: "non-loopback peers are refused".to_string(),
            }),
        }
    }
}

async fn echo_session(conn: Connection) {
    loop {
        let (mut tx, mut rx) = match conn.accept_bi().await {
            Ok(pair) => pair,
            Err(_) => return,
        };
        tokio::spawn(async move {
            let mut buf = [0u8; 4096];
            loop {
                match rx.read(&mut buf).await {
                    Ok(Some(n)) => {
                        if tx.write_all(&buf[..n]).await.is_err() {
                            return;
                        }
                    }
                    Ok(None) | Err(_) => {
                        let _ = tx.finish();
                        return;
                    }
                }
            }
        });
    }
}

fn quinn_config() -> Result<quinn::ServerConfig, Box<dyn std::error::Error>> {
    let cert = generate_simple_self_signed(vec!["localhost".to_string()])?;
    let cert_der = rustls_pki_types::CertificateDer::from(cert.cert);
    let priv_key = rustls_pki_types::PrivatePkcs8KeyDer::from(cert.signing_key.serialize_der());
    Ok(quinn::ServerConfig::with_single_cert(
        vec![cert_der],
        priv_key.into(),
    )?)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let server = QuicServer::builder(quinn_config()?)
        .gate(Arc::new(LoopbackOnlyGate))
        .max_concurrent_sessions(16)
        .handshake_timeout(Duration::from_secs(5))
        .session_handler(echo_session)
        .build(SocketAddr::from(([127, 0, 0, 1], 0)))?;
    println!("[gateway] bound on {}", server.endpoint()?);

    let app = App::builder()
        .name("quic-gateway-demo")
        .version("0.0.1")
        .erased_server(Arc::new(server))
        .stop_timeout(Duration::from_secs(10))
        .build();
    let result = app.run(StopSignal::new()).await;
    println!("[gateway] lifecycle finished: {result:?}");
    Ok(())
}
