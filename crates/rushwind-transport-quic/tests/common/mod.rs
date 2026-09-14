//! Shared test fixtures: an rcgen self-signed server configuration, an
//! any-certificate client endpoint, and an echo session handler.
//!
//! Test-only; the certificate machinery exists because QUIC has no
//! plaintext mode and the conformance and integration suites need
//! endpoints on demand.
#![allow(dead_code)]

use std::net::SocketAddr;
use std::sync::Arc;

use quinn::Connection;
use rustls::pki_types::{CertificateDer, PrivatePkcs8KeyDer, ServerName, UnixTime};

/// A loopback bind address with an OS-assigned port.
pub fn bind_addr() -> SocketAddr {
    SocketAddr::from(([127, 0, 0, 1], 0))
}

/// Installs the ring provider as the process default, once. The
/// dependency graph unions more than one rustls provider feature, so
/// the implicit builder-provider resolution is ambiguous and panics;
/// pinning the provider here keeps the fixtures hermetic to that
/// union. Idempotent: a second call is a no-op error that is ignored.
fn ensure_provider() {
    let _ = rustls::crypto::ring::default_provider().install_default();
}

/// A self-signed quinn server configuration for "localhost".
pub fn server_config() -> quinn::ServerConfig {
    ensure_provider();
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".to_string()])
        .expect("self-signed certificate generation must succeed");
    let cert_der = CertificateDer::from(cert.cert);
    let priv_key = PrivatePkcs8KeyDer::from(cert.signing_key.serialize_der());
    quinn::ServerConfig::with_single_cert(vec![cert_der], priv_key.into())
        .expect("server config must build")
}

/// A client endpoint that accepts any server certificate.
pub fn client_endpoint() -> quinn::Endpoint {
    ensure_provider();
    let mut endpoint = quinn::Endpoint::client(SocketAddr::from(([127, 0, 0, 1], 0)))
        .expect("client bind must succeed");
    let rustls_config = rustls::ClientConfig::builder()
        .dangerous()
        .with_custom_certificate_verifier(SkipServerVerification::new())
        .with_no_client_auth();
    let quinn_config = quinn::ClientConfig::new(Arc::new(
        quinn::crypto::rustls::QuicClientConfig::try_from(rustls_config)
            .expect("client crypto config must build"),
    ));
    endpoint.set_default_client_config(quinn_config);
    endpoint
}

/// Connects `client` to `addr`, completing the QUIC handshake.
pub async fn connect(client: &quinn::Endpoint, addr: SocketAddr) -> Connection {
    client
        .connect(addr, "localhost")
        .expect("connect must start")
        .await
        .expect("connect must succeed")
}

/// An echo session: every bidirectional stream echoes its bytes back and
/// finishes.
pub async fn echo_session(conn: Connection) {
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

/// One echo roundtrip attempt over a fresh bidirectional stream.
///
/// `true` when the session is alive and echoed the expected bytes; `false`
/// on any connection, stream, or response-timeout failure. Opening a
/// stream is a local flow-control check on QUIC — aliveness is only
/// proven by the echo actually coming back.
pub async fn echo_alive(conn: &Connection) -> bool {
    let (mut tx, mut rx) = match conn.open_bi().await {
        Ok(pair) => pair,
        Err(_) => return false,
    };
    if tx.write_all(b"ping").await.is_err() {
        return false;
    }
    if tx.finish().is_err() {
        return false;
    }
    let mut data = Vec::new();
    let mut buf = [0u8; 4096];
    loop {
        match tokio::time::timeout(std::time::Duration::from_secs(2), rx.read(&mut buf)).await {
            Err(_elapsed) => return false,
            Ok(Err(_stream_error)) => return false,
            Ok(Ok(None)) => break,
            Ok(Ok(Some(n))) => data.extend_from_slice(&buf[..n]),
        }
    }
    data.as_slice() == &b"ping"[..]
}

/// Asserts a live echo roundtrip.
pub async fn echo_roundtrip(conn: &Connection) {
    assert!(
        echo_alive(conn).await,
        "echo roundtrip must succeed on a live session"
    );
}

/// Repeatedly probes the connection until an echo attempt fails, proving
/// the server tore the session down; panics if the connection is still
/// echoing after `window`.
pub async fn await_connection_death(conn: &Connection, window: std::time::Duration) {
    let deadline = std::time::Instant::now() + window;
    while echo_alive(conn).await {
        assert!(
            std::time::Instant::now() < deadline,
            "session survived lifecycle shutdown"
        );
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
}

/// A certificate verifier that accepts anything.
///
/// Test-only: this is exactly the construction quinn's own examples use,
/// vulnerable to machine-in-the-middle by design.
#[derive(Debug)]
struct SkipServerVerification(Arc<rustls::crypto::CryptoProvider>);

impl SkipServerVerification {
    fn new() -> Arc<Self> {
        Arc::new(Self(Arc::new(rustls::crypto::ring::default_provider())))
    }
}

impl rustls::client::danger::ServerCertVerifier for SkipServerVerification {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp: &[u8],
        _now: UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(
            message,
            cert,
            dss,
            &self.0.signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(
            message,
            cert,
            dss,
            &self.0.signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        self.0.signature_verification_algorithms.supported_schemes()
    }
}
