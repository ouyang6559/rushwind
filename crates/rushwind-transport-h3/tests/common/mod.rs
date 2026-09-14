//! Shared test fixtures: an rcgen self-signed server configuration, an
//! any-certificate quinn client endpoint, and h3 client plumbing.
//!
//! Test-only; the certificate machinery exists because QUIC has no
//! plaintext mode and the conformance and integration suites need
//! endpoints on demand.
#![allow(dead_code)]

use std::net::SocketAddr;
use std::sync::Arc;

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

/// A self-signed quinn server configuration for "localhost",
/// advertising the `h3` ALPN protocol — the default single-cert
/// builder advertises none, and the h3 handshake then fails.
pub fn server_config() -> quinn::ServerConfig {
    ensure_provider();
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".to_string()])
        .expect("self-signed certificate generation must succeed");
    let cert_der = CertificateDer::from(cert.cert);
    let priv_key = PrivatePkcs8KeyDer::from(cert.signing_key.serialize_der());
    let mut rustls_config = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(vec![cert_der], priv_key.into())
        .expect("rustls server config must build");
    rustls_config.alpn_protocols = vec![b"h3".to_vec()];
    quinn::ServerConfig::with_crypto(Arc::new(
        quinn::crypto::rustls::QuicServerConfig::try_from(rustls_config)
            .expect("quic server config must build"),
    ))
}

/// A client endpoint that accepts any server certificate and
/// negotiates the `h3` ALPN protocol.
pub fn client_endpoint() -> quinn::Endpoint {
    ensure_provider();
    let mut endpoint = quinn::Endpoint::client(bind_addr()).expect("client bind must succeed");
    let mut rustls_config = rustls::ClientConfig::builder()
        .dangerous()
        .with_custom_certificate_verifier(SkipServerVerification::new())
        .with_no_client_auth();
    rustls_config.alpn_protocols = vec![b"h3".to_vec()];
    let quinn_config = quinn::ClientConfig::new(Arc::new(
        quinn::crypto::rustls::QuicClientConfig::try_from(rustls_config)
            .expect("client crypto config must build"),
    ));
    endpoint.set_default_client_config(quinn_config);
    endpoint
}

/// The h3 client request-sender type over h3-quinn streams.
type H3SendRequest = h3::client::SendRequest<h3_quinn::OpenStreams, bytes::Bytes>;

/// An h3 client connection over a fresh quinn connection. The
/// returned [`h3::client::SendRequest`] issues requests on the
/// connection; its protocol driver maintains the connection state on
/// a task of its own — dropping the driver closes the control
/// stream, killing the connection.
pub async fn h3_client(client: &quinn::Endpoint, addr: SocketAddr) -> Option<H3SendRequest> {
    let conn = client.connect(addr, "localhost").ok()?.await.ok()?;
    let (mut connection, send_request) = h3::client::new(h3_quinn::Connection::new(conn))
        .await
        .ok()?;
    tokio::spawn(async move {
        let _ = connection.wait_idle().await;
    });
    Some(send_request)
}

/// One request-response attempt on an existing connection. The
/// optional `x-probe` header rides the request. `None` marks failure
/// at any stage — request, stream, or response.
pub async fn h3_request_via(
    send_request: &mut H3SendRequest,
    probe: Option<&str>,
) -> Option<http::Response<()>> {
    let mut builder = http::Request::get("https://localhost/probe");
    if let Some(probe) = probe {
        builder = builder.header("x-probe", probe);
    }
    let request = match builder.body(()) {
        Ok(request) => request,
        Err(_error) => {
            return None;
        }
    };
    let mut req_stream = match send_request.send_request(request).await {
        Ok(stream) => stream,
        Err(_error) => {
            return None;
        }
    };
    if req_stream.finish().await.is_err() {
        return None;
    }
    let response = match req_stream.recv_response().await {
        Ok(response) => response,
        Err(_error) => {
            return None;
        }
    };
    Some(response)
}

/// One request-response attempt on a fresh connection.
pub async fn h3_request(
    client: &quinn::Endpoint,
    addr: SocketAddr,
    probe: Option<&str>,
) -> Option<http::Response<()>> {
    let mut send_request = h3_client(client, addr).await?;
    h3_request_via(&mut send_request, probe).await
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
