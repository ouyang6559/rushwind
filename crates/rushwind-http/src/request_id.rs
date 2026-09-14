//! The request-id middleware — the Go `requestid`.
//!
//! Echoes the inbound `x-request-id` or mints one from the OS CSPRNG
//! (32 hex chars, the same shape the session engine's IDs use), inserts
//! [`RequestId`] into the request extensions, and stamps the response
//! header so clients and logs can be correlated.

use std::sync::Arc;

use axum::extract::Request;
use axum::middleware::Next;

/// The request-id header — lowercase, the wire form HTTP/2 normalizes to.
pub const HEADER_X_REQUEST_ID: &str = "x-request-id";

/// The request id, inserted into the request extensions by
/// [`with_request_id`]. Clone into logs and downstream calls.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RequestId(pub Arc<str>);

impl RequestId {
    /// The id string.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Wraps the router with the request-id middleware using the default
/// generator (16 CSPRNG bytes, hex).
pub fn with_request_id(router: axum::Router) -> axum::Router {
    with_request_id_generator(router, Arc::new(generate_request_id))
}

/// Wraps the router with the request-id middleware using a custom
/// generator (trace-context ids, a counter in tests, …).
pub fn with_request_id_generator(
    router: axum::Router,
    generate: Arc<dyn Fn() -> String + Send + Sync>,
) -> axum::Router {
    router.layer(axum::middleware::from_fn(
        move |mut req: Request, next: Next| {
            let generate = Arc::clone(&generate);
            async move {
                let request_id = req
                    .headers()
                    .get(HEADER_X_REQUEST_ID)
                    .and_then(|value| value.to_str().ok())
                    .map(str::to_owned)
                    .unwrap_or_else(|| generate());
                req.extensions_mut()
                    .insert(RequestId(request_id.clone().into()));
                let mut response = next.run(req).await;
                if let Ok(value) = axum::http::HeaderValue::from_str(&request_id) {
                    response.headers_mut().insert(
                        axum::http::HeaderName::from_static(HEADER_X_REQUEST_ID),
                        value,
                    );
                }
                response
            }
        },
    ))
}

/// Mints a request id from the OS CSPRNG: 16 bytes, 32 hex chars —
/// the same shape the session engine's session ids use.
pub fn generate_request_id() -> String {
    let mut bytes = [0u8; 16];
    getrandom::fill(&mut bytes).expect("the OS CSPRNG does not fail");
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(32);
    for byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0xf) as usize] as char);
    }
    out
}
