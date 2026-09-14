//! The request-logging middleware — the Go `logging`.
//!
//! One `tracing` span per request under the `rushwind.http` target, the
//! same convention `rushwind-storage-observe` uses for the storage line
//! (`rushwind.storage`). The span carries method, path, the
//! [`RequestId`](crate::RequestId) when the request-id layer wrapped the
//! route first, and records status plus wall-clock latency on
//! completion. Export is the subscriber's job — the crate never
//! initializes one.

use std::time::Instant;

use axum::extract::Request;
use axum::middleware::Next;
use axum::response::Response;

/// Wraps the router with the request-logging middleware.
pub fn with_logging(router: axum::Router) -> axum::Router {
    router.layer(axum::middleware::from_fn(
        |req: Request, next: Next| async move {
            let method = req.method().clone();
            let path = req.uri().path().to_owned();
            let request_id = req
                .extensions()
                .get::<crate::RequestId>()
                .map(|id| id.as_str().to_owned());
            let span = tracing::info_span!(
                target: "rushwind.http",
                "request",
                http.method = %method,
                http.path = %path,
                http.request_id = request_id.as_deref().unwrap_or(""),
                http.status = tracing::field::Empty,
                http.latency_ms = tracing::field::Empty,
            );
            let started = Instant::now();
            let response: Response = next.run(req).await;
            span.record("http.status", response.status().as_u16());
            span.record("http.latency_ms", started.elapsed().as_millis() as u64);
            tracing::info!(target: "rushwind.http", parent: &span, "request handled");
            response
        },
    ))
}
