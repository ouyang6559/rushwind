//! The request-timeout middleware.
//!
//! Bounds the whole downstream chain (handlers included) by a wall-clock
//! budget. An exceeded budget drops the inner future and answers `504`
//! with reason `DEADLINE_EXCEEDED` — the client sees the envelope, the
//! handler is simply gone (the request context
//! cancels).

use std::time::Duration;

use axum::extract::Request;
use axum::middleware::Next;
use axum::response::IntoResponse;

use crate::error::{HttpError, REASON_DEADLINE_EXCEEDED};

/// Wraps the router with the timeout middleware. `budget` bounds each
/// request's downstream execution.
pub fn with_timeout(router: axum::Router, budget: Duration) -> axum::Router {
    router.layer(axum::middleware::from_fn(
        move |req: Request, next: Next| {
            let bounded = next.run(req);
            async move {
                match tokio::time::timeout(budget, bounded).await {
                    Ok(response) => response,
                    Err(_) => HttpError::deadline_exceeded(
                        REASON_DEADLINE_EXCEEDED,
                        "the request exceeded its time budget",
                    )
                    .into_response(),
                }
            }
        },
    ))
}
