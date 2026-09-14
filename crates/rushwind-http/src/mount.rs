//! The domain mounts — standard wire locations for the health
//! aggregator and the Prometheus reporter, so every RushWind HTTP
//! service exposes the same probe and scrape paths. Each mount rides
//! its feature: `health`, `metrics`.

#[cfg(feature = "health")]
pub use health_mount::{mount_health, LIVENESS_PATH, READINESS_PATH};
#[cfg(feature = "metrics")]
pub use metrics_mount::{mount_metrics, METRICS_PATH};

#[cfg(feature = "health")]
mod health_mount {
    use std::sync::Arc;

    use axum::routing::get;
    use axum::Router;

    /// The liveness probe path — a constant `200` from
    /// [`liveness_handler`](rushwind_health::liveness_handler): if the
    /// process serves HTTP, it is alive.
    pub const LIVENESS_PATH: &str = "/healthz";
    /// The readiness probe path — `200` when the aggregate is up or
    /// unknown, `503` when any checker is down.
    pub const READINESS_PATH: &str = "/readyz";

    /// Mounts the health aggregator's liveness and readiness probes.
    pub fn mount_health(router: Router, health: Arc<rushwind_health::Health>) -> Router {
        let readiness = Arc::clone(&health);
        router
            .route(
                LIVENESS_PATH,
                get(|| async { rushwind_health::liveness_handler().await }),
            )
            .route(
                READINESS_PATH,
                get(move || {
                    let health = Arc::clone(&readiness);
                    async move { rushwind_health::readiness_handler(health).await }
                }),
            )
    }
}

#[cfg(feature = "metrics")]
mod metrics_mount {
    use std::sync::Arc;

    use axum::response::IntoResponse;
    use axum::routing::get;
    use axum::Router;

    use crate::error::HttpError;

    /// The Prometheus scrape path.
    pub const METRICS_PATH: &str = "/metrics";

    /// The Prometheus exposition content type — the text format version
    /// the client library renders.
    const PROMETHEUS_CONTENT_TYPE: &str = "text/plain; version=0.0.4; charset=utf-8";

    /// Mounts the Prometheus reporter's scrape endpoint. Encode failures
    /// answer the error envelope as `500 / METRICS_ENCODE_FAILED`.
    pub fn mount_metrics(
        router: Router,
        metrics: Arc<rushwind_metrics_prometheus::PrometheusMetrics>,
    ) -> Router {
        router.route(
            METRICS_PATH,
            get(move || {
                let metrics = Arc::clone(&metrics);
                async move {
                    match metrics.encode() {
                        Ok(text) => (
                            [(axum::http::header::CONTENT_TYPE, PROMETHEUS_CONTENT_TYPE)],
                            text,
                        )
                            .into_response(),
                        Err(error) => HttpError::internal(
                            "METRICS_ENCODE_FAILED",
                            format!("metrics encode failed: {error}"),
                        )
                        .into_response(),
                    }
                }
            }),
        )
    }
}
