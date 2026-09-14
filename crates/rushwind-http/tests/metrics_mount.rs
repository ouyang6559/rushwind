#![cfg(feature = "metrics")]

//! The metrics mount over oneshot: `/metrics` answers the Prometheus
//! text format with its content type.

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use axum::Router;
use rushwind_http::{mount_metrics, METRICS_PATH};
use rushwind_metrics_prometheus::{PrometheusMetrics, PrometheusOptions};
use tower::ServiceExt;

#[tokio::test]
async fn metrics_scrape_answers_the_text_format() {
    let metrics = Arc::new(PrometheusMetrics::new(PrometheusOptions::default()));
    let app = mount_metrics(Router::new(), metrics);

    let response = app
        .oneshot(
            Request::builder()
                .uri(METRICS_PATH)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let content_type = response
        .headers()
        .get("content-type")
        .and_then(|value| value.to_str().ok())
        .unwrap()
        .to_owned();
    assert!(content_type.starts_with("text/plain"), "{content_type}");
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    assert!(bytes.is_empty() || bytes.starts_with(b"#"));
}
