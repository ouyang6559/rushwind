#![cfg(feature = "health")]

//! The health mounts over oneshot: `/healthz` answers a constant 200,
//! `/readyz` runs the aggregator — 503 when a checker reports down.

use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use axum::Router;
use rushwind_health::{Health, HealthOptions};
use rushwind_http::{mount_health, LIVENESS_PATH, READINESS_PATH};
use serde_json::Value;
use tower::ServiceExt;

async fn get_json(app: Router, uri: &str) -> (StatusCode, Value) {
    let response = app
        .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    (status, serde_json::from_slice(&bytes).unwrap())
}

#[tokio::test]
async fn liveness_is_a_constant_200() {
    let app = mount_health(Router::new(), Arc::new(Health::default()));
    let (status, body) = get_json(app, LIVENESS_PATH).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["status"], Value::String("up".into()));
}

#[tokio::test]
async fn readiness_runs_the_aggregate() {
    let health = Arc::new(Health::new(HealthOptions {
        timeout: Duration::from_secs(1),
    }));
    health
        .register_ping("database", || async {
            Err("connection refused".to_owned())
        })
        .await;
    let app = mount_health(Router::new(), health);

    let (status, body) = get_json(app.clone(), READINESS_PATH).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(body["status"], Value::String("down".into()));
    assert_eq!(
        body["checks"]["database"]["status"],
        Value::String("down".into())
    );

    let (status, _) = get_json(app, LIVENESS_PATH).await;
    assert_eq!(status, StatusCode::OK);
}
