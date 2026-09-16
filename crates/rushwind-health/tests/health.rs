//! The health domain's conformance suite: aggregation rules, per-check
//! timeouts, combinators, and the HTTP edge.

#![cfg(test)]

use std::sync::Arc;
use std::time::Duration;

use rushwind_health::{
    all, any, http, liveness_handler, ping, readiness_handler, tcp, Health, HealthOptions,
};

/// No checkers registered: the aggregate is up with the
/// "no checkers registered" message.
#[tokio::test]
async fn empty_aggregate_is_up() {
    let health = Health::new(HealthOptions::default());
    let result = health.check().await;
    assert_eq!(result.status.as_str(), "up");
    assert_eq!(result.message, "no checkers registered");
}

/// Any down checker makes the aggregate down; unknown only surfaces
/// when nothing is down — the aggregation rules.
#[tokio::test]
async fn aggregation_rules() {
    let health = Health::new(HealthOptions::default());
    health.register_ping("ok", || async { Ok(()) }).await;
    health
        .register_ping("broken", || async { Err("connection refused".to_string()) })
        .await;
    let result = health.check().await;
    assert_eq!(result.status.as_str(), "down");
    assert_eq!(
        result.checks.get("broken").map(|d| d.message.as_str()),
        Some("connection refused")
    );

    // Replace the failing checker; unknown now surfaces.
    health.deregister("broken").await;
    health
        .register_ping("unsure", || async { Err("not checked".to_string()) })
        .await;
    let result = health.check().await;
    assert_eq!(result.status.as_str(), "down");
}

/// A timed-out checker reports down with a timeout message.
#[tokio::test]
async fn timeout_reports_down() {
    let health = Health::new(HealthOptions {
        timeout: Duration::from_millis(100),
    });
    health
        .register_ping("slow", || async {
            tokio::time::sleep(Duration::from_secs(2)).await;
            Ok(())
        })
        .await;
    let result = health.check().await;
    assert_eq!(result.status.as_str(), "down");
    assert!(
        result
            .checks
            .get("slow")
            .map(|detail| detail.message.contains("timed out"))
            .unwrap_or(false),
        "the timed-out checker must carry a timeout message"
    );
}

/// The TCP checker dials a local listener: up when listening, down
/// otherwise.
#[tokio::test]
async fn tcp_checker_dials() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind works");
    let addr = listener.local_addr().expect("local addr");

    let checker = tcp(addr.to_string(), Some(Duration::from_secs(2)));
    assert_eq!(checker().await.status.as_str(), "up");

    drop(listener);
    let checker = tcp(addr.to_string(), Some(Duration::from_millis(300)));
    assert_eq!(checker().await.status.as_str(), "down");
}

/// The all/any combinators short-circuit on their first verdict.
#[tokio::test]
async fn combinators() {
    let up = ping(|| async { Ok::<(), String>(()) });
    let down = ping(|| async { Err("boom".to_string()) });

    let every = all(vec![up.clone(), down.clone(), up.clone()]);
    assert_eq!(every().await.status.as_str(), "down");

    let either = any(vec![down.clone(), up.clone()]);
    assert_eq!(either().await.status.as_str(), "up");

    let all_failed = any(vec![down.clone(), down]);
    assert_eq!(all_failed().await.status.as_str(), "down");
}

/// The HTTP edge: readiness runs the aggregate (503 when down), and
/// liveness is a constant 200.
#[tokio::test]
async fn readiness_and_liveness_handlers() {
    let health = Arc::new(Health::new(HealthOptions::default()));
    health
        .register_ping("dependency", || async { Err("unreachable".to_string()) })
        .await;

    let (status_code, body) = readiness_handler(Arc::clone(&health)).await;
    assert_eq!(status_code, axum::http::StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(body.status, "down");

    health.deregister("dependency").await;
    let (status_code, body) = readiness_handler(Arc::clone(&health)).await;
    assert_eq!(status_code, axum::http::StatusCode::OK);
    assert_eq!(body.status, "up");

    let (status_code, body) = liveness_handler().await;
    assert_eq!(status_code, axum::http::StatusCode::OK);
    assert_eq!(body.status, "up");
}

/// The HTTP checker validates a real endpoint's status.
#[tokio::test]
async fn http_checker_checks_status() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind works");
    let addr = listener.local_addr().expect("local addr");
    tokio::spawn(async move {
        loop {
            let Ok((mut stream, _)) = listener.accept().await else {
                return;
            };
            tokio::spawn(async move {
                // A minimal HTTP/1.1 404 responder: any GET gets 404,
                // proving the checker rejects non-2xx.
                let mut buffer = [0u8; 256];
                let _ = tokio::io::AsyncReadExt::read(&mut stream, &mut buffer).await;
                let _ = tokio::io::AsyncWriteExt::write_all(
                    &mut stream,
                    b"HTTP/1.1 404 Not Found
Content-Length: 0

",
                )
                .await;
            });
        }
    });

    let checker = http(
        format!("http://{addr}/healthz"),
        Some(Duration::from_secs(2)),
    );
    let result = checker().await;
    assert_eq!(result.status.as_str(), "down");
}
