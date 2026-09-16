//! Live DTM suite: submits real transactions to a running dtmsvr
//! (`DTM_SERVER`, default the DTM localhost default) and
//! requires DTM itself to drive the participant endpoints. Off by
//! default; CI runs it against a dtm service container.
#![cfg(feature = "live")]

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::http::StatusCode;
use rushwind_transaction_dtm::DtmClient;

fn server() -> String {
    std::env::var("DTM_SERVER").unwrap_or_else(|_| "http://localhost:36789/api/dtmsvr".into())
}

fn unique_gid(prefix: &str) -> String {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or_default();
    format!("{prefix}-{millis}")
}

fn ok_body() -> (StatusCode, &'static str) {
    (StatusCode::OK, r#"{"dtm_result":"SUCCESS"}"#)
}

fn failure_body() -> (StatusCode, &'static str) {
    (StatusCode::OK, r#"{"dtm_result":"FAILURE"}"#)
}

/// Serves the TCC participant endpoints, counting calls per path.
async fn spawn_tcc_busi() -> (String, Arc<AtomicUsize>, Arc<AtomicUsize>) {
    let try_calls = Arc::new(AtomicUsize::new(0));
    let confirm_calls = Arc::new(AtomicUsize::new(0));
    let try_for_route = try_calls.clone();
    let confirm_for_route = confirm_calls.clone();
    let app = axum::Router::new()
        .route(
            "/try",
            axum::routing::post(move || async move {
                try_for_route.fetch_add(1, Ordering::SeqCst);
                ok_body()
            }),
        )
        .route(
            "/confirm",
            axum::routing::post(move || async move {
                confirm_for_route.fetch_add(1, Ordering::SeqCst);
                ok_body()
            }),
        )
        .route("/cancel", axum::routing::post(|| async move { ok_body() }));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (format!("http://{address}"), try_calls, confirm_calls)
}

/// Serves the msg participant endpoints, counting calls per path.
async fn spawn_msg_busi() -> (String, Arc<AtomicUsize>) {
    let action_calls = Arc::new(AtomicUsize::new(0));
    let action_for_route = action_calls.clone();
    let app = axum::Router::new().route(
        "/action",
        axum::routing::post(move || async move {
            action_for_route.fetch_add(1, Ordering::SeqCst);
            ok_body()
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (format!("http://{address}"), action_calls)
}

/// Serves the query-prepared endpoint for msg.
async fn spawn_query_prepared() -> String {
    let app = axum::Router::new().route(
        "/query-prepared",
        axum::routing::get(|| async move { ok_body() }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    format!("http://{address}/query-prepared")
}

#[tokio::test]
async fn tcc_branches_confirm_against_a_real_dtmsvr() {
    let (busi, try_calls, confirm_calls) = spawn_tcc_busi().await;
    let client = DtmClient::with_server(server());

    client
        .tcc_global_transaction(unique_gid("rushwind-live-tcc"), |mut tcc| async move {
            tcc.call_branch(
                &serde_json::json!({ "item": 1 }),
                &format!("{busi}/try"),
                &format!("{busi}/confirm"),
                &format!("{busi}/cancel"),
            )
            .await
        })
        .await
        .unwrap();

    wait_for(&confirm_calls, "the tcc confirm").await;
    assert_eq!(try_calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn msg_action_runs_after_submit_against_a_real_dtmsvr() {
    let (busi, action_calls) = spawn_msg_busi().await;
    let query_prepared = spawn_query_prepared().await;
    let client = DtmClient::with_server(server());

    client
        .msg(unique_gid("rushwind-live-msg"))
        .add(&format!("{busi}/action"), &serde_json::json!({ "n": 1 }))
        .unwrap()
        .do_and_submit(query_prepared, || async { Ok(()) })
        .await
        .unwrap();

    wait_for(&action_calls, "the msg action").await;
}

/// Serves the participant endpoints, counting calls per path.
async fn spawn_busi() -> (String, Arc<AtomicUsize>, Arc<AtomicUsize>) {
    let action_calls = Arc::new(AtomicUsize::new(0));
    let compensate_calls = Arc::new(AtomicUsize::new(0));
    let action_for_route = action_calls.clone();
    let action_fails_for_route = action_calls.clone();
    let compensate_for_route = compensate_calls.clone();
    let app = axum::Router::new()
        .route(
            "/action",
            axum::routing::post(move || async move {
                action_for_route.fetch_add(1, Ordering::SeqCst);
                ok_body()
            }),
        )
        .route(
            "/action-fails",
            axum::routing::post(move || async move {
                action_fails_for_route.fetch_add(1, Ordering::SeqCst);
                failure_body()
            }),
        )
        .route(
            "/compensate",
            axum::routing::post(move || async move {
                compensate_for_route.fetch_add(1, Ordering::SeqCst);
                ok_body()
            }),
        );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (format!("http://{address}"), action_calls, compensate_calls)
}

async fn wait_for(counter: &Arc<AtomicUsize>, what: &str) {
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    while counter.load(Ordering::SeqCst) == 0 {
        assert!(
            std::time::Instant::now() < deadline,
            "dtm never called {what}"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

#[tokio::test]
async fn saga_forward_action_runs_against_a_real_dtmsvr() {
    let (busi, action_calls, compensate_calls) = spawn_busi().await;
    let client = DtmClient::with_server(server());

    client
        .saga(unique_gid("rushwind-live-saga"))
        .add(
            format!("{busi}/action"),
            format!("{busi}/compensate"),
            &serde_json::json!({ "n": 1 }),
        )
        .unwrap()
        .submit()
        .await
        .unwrap();

    wait_for(&action_calls, "the saga action").await;
    assert_eq!(
        compensate_calls.load(Ordering::SeqCst),
        0,
        "a successful action must not be compensated"
    );
}

#[tokio::test]
async fn failing_saga_action_gets_compensated() {
    let (busi, action_calls, compensate_calls) = spawn_busi().await;
    let client = DtmClient::with_server(server());

    client
        .saga(unique_gid("rushwind-live-saga-fail"))
        .add(
            format!("{busi}/action-fails"),
            format!("{busi}/compensate"),
            &serde_json::json!({ "n": 2 }),
        )
        .unwrap()
        .submit()
        .await
        .unwrap();

    wait_for(&compensate_calls, "the saga compensate").await;
    assert!(action_calls.load(Ordering::SeqCst) >= 1);
}
