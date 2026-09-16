//! Wire conformance for the DTM engine, against a local axum mock
//! dtmsvr: the trans-base JSON for each pattern, the branch query
//! parameters, the registerBranch bodies, the prepare/submit/abort
//! flows, and the error mappings — pinned against the DTM protocol
//! as spoken by the reference `dtmcli` client.

use std::sync::{Arc, Mutex};

use axum::body::Bytes;
use axum::extract::State;
use axum::http::header::CONTENT_TYPE;
use axum::http::{Method, StatusCode, Uri};
use axum::response::{IntoResponse, Response};
use rushwind_transaction::{TransactionClient, TransactionError};
use rushwind_transaction_dtm::DtmClient;

/// What one request looked like on the wire.
#[derive(Debug)]
struct Recorded {
    method: String,
    path: String,
    query: Option<String>,
    body: serde_json::Value,
}

#[derive(Clone)]
enum Reply {
    /// 200 with a dtmsvr success body.
    Success,
    /// An arbitrary status + body (for the error mappings).
    Status(StatusCode, &'static str),
}

#[derive(Clone)]
struct AppState {
    recorded: Arc<Mutex<Vec<Recorded>>>,
    reply: Reply,
}

async fn handler(method: Method, uri: Uri, State(state): State<AppState>, body: Bytes) -> Response {
    state.recorded.lock().unwrap().push(Recorded {
        method: method.to_string(),
        path: uri.path().to_string(),
        query: uri.query().map(str::to_string),
        body: serde_json::from_slice(&body).unwrap_or(serde_json::Value::Null),
    });
    match &state.reply {
        Reply::Success => (
            StatusCode::OK,
            [(CONTENT_TYPE, "application/json")],
            r#"{"dtm_result":"SUCCESS"}"#.to_string(),
        )
            .into_response(),
        Reply::Status(code, text) => (*code, text.to_string()).into_response(),
    }
}

/// Spawns the mock dtmsvr on an ephemeral port; returns the server
/// URL and the request sequence.
async fn spawn(reply: Reply) -> (String, Arc<Mutex<Vec<Recorded>>>) {
    let recorded = Arc::new(Mutex::new(Vec::new()));
    let state = AppState {
        recorded: recorded.clone(),
        reply,
    };
    let app = axum::Router::new().fallback(handler).with_state(state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (format!("http://{address}"), recorded)
}

/// A business endpoint suite on the same mock (any path not named
/// above hits the same fallback), with its own canned reply.
async fn spawn_pair(
    dtm_reply: Reply,
    branch_reply: Reply,
) -> (
    (String, Arc<Mutex<Vec<Recorded>>>),
    (String, Arc<Mutex<Vec<Recorded>>>),
) {
    (spawn(dtm_reply).await, spawn(branch_reply).await)
}

fn query_of(recorded: &[Recorded], index: usize) -> Vec<(String, String)> {
    fn decode(value: &str) -> String {
        value
            .replace("%3A", ":")
            .replace("%2F", "/")
            .replace("%3F", "?")
            .replace("%3D", "=")
            .replace("%26", "&")
    }
    let query = recorded[index].query.as_deref().unwrap_or("");
    query
        .split('&')
        .map(|pair| {
            let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
            (decode(key), decode(value))
        })
        .collect()
}

#[tokio::test]
async fn saga_submit_carries_steps_payloads_and_go_tags() {
    let (server, recorded) = spawn(Reply::Success).await;
    let client = DtmClient::with_server(&server);

    let saga = client
        .saga("order-123")
        .add(
            "/order/create",
            "/order/create-compensate",
            &serde_json::json!({"amount": 10}),
        )
        .unwrap()
        .add(
            "/stock/deduct",
            "/stock/deduct-compensate",
            &serde_json::json!({}),
        )
        .unwrap();
    saga.submit().await.unwrap();

    let recorded = recorded.lock().unwrap();
    assert_eq!(recorded.len(), 1);
    assert_eq!(recorded[0].method, "POST");
    assert_eq!(recorded[0].path, "/submit");
    assert_eq!(recorded[0].body["gid"], "order-123");
    assert_eq!(recorded[0].body["trans_type"], "saga");
    assert_eq!(recorded[0].body["concurrent"], false);
    assert_eq!(recorded[0].body["protocol"], "");
    assert_eq!(recorded[0].body["steps"][0]["action"], "/order/create");
    assert_eq!(
        recorded[0].body["steps"][0]["compensate"],
        "/order/create-compensate"
    );
    assert_eq!(recorded[0].body["steps"][1]["action"], "/stock/deduct");
    // Payloads ride as parallel JSON-encoded strings, per the DTM
    // protocol.
    assert_eq!(
        recorded[0].body["payloads"][0],
        serde_json::json!("{\"amount\":10}")
    );
    assert_eq!(recorded[0].body["payloads"][1], serde_json::json!("{}"));
    assert!(recorded[0].body.get("custom_data").is_none());
    assert!(recorded[0].body.get("query_prepared").is_none());
}

#[tokio::test]
async fn concurrent_saga_stamps_orders_into_custom_data() {
    let (server, recorded) = spawn(Reply::Success).await;
    let client = DtmClient::with_server(&server);

    client
        .saga("transfer-001")
        .add("/out", "/out-compensate", &serde_json::json!({}))
        .unwrap()
        .add("/in", "/in-compensate", &serde_json::json!({}))
        .unwrap()
        .add_branch_order(1, vec![0])
        .set_concurrent()
        .submit()
        .await
        .unwrap();

    let recorded = recorded.lock().unwrap();
    assert_eq!(recorded[0].body["concurrent"], true);
    let custom: serde_json::Value =
        serde_json::from_str(recorded[0].body["custom_data"].as_str().unwrap()).unwrap();
    assert_eq!(custom["concurrent"], true);
    assert_eq!(custom["orders"]["1"], serde_json::json!([0]));
}

#[tokio::test]
async fn dtm_failure_body_maps_to_typed_error() {
    let (server, _recorded) =
        spawn(Reply::Status(StatusCode::OK, r#"{"dtm_result":"FAILURE"}"#)).await;
    let client = DtmClient::with_server(&server);
    let error = client
        .saga("g")
        .add("/a", "/c", &serde_json::json!({}))
        .unwrap()
        .submit()
        .await
        .unwrap_err();
    assert!(matches!(error, TransactionError::Dtm(_)));

    let (server, _recorded) = spawn(Reply::Status(StatusCode::INTERNAL_SERVER_ERROR, "down")).await;
    let client = DtmClient::with_server(&server);
    let error = client.saga("g").submit().await.unwrap_err();
    assert!(matches!(error, TransactionError::Dtm(body) if body == "down"));
}

#[tokio::test]
async fn msg_prepare_and_submit_with_topic_and_delay() {
    let (server, recorded) = spawn(Reply::Success).await;
    let client = DtmClient::with_server(&server);

    let msg = client
        .msg("msg-001")
        .add("/api/send-email", &serde_json::json!({"to": "a@b.c"}))
        .unwrap()
        .add_topic("order-paid", &serde_json::json!({"id": 7}))
        .unwrap()
        .set_delay(5);
    msg.prepare("/api/query-prepared").await.unwrap();
    msg.submit().await.unwrap();

    let recorded = recorded.lock().unwrap();
    assert_eq!(recorded.len(), 2);
    assert_eq!(recorded[0].path, "/prepare");
    assert_eq!(recorded[0].body["query_prepared"], "/api/query-prepared");
    assert_eq!(recorded[0].body["steps"][1]["action"], "topic://order-paid");
    assert_eq!(recorded[1].path, "/submit");
    let custom: serde_json::Value =
        serde_json::from_str(recorded[1].body["custom_data"].as_str().unwrap()).unwrap();
    assert_eq!(custom["delay"], 5);
}

#[tokio::test]
async fn msg_do_and_submit_happy_path_skips_the_query() {
    let (server, recorded) = spawn(Reply::Success).await;
    let client = DtmClient::with_server(&server);

    client
        .msg("msg-002")
        .add("/api/send", &serde_json::json!({}))
        .unwrap()
        .do_and_submit("/api/query-prepared", || async { Ok(()) })
        .await
        .unwrap();

    let recorded = recorded.lock().unwrap();
    let paths: Vec<&str> = recorded.iter().map(|r| r.path.as_str()).collect();
    assert_eq!(paths, vec!["/prepare", "/submit"]);
}

#[tokio::test]
async fn msg_do_and_submit_queries_prepared_on_business_error() {
    let (server, recorded) = spawn(Reply::Success).await;
    let client = DtmClient::with_server(&server);
    let dtm_server = server.clone();
    // The query-prepared URL is absolute on the wire — DTM calls it
    // across services, and both resty and reqwest reject relative
    // URLs.
    let query_prepared = format!("{server}/check-prepared");

    let result = client
        .msg("msg-003")
        .add("/api/send", &serde_json::json!({}))
        .unwrap()
        .do_and_submit(query_prepared, || async {
            Err::<(), TransactionError>(TransactionError::Request("local db down".to_string()))
        })
        .await;
    match result {
        Err(TransactionError::Request(message)) => assert_eq!(message, "local db down"),
        other => panic!("unexpected outcome: {other:?}"),
    }

    let recorded = recorded.lock().unwrap();
    let paths: Vec<&str> = recorded.iter().map(|r| r.path.as_str()).collect();
    assert_eq!(paths, vec!["/prepare", "/check-prepared", "/submit"]);
    // The query-prepared branch request: GET with the protocol's query set.
    assert_eq!(recorded[1].method, "GET");
    let query = query_of(&recorded, 1);
    assert!(query.contains(&("gid".to_string(), "msg-003".to_string())));
    assert!(query.contains(&("branch_id".to_string(), "00".to_string())));
    assert!(query.contains(&("op".to_string(), "msg".to_string())));
    assert!(query.contains(&("trans_type".to_string(), "msg".to_string())));
    assert!(query.contains(&("dtm".to_string(), dtm_server.clone())));
}

#[tokio::test]
async fn msg_do_and_submit_aborts_on_business_failure() {
    let (server, recorded) = spawn(Reply::Success).await;
    let client = DtmClient::with_server(&server);

    let result = client
        .msg("msg-004")
        .add("/api/send", &serde_json::json!({}))
        .unwrap()
        .do_and_submit("/check-prepared", || async {
            Err(TransactionError::Failure("constraint violated".to_string()))
        })
        .await;
    assert!(matches!(result, Err(TransactionError::Failure(_))));

    let recorded = recorded.lock().unwrap();
    let paths: Vec<&str> = recorded.iter().map(|r| r.path.as_str()).collect();
    assert_eq!(paths, vec!["/prepare", "/abort"]);
}

#[tokio::test]
async fn tcc_global_registers_branch_then_invokes_try() {
    let ((dtm, dtm_log), (busi, busi_log)) = spawn_pair(Reply::Success, Reply::Success).await;
    let client = DtmClient::with_server(&dtm);
    let busi_assert = busi.clone();

    client
        .tcc_global_transaction("tcc-001", |mut tcc| async move {
            tcc.call_branch(
                &serde_json::json!({"item": 1}),
                &format!("{busi}/try"),
                &format!("{busi}/confirm"),
                &format!("{busi}/cancel"),
            )
            .await
        })
        .await
        .unwrap();

    let dtm_log = dtm_log.lock().unwrap();
    let paths: Vec<&str> = dtm_log.iter().map(|r| r.path.as_str()).collect();
    assert_eq!(paths, vec!["/prepare", "/registerBranch", "/submit"]);
    assert_eq!(dtm_log[1].body["gid"], "tcc-001");
    assert_eq!(dtm_log[1].body["trans_type"], "tcc");
    assert_eq!(dtm_log[1].body["branch_id"], "01");
    assert_eq!(dtm_log[1].body["confirm"], format!("{busi_assert}/confirm"));
    assert_eq!(dtm_log[1].body["cancel"], format!("{busi_assert}/cancel"));
    assert_eq!(dtm_log[1].body["data"], serde_json::json!("{\"item\":1}"));

    let busi_log = busi_log.lock().unwrap();
    assert_eq!(busi_log[0].method, "POST");
    assert_eq!(busi_log[0].path, "/try");
    assert_eq!(busi_log[0].body["item"], 1);
    let query = query_of(&busi_log, 0);
    assert!(query.contains(&("gid".to_string(), "tcc-001".to_string())));
    assert!(query.contains(&("branch_id".to_string(), "01".to_string())));
    assert!(query.contains(&("op".to_string(), "try".to_string())));
    assert!(query.contains(&("trans_type".to_string(), "tcc".to_string())));
    assert!(query.contains(&("dtm".to_string(), dtm.clone())));
}

#[tokio::test]
async fn tcc_business_error_aborts_with_rollback_reason() {
    let (dtm, dtm_log) = spawn(Reply::Success).await;
    let client = DtmClient::with_server(&dtm);

    let result = client
        .tcc_global_transaction("tcc-002", |_tcc| async move {
            Err(TransactionError::Request("try failed".to_string()))
        })
        .await;
    assert!(matches!(result, Err(TransactionError::Request(_))));

    let dtm_log = dtm_log.lock().unwrap();
    let paths: Vec<&str> = dtm_log.iter().map(|r| r.path.as_str()).collect();
    assert_eq!(paths, vec!["/prepare", "/abort"]);
    assert_eq!(
        dtm_log[1].body["rollback_reason"],
        "transaction: request failed: try failed"
    );
}

#[tokio::test]
async fn xa_global_invokes_branch_with_phase2_url() {
    let ((dtm, dtm_log), (busi, busi_log)) = spawn_pair(Reply::Success, Reply::Success).await;
    let client = DtmClient::with_server(&dtm);

    let busi_assert = busi.clone();
    client
        .xa_global_transaction("xa-001", |mut xa| async move {
            xa.call_branch(
                &serde_json::json!({"sql": "update"}),
                &format!("{busi}/branch"),
            )
            .await
        })
        .await
        .unwrap();

    let dtm_log = dtm_log.lock().unwrap();
    let paths: Vec<&str> = dtm_log.iter().map(|r| r.path.as_str()).collect();
    assert_eq!(paths, vec!["/prepare", "/submit"]);
    assert_eq!(dtm_log[0].body["trans_type"], "xa");

    let busi_log = busi_log.lock().unwrap();
    assert_eq!(busi_log[0].path, "/branch");
    assert_eq!(busi_log[0].body["sql"], "update");
    let query = query_of(&busi_log, 0);
    assert!(query.contains(&("gid".to_string(), "xa-001".to_string())));
    assert!(query.contains(&("op".to_string(), "action".to_string())));
    assert!(query.contains(&("phase2_url".to_string(), format!("{busi_assert}/branch"))));
}

#[tokio::test]
async fn xa_business_error_aborts() {
    let (dtm, dtm_log) = spawn(Reply::Success).await;
    let client = DtmClient::with_server(&dtm);

    let result = client
        .xa_global_transaction("xa-002", |_xa| async move {
            Err(TransactionError::Failure("branch rolled back".to_string()))
        })
        .await;
    assert!(matches!(result, Err(TransactionError::Failure(_))));

    let dtm_log = dtm_log.lock().unwrap();
    let paths: Vec<&str> = dtm_log.iter().map(|r| r.path.as_str()).collect();
    assert_eq!(paths, vec!["/prepare", "/abort"]);
}

#[tokio::test]
async fn branch_statuses_map_to_failure_ongoing_and_dtm() {
    for (status, expected) in [
        (StatusCode::CONFLICT, "Failure"),
        (StatusCode::TOO_EARLY, "Ongoing"),
        (StatusCode::INTERNAL_SERVER_ERROR, "Dtm"),
    ] {
        let ((dtm, _), (busi, _)) = spawn_pair(Reply::Success, Reply::Status(status, "no")).await;
        let client = DtmClient::with_server(&dtm);
        let result = client
            .xa_global_transaction("xa-map", |mut xa| async move {
                xa.call_branch(&serde_json::json!({}), &format!("{busi}/branch"))
                    .await
            })
            .await;
        let error = result.unwrap_err();
        let actual = match error {
            TransactionError::Failure(_) => "Failure",
            TransactionError::Ongoing(_) => "Ongoing",
            TransactionError::Dtm(_) => "Dtm",
            other => panic!("unexpected error: {other}"),
        };
        assert_eq!(actual, expected, "status {status}");
    }
}

#[tokio::test]
async fn close_is_a_noop_and_default_is_the_go_default() {
    let client = DtmClient::new();
    assert_eq!(client.server(), "http://localhost:36789/api/dtmsvr");
    client.close().await.unwrap();
}
