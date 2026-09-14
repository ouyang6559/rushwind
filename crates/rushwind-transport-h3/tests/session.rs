//! Integration tests: gated HTTP/3 requests under the full RushWind
//! lifecycle, exercised with a real h3 client over quinn.
//!
//! Each test spins the complete stack — an [`H3Server`] with its
//! gates, cap and handshake deadline, driven by `App::run` — and
//! asserts one slice of the contract: request-time gate evaluation
//! on the request's headers, gate-rejection status mapping,
//! admission-cap closure, request round trip, and lifecycle-aligned
//! connection teardown.

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use rushwind_core::App;
use rushwind_transport::{GateVerdict, Handshake, HandshakeGate, ServerError, StopSignal};
use rushwind_transport_h3::H3Server;
use tokio::time::timeout;

mod common;

struct DenyAll;

impl HandshakeGate for DenyAll {
    fn name(&self) -> String {
        "deny-all".to_string()
    }
    fn inspect(&self, _handshake: &Handshake) -> GateVerdict {
        GateVerdict::Reject(rushwind_transport::Rejection {
            status: 451,
            reason: "denied by test gate".to_string(),
        })
    }
}

/// Records the header pairs each handshake snapshot carried.
/// The shared header-snapshot log type.
type HeaderLog = Arc<Mutex<Vec<Vec<(String, String)>>>>;

struct HeaderRecorder {
    log: HeaderLog,
}

impl HandshakeGate for HeaderRecorder {
    fn name(&self) -> String {
        "header-recorder".to_string()
    }
    fn inspect(&self, handshake: &Handshake) -> GateVerdict {
        self.log
            .lock()
            .expect("log poisoned")
            .push(handshake.headers.clone());
        GateVerdict::Continue
    }
}

/// The echo request handler: answers 200, echoing the request's
/// `x-probe` header into the `x-echo` response header.
async fn echo_request(
    request: http::Request<()>,
    stream: h3::server::RequestStream<h3_quinn::BidiStream<bytes::Bytes>, bytes::Bytes>,
) {
    let mut stream = stream;
    let probe = request
        .headers()
        .get("x-probe")
        .and_then(|value| value.to_str().ok())
        .map(String::from);
    let mut builder = http::Response::builder().status(http::StatusCode::OK);
    if let Some(probe) = probe {
        builder = builder.header("x-echo", probe);
    }
    let response = builder.body(()).expect("response builds");
    let _ = stream.send_response(response).await;
    let _ = stream.finish().await;
}

/// Builds the full lifecycle around `server` and returns its address, the
/// shutdown trigger, and the run task's join handle.
#[allow(clippy::type_complexity)]
async fn spawn_lifecycle(
    server: H3Server,
) -> (
    SocketAddr,
    StopSignal,
    tokio::task::JoinHandle<Result<(), ServerError>>,
) {
    let addr = server.local_addr();
    let app = Arc::new(
        App::builder()
            .erased_server(Arc::new(server))
            .stop_timeout(Duration::from_millis(500))
            .build(),
    );
    let run_app = Arc::clone(&app);
    let trigger = StopSignal::new();
    let run_trigger = trigger.clone();
    let handle = tokio::spawn(async move { run_app.run(run_trigger).await });
    (addr, trigger, handle)
}

/// Drives the run task to completion after triggering shutdown and asserts
/// a clean outcome.
async fn finish_lifecycle(
    trigger: StopSignal,
    run: tokio::task::JoinHandle<Result<(), ServerError>>,
) {
    trigger.signal();
    let outcome = timeout(Duration::from_secs(5), run)
        .await
        .expect("run must finish")
        .expect("run task must not panic");
    assert!(outcome.is_ok());
}

/// The round trip: the request reaches the handler and the response
/// carries its header back.
#[tokio::test]
async fn requests_round_trip_through_the_handler() {
    let server = H3Server::builder(common::server_config())
        .request_handler(echo_request)
        .build(common::bind_addr())
        .expect("server must build");
    let (addr, trigger, run) = spawn_lifecycle(server).await;

    let client = common::client_endpoint();
    let response = timeout(
        Duration::from_secs(10),
        common::h3_request(&client, addr, Some("marker")),
    )
    .await
    .expect("request must settle")
    .expect("request must round trip");
    assert_eq!(response.status(), http::StatusCode::OK);
    assert_eq!(
        response
            .headers()
            .get("x-echo")
            .and_then(|v| v.to_str().ok()),
        Some("marker"),
        "the handler's echo header must arrive"
    );

    finish_lifecycle(trigger, run).await;
}

/// Gates observe the request's header pairs — the HTTP-family
/// handshake snapshot — at request time.
#[tokio::test]
async fn gates_observe_request_headers() {
    let log: HeaderLog = Arc::new(Mutex::new(Vec::new()));
    let recorder = Arc::new(HeaderRecorder {
        log: Arc::clone(&log),
    });
    let server = H3Server::builder(common::server_config())
        .gate(recorder)
        .request_handler(echo_request)
        .build(common::bind_addr())
        .expect("server must build");
    let (addr, trigger, run) = spawn_lifecycle(server).await;

    let client = common::client_endpoint();
    let response = timeout(
        Duration::from_secs(10),
        common::h3_request(&client, addr, Some("gate-marker")),
    )
    .await
    .expect("request must settle")
    .expect("request must round trip");
    assert_eq!(response.status(), http::StatusCode::OK);

    let snapshot = {
        let observed = log.lock().expect("log poisoned");
        assert_eq!(observed.len(), 1, "exactly one handshake snapshot");
        observed[0].clone()
    };
    assert!(
        snapshot
            .iter()
            .any(|(name, value)| name == "x-probe" && value == "gate-marker"),
        "the request header must be visible to the gate, got {:?}",
        snapshot
    );

    finish_lifecycle(trigger, run).await;
}

/// A gate rejection answers the request with the rejection's status.
#[tokio::test]
async fn gate_rejection_answers_with_status() {
    let server = H3Server::builder(common::server_config())
        .gate(Arc::new(DenyAll))
        .request_handler(echo_request)
        .build(common::bind_addr())
        .expect("server must build");
    let (addr, trigger, run) = spawn_lifecycle(server).await;

    let client = common::client_endpoint();
    let response = timeout(
        Duration::from_secs(10),
        common::h3_request(&client, addr, Some("marker")),
    )
    .await
    .expect("request must settle")
    .expect("the rejection answer must arrive");
    assert_eq!(
        response.status(),
        http::StatusCode::from_u16(451).expect("451 is a valid status"),
        "the rejection must map to its status"
    );
    assert!(
        response.headers().get("x-echo").is_none(),
        "the handler must not have run"
    );

    finish_lifecycle(trigger, run).await;
}

/// The admission cap: with one live connection, the second
/// connection is closed by the server, while the first keeps
/// answering.
#[tokio::test]
async fn admission_cap_closes_excess_connections() {
    let server = H3Server::builder(common::server_config())
        .max_concurrent_sessions(1)
        .request_handler(echo_request)
        .build(common::bind_addr())
        .expect("server must build");
    let (addr, trigger, run) = spawn_lifecycle(server).await;

    // Hold one live connection.
    let first_client = common::client_endpoint();
    let mut first = timeout(
        Duration::from_secs(10),
        common::h3_client(&first_client, addr),
    )
    .await
    .expect("first connect must settle")
    .expect("first connection must establish");

    // The second connection is closed by the server at its
    // admission check: its request must fail.
    let second_client = common::client_endpoint();
    let second = timeout(
        Duration::from_secs(10),
        common::h3_request(&second_client, addr, Some("marker")),
    )
    .await
    .expect("second request must settle");
    assert!(second.is_none(), "the over-cap connection must be dead");

    // The first connection is unaffected.
    let alive = timeout(
        Duration::from_secs(10),
        common::h3_request_via(&mut first, Some("marker")),
    )
    .await
    .expect("probe must settle")
    .expect("the first connection must keep answering");
    assert_eq!(alive.status(), http::StatusCode::OK);

    drop(first);

    finish_lifecycle(trigger, run).await;
}

/// Lifecycle teardown: when the shutdown signal fires, the
/// connection is torn down with the listener.
#[tokio::test]
async fn lifecycle_stop_tears_down_connections() {
    let server = H3Server::builder(common::server_config())
        .request_handler(echo_request)
        .build(common::bind_addr())
        .expect("server must build");
    let (addr, trigger, run) = spawn_lifecycle(server).await;

    let client = common::client_endpoint();
    let mut connection = timeout(Duration::from_secs(10), common::h3_client(&client, addr))
        .await
        .expect("connect must settle")
        .expect("connection must establish");

    let alive = timeout(
        Duration::from_secs(10),
        common::h3_request_via(&mut connection, Some("marker")),
    )
    .await
    .expect("probe must settle")
    .expect("connection must answer before teardown");
    assert_eq!(alive.status(), http::StatusCode::OK);

    finish_lifecycle(trigger, run).await;

    let dead = timeout(
        Duration::from_secs(10),
        common::h3_request_via(&mut connection, Some("marker")),
    )
    .await
    .expect("probe must settle");
    assert!(dead.is_none(), "connection must be dead after teardown");
}
