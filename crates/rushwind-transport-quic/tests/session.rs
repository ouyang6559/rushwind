//! Integration tests: gated QUIC sessions under the full RushWind
//! lifecycle, exercised with real quinn clients.
//!
//! Each test spins the complete stack — a [`QuicServer`] with its gates,
//! cap and handshake deadline, driven by `App::run` — and asserts one
//! slice of the session contract: gate refusal, admission-cap closure,
//! lifecycle-aligned session teardown, handshake-deadline responsiveness,
//! and peer-address visibility to gates.

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use rushwind_core::App;
use rushwind_transport::{GateVerdict, Handshake, HandshakeGate, ServerError, StopSignal};
use rushwind_transport_quic::QuicServer;
use tokio::time::{sleep, timeout};

mod common;

struct DenyAll;

impl HandshakeGate for DenyAll {
    fn name(&self) -> String {
        "deny-all".to_string()
    }
    fn inspect(&self, _handshake: &Handshake) -> GateVerdict {
        GateVerdict::Reject(rushwind_transport::Rejection {
            status: 403,
            reason: "denied by test gate".to_string(),
        })
    }
}

/// Records the peer address each handshake snapshot carried.
struct RemoteRecorder {
    log: Arc<Mutex<Vec<Option<SocketAddr>>>>,
}

impl HandshakeGate for RemoteRecorder {
    fn name(&self) -> String {
        "remote-recorder".to_string()
    }
    fn inspect(&self, handshake: &Handshake) -> GateVerdict {
        self.log
            .lock()
            .expect("log poisoned")
            .push(handshake.remote);
        GateVerdict::Continue
    }
}

/// Builds the full lifecycle around `server` and returns its address, the
/// shutdown trigger, and the run task's join handle.
#[allow(clippy::type_complexity)]
async fn spawn_lifecycle(
    server: QuicServer,
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

fn build_server() -> QuicServer {
    QuicServer::builder(common::server_config())
        .session_handler(common::echo_session)
        .build(common::bind_addr())
        .expect("ephemeral bind must succeed")
}

#[tokio::test]
async fn gate_rejection_refuses_handshake() {
    let server = QuicServer::builder(common::server_config())
        .gate(Arc::new(DenyAll))
        .session_handler(common::echo_session)
        .build(common::bind_addr())
        .expect("ephemeral bind must succeed");
    let (addr, trigger, run) = spawn_lifecycle(server).await;

    let client = common::client_endpoint();
    let connecting = client
        .connect(addr, "localhost")
        .expect("connect must start");
    let refused = timeout(Duration::from_secs(5), connecting)
        .await
        .expect("attempt must complete");
    assert!(refused.is_err(), "gate must refuse the handshake");

    finish_lifecycle(trigger, run).await;
}

#[tokio::test]
async fn session_cap_closes_second_connection() {
    let server = QuicServer::builder(common::server_config())
        .max_concurrent_sessions(1)
        .session_handler(common::echo_session)
        .build(common::bind_addr())
        .expect("ephemeral bind must succeed");
    let (addr, trigger, run) = spawn_lifecycle(server).await;

    // Hold one live session.
    let client1 = common::client_endpoint();
    let conn1 = common::connect(&client1, addr).await;
    // Let the server-side admission register before the second handshake.
    sleep(Duration::from_millis(300)).await;

    // The handshake completes client-side; the server closes the
    // over-cap connection immediately after its admission check.
    let client2 = common::client_endpoint();
    let conn2 = common::connect(&client2, addr).await;
    common::await_connection_death(&conn2, Duration::from_secs(5)).await;

    // The first session is unaffected.
    common::echo_roundtrip(&conn1).await;

    finish_lifecycle(trigger, run).await;
}

#[tokio::test]
async fn session_echoes_and_dies_with_lifecycle() {
    let server = build_server();
    let (addr, trigger, run) = spawn_lifecycle(server).await;

    let client = common::client_endpoint();
    let conn = common::connect(&client, addr).await;
    common::echo_roundtrip(&conn).await;

    // Lifecycle shutdown must tear the session down with the listener:
    // every echo probe fails once the close lands, and run returns a clean
    // outcome.
    trigger.signal();
    common::await_connection_death(&conn, Duration::from_secs(5)).await;
    let outcome = timeout(Duration::from_secs(5), run)
        .await
        .expect("run must finish")
        .expect("run task must not panic");
    assert!(outcome.is_ok());
}

#[tokio::test]
async fn handshake_deadline_keeps_accept_loop_responsive() {
    let server = QuicServer::builder(common::server_config())
        .handshake_timeout(Duration::from_millis(100))
        .session_handler(common::echo_session)
        .build(common::bind_addr())
        .expect("ephemeral bind must succeed");
    let (addr, trigger, run) = spawn_lifecycle(server).await;

    // A stalled handshake: poll the client's handshake briefly, then drop
    // it — the server side stalls mid-handshake until its deadline lapses
    // and the handshake is dropped.
    let stall_client = common::client_endpoint();
    let stall_connecting = stall_client
        .connect(addr, "localhost")
        .expect("stall connect must start");
    let _ = timeout(Duration::from_millis(1), stall_connecting).await;
    drop(stall_client);
    sleep(Duration::from_millis(300)).await;

    // A real client must still be served promptly — the accept loop must
    // not be wedged on the abandoned handshake.
    let client = common::client_endpoint();
    let conn = common::connect(&client, addr).await;
    common::echo_roundtrip(&conn).await;

    finish_lifecycle(trigger, run).await;
}

#[tokio::test]
async fn gates_observe_peer_address() {
    let log: Arc<Mutex<Vec<Option<SocketAddr>>>> = Arc::new(Mutex::new(Vec::new()));
    let server = QuicServer::builder(common::server_config())
        .gate(Arc::new(RemoteRecorder {
            log: Arc::clone(&log),
        }))
        .session_handler(common::echo_session)
        .build(common::bind_addr())
        .expect("ephemeral bind must succeed");
    let (addr, trigger, run) = spawn_lifecycle(server).await;

    let client = common::client_endpoint();
    let conn = common::connect(&client, addr).await;
    common::echo_roundtrip(&conn).await;

    {
        let log = log.lock().expect("log poisoned");
        assert_eq!(log.len(), 1, "the gate must see exactly one handshake");
        let remote = log[0].expect("raw transport must populate the peer address");
        assert!(remote.ip().is_loopback());
    }

    drop(conn);
    finish_lifecycle(trigger, run).await;
}
