//! Integration tests: gated WebTransport sessions under the full
//! RushWind lifecycle, exercised with real wtransport clients.
//!
//! Each test spins the complete stack — a
//! [`WebTransportServer`] with its gates, cap and handshake
//! deadline, driven by `App::run` — and asserts one slice of the
//! session contract: gate refusal, HTTP-family handshake evidence,
//! admission-cap closure, payload round trip, and lifecycle-aligned
//! session teardown.

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use rushwind_core::App;
use rushwind_transport::{GateVerdict, Handshake, HandshakeGate, ServerError, StopSignal};
use rushwind_transport_webtransport::WebTransportServer;
use tokio::time::timeout;

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

/// The idle session handler: never returns, keeping the session
/// alive until the lifecycle tears it down.
async fn idle_session(_session: wtransport::Connection) {
    std::future::pending::<()>().await
}

/// The echo session handler: every bidirectional stream is echoed
/// verbatim, one response per stream.
async fn echo_session(session: wtransport::Connection) {
    let mut buffer = Vec::new();
    let mut chunk = [0u8; 1024];
    loop {
        let Ok((mut send, mut recv)) = session.accept_bi().await else {
            return;
        };
        buffer.clear();
        while let Ok(Some(n)) = recv.read(&mut chunk).await {
            buffer.extend_from_slice(&chunk[..n]);
        }
        let _ = send.write_all(&buffer).await;
        let _ = send.finish().await;
    }
}

/// Builds the full lifecycle around `server` and returns its address, the
/// shutdown trigger, and the run task's join handle.
#[allow(clippy::type_complexity)]
async fn spawn_lifecycle(
    server: WebTransportServer,
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

/// The wtransport client endpoint type.
type ClientEndpoint = wtransport::endpoint::Endpoint<wtransport::endpoint::endpoint_side::Client>;

/// Builds a wtransport client endpoint that skips certificate
/// validation (the servers under test run self-signed).
fn client_endpoint() -> ClientEndpoint {
    let config = wtransport::ClientConfig::builder()
        .with_bind_default()
        .with_no_cert_validation()
        .build();
    wtransport::endpoint::Endpoint::client(config).expect("client endpoint")
}

/// One echo probe: alive when the full payload comes back.
async fn echo_probe(connection: &wtransport::Connection) -> bool {
    let mut stream = match connection.open_bi().await {
        Err(_) => return false,
        Ok(opening) => match opening.await {
            Err(_) => return false,
            Ok(stream) => stream,
        },
    };
    if stream.0.write_all(b"ping").await.is_err() {
        return false;
    }
    if stream.0.finish().await.is_err() {
        return false;
    }
    let mut echo = Vec::new();
    let mut chunk = [0u8; 128];
    while let Ok(Some(n)) = stream.1.read(&mut chunk).await {
        echo.extend_from_slice(&chunk[..n]);
    }
    echo == b"ping"
}

/// Repeatedly probes the connection until an echo attempt fails,
/// proving the server tore the session down; panics if the
/// connection is still echoing after `window`.
async fn await_connection_death(connection: &wtransport::Connection, window: Duration) {
    let deadline = std::time::Instant::now() + window;
    while echo_probe(connection).await {
        assert!(
            std::time::Instant::now() < deadline,
            "session survived teardown"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Gate-refused sessions are dropped: the client's connect attempt
/// fails with a connection error, because the refusal happens at
/// session-request time, before any CONNECT response.
#[tokio::test]
async fn denied_sessions_are_dropped() {
    let server = WebTransportServer::builder(identity())
        .gate(Arc::new(DenyAll))
        .session_handler(idle_session)
        .build("127.0.0.1:0".parse().unwrap())
        .expect("server must build");
    let (addr, trigger, run) = spawn_lifecycle(server).await;

    let client = client_endpoint();
    let connect = timeout(
        Duration::from_secs(10),
        client.connect(format!("https://{addr}")),
    )
    .await
    .expect("connect must settle");
    assert!(connect.is_err(), "refused session must fail to connect");

    finish_lifecycle(trigger, run).await;
}

/// Gates observe the HTTP-family handshake evidence: the
/// `:authority` and `:path` pseudo-headers the client sent.
#[tokio::test]
async fn gates_observe_authority_and_path() {
    let log: HeaderLog = Arc::new(Mutex::new(Vec::new()));
    let recorder = Arc::new(HeaderRecorder {
        log: Arc::clone(&log),
    });
    let server = WebTransportServer::builder(identity())
        .gate(recorder)
        .session_handler(idle_session)
        .build("127.0.0.1:0".parse().unwrap())
        .expect("server must build");
    let (addr, trigger, run) = spawn_lifecycle(server).await;

    let client = client_endpoint();
    let connection = timeout(
        Duration::from_secs(10),
        client.connect(format!("https://{addr}")),
    )
    .await
    .expect("connect must settle")
    .expect("unrefused session must connect");

    let snapshot = {
        let observed = log.lock().expect("log poisoned");
        assert_eq!(observed.len(), 1, "exactly one handshake snapshot");
        observed[0].clone()
    };
    let headers = &snapshot;
    assert!(
        headers
            .iter()
            .any(|(name, value)| name == ":authority" && value.contains("127.0.0.1")),
        "the authority pseudo-header the client sent must be visible, got {headers:?}"
    );
    assert!(
        headers.iter().any(|(name, _)| name == ":path"),
        "the path pseudo-header must be visible, got {headers:?}"
    );
    drop(connection);

    finish_lifecycle(trigger, run).await;
}

/// The session round trip: a bidirectional stream through the echo
/// handler carries the payload back verbatim.
#[tokio::test]
async fn sessions_round_trip_bytes() {
    let server = WebTransportServer::builder(identity())
        .session_handler(echo_session)
        .build("127.0.0.1:0".parse().unwrap())
        .expect("server must build");
    let (addr, trigger, run) = spawn_lifecycle(server).await;

    let client = client_endpoint();
    let connection = timeout(
        Duration::from_secs(10),
        client.connect(format!("https://{addr}")),
    )
    .await
    .expect("connect must settle")
    .expect("session must connect");

    // Alive: the echo probe must succeed while the session lives.
    assert!(
        timeout(Duration::from_secs(10), echo_probe(&connection))
            .await
            .expect("probe must settle"),
        "echo must succeed on the live session"
    );

    finish_lifecycle(trigger, run).await;
    await_connection_death(&connection, Duration::from_secs(5)).await;
}

/// The admission cap: with one live session, the second session is
/// closed by the server at establishment, while the first keeps
/// echoing.
#[tokio::test]
async fn admission_cap_closes_excess_sessions() {
    let server = WebTransportServer::builder(identity())
        .max_concurrent_sessions(1)
        .session_handler(echo_session)
        .build("127.0.0.1:0".parse().unwrap())
        .expect("server must build");
    let (addr, trigger, run) = spawn_lifecycle(server).await;

    // Hold one live session.
    let first_client = client_endpoint();
    let first = timeout(
        Duration::from_secs(10),
        first_client.connect(format!("https://{addr}")),
    )
    .await
    .expect("first connect must settle")
    .expect("first session must connect");

    // The second handshake either completes client-side and the
    // server closes the over-cap connection immediately after its
    // admission check, or the close beats the handshake outright —
    // both are the cap refusing the session.
    let second_client = client_endpoint();
    let second = timeout(
        Duration::from_secs(10),
        second_client.connect(format!("https://{addr}")),
    )
    .await
    .expect("second connect must settle");
    match second {
        Err(_already_closed) => {}
        Ok(second) => {
            await_connection_death(&second, Duration::from_secs(5)).await;
        }
    }

    // The first session is unaffected.
    assert!(timeout(Duration::from_secs(10), echo_probe(&first))
        .await
        .expect("probe must settle"));

    drop(first);

    finish_lifecycle(trigger, run).await;
}

/// Lifecycle teardown: when the shutdown signal fires, the session
/// is torn down with the listener — every echo probe fails once the
/// close lands, and the run task returns a clean outcome.
#[tokio::test]
async fn lifecycle_stop_tears_down_sessions() {
    let server = WebTransportServer::builder(identity())
        .session_handler(echo_session)
        .build("127.0.0.1:0".parse().unwrap())
        .expect("server must build");
    let (addr, trigger, run) = spawn_lifecycle(server).await;

    let client = client_endpoint();
    let connection = timeout(
        Duration::from_secs(10),
        client.connect(format!("https://{addr}")),
    )
    .await
    .expect("connect must settle")
    .expect("session must connect");

    assert!(
        timeout(Duration::from_secs(10), echo_probe(&connection))
            .await
            .expect("probe must settle"),
        "echo must succeed before teardown"
    );

    finish_lifecycle(trigger, run).await;
    await_connection_death(&connection, Duration::from_secs(5)).await;
}

/// A self-signed identity for the test servers.
fn identity() -> wtransport::Identity {
    wtransport::Identity::self_signed(["localhost"]).expect("identity")
}
