//! Integration tests: the MQTT consumer bridge against an embedded
//! rumqttd broker, under the full RushWind lifecycle.
//!
//! The bridge's contract surface is deliberately small — one pre-trusted
//! broker connection, no session middleware (see
//! `docs/session-middleware.md`) — so the integration suite asserts the
//! two things that matter: delivery of subscribed messages to the
//! handler, and a clean lifecycle shutdown while the pump is live.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use rushwind_core::App;
use rushwind_transport::{ServerError, StopSignal};
use rushwind_transport_mqtt::MqttBridge;
use tokio::time::{sleep, timeout};

mod common;

/// Builds the full lifecycle around `bridge` and returns the shutdown
/// trigger and the run task's join handle.
#[allow(clippy::type_complexity)]
async fn spawn_lifecycle(
    bridge: MqttBridge,
) -> (StopSignal, tokio::task::JoinHandle<Result<(), ServerError>>) {
    let app = Arc::new(
        App::builder()
            .erased_server(Arc::new(bridge))
            .stop_timeout(Duration::from_millis(500))
            .build(),
    );
    let run_app = Arc::clone(&app);
    let trigger = StopSignal::new();
    let run_trigger = trigger.clone();
    let handle = tokio::spawn(async move { run_app.run(run_trigger).await });
    (trigger, handle)
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

#[tokio::test]
async fn subscribed_messages_reach_the_handler() {
    let addr = common::broker();
    let log: Arc<Mutex<Vec<(String, String)>>> = Arc::new(Mutex::new(Vec::new()));
    let handler_log = Arc::clone(&log);

    let bridge = MqttBridge::builder(addr, "rushwind-bridge-test")
        .subscribe("demo/#")
        .session_handler(move |message| {
            let log = Arc::clone(&handler_log);
            async move {
                log.lock().expect("log poisoned").push((
                    message.topic,
                    String::from_utf8_lossy(&message.payload).into_owned(),
                ));
            }
        })
        .build()
        .expect("bridge must build");
    let (trigger, run) = spawn_lifecycle(bridge).await;

    // Let the subscription register with the broker before publishing.
    sleep(Duration::from_millis(300)).await;
    common::publish(addr, "publisher-1", "demo/one", b"hello").await;
    common::publish(addr, "publisher-1", "demo/two", b"world").await;

    // Both messages must land, with topic and payload intact.
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        {
            let log = log.lock().expect("log poisoned");
            let has_one = log
                .iter()
                .any(|(topic, payload)| topic == "demo/one" && payload == "hello");
            let has_two = log
                .iter()
                .any(|(topic, payload)| topic == "demo/two" && payload == "world");
            if has_one && has_two {
                break;
            }
        }
        assert!(
            std::time::Instant::now() < deadline,
            "subscribed messages must reach the handler"
        );
        sleep(Duration::from_millis(50)).await;
    }

    // A clean lifecycle shutdown while the pump is live.
    finish_lifecycle(trigger, run).await;
}
