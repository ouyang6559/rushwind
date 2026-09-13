//! Live conformance against a real MQTT broker (EMQX works), driven
//! by CI's service container. Compile-gated behind the `live`
//! feature; the address comes from `BROKER_MQTT_ADDRESS` as
//! `host:port`. These tests pin the interoperability contract where
//! it matters most — payload-only QoS-1 publishes, wildcard topic
//! filters, and subscription teardown — against a real broker.

#![cfg(feature = "live")]

use std::time::Duration;

use rushwind_broker::{json_message, Broker, BrokerError, Message};
use rushwind_broker_mqtt::{event_channel, MqttBroker, MqttOptions};

fn address() -> String {
    std::env::var("BROKER_MQTT_ADDRESS").unwrap_or_else(|_| "127.0.0.1:1883".to_string())
}

fn broker() -> MqttBroker {
    // The engine's default client id is random: parallel tests must
    // not share one, or the broker kicks the older connection.
    MqttBroker::new(MqttOptions::new(address()))
}

/// The round trip: subscribe a wildcard filter, publish onto a
/// matching topic, and receive the payload — payload-only, as the Go
/// engine publishes.
#[tokio::test]
async fn publish_subscribes_and_delivers() {
    let broker = broker();
    broker.connect().await.expect("broker connects");

    let (mut events, forward) = event_channel();
    let mut subscriber = broker
        .subscribe("orders/+/created", std::sync::Arc::new(forward))
        .await
        .expect("subscribe must succeed");
    assert_eq!(subscriber.topic(), "orders/+/created");

    broker
        .publish(
            "orders/1/created",
            json_message(&serde_json::json!({ "order": 1 })).expect("json encode"),
        )
        .await
        .expect("publish must succeed");

    let event = tokio::time::timeout(Duration::from_secs(10), events.recv())
        .await
        .expect("delivery arrives")
        .expect("channel open");
    assert_eq!(event.topic(), "orders/1/created");
    assert_eq!(
        event.message().payload,
        serde_json::json!({ "order": 1 }).to_string().into_bytes()
    );

    subscriber.unsubscribe().await.expect("unsubscribe");
}

/// The payload-only rule: headers and metadata have no MQTT 3.1.1
/// carrier, so they are absent on delivery — the Go engine's shape.
#[tokio::test]
async fn deliveries_carry_payload_only() {
    let broker = broker();
    broker.connect().await.expect("broker connects");

    let (mut events, forward) = event_channel();
    let mut subscriber = broker
        .subscribe("probe/payload-only", std::sync::Arc::new(forward))
        .await
        .expect("subscribe must succeed");

    let mut message = Message::from_payload(b"body".to_vec()).with_header("drop", "me");
    message.id = "dropped-too".to_string();
    broker
        .publish("probe/payload-only", message)
        .await
        .expect("publish must succeed");

    let event = tokio::time::timeout(Duration::from_secs(10), events.recv())
        .await
        .expect("delivery arrives")
        .expect("channel open");
    assert!(event.message().headers.is_empty(), "headers are dropped");
    assert!(event.message().id.is_empty(), "ids are not carried");
    assert_eq!(event.message().payload, b"body".to_vec());
    event.ack().await.expect("ack is a no-op on mqtt");
    let _ = subscriber.unsubscribe().await;
}

/// After unsubscribing, publishes on the filter no longer deliver.
#[tokio::test]
async fn unsubscribe_stops_deliveries() {
    let broker = broker();
    broker.connect().await.expect("broker connects");

    let (mut events, forward) = event_channel();
    let mut subscriber = broker
        .subscribe("probe/unsub", std::sync::Arc::new(forward))
        .await
        .expect("subscribe must succeed");
    subscriber
        .unsubscribe()
        .await
        .expect("unsubscribe must succeed");

    broker
        .publish("probe/unsub", Message::from_payload(b"late".to_vec()))
        .await
        .expect("publish must succeed");

    // After unsubscribing the handler is gone, which also closes the
    // forwarding channel — "no delivery" is a timeout or a closed
    // channel, never a received event.
    let delivery = tokio::time::timeout(Duration::from_secs(3), events.recv()).await;
    assert!(
        !matches!(delivery, Ok(Some(_))),
        "no delivery after unsubscribe"
    );
}

/// Publishing without a connection fails with NotConnected.
#[tokio::test]
async fn publish_without_connect_fails() {
    let broker = MqttBroker::new(MqttOptions::new(address()));
    let result = broker
        .publish("probe/never", Message::from_payload(vec![]))
        .await;
    assert!(
        matches!(result, Err(BrokerError::NotConnected)),
        "unconnected publish must fail with NotConnected, got {result:?}"
    );
}
