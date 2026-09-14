//! Live conformance against Apache Pulsar standalone, driven by CI's
//! service container. Compile-gated behind the `live` feature; the
//! service address comes from `BROKER_PULSAR_ADDRESS` as a
//! `pulsar://host:port` URL. New subscriptions start at the latest
//! message, so every test subscribes before it publishes. These
//! tests pin the interoperability contract where it matters most —
//! raw payload delivery including binary-unsafe bytes, the user
//! properties' round trip into the contract headers, the partition
//! key's pass-through, and subscription teardown — against a real
//! broker.

#![cfg(feature = "live")]

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use rushwind_broker::{Broker, BrokerError, Event, Message};
use rushwind_broker_pulsar::PulsarBroker;

fn address() -> String {
    std::env::var("BROKER_PULSAR_ADDRESS").unwrap_or_else(|_| "pulsar://127.0.0.1:6650".to_string())
}

/// The round trip: subscribe, publish onto the same topic, and
/// receive the payload with the headers and key the engine mapped.
#[tokio::test]
async fn publish_subscribes_and_delivers() {
    let broker = PulsarBroker::connect(&address())
        .await
        .expect("broker connects");

    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<Event>();
    let mut subscriber = broker
        .subscribe(
            "probe-pulsar-roundtrip",
            Arc::new(move |event| {
                let tx = tx.clone();
                Box::pin(async move {
                    let _ = tx.send(event);
                    Ok(())
                })
            }),
        )
        .await
        .expect("subscribe must succeed");
    assert_eq!(subscriber.topic(), "probe-pulsar-roundtrip");

    // Binary-unsafe bytes on purpose: the record payload carries
    // them raw. The headers ride the user properties, the key the
    // partition key.
    let payload: Vec<u8> = vec![0, 1, 2, 0, 255, b'p', b'p'];
    let mut headers = HashMap::new();
    headers.insert(String::from("probe"), String::from("pulsar"));
    let mut message = Message::from_payload(payload.clone());
    message.headers = headers.clone();
    message.key = String::from("probe.pulsar.key");
    broker
        .publish("probe-pulsar-roundtrip", message)
        .await
        .expect("publish must succeed");

    let event = tokio::time::timeout(Duration::from_secs(10), rx.recv())
        .await
        .expect("delivery arrives")
        .expect("channel open");
    assert_eq!(event.topic(), "probe-pulsar-roundtrip");
    let message = event.message();
    assert_eq!(message.payload, payload);
    assert_eq!(message.headers, headers);
    assert_eq!(message.key, "probe.pulsar.key");
    event.ack().await.expect("ack is a no-op on pulsar");
    subscriber.unsubscribe().await.expect("unsubscribe");
}

/// After unsubscribing, publishes on the topic no longer reach the
/// former subscriber.
#[tokio::test]
async fn unsubscribe_stops_deliveries() {
    let broker = PulsarBroker::connect(&address())
        .await
        .expect("broker connects");

    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<Event>();
    let mut subscriber = broker
        .subscribe(
            "probe-pulsar-unsub",
            Arc::new(move |event| {
                let tx = tx.clone();
                Box::pin(async move {
                    let _ = tx.send(event);
                    Ok(())
                })
            }),
        )
        .await
        .expect("subscribe must succeed");
    subscriber
        .unsubscribe()
        .await
        .expect("unsubscribe must succeed");

    broker
        .publish(
            "probe-pulsar-unsub",
            Message::from_payload(b"late".to_vec()),
        )
        .await
        .expect("publish must succeed");

    let delivery = tokio::time::timeout(Duration::from_secs(3), rx.recv()).await;
    assert!(
        !matches!(delivery, Ok(Some(_))),
        "no delivery after unsubscribe"
    );
}

/// After a disconnect, publishing fails with NotConnected.
#[tokio::test]
async fn publish_after_disconnect_fails() {
    let broker = PulsarBroker::connect(&address())
        .await
        .expect("broker connects");
    broker.disconnect().await.expect("disconnect");
    let result = broker
        .publish("probe-pulsar-never", Message::from_payload(vec![]))
        .await;
    assert!(
        matches!(result, Err(BrokerError::NotConnected)),
        "post-disconnect publish must fail with NotConnected, got {result:?}"
    );
}
