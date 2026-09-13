//! Live conformance against RabbitMQ's stomp plugin, driven by CI's
//! service container. Compile-gated behind the `live` feature; the
//! listener address comes from `BROKER_STOMP_ADDRESS` as
//! `host:port`. These tests pin the interoperability contract where
//! it matters most — binary-safe SEND frames through
//! `content-length`, `/topic/` destinations routing over `amq.topic`,
//! and subscription teardown — against a real broker.

#![cfg(feature = "live")]

use std::sync::Arc;
use std::time::Duration;

use rushwind_broker::{Broker, BrokerError, Event, Message};
use rushwind_broker_stomp::StompBroker;

fn address() -> String {
    std::env::var("BROKER_STOMP_ADDRESS").unwrap_or_else(|_| "127.0.0.1:61613".to_string())
}

async fn broker() -> StompBroker {
    StompBroker::new(address())
}

/// The round trip: subscribe a `/topic/` destination, publish onto
/// the same one, and receive the binary-safe payload.
#[tokio::test]
async fn publish_subscribes_and_delivers() {
    let broker = broker().await;
    broker.connect().await.expect("broker connects");

    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<Event>();
    let mut subscriber = broker
        .subscribe(
            "probe.stomp.roundtrip",
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

    // Binary-unsafe bytes on purpose: the content-length framing must
    // carry them through intact.
    let payload: Vec<u8> = vec![0, 1, 2, 0, 255, b'h', b'i'];
    broker
        .publish(
            "probe.stomp.roundtrip",
            Message::from_payload(payload.clone()),
        )
        .await
        .expect("publish must succeed");

    let event = tokio::time::timeout(Duration::from_secs(10), rx.recv())
        .await
        .expect("delivery arrives")
        .expect("channel open");
    assert_eq!(event.topic(), "probe.stomp.roundtrip");
    assert_eq!(event.message().payload, payload);
    event.ack().await.expect("ack is a no-op on stomp");
    subscriber.unsubscribe().await.expect("unsubscribe");
}

/// After unsubscribing, publishes on the destination no longer reach
/// the former subscriber.
#[tokio::test]
async fn unsubscribe_stops_deliveries() {
    let broker = broker().await;
    broker.connect().await.expect("broker connects");

    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<Event>();
    let mut subscriber = broker
        .subscribe(
            "probe.stomp.unsub",
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
        .publish("probe.stomp.unsub", Message::from_payload(b"late".to_vec()))
        .await
        .expect("publish must succeed");

    let delivery = tokio::time::timeout(Duration::from_secs(3), rx.recv()).await;
    assert!(
        !matches!(delivery, Ok(Some(_))),
        "no delivery after unsubscribe"
    );
}

/// Publishing without a connection fails with NotConnected.
#[tokio::test]
async fn publish_without_connect_fails() {
    let broker = StompBroker::new(address());
    let result = broker
        .publish("probe/never", Message::from_payload(vec![]))
        .await;
    assert!(
        matches!(result, Err(BrokerError::NotConnected)),
        "unconnected publish must fail with NotConnected, got {result:?}"
    );
}
