//! Live conformance against a real Redis, driven by CI's service
//! container. Compile-gated behind the `live` feature; the connection
//! URL comes from `BROKER_REDIS_URL`. These tests pin the pub-sub
//! contract where it matters most — payload-only PUBLISH, dedicated
//! subscriber connections, and teardown semantics — against a real
//! Redis.

#![cfg(feature = "live")]

use std::sync::Arc;
use std::time::Duration;

use rushwind_broker::{Broker, Event, Message};
use rushwind_broker_redis::RedisBroker;

fn url() -> String {
    std::env::var("BROKER_REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".to_string())
}

fn broker() -> RedisBroker {
    RedisBroker::from_settings(serde_json::json!({ "url": url() })).expect("settings parse")
}

/// The round trip: subscribe an exact topic, publish through a second
/// broker instance (a separate publisher connection, like two
/// processes), and receive the payload.
#[tokio::test]
async fn publish_subscribes_and_delivers() {
    let subscriber_broker = broker();
    let publisher = broker();
    subscriber_broker.connect().await.expect("connect");

    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<Message>();
    let mut subscriber = subscriber_broker
        .subscribe(
            "probe/redis-roundtrip",
            Arc::new(move |event| {
                let tx = tx.clone();
                Box::pin(async move {
                    let _ = tx.send(event.message().clone());
                    Ok(())
                })
            }),
        )
        .await
        .expect("subscribe must succeed");

    publisher
        .publish(
            "probe/redis-roundtrip",
            Message::from_payload(b"hello".to_vec()),
        )
        .await
        .expect("publish must succeed");

    let delivered = tokio::time::timeout(Duration::from_secs(10), rx.recv())
        .await
        .expect("delivery arrives")
        .expect("channel open");
    assert_eq!(delivered.payload, b"hello".to_vec());
    subscriber.unsubscribe().await.expect("unsubscribe");
}

/// Publishes carry only the payload — headers and metadata have no
/// Redis pub-sub carrier, exactly as the Go engine publishes.
#[tokio::test]
async fn deliveries_carry_payload_only() {
    let subscriber_broker = broker();
    let publisher = broker();
    subscriber_broker.connect().await.expect("connect");

    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<Message>();
    let mut subscriber = subscriber_broker
        .subscribe(
            "probe/redis-payload-only",
            Arc::new(move |event| {
                let tx = tx.clone();
                Box::pin(async move {
                    let _ = tx.send(event.message().clone());
                    Ok(())
                })
            }),
        )
        .await
        .expect("subscribe must succeed");

    let mut message = Message::from_payload(b"body".to_vec()).with_header("drop", "me");
    message.id = "dropped-too".to_string();
    publisher
        .publish("probe/redis-payload-only", message)
        .await
        .expect("publish must succeed");

    let delivered = tokio::time::timeout(Duration::from_secs(10), rx.recv())
        .await
        .expect("delivery arrives")
        .expect("channel open");
    assert!(delivered.headers.is_empty(), "headers are dropped");
    assert!(delivered.id.is_empty(), "ids are not carried");
    assert_eq!(delivered.payload, b"body".to_vec());
    let _ = subscriber.unsubscribe().await;
}

/// After unsubscribing, publishes on the topic no longer reach the
/// former subscriber — the subscribed connection is closed.
#[tokio::test]
async fn unsubscribe_stops_deliveries() {
    let subscriber_broker = broker();
    let publisher = broker();
    subscriber_broker.connect().await.expect("connect");

    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<Event>();
    let mut subscriber = subscriber_broker
        .subscribe(
            "probe/redis-unsub",
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

    publisher
        .publish("probe/redis-unsub", Message::from_payload(b"late".to_vec()))
        .await
        .expect("publish must succeed");

    // Unsubscribing closed the forwarding channel; any delivery would
    // have arrived before it closed.
    let delivery = tokio::time::timeout(Duration::from_secs(3), rx.recv()).await;
    assert!(
        !matches!(delivery, Ok(Some(_))),
        "no delivery after unsubscribe"
    );
}
