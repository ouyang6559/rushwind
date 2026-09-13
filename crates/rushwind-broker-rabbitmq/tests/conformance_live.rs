//! Live conformance against a real RabbitMQ, driven by CI's service
//! container. Compile-gated behind the `live` feature; the URL comes
//! from `BROKER_RABBITMQ_URL`. These tests pin the interoperability
//! contract where it matters most — `amq.topic` routing, the headers
//! table round trip (RabbitMQ carries them, unlike MQTT/Redis/NATS),
//! wildcard topic routing, and subscription teardown — against a real
//! broker.

#![cfg(feature = "live")]

use std::sync::Arc;
use std::time::Duration;

use rushwind_broker::{Broker, Event, Message};
use rushwind_broker_rabbitmq::RabbitmqBroker;

fn url() -> String {
    std::env::var("BROKER_RABBITMQ_URL")
        .unwrap_or_else(|_| "amqp://guest:guest@127.0.0.1:5672".to_string())
}

async fn broker() -> RabbitmqBroker {
    RabbitmqBroker::connect(&url())
        .await
        .expect("rabbitmq connects")
}

/// The round trip: bind a wildcard routing key, publish onto a
/// matching key, and receive payload plus headers — RabbitMQ carries
/// the headers table, the one engine that does.
#[tokio::test]
async fn publish_subscribes_and_delivers() {
    let broker = broker().await;

    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<Event>();
    let mut subscriber = broker
        .subscribe(
            "orders.*.created",
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
    assert_eq!(subscriber.topic(), "orders.*.created");

    let mut message = Message::from_payload(b"hello".to_vec()).with_header("origin", "rushwind");
    message.id = "carried-not".to_string();
    broker
        .publish("orders.1.created", message)
        .await
        .expect("publish must succeed");

    let event = tokio::time::timeout(Duration::from_secs(10), rx.recv())
        .await
        .expect("delivery arrives")
        .expect("channel open");
    assert_eq!(event.topic(), "orders.1.created");
    assert_eq!(event.message().payload, b"hello".to_vec());
    assert_eq!(
        event.message().headers.get("origin").map(String::as_str),
        Some("rushwind"),
        "RabbitMQ carries the headers table"
    );
    subscriber.unsubscribe().await.expect("unsubscribe");
}

/// Topic routing follows AMQP rules: `#` matches zero or more words,
/// so a bare `orders.#` filter also receives a one-word-key publish.
#[tokio::test]
async fn hash_filter_matches_one_word_keys() {
    let broker = broker().await;

    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<Event>();
    let mut subscriber = broker
        .subscribe(
            "orders.#",
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

    broker
        .publish("orders", Message::from_payload(b"bare".to_vec()))
        .await
        .expect("publish must succeed");

    let event = tokio::time::timeout(Duration::from_secs(10), rx.recv())
        .await
        .expect("delivery arrives")
        .expect("channel open");
    assert_eq!(event.topic(), "orders");
    subscriber.unsubscribe().await.expect("unsubscribe");
}

/// After unsubscribing, publishes on the routing key no longer reach
/// the former subscriber — the exclusive queue is gone.
#[tokio::test]
async fn unsubscribe_stops_deliveries() {
    let broker = broker().await;

    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<Event>();
    let mut subscriber = broker
        .subscribe(
            "probe.rabbit.unsub",
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
            "probe.rabbit.unsub",
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
