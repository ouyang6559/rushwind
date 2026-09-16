//! Live conformance against a real NATS, driven by CI's service
//! container. Compile-gated behind the `live` feature; the address
//! comes from `BROKER_NATS_ADDRESS` as a URL. These tests pin the
//! core-NATS contract where it matters most — payload-only
//! fire-and-forget publishes, subject wildcards, and teardown —
//! against a real server.

#![cfg(feature = "live")]

use std::sync::Arc;
use std::time::Duration;

use rushwind_broker::{Broker, Event, Message};
use rushwind_broker_nats::NatsBroker;

fn address() -> String {
    std::env::var("BROKER_NATS_ADDRESS").unwrap_or_else(|_| "nats://127.0.0.1:4222".to_string())
}

async fn broker() -> NatsBroker {
    NatsBroker::connect(&address())
        .await
        .expect("nats connects")
}

/// The round trip: subscribe a wildcard subject, publish onto a
/// matching subject, and receive the payload — payload-only.
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

    broker
        .publish("orders.1.created", Message::from_payload(b"hello".to_vec()))
        .await
        .expect("publish must succeed");

    let event = tokio::time::timeout(Duration::from_secs(10), rx.recv())
        .await
        .expect("delivery arrives")
        .expect("channel open");
    assert_eq!(event.topic(), "orders.1.created");
    assert_eq!(event.message().payload, b"hello".to_vec());
    event.ack().await.expect("ack is a no-op on nats");
    subscriber.unsubscribe().await.expect("unsubscribe");
}

/// After unsubscribing, publishes on the subject no longer reach the
/// former subscriber.
#[tokio::test]
async fn unsubscribe_stops_deliveries() {
    let broker = broker().await;

    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<Event>();
    let mut subscriber = broker
        .subscribe(
            "probe.nats.unsub",
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
        .publish("probe.nats.unsub", Message::from_payload(b"late".to_vec()))
        .await
        .expect("publish must succeed");

    let delivery = tokio::time::timeout(Duration::from_secs(3), rx.recv()).await;
    assert!(
        !matches!(delivery, Ok(Some(_))),
        "no delivery after unsubscribe"
    );
}

/// Request/reply: a native responder (async-nats directly — the
/// contract has no reply surface, so the responder side cannot ride
/// the engine) echoes requests onto the request's reply subject, and
/// the engine's `request` surfaces the response payload.
#[tokio::test]
async fn request_round_trips_through_native_responder() {
    let broker = broker().await;

    let responder = async_nats::connect(address())
        .await
        .expect("responder connects");
    let mut subscription = responder
        .subscribe(String::from("probe.nats.request"))
        .await
        .expect("responder subscribes");
    let echo = responder.clone();
    tokio::spawn(async move {
        // The native responder loop: publish each request's payload
        // back onto its reply subject.
        while let Some(message) = futures::StreamExt::next(&mut subscription).await {
            if let Some(reply) = message.reply {
                let _ = echo.publish(reply, message.payload).await;
            }
        }
    });

    let response = broker
        .request(
            "probe.nats.request",
            Message::from_payload(b"ping".to_vec()),
        )
        .await
        .expect("request must round trip");
    assert_eq!(response.payload, b"ping".to_vec());
}
