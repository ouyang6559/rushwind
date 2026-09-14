//! Live conformance against Apache Kafka in KRaft single-node mode,
//! driven by CI's service container. Compile-gated behind the `live`
//! feature; the bootstrap address comes from `BROKER_KAFKA_ADDRS` as
//! `host:port`. These tests pin the interoperability contract where
//! it matters most — group-consumer delivery of the published
//! payload, the record key's lossy pass-through, partition and
//! offset surfacing, and subscription teardown — against a real
//! broker. Fresh topics are created through samsa's admin protocol
//! on the controller connection, and the readiness loop waits for
//! the topic to appear in cluster metadata (leader election) before
//! the engine touches it. Delivery waits run long: the group join
//! and first fetch land within seconds, and a fresh group reads
//! from offset zero, so every publish before or during the join is
//! eventually delivered.

#![cfg(feature = "live")]

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use rushwind_broker::{BoxFuture, Broker, BrokerError, Event, Message};
use rushwind_broker_kafka::KafkaBroker;
use samsa::prelude::{create_topics, BrokerAddress, ClusterMetadata, KafkaCode, TcpConnection};

/// The metadata client identity, matching samsa's internal
/// defaults.
const CORRELATION_ID: i32 = 1;
/// The metadata client id, matching samsa's internal default.
const CLIENT_ID: &str = "samsa";

fn addr_string() -> String {
    std::env::var("BROKER_KAFKA_ADDRS").unwrap_or_else(|_| "127.0.0.1:9092".to_string())
}

fn broker_address() -> BrokerAddress {
    let addr = addr_string();
    let (host, port) = addr
        .split_once(':')
        .expect("BROKER_KAFKA_ADDRS must be host:port");
    BrokerAddress {
        host: host.to_string(),
        port: port.parse().expect("BROKER_KAFKA_ADDRS port"),
    }
}

/// A per-run suffix for unique topic names.
fn unique_suffix() -> String {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_nanos().to_string())
        .unwrap_or_default()
}

/// Creates a one-partition topic through samsa's admin protocol on
/// the controller connection — the deterministic alternative to the
/// Go engine's auto-create-topic-on-subscribe option.
async fn create_topic(topic: &str) {
    let mut metadata = ClusterMetadata::<TcpConnection>::new(
        vec![broker_address()],
        CORRELATION_ID,
        CLIENT_ID.to_string(),
        vec![],
    )
    .await
    .expect("metadata fetch");
    let controller = metadata.controller_id;
    let Some(conn) = metadata.broker_connections.remove(&controller) else {
        panic!("no controller connection");
    };
    let response = create_topics(conn, CORRELATION_ID, CLIENT_ID, HashMap::from([(topic, 1)]))
        .await
        .expect("topic creation");
    assert_eq!(response.topics[0].error_code, KafkaCode::None);
}

/// Waits until the fresh topic shows up in cluster metadata —
/// leader election completes asynchronously after creation.
async fn wait_for_topic(topic: &str) {
    for _ in 0..60 {
        let metadata = ClusterMetadata::<TcpConnection>::new(
            vec![broker_address()],
            CORRELATION_ID,
            CLIENT_ID.to_string(),
            vec![topic.to_string()],
        )
        .await;
        if let Ok(metadata) = metadata {
            if metadata
                .topics
                .iter()
                .any(|candidate| candidate.name == topic)
            {
                return;
            }
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    panic!("topic never appeared in cluster metadata: {topic}");
}

/// The round trip: join a fresh group on a fresh topic, publish
/// onto the same one, and receive the binary-safe payload with its
/// key, partition, and offset. The key round-trips through the
/// contract's lossy string key; the single-partition topic pins the
/// partition mapping.
#[tokio::test]
async fn publish_subscribes_and_delivers() {
    let topic = format!("probe.kafka.roundtrip.{}", unique_suffix());
    create_topic(&topic).await;
    wait_for_topic(&topic).await;

    let broker = KafkaBroker::new(vec![addr_string()]);
    broker.connect().await.expect("broker connects");

    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<Event>();
    let mut subscriber = broker
        .subscribe(
            &topic,
            Arc::new(move |event| {
                let tx = tx.clone();
                let future: BoxFuture<'static, Result<(), BrokerError>> = Box::pin(async move {
                    let _ = tx.send(event);
                    Ok(())
                });
                future
            }),
        )
        .await
        .expect("subscribe must succeed");

    // Binary-unsafe bytes on purpose: the record value must carry
    // them through intact.
    let payload: Vec<u8> = vec![0, 1, 2, 0, 255, b'k', b'a'];
    let key = "probe.kafka.key".to_string();
    let mut message = Message::from_payload(payload.clone());
    message.key = key.clone();
    broker
        .publish(&topic, message)
        .await
        .expect("publish must succeed");

    let event = tokio::time::timeout(Duration::from_secs(30), rx.recv())
        .await
        .expect("delivery arrives")
        .expect("channel open");
    assert_eq!(event.topic(), topic.as_str());
    let message = event.message();
    assert_eq!(message.payload, payload);
    assert_eq!(message.key, key);
    assert_eq!(message.partition, Some(0));
    assert!(message.offset.is_some());
    event.ack().await.expect("ack is a no-op on kafka");
    subscriber.unsubscribe().await.expect("unsubscribe");
}

/// After unsubscribing, publishes on the topic no longer reach the
/// former subscriber.
#[tokio::test]
async fn unsubscribe_stops_deliveries() {
    let topic = format!("probe.kafka.unsub.{}", unique_suffix());
    create_topic(&topic).await;
    wait_for_topic(&topic).await;

    let broker = KafkaBroker::new(vec![addr_string()]);
    broker.connect().await.expect("broker connects");

    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<Event>();
    let mut subscriber = broker
        .subscribe(
            &topic,
            Arc::new(move |event| {
                let tx = tx.clone();
                let future: BoxFuture<'static, Result<(), BrokerError>> = Box::pin(async move {
                    let _ = tx.send(event);
                    Ok(())
                });
                future
            }),
        )
        .await
        .expect("subscribe must succeed");
    subscriber
        .unsubscribe()
        .await
        .expect("unsubscribe must succeed");

    broker
        .publish(&topic, Message::from_payload(b"late".to_vec()))
        .await
        .expect("publish must succeed");

    let delivery = tokio::time::timeout(Duration::from_secs(3), rx.recv()).await;
    assert!(
        !matches!(delivery, Ok(Some(_))),
        "no delivery after unsubscribe"
    );
}

/// Publishing and subscribing without a connection fail with
/// NotConnected.
#[tokio::test]
async fn publish_and_subscribe_without_connect_fail() {
    let broker = KafkaBroker::new(vec![addr_string()]);
    let publish = broker
        .publish("probe.kafka.never", Message::from_payload(vec![]))
        .await;
    assert!(
        matches!(publish, Err(BrokerError::NotConnected)),
        "unconnected publish must fail with NotConnected, got {publish:?}"
    );
    let handler: rushwind_broker::Handler = Arc::new(|_event: Event| {
        let future: BoxFuture<'static, Result<(), BrokerError>> = Box::pin(async { Ok(()) });
        future
    });
    let subscribe = broker.subscribe("probe.kafka.never", handler).await;
    assert!(
        matches!(subscribe, Err(BrokerError::NotConnected)),
        "unconnected subscribe must fail with NotConnected"
    );
}
