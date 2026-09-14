//! Pulsar engine for the RushWind broker contract — the Go
//! `go-wind-plugins/broker/pulsar` ported onto `pulsar` (pulsar-rs).
//!
//! # The wire behavior
//!
//! Publishes go through the crate's multi-topic producer, which
//! creates and caches one producer per topic — the Go engine's
//! producer map, with the caching inside the client library. The
//! payload rides raw (the crate's byte passthrough), the message
//! headers map to the record's user properties, and a nonempty
//! contract key becomes the partition key. The send awaits the
//! broker's receipt, as the Go engine's `Send` does.
//!
//! Subscribes build a `Shared`-type consumer under the Go engine's
//! fixed subscription name. Deliveries surface as events on their
//! origin topic with the payload (raw), the user properties as
//! headers, and the partition key lossily decoded into the
//! contract's string key. Each delivery is acknowledged
//! immediately after its handler is spawned — the Go engine's
//! `AutoAck` default acked after the handler returned — so this is
//! an implicit-acknowledgment engine and [`Event::ack`] is a no-op.
//!
//! The consumer ends on `unsubscribe` or on the stream's own end —
//! a closed consumer or an error — without a retry loop, the Go
//! engine's shape (its channel loop ended the same way).
//!
//! # Divergences from the Go engine
//!
//! - Unsubscribe closes the consumer but sends no
//!   unsubscribe-request: the server-side subscription and its
//!   backlog survive until the broker's retention reaps them. The
//!   Go engine called the client's `Unsubscribe`, which deletes the
//!   subscription; pulsar-rs exposes no equivalent.
//! - The Go engine's producer-rotation retry on a cached producer's
//!   failure is not ported: a failed send surfaces as an error and
//!   the producer stays cached (the library owns the cache).
//! - Batch knobs, send timeouts, TLS and auth dialer knobs,
//!   dead-letter policies, and tracers are not ported: the
//!   constructor takes the address only. An empty address falls
//!   back to `pulsar://127.0.0.1:6650`, the Go engine's default.
//!
//! # Testing
//!
//! Live conformance tests run against a real Apache Pulsar
//! (standalone) via the `live` feature; there is no embedded Pulsar
//! for CI's unit lanes.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use futures::TryStreamExt;
use rushwind_broker::{BoxFuture, Broker, BrokerError, Event, Handler, Message, Subscriber};
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

/// The subscription name every consumer registers with — the Go
/// engine's fixed default.
const SUBSCRIPTION_NAME: &str = "my-subscription";

/// Wraps a pulsar-rs error as a broker failure.
fn pulsar_err(error: pulsar::Error) -> BrokerError {
    BrokerError::Failed(format!("pulsar: {error}"))
}

struct Inner {
    client: Mutex<Option<pulsar::Pulsar<pulsar::TokioExecutor>>>,
    producer: Mutex<Option<pulsar::MultiTopicProducer<pulsar::TokioExecutor>>>,
    connected: AtomicBool,
}

/// A Pulsar-backed broker over pulsar-rs.
pub struct PulsarBroker {
    inner: Arc<Inner>,
}

/// The bootstrap factory's settings wire shape for
/// [`PulsarBroker::from_settings`].
#[derive(serde::Deserialize)]
pub struct PulsarSettings {
    /// The Pulsar service URL (`pulsar://host:port`). Absent falls
    /// back to the Go engine's default localhost service.
    pub addr: Option<String>,
}

impl PulsarBroker {
    /// Connects to the Pulsar service at `addr` (e.g.
    /// `pulsar://127.0.0.1:6650`). An empty address falls back to
    /// the Go engine's default.
    pub async fn connect(addr: &str) -> Result<Self, BrokerError> {
        let addr = if addr.is_empty() {
            "pulsar://127.0.0.1:6650"
        } else {
            addr
        };
        let client: pulsar::Pulsar<pulsar::TokioExecutor> =
            pulsar::Pulsar::builder(addr.to_string(), pulsar::TokioExecutor)
                .build()
                .await
                .map_err(pulsar_err)?;
        let producer = client.clone().producer().build_multi_topic();
        Ok(Self {
            inner: Arc::new(Inner {
                client: Mutex::new(Some(client)),
                producer: Mutex::new(Some(producer)),
                connected: AtomicBool::new(true),
            }),
        })
    }

    /// Constructs from the bootstrap factory's settings wire shape:
    /// `addr` (optional; empty falls back to the Go engine's default
    /// localhost service).
    pub async fn from_settings(settings: serde_json::Value) -> Result<Self, BrokerError> {
        let settings: PulsarSettings = serde_json::from_value(settings)
            .map_err(|e| BrokerError::Failed(format!("settings parse: {e}")))?;
        Self::connect(settings.addr.as_deref().unwrap_or("")).await
    }
}

/// Maps a delivered record onto a contract event: raw payload, user
/// properties as headers, partition key as the contract's string
/// key.
fn to_event(record: &pulsar::consumer::Message<Vec<u8>>) -> Event {
    let mut message = Message::from_payload(record.deserialize());
    for pair in &record.metadata().properties {
        message.headers.insert(pair.key.clone(), pair.value.clone());
    }
    message.key = record.key().unwrap_or_default();
    Event::new(record.topic.clone(), message)
}

impl Broker for PulsarBroker {
    fn name(&self) -> &'static str {
        "pulsar"
    }

    fn connect(&self) -> BoxFuture<'_, Result<(), BrokerError>> {
        // The client is live from construction.
        Box::pin(async { Ok(()) })
    }

    fn disconnect(&self) -> BoxFuture<'_, Result<(), BrokerError>> {
        Box::pin(async move {
            self.inner.connected.store(false, Ordering::SeqCst);
            // Close every per-topic producer, then drop the
            // multi-topic producer and the client.
            let mut guard = self.inner.producer.lock().await;
            if let Some(producer) = guard.as_mut() {
                for topic in producer.topics() {
                    let _ = producer.close_producer(topic).await;
                }
            }
            *guard = None;
            *self.inner.client.lock().await = None;
            Ok(())
        })
    }

    fn publish<'a>(
        &'a self,
        topic: &'a str,
        message: Message,
    ) -> BoxFuture<'a, Result<(), BrokerError>> {
        Box::pin(async move {
            if !self.inner.connected.load(Ordering::SeqCst) {
                return Err(BrokerError::NotConnected);
            }
            let mut guard = self.inner.producer.lock().await;
            let Some(producer) = guard.as_mut() else {
                return Err(BrokerError::NotConnected);
            };
            // The record carries the payload raw, the headers as user
            // properties, and a nonempty key as the partition key —
            // the Go engine's mapping.
            let record = pulsar::producer::Message {
                payload: message.payload,
                properties: message.headers,
                partition_key: if message.key.is_empty() {
                    None
                } else {
                    Some(message.key)
                },
                ..Default::default()
            };
            producer
                .send_non_blocking(topic, record)
                .await
                .map_err(pulsar_err)?
                .await
                .map_err(pulsar_err)?;
            Ok(())
        })
    }

    fn subscribe<'a>(
        &'a self,
        topic: &'a str,
        handler: Handler,
    ) -> BoxFuture<'a, Result<Box<dyn Subscriber>, BrokerError>> {
        Box::pin(async move {
            if !self.inner.connected.load(Ordering::SeqCst) {
                return Err(BrokerError::NotConnected);
            }
            let client = match self.inner.client.lock().await.clone() {
                Some(client) => client,
                None => return Err(BrokerError::NotConnected),
            };
            let consumer: pulsar::consumer::Consumer<Vec<u8>, pulsar::TokioExecutor> = client
                .consumer()
                .with_topic(topic.to_string())
                .with_subscription_type(pulsar::SubType::Shared)
                .with_subscription(SUBSCRIPTION_NAME)
                .build()
                .await
                .map_err(pulsar_err)?;

            let done = CancellationToken::new();
            let task_done = done.clone();
            let handler = Arc::clone(&handler);
            let task = tokio::spawn(async move {
                let mut consumer = consumer;
                loop {
                    let delivery = tokio::select! {
                        _ = task_done.cancelled() => {
                            let _ = consumer.close().await;
                            return;
                        }
                        delivery = consumer.try_next() => delivery,
                    };
                    match delivery {
                        Ok(Some(record)) => {
                            let event = to_event(&record);
                            tokio::spawn(handler(event));
                            // The Go engine's AutoAck default:
                            // every delivery is acknowledged once
                            // dispatched.
                            let _ = consumer.ack(&record).await;
                        }
                        Ok(None) | Err(_) => {
                            // The stream ended: closed consumer or an
                            // error. The pump ends with it, as the Go
                            // channel loop did.
                            let _ = consumer.close().await;
                            return;
                        }
                    }
                }
            });
            Ok(Box::new(PulsarSubscriber {
                topic: topic.to_string(),
                done,
                task: Mutex::new(Some(task)),
            }) as Box<dyn Subscriber>)
        })
    }
}

/// The Pulsar subscription handle: the token and task tear the
/// delivery pump down on unsubscribe or drop.
struct PulsarSubscriber {
    topic: String,
    done: CancellationToken,
    task: Mutex<Option<tokio::task::JoinHandle<()>>>,
}

impl Subscriber for PulsarSubscriber {
    fn topic(&self) -> &str {
        &self.topic
    }

    fn unsubscribe(&mut self) -> BoxFuture<'_, Result<(), BrokerError>> {
        Box::pin(async move {
            self.done.cancel();
            if let Some(task) = self.task.lock().await.take() {
                let _ = task.await;
            }
            Ok(())
        })
    }
}

impl Drop for PulsarSubscriber {
    fn drop(&mut self) {
        // Best-effort: cancel and abort without awaiting the task.
        self.done.cancel();
        if let Ok(mut task) = self.task.try_lock() {
            if let Some(task) = task.take() {
                task.abort();
            }
        }
    }
}
