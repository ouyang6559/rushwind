//! Redis engine for the RushWind broker contract — the Go
//! `go-wind-plugins/broker/redis` pub-sub mode ported onto the `redis`
//! crate.
//!
//! # The wire behavior
//!
//! Identical to the Go pub-sub engine's: publishes are `PUBLISH
//! topic payload` — payload-only, headers and metadata have no Redis
//! carrier. Each subscription owns a dedicated connection in
//! SUBSCRIBE mode (Redis turns a subscribed connection into a
//! push-only channel), reading `message` events and routing them to
//! the handler. Exact topics only — the Go engine subscribes without
//! patterns, so there is no wildcard matching here, unlike the MQTT
//! engine.
//!
//! Redis pub/sub has no acknowledgment and no persistence: a
//! delivery is at-most-once, the event's ack is a no-op, and
//! unsubscribing drops the connection — after which the topic's
//! messages are simply never seen, the Go subscriber's
//! `conn.Close` semantics.
//!
//! # Divergences from the Go engine
//!
//! - Publishes ride a shared multiplexed connection; the Go engine
//!   pulls a pooled connection per publish.
//! - The Go `stream` mode (Redis Streams with consumer groups) is
//!   not ported — it is a different delivery contract (at-least-once,
//!   acknowledgments, replay) and would be its own engine.
//!
//! # Testing
//!
//! Live conformance tests run against a real Redis via the `live`
//! feature; there is no embedded Redis for CI's unit lanes.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

use std::collections::HashMap;
use std::sync::Arc;

use futures::StreamExt;
use rushwind_broker::{BoxFuture, Broker, BrokerError, Event, Handler, Message, Subscriber};

use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

struct Inner {
    client: redis::Client,
    /// Watched services' subscriptions, keyed by topic.
    subscriptions: Mutex<HashMap<String, Arc<CancellationToken>>>,
}

/// A Redis pub/sub-backed broker.
pub struct RedisBroker {
    inner: Arc<Inner>,
}

impl RedisBroker {
    /// Connects to Redis at `url` (e.g. `redis://127.0.0.1:6379`).
    pub fn connect(url: &str) -> Result<Self, BrokerError> {
        let client = redis::Client::open(url.to_string())
            .map_err(|e| BrokerError::Failed(format!("redis client open: {e}")))?;
        Ok(Self {
            inner: Arc::new(Inner {
                client,
                subscriptions: Mutex::new(HashMap::new()),
            }),
        })
    }

    /// Constructs from the bootstrap factory's settings wire shape:
    /// `url` (required).
    pub fn from_settings(settings: serde_json::Value) -> Result<Self, BrokerError> {
        let settings: RedisSettings = serde_json::from_value(settings)
            .map_err(|e| BrokerError::Failed(format!("settings parse: {e}")))?;
        Self::connect(&settings.url)
    }
}

/// The bootstrap factory's settings wire shape for
/// [`RedisBroker::from_settings`].
#[derive(serde::Deserialize)]
pub struct RedisSettings {
    /// The Redis connection URL.
    pub url: String,
}

impl Broker for RedisBroker {
    fn name(&self) -> &'static str {
        "redis"
    }

    fn connect(&self) -> BoxFuture<'_, Result<(), BrokerError>> {
        // The pub-sub engine connects lazily per publish and per
        // subscription, as the Go pool does.
        Box::pin(async move { Ok(()) })
    }

    fn disconnect(&self) -> BoxFuture<'_, Result<(), BrokerError>> {
        // Nothing process-wide to close: publishes use short-lived
        // pooled handles and subscriptions close on unsubscribe.
        Box::pin(async move { Ok(()) })
    }

    fn publish<'a>(
        &'a self,
        topic: &'a str,
        message: Message,
    ) -> BoxFuture<'a, Result<(), BrokerError>> {
        Box::pin(async move {
            // The Go engine publishes payload-only through PUBLISH.
            let mut connection = self
                .inner
                .client
                .get_multiplexed_async_connection()
                .await
                .map_err(|e| BrokerError::Failed(format!("redis connect: {e}")))?;
            let (): () = redis::AsyncCommands::publish(&mut connection, topic, message.payload)
                .await
                .map_err(|e| BrokerError::Failed(format!("redis publish: {e}")))?;
            Ok(())
        })
    }

    fn subscribe<'a>(
        &'a self,
        topic: &'a str,
        handler: Handler,
    ) -> BoxFuture<'a, Result<Box<dyn Subscriber>, BrokerError>> {
        Box::pin(async move {
            // A subscribed Redis connection is push-only: each
            // subscription owns one, exactly like the Go PubSubConn.
            let mut pubsub = self
                .inner
                .client
                .get_async_pubsub()
                .await
                .map_err(|e| BrokerError::Failed(format!("redis subscribe {topic}: {e}")))?;
            pubsub
                .subscribe(topic)
                .await
                .map_err(|e| BrokerError::Failed(format!("redis subscribe {topic}: {e}")))?;

            let done = CancellationToken::new();
            let task_done = done.clone();
            let topic_for_task = topic.to_string();
            let reader = tokio::spawn(async move {
                let mut stream = pubsub.into_on_message();
                loop {
                    let message = tokio::select! {
                        _ = task_done.cancelled() => return,
                        message = stream.next() => match message {
                            Some(message) => message,
                            None => return,
                        },
                    };
                    let payload = message.get_payload_bytes().to_vec();
                    let event = Event::new(topic_for_task.clone(), Message::from_payload(payload));
                    tokio::spawn(handler(event));
                }
            });

            self.inner
                .subscriptions
                .lock()
                .await
                .insert(topic.to_string(), Arc::new(done.clone()));

            Ok(Box::new(RedisSubscriber {
                topic: topic.to_string(),
                done: done.clone(),
                reader: Mutex::new(Some(reader)),
                inner: Arc::clone(&self.inner),
            }) as Box<dyn Subscriber>)
        })
    }
}

/// The Redis subscription handle.
struct RedisSubscriber {
    topic: String,
    done: CancellationToken,
    reader: Mutex<Option<tokio::task::JoinHandle<()>>>,
    inner: Arc<Inner>,
}

impl Subscriber for RedisSubscriber {
    fn topic(&self) -> &str {
        &self.topic
    }

    fn unsubscribe(&mut self) -> BoxFuture<'_, Result<(), BrokerError>> {
        Box::pin(async move {
            self.inner.subscriptions.lock().await.remove(&self.topic);
            // Cancel ends the reader task; awaiting it closes the
            // subscribed connection before returning — the Go
            // conn.Close, with the close observed.
            self.done.cancel();
            if let Some(reader) = self.reader.lock().await.take() {
                let _ = reader.await;
            }
            Ok(())
        })
    }
}

impl Drop for RedisSubscriber {
    fn drop(&mut self) {
        // Same teardown as unsubscribe, best-effort when the explicit
        // path was skipped.
        self.done.cancel();
        if let Ok(mut reader) = self.reader.try_lock() {
            if let Some(reader) = reader.take() {
                reader.abort();
            }
        }
    }
}
