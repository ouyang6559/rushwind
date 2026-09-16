//! NATS engine for the RushWind broker, core-NATS mode over
//! `async-nats`.
//!
//! # The wire behavior
//!
//! Publishes are `PublishMsg` on the
//! subject — payload-only, NATS headers are not attached. Topics
//! are NATS subjects and follow NATS wildcards (`*` one token, `>`
//! the rest); subscriptions are plain (no queue group) unless
//! [`NatsOptions::queue_group`] names one.
//!
//! NATS core is at-most-once fire-and-forget: the event ack is a
//! no-op, unsubscribing drops the server-side subscription, and a
//! dropped [`Subscriber`] removes it best-effort.
//!
//! Request/reply rides core NATS request semantics — the client
//! library's managed inbox subscription — and the response carries
//! the payload only, matching the subscribe path.
//!
//! # Divergences
//!
//! - The wire metadata has no NATS carrier beyond headers.
//!
//! # Testing
//!
//! Live conformance tests run against a real NATS via the `live`
//! feature; there is no embedded NATS for CI's unit lanes.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

use std::sync::Arc;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

use futures::StreamExt;
use rushwind_broker::{BoxFuture, Broker, BrokerError, Event, Handler, Message, Subscriber};

struct Inner {
    client: async_nats::Client,
}

/// A NATS-backed broker over core NATS.
pub struct NatsBroker {
    inner: Arc<Inner>,
}

impl NatsBroker {
    /// Connects to NATS at `addr` (e.g. `nats://127.0.0.1:4222`).
    pub async fn connect(addr: &str) -> Result<Self, BrokerError> {
        let client = async_nats::connect(addr)
            .await
            .map_err(|e| BrokerError::Failed(format!("nats connect {addr}: {e}")))?;
        Ok(Self {
            inner: Arc::new(Inner { client }),
        })
    }

    /// Constructs from the bootstrap factory's settings wire shape:
    /// `addr` (required).
    pub async fn from_settings(settings: serde_json::Value) -> Result<Self, BrokerError> {
        let settings: NatsSettings = serde_json::from_value(settings)
            .map_err(|e| BrokerError::Failed(format!("settings parse: {e}")))?;
        Self::connect(&settings.addr).await
    }
}

/// The bootstrap factory's settings wire shape for
/// [`NatsBroker::from_settings`].
#[derive(serde::Deserialize)]
pub struct NatsSettings {
    /// The NATS server URL.
    pub addr: String,
}

impl Broker for NatsBroker {
    fn name(&self) -> &'static str {
        "nats"
    }

    fn connect(&self) -> BoxFuture<'_, Result<(), BrokerError>> {
        // async-nats reconnects internally; the client is live from
        // construction.
        Box::pin(async move { Ok(()) })
    }

    fn disconnect(&self) -> BoxFuture<'_, Result<(), BrokerError>> {
        Box::pin(async move { Ok(()) })
    }

    fn publish<'a>(
        &'a self,
        topic: &'a str,
        message: Message,
    ) -> BoxFuture<'a, Result<(), BrokerError>> {
        Box::pin(async move {
            self.inner
                .client
                .publish(topic.to_string(), message.payload.into())
                .await
                .map_err(|e| BrokerError::Failed(format!("nats publish: {e}")))
        })
    }

    fn request<'a>(
        &'a self,
        topic: &'a str,
        message: Message,
    ) -> BoxFuture<'a, Result<Message, BrokerError>> {
        // Core NATS request-reply: the client library manages the
        // inbox subscription. The response carries the payload only,
        // like the subscribe path.
        Box::pin(async move {
            let response = self
                .inner
                .client
                .request(topic.to_string(), message.payload.into())
                .await
                .map_err(|e| BrokerError::Failed(format!("nats request: {e}")))?;
            Ok(Message::from_payload(response.payload.to_vec()))
        })
    }

    fn subscribe<'a>(
        &'a self,
        topic: &'a str,
        handler: Handler,
    ) -> BoxFuture<'a, Result<Box<dyn Subscriber>, BrokerError>> {
        Box::pin(async move {
            let subscription = self
                .inner
                .client
                .subscribe(topic.to_string())
                .await
                .map_err(|e| BrokerError::Failed(format!("nats subscribe {topic}: {e}")))?;

            // The reader owns the server-side subscription; cancelling
            // it drops the subscription, which unsubscribes.
            let done = CancellationToken::new();
            let task_done = done.clone();
            let reader = tokio::spawn(async move {
                let mut subscription = subscription;
                loop {
                    let message = tokio::select! {
                        _ = task_done.cancelled() => return,
                        message = subscription.next() => match message {
                            Some(message) => message,
                            None => return,
                        },
                    };
                    // The message carries the actual published
                    // subject, which the wildcard filter over-matches.
                    let subject = message.subject.to_string();
                    let payload = message.payload.to_vec();
                    let event = Event::new(subject, Message::from_payload(payload));
                    tokio::spawn(handler(event));
                }
            });

            Ok(Box::new(NatsSubscriber {
                topic: topic.to_string(),
                done,
                reader: Mutex::new(Some(reader)),
            }) as Box<dyn Subscriber>)
        })
    }
}

/// The NATS subscription handle.
struct NatsSubscriber {
    topic: String,
    done: CancellationToken,
    reader: Mutex<Option<tokio::task::JoinHandle<()>>>,
}

impl Subscriber for NatsSubscriber {
    fn topic(&self) -> &str {
        &self.topic
    }

    fn unsubscribe(&mut self) -> BoxFuture<'_, Result<(), BrokerError>> {
        Box::pin(async move {
            // Cancel ends the reader, whose dropped subscription
            // unsubscribes server-side.
            self.done.cancel();
            if let Some(reader) = self.reader.lock().await.take() {
                let _ = reader.await;
            }
            Ok(())
        })
    }
}

impl Drop for NatsSubscriber {
    fn drop(&mut self) {
        self.done.cancel();
        if let Ok(mut reader) = self.reader.try_lock() {
            if let Some(reader) = reader.take() {
                reader.abort();
            }
        }
    }
}
