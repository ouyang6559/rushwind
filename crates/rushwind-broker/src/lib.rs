//! Message broker contract for RushWind, extracted from the Go
//! predecessor `go-wind-plugins/broker`: the [`Broker`] interface with
//! its connect/publish/subscribe/request lifecycle, the [`Message`]
//! wire shape, the delivery [`Event`], and the [`Subscriber`] handle.
//!
//! # The Go shapes, translated
//!
//! The Go contract carries the payload as `Body any` plus a codec
//! (JSON by default) and typed handlers that cast the decoded body.
//! Rust needs none of that machinery: the message travels as bytes
//! ([`Message::payload`]) and applications encode and decode at the
//! edges with serde. [`json_handler`] is the typed-helper equivalent —
//! it wraps a typed closure into a [`Handler`] that JSON-decodes the
//! payload first, and [`json_message`] builds a JSON-encoded
//! [`Message`].
//!
//! Go's `Event.Ack()` survives as [`Event::ack`]: engines whose
//! deliveries need acknowledging (Kafka offsets, RabbitMQ acks) wire
//! the native ack into the event; engines with implicit acknowledgment
//! (MQTT) leave it a no-op, exactly as the Go mqtt publication does.
//! Go's `Metadata map[string]any` narrows to string values — the
//! native broker metadata surfaces are string-typed. Go's
//! `Msg any` native-handle slot has no equivalent: engines expose what
//! their delivery actually needs through the event.
//!
//! # Engines
//!
//! Engines live in `rushwind-broker-*` crates (`mqtt`, `nats`, ...)
//! and implement [`Broker`] over their native client. The Go
//! per-call publish/subscribe options narrow to what the engine can
//! actually honor; engine-specific knobs live on the engine's
//! constructor options, not per call.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

/// Future type used across the broker contract.
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Errors surfaced by broker engines.
#[derive(Debug)]
#[non_exhaustive]
pub enum BrokerError {
    /// The engine could not complete the operation.
    Failed(String),
    /// The broker connection is not established.
    NotConnected,
}

impl std::fmt::Display for BrokerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Failed(msg) => write!(f, "broker operation failed: {msg}"),
            Self::NotConnected => write!(f, "broker not connected"),
        }
    }
}

impl std::error::Error for BrokerError {}

/// The message wire shape — the Go `broker.Message` with the payload
/// as bytes.
#[derive(Debug, Clone, Default)]
pub struct Message {
    /// Message id. Engines generate one when the native message
    /// carries none.
    pub id: String,
    /// String headers carried with the message.
    pub headers: HashMap<String, String>,
    /// The payload bytes.
    pub payload: Vec<u8>,
    /// The ordering key — Kafka's key, RabbitMQ's routing key.
    pub key: String,
    /// Additional string metadata about the message.
    pub metadata: HashMap<String, String>,
    /// The partition the message was read from (Kafka), when known.
    pub partition: Option<i64>,
    /// The offset of the message within its partition (Kafka), when
    /// known.
    pub offset: Option<i64>,
}

impl Message {
    /// Builds a message from a payload.
    pub fn from_payload(payload: Vec<u8>) -> Self {
        Self {
            payload,
            ..Message::default()
        }
    }

    /// Sets a header, replacing any previous value.
    pub fn with_header(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.headers.insert(key.into(), value.into());
        self
    }

    /// Reads a header, defaulting to empty — the Go `GetHeader`.
    pub fn header(&self, key: &str) -> &str {
        self.headers
            .get(key)
            .map(|value| value.as_str())
            .unwrap_or("")
    }

    /// Sets a metadata entry, replacing any previous value.
    pub fn with_metadata(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.metadata.insert(key.into(), value.into());
        self
    }
}

/// The acknowledgment hook an engine wires into an event when the
/// native delivery needs one.
pub type Acker = Box<dyn FnOnce() -> BoxFuture<'static, Result<(), BrokerError>> + Send>;

/// One delivery: the subscribed topic and the message, with the
/// engine's acknowledgment wired in when the native delivery needs
/// one.
pub struct Event {
    topic: String,
    message: Message,
    acker: Option<Acker>,
}

impl Event {
    /// Builds an event without an acknowledgment hook — the shape of
    /// every engine whose deliveries acknowledge implicitly.
    pub fn new(topic: impl Into<String>, message: Message) -> Self {
        Self {
            topic: topic.into(),
            message,
            acker: None,
        }
    }

    /// Builds an event with an acknowledgment hook.
    pub fn with_acker(
        topic: impl Into<String>,
        message: Message,
        acker: impl FnOnce() -> BoxFuture<'static, Result<(), BrokerError>> + Send + 'static,
    ) -> Self {
        Self {
            topic: topic.into(),
            message,
            acker: Some(Box::new(acker)),
        }
    }

    /// The topic the message arrived on — the Go `Event.Topic`.
    pub fn topic(&self) -> &str {
        &self.topic
    }

    /// The message — the Go `Event.Message`.
    pub fn message(&self) -> &Message {
        &self.message
    }

    /// Acknowledges the delivery. A no-op for engines with implicit
    /// acknowledgment — the Go mqtt publication's `Ack`.
    pub async fn ack(self) -> Result<(), BrokerError> {
        match self.acker {
            Some(acker) => acker().await,
            None => Ok(()),
        }
    }
}

/// The handler invoked per delivery — the Go `broker.Handler` with
/// the context parameter dropped (cancellation rides the future).
pub type Handler = Arc<dyn Fn(Event) -> BoxFuture<'static, Result<(), BrokerError>> + Send + Sync>;

/// The subscription handle — the Go `broker.Subscriber`.
pub trait Subscriber: Send {
    /// The subscribed topic — the Go `Subscriber.Topic`.
    fn topic(&self) -> &str;

    /// Unsubscribes and releases the engine-side subscription.
    fn unsubscribe(&mut self) -> BoxFuture<'_, Result<(), BrokerError>>;
}

/// The broker interface — the Go `broker.Broker`. Engines must be
/// callable through shared references (`&self`).
pub trait Broker: Send + Sync {
    /// The engine name — the Go `Broker.Name`.
    fn name(&self) -> &'static str;

    /// Establishes the broker connection — the Go `Broker.Connect`.
    /// Idempotent engines may treat repeated calls as no-ops.
    fn connect(&self) -> BoxFuture<'_, Result<(), BrokerError>>;

    /// Tears the connection down — the Go `Broker.Disconnect`.
    fn disconnect(&self) -> BoxFuture<'_, Result<(), BrokerError>>;

    /// Publishes a message to a topic — the Go `Broker.Publish`.
    fn publish<'a>(
        &'a self,
        topic: &'a str,
        message: Message,
    ) -> BoxFuture<'a, Result<(), BrokerError>>;

    /// Subscribes a handler to a topic — the Go `Broker.Subscribe`.
    /// Topic filters follow the engine's native syntax (MQTT's `+`/`#`
    /// wildcards, NATS's `*`/`>`, Kafka's literal topics).
    fn subscribe<'a>(
        &'a self,
        topic: &'a str,
        handler: Handler,
    ) -> BoxFuture<'a, Result<Box<dyn Subscriber>, BrokerError>>;
}

/// Builds a JSON-encoded [`Message`] from any serializable value —
/// the encode half of the Go codec default.
pub fn json_message<T: Serialize>(value: &T) -> Result<Message, BrokerError> {
    let payload =
        serde_json::to_vec(value).map_err(|e| BrokerError::Failed(format!("json encode: {e}")))?;
    Ok(Message::from_payload(payload))
}

/// Wraps a typed closure into a [`Handler`] that JSON-decodes each
/// delivery's payload first — the Go `Subscribe[T]` typed-helper
/// shape: a decode failure surfaces as a handler error.
pub fn json_handler<T, F, Fut>(handler: F) -> Handler
where
    T: DeserializeOwned + Send + 'static,
    F: Fn(Event, T) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<(), BrokerError>> + Send + 'static,
{
    Arc::new(move |event: Event| {
        let future = {
            let payload = event.message().payload.clone();
            match serde_json::from_slice::<T>(&payload) {
                Ok(value) => handler(event, value),
                Err(e) => {
                    return Box::pin(async move {
                        Err::<(), BrokerError>(BrokerError::Failed(format!("json decode: {e}")))
                    }) as BoxFuture<'static, Result<(), BrokerError>>;
                }
            }
        };
        Box::pin(future) as BoxFuture<'static, Result<(), BrokerError>>
    })
}

use serde::de::DeserializeOwned;
use serde::Serialize;

#[cfg(test)]
mod tests {
    use super::*;

    /// The JSON helpers round trip a payload through a message.
    #[tokio::test]
    async fn json_roundtrip() {
        #[derive(Debug, serde::Serialize, serde::Deserialize, PartialEq)]
        struct Payload {
            count: u32,
        }

        let message = json_message(&Payload { count: 7 }).expect("json encode");
        let event = Event::new("topic", message);

        let handler = json_handler(|event: Event, payload: Payload| async move {
            assert_eq!(payload.count, 7);
            assert_eq!(event.topic(), "topic");
            Ok(())
        });
        handler(Event::new("topic", event.message().clone()))
            .await
            .expect("handler must succeed");
    }

    /// A decode failure surfaces as a handler error, not a panic —
    /// the Go typed helper's "unsupported type" error path.
    #[tokio::test]
    async fn json_decode_failure_is_an_error() {
        #[derive(Debug, serde::Deserialize)]
        #[allow(dead_code)]
        struct Payload {
            count: u32,
        }

        let handler =
            json_handler(
                |_event: Event, _payload: Payload| async move { Ok::<(), BrokerError>(()) },
            );
        let result = handler(Event::new(
            "topic",
            Message::from_payload(b"not json".to_vec()),
        ))
        .await;
        assert!(result.is_err(), "decode failure must surface as an error");
    }

    /// Headers and metadata follow the Go builder semantics.
    #[test]
    fn message_builder_semantics() {
        let message = Message::from_payload(vec![])
            .with_header("a", "1")
            .with_header("a", "2")
            .with_metadata("kind", "demo");
        assert_eq!(message.header("a"), "2", "later writes replace");
        assert_eq!(message.header("missing"), "", "missing headers read empty");
        assert_eq!(
            message.metadata.get("kind").map(String::as_str),
            Some("demo")
        );
    }
}
