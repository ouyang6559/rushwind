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
//! native broker metadata surfaces are string-typed. Go's `Msg any`
//! native-handle slot has no equivalent: engines expose what
//! their delivery actually needs through the event.
//!
//! Go's `Broker.Request` survives as [`Broker::request`]: a default
//! trait method returning the not-implemented error — the shape of
//! every Go engine without a native request-reply surface — which
//! engines with one (NATS) override.
//!
//! # Middleware
//!
//! Go's `PublishMiddleware`/`SubscriberMiddleware` chains — carried on
//! the Go broker options — survive as [`MiddlewareBroker`], a decorator
//! that runs the chains around another broker. The chain semantics are
//! the Go ones the Go chain tests pin: middlewares apply backward, so
//! the first-registered middleware is the outermost wrapper and runs
//! first.
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

    /// Sends a request and awaits a response — the Go
    /// `Broker.Request`. This default is the Go stub shape: every
    /// engine without a native request-reply surface returns the
    /// not-implemented error; engines with one override this.
    fn request<'a>(
        &'a self,
        topic: &'a str,
        message: Message,
    ) -> BoxFuture<'a, Result<Message, BrokerError>> {
        let _ = (topic, message);
        Box::pin(async {
            Err(BrokerError::Failed(
                "request not implemented by this engine".to_string(),
            ))
        })
    }
}

/// The publish-call surface a [`PublishMiddleware`] wraps — the Go
/// `PublishHandler` reshaped: engine publish futures borrow their
/// brokers, so the wrapped call is a by-reference trait object
/// rather than an `Fn` returning `'static` futures.
pub trait PublishCall: Send + Sync {
    /// Performs the wrapped publish.
    fn call<'a>(
        &'a self,
        topic: &'a str,
        message: Message,
    ) -> BoxFuture<'a, Result<(), BrokerError>>;
}

/// Wraps a publish-call surface — the Go `PublishMiddleware`,
/// polymorphic over the wrapped surface's object lifetime.
pub type PublishMiddleware =
    Arc<dyn for<'x> Fn(Arc<dyn PublishCall + 'x>) -> Arc<dyn PublishCall + 'x> + Send + Sync>;

/// Chains publish middlewares around a base surface — the Go
/// `ChainPublishMiddleware` semantics its test pins: middlewares apply
/// backward, so the first-registered middleware is the outermost
/// wrapper and runs first.
fn chain_publish<'a>(
    base: Arc<dyn PublishCall + 'a>,
    middlewares: &[PublishMiddleware],
) -> Arc<dyn PublishCall + 'a> {
    let mut handler = base;
    for middleware in middlewares.iter().rev() {
        handler = middleware(handler);
    }
    handler
}

/// The [`PublishCall`] view of the wrapped broker's publish — the
/// chain's innermost element.
struct BrokerPublishCall<'a, B: Broker> {
    broker: &'a B,
}

impl<B: Broker> PublishCall for BrokerPublishCall<'_, B> {
    fn call<'b>(
        &'b self,
        topic: &'b str,
        message: Message,
    ) -> BoxFuture<'b, Result<(), BrokerError>> {
        self.broker.publish(topic, message)
    }
}

/// Wraps a subscriber handler — the Go `SubscriberMiddleware`.
pub type SubscriberMiddleware = Arc<dyn Fn(Handler) -> Handler + Send + Sync>;

/// Chains subscriber middlewares around a handler — the Go
/// `ChainSubscriberMiddleware` semantics its test pins:
/// first-registered runs first.
fn chain_subscriber(base: Handler, middlewares: &[SubscriberMiddleware]) -> Handler {
    let mut handler = base;
    for middleware in middlewares.iter().rev() {
        handler = middleware(handler);
    }
    handler
}

/// A [`Broker`] decorator running publish and subscriber middleware
/// chains around another broker — the Go options'
/// `PublishMiddlewares`/`SubscriberMiddlewares`, reshaped from
/// constructor options into a composable layer: the wrapped broker
/// stays untouched, the chains apply only through the decorator, and
/// every non-publish/non-subscribe call delegates unchanged.
///
/// The chains follow the Go order its chain tests pin: the
/// first-registered middleware is the outermost wrapper and runs
/// first.
pub struct MiddlewareBroker<B: Broker> {
    inner: Arc<B>,
    publish_middlewares: Vec<PublishMiddleware>,
    subscriber_middlewares: Vec<SubscriberMiddleware>,
}

impl<B: Broker> MiddlewareBroker<B> {
    /// Wraps `inner`: publishes made through the decorator run
    /// through the publish chain, handlers subscribed through it
    /// through the subscriber chain.
    pub fn new(
        inner: Arc<B>,
        publish_middlewares: Vec<PublishMiddleware>,
        subscriber_middlewares: Vec<SubscriberMiddleware>,
    ) -> Self {
        Self {
            inner,
            publish_middlewares,
            subscriber_middlewares,
        }
    }
}

impl<B: Broker> Broker for MiddlewareBroker<B> {
    fn name(&self) -> &'static str {
        self.inner.name()
    }

    fn connect(&self) -> BoxFuture<'_, Result<(), BrokerError>> {
        let inner = Arc::clone(&self.inner);
        Box::pin(async move { inner.connect().await })
    }

    fn disconnect(&self) -> BoxFuture<'_, Result<(), BrokerError>> {
        let inner = Arc::clone(&self.inner);
        Box::pin(async move { inner.disconnect().await })
    }

    fn publish<'a>(
        &'a self,
        topic: &'a str,
        message: Message,
    ) -> BoxFuture<'a, Result<(), BrokerError>> {
        let base: Arc<dyn PublishCall + 'a> = Arc::new(BrokerPublishCall {
            broker: self.inner.as_ref(),
        });
        let chained = chain_publish(base, &self.publish_middlewares);
        // The block owns the chained surfaces (their Arcs); every
        // borrow the nested calls take closes inside it.
        Box::pin(async move { chained.call(topic, message).await })
    }

    fn subscribe<'a>(
        &'a self,
        topic: &'a str,
        handler: Handler,
    ) -> BoxFuture<'a, Result<Box<dyn Subscriber>, BrokerError>> {
        let inner = Arc::clone(&self.inner);
        let chained = chain_subscriber(handler, &self.subscriber_middlewares);
        Box::pin(async move { inner.subscribe(topic, chained).await })
    }

    fn request<'a>(
        &'a self,
        topic: &'a str,
        message: Message,
    ) -> BoxFuture<'a, Result<Message, BrokerError>> {
        // Go leaves Request unwrapped by the publish chain; the
        // decorator delegates it unchanged.
        let inner = Arc::clone(&self.inner);
        Box::pin(async move { inner.request(topic, message).await })
    }
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

    /// The default [`Broker::request`] returns the Go stubs'
    /// not-implemented error.
    #[tokio::test]
    async fn default_request_is_not_implemented() {
        let broker = MiddlewareBroker::new(Arc::new(NullBroker), vec![], vec![]);
        let result = broker.request("topic", Message::from_payload(vec![])).await;
        assert!(
            matches!(result, Err(BrokerError::Failed(_))),
            "default request must be the not-implemented error, got {result:?}"
        );
    }

    /// The middleware chains apply backward — the first-registered
    /// middleware is the outermost wrapper and runs first, wrapping
    /// the base handler last — the order the Go
    /// ChainXxxMiddleware tests pin.
    #[tokio::test]
    async fn middleware_chain_order_is_go_shaped() {
        let publish_log = Arc::new(std::sync::Mutex::new(Vec::<&'static str>::new()));
        let subscribe_log = Arc::new(std::sync::Mutex::new(Vec::<&'static str>::new()));

        // Base publish handler: the wrapped broker.
        let broker_publish_log = Arc::clone(&publish_log);
        let broker = Arc::new(RecordingBroker {
            publish_log: broker_publish_log,
            subscribe_log: Arc::clone(&subscribe_log),
        });

        // Publish middlewares p1, p2: each records its marker, then
        // runs the wrapped call surface.
        let publish_middlewares: Vec<PublishMiddleware> = [1, 2]
            .map(|index| {
                let log = Arc::clone(&publish_log);
                let marker: &'static str = if index == 1 { "p1" } else { "p2" };
                let middleware: PublishMiddleware =
                    Arc::new(move |wrapped| marker_publish_call(wrapped, log.clone(), marker));
                middleware
            })
            .into();

        // Subscriber middlewares s1, s2 wrapping a base handler that
        // records "s-base".
        let subscriber_middlewares = [1, 2].map(|index| {
            let log = Arc::clone(&subscribe_log);
            let marker: &'static str = if index == 1 { "s1" } else { "s2" };
            Arc::new(move |handler: Handler| {
                let log = Arc::clone(&log);
                Arc::new(move |event: Event| {
                    let log = Arc::clone(&log);
                    let handler = Arc::clone(&handler);
                    Box::pin(async move {
                        log.lock().unwrap().push(marker);
                        handler(event).await
                    }) as BoxFuture<'static, Result<(), BrokerError>>
                }) as Handler
            }) as SubscriberMiddleware
        });
        let base_subscriber_log = Arc::clone(&subscribe_log);
        let base_subscriber: Handler = Arc::new(move |_event: Event| {
            let log = Arc::clone(&base_subscriber_log);
            Box::pin(async move {
                log.lock().unwrap().push("s-base");
                Ok(())
            }) as BoxFuture<'static, Result<(), BrokerError>>
        });

        let decorated =
            MiddlewareBroker::new(broker, publish_middlewares, subscriber_middlewares.into());

        // The publish chain: p1 (first-registered) outermost, then
        // p2, then the wrapped broker.
        decorated
            .publish("topic", Message::from_payload(vec![]))
            .await
            .expect("decorated publish");
        assert_eq!(
            *publish_log.lock().unwrap(),
            vec!["p1", "p2", "p-base"],
            "publish chain runs first-registered middleware first"
        );

        // The subscriber chain: s1, s2, then the base handler; the
        // wrapped broker observes only the chained handler.
        decorated
            .subscribe("topic", base_subscriber)
            .await
            .expect("decorated subscribe");
        assert_eq!(
            *subscribe_log.lock().unwrap(),
            vec!["base-sub", "s1", "s2", "s-base"],
            "subscriber chain runs first-registered middleware first"
        );
    }

    /// A [`PublishCall`] wrapper recording its marker before
    /// delegating — the chain-order test's middleware element.
    struct MarkerPublishCall<'a> {
        wrapped: Arc<dyn PublishCall + 'a>,
        log: Arc<std::sync::Mutex<Vec<&'static str>>>,
        marker: &'static str,
    }

    impl PublishCall for MarkerPublishCall<'_> {
        fn call<'b>(
            &'b self,
            topic: &'b str,
            message: Message,
        ) -> BoxFuture<'b, Result<(), BrokerError>> {
            self.log.lock().unwrap().push(self.marker);
            self.wrapped.call(topic, message)
        }
    }

    /// Builds a [`MarkerPublishCall`] as a publish-call surface — the
    /// helper gives the middleware closure a concrete
    /// lifetime-polymorphic signature.
    fn marker_publish_call<'a>(
        wrapped: Arc<dyn PublishCall + 'a>,
        log: Arc<std::sync::Mutex<Vec<&'static str>>>,
        marker: &'static str,
    ) -> Arc<dyn PublishCall + 'a> {
        Arc::new(MarkerPublishCall {
            wrapped,
            log,
            marker,
        })
    }

    /// The [`Broker`] stub the chain-order test wraps: its publish
    /// records `p-base`, its subscribe invokes the chained handler and
    /// records `base-sub`.
    struct RecordingBroker {
        publish_log: Arc<std::sync::Mutex<Vec<&'static str>>>,
        subscribe_log: Arc<std::sync::Mutex<Vec<&'static str>>>,
    }

    impl Broker for RecordingBroker {
        fn name(&self) -> &'static str {
            "recording"
        }

        fn connect(&self) -> BoxFuture<'_, Result<(), BrokerError>> {
            Box::pin(async { Ok(()) })
        }

        fn disconnect(&self) -> BoxFuture<'_, Result<(), BrokerError>> {
            Box::pin(async { Ok(()) })
        }

        fn publish<'a>(
            &'a self,
            topic: &'a str,
            message: Message,
        ) -> BoxFuture<'a, Result<(), BrokerError>> {
            let _ = (topic, message);
            self.publish_log.lock().unwrap().push("p-base");
            Box::pin(async { Ok(()) })
        }

        fn subscribe<'a>(
            &'a self,
            topic: &'a str,
            handler: Handler,
        ) -> BoxFuture<'a, Result<Box<dyn Subscriber>, BrokerError>> {
            let _ = topic;
            self.subscribe_log.lock().unwrap().push("base-sub");
            let chained = handler(Event::new("topic", Message::from_payload(vec![])));
            Box::pin(async move {
                chained.await?;
                Ok(Box::new(NullSubscriber) as Box<dyn Subscriber>)
            })
        }
    }

    /// A [`Broker`] stub with no behavior, for the default-request
    /// test.
    struct NullBroker;

    impl Broker for NullBroker {
        fn name(&self) -> &'static str {
            "null"
        }

        fn connect(&self) -> BoxFuture<'_, Result<(), BrokerError>> {
            Box::pin(async { Ok(()) })
        }

        fn disconnect(&self) -> BoxFuture<'_, Result<(), BrokerError>> {
            Box::pin(async { Ok(()) })
        }

        fn publish<'a>(
            &'a self,
            topic: &'a str,
            message: Message,
        ) -> BoxFuture<'a, Result<(), BrokerError>> {
            let _ = (topic, message);
            Box::pin(async { Ok(()) })
        }

        fn subscribe<'a>(
            &'a self,
            topic: &'a str,
            handler: Handler,
        ) -> BoxFuture<'a, Result<Box<dyn Subscriber>, BrokerError>> {
            let _ = (topic, handler);
            Box::pin(async { Ok(Box::new(NullSubscriber) as Box<dyn Subscriber>) })
        }
    }

    /// A [`Subscriber`] stub.
    struct NullSubscriber;

    impl Subscriber for NullSubscriber {
        fn topic(&self) -> &str {
            "null"
        }

        fn unsubscribe(&mut self) -> BoxFuture<'_, Result<(), BrokerError>> {
            Box::pin(async { Ok(()) })
        }
    }
}
