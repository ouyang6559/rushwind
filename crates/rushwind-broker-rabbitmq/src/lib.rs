//! RabbitMQ engine for the RushWind broker, over `lapin`.
//!
//! # The wire behavior
//!
//! Publishes go to the **`amq.topic` topic exchange** (durable and
//! pre-existing on every broker) with
//! the topic as the routing key; the message headers ride the AMQP
//! `headers` table — RabbitMQ, unlike MQTT/Redis/NATS, does carry
//! them. Each subscription declares an anonymous, exclusive,
//! auto-delete queue bound to the exchange with the topic as the
//! routing key, and consumes it with automatic acknowledgment.
//!
//! Topic routing follows AMQP topic-match rules (`*` one word, `#`
//! zero or more words, `.` separators).
//!
//! # Divergences
//!
//! - One exchange (`amq.topic`); per-call exchange selection
//!   and custom exchange registration are not available.
//! - Deliveries auto-acknowledge; the event ack is a no-op.
//! - The delivery mode (persistent/transient) is not configurable —
//!   AMQP's unset default treats deliveries as transient.
//!
//! # Testing
//!
//! Live conformance tests run against a real RabbitMQ via the `live`
//! feature; there is no embedded RabbitMQ for CI's unit lanes.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

use futures::StreamExt;
use lapin::options::{
    BasicAckOptions, BasicConsumeOptions, BasicPublishOptions, QueueBindOptions,
    QueueDeclareOptions,
};
use lapin::types::FieldTable;
use lapin::{BasicProperties, Channel, Connection, ConnectionProperties};
use rushwind_broker::{BoxFuture, Broker, BrokerError, Event, Handler, Message, Subscriber};
use std::future::Future;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

/// The exchange every publish and bind goes through.
const DEFAULT_EXCHANGE: &str = "amq.topic";

struct Inner {
    connection: Arc<Connection>,
}

/// A RabbitMQ-backed broker over AMQP 0-9-1.
pub struct RabbitmqBroker {
    inner: Arc<Inner>,
}

/// The bootstrap factory's settings wire shape for
/// [`RabbitmqBroker::from_settings`].
#[derive(serde::Deserialize)]
pub struct RabbitmqSettings {
    /// The AMQP connection URL.
    pub url: String,
}

use std::sync::Arc;

impl RabbitmqBroker {
    /// Connects to RabbitMQ at `url` (e.g.
    /// `amqp://guest:guest@127.0.0.1:5672`).
    pub async fn connect(url: &str) -> Result<Self, BrokerError> {
        let connection = Connection::connect(url, ConnectionProperties::default())
            .await
            .map_err(|e| BrokerError::Failed(format!("rabbitmq connect {url}: {e}")))?;
        Ok(Self {
            inner: Arc::new(Inner {
                connection: Arc::new(connection),
            }),
        })
    }

    /// Constructs from the bootstrap factory's settings wire shape:
    /// `url` (required).
    pub async fn from_settings(settings: serde_json::Value) -> Result<Self, BrokerError> {
        let settings: RabbitmqSettings = serde_json::from_value(settings)
            .map_err(|e| BrokerError::Failed(format!("settings parse: {e}")))?;
        Self::connect(&settings.url).await
    }

    fn publish_channel(&self) -> impl Future<Output = Result<Channel, BrokerError>> {
        let connection = self.inner.connection.clone();
        async move {
            connection
                .create_channel()
                .await
                .map_err(|e| BrokerError::Failed(format!("rabbitmq channel: {e}")))
        }
    }
}

impl Broker for RabbitmqBroker {
    fn name(&self) -> &'static str {
        "rabbitmq"
    }

    fn connect(&self) -> BoxFuture<'_, Result<(), BrokerError>> {
        // The AMQP connection is live from construction; a channel
        // probe verifies it eagerly.
        Box::pin(async move {
            self.publish_channel().await?;
            Ok(())
        })
    }

    fn disconnect(&self) -> BoxFuture<'_, Result<(), BrokerError>> {
        Box::pin(async move {
            self.inner
                .connection
                .close(200, lapin::types::ShortString::from("broker disconnect"))
                .await
                .map_err(|e| BrokerError::Failed(format!("rabbitmq disconnect: {e}")))
        })
    }

    fn publish<'a>(
        &'a self,
        topic: &'a str,
        message: Message,
    ) -> BoxFuture<'a, Result<(), BrokerError>> {
        Box::pin(async move {
            let channel = self.publish_channel().await?;
            let mut headers = FieldTable::default();
            for (key, value) in &message.headers {
                headers.insert(
                    key.as_str().into(),
                    lapin::types::AMQPValue::LongString(value.as_bytes().into()),
                );
            }
            channel
                .basic_publish(
                    DEFAULT_EXCHANGE.into(),
                    topic.into(),
                    BasicPublishOptions::default(),
                    &message.payload,
                    BasicProperties::default().with_headers(headers),
                )
                .await
                .map_err(|e| BrokerError::Failed(format!("rabbitmq publish: {e}")))?
                .await
                .map_err(|e| BrokerError::Failed(format!("rabbitmq publish confirm: {e}")))?;
            Ok(())
        })
    }

    fn subscribe<'a>(
        &'a self,
        topic: &'a str,
        handler: Handler,
    ) -> BoxFuture<'a, Result<Box<dyn Subscriber>, BrokerError>> {
        Box::pin(async move {
            let channel = self
                .inner
                .connection
                .create_channel()
                .await
                .map_err(|e| BrokerError::Failed(format!("rabbitmq channel: {e}")))?;

            // The consume path: an anonymous, exclusive,
            // auto-delete queue bound to the exchange with the topic
            // as the routing key, consumed with automatic acks.
            let queue = channel
                .queue_declare(
                    "".into(),
                    QueueDeclareOptions {
                        exclusive: true,
                        auto_delete: true,
                        ..QueueDeclareOptions::default()
                    },
                    FieldTable::default(),
                )
                .await
                .map_err(|e| BrokerError::Failed(format!("rabbitmq queue declare: {e}")))?
                .name()
                .to_string();
            channel
                .queue_bind(
                    queue.as_str().into(),
                    DEFAULT_EXCHANGE.into(),
                    topic.into(),
                    QueueBindOptions::default(),
                    FieldTable::default(),
                )
                .await
                .map_err(|e| BrokerError::Failed(format!("rabbitmq bind {topic}: {e}")))?;

            let consumer = channel
                .basic_consume(
                    queue.as_str().into(),
                    "".into(),
                    BasicConsumeOptions::default(),
                    FieldTable::default(),
                )
                .await
                .map_err(|e| BrokerError::Failed(format!("rabbitmq consume {topic}: {e}")))?;

            // The reader dispatches deliveries until unsubscribed or
            // the broker closes the stream; each delivery acknowledges
            // after dispatch.
            let done = CancellationToken::new();
            let task_done = done.clone();
            let reader = tokio::spawn(async move {
                let mut consumer = consumer;
                loop {
                    let delivery = tokio::select! {
                        _ = task_done.cancelled() => return,
                        delivery = consumer.next() => match delivery {
                            Some(Ok(delivery)) => delivery,
                            _ => return,
                        },
                    };
                    let routing_key = delivery.routing_key.to_string();
                    let payload = delivery.data.clone();
                    let mut message = Message::from_payload(payload);
                    if let Some(table) = delivery.properties.headers() {
                        for (key, value) in table.inner().iter() {
                            if let Some(bytes) = value.as_long_string() {
                                message.headers.insert(
                                    key.to_string(),
                                    String::from_utf8_lossy(bytes.as_bytes()).to_string(),
                                );
                            }
                        }
                    }
                    let event = Event::new(routing_key, message);
                    tokio::spawn(handler(event));
                    let _ = delivery.ack(BasicAckOptions::default()).await;
                }
            });

            Ok(Box::new(RabbitmqSubscriber {
                topic: topic.to_string(),
                done,
                channel: Some(channel),
                reader: Mutex::new(Some(reader)),
            }) as Box<dyn Subscriber>)
        })
    }
}

/// The RabbitMQ subscription handle: owning the consumer channel
/// keeps the subscription alive; dropping or unsubscribing tears it
/// down.
struct RabbitmqSubscriber {
    topic: String,
    done: CancellationToken,
    channel: Option<Channel>,
    reader: Mutex<Option<tokio::task::JoinHandle<()>>>,
}

impl Subscriber for RabbitmqSubscriber {
    fn topic(&self) -> &str {
        &self.topic
    }

    fn unsubscribe(&mut self) -> BoxFuture<'_, Result<(), BrokerError>> {
        Box::pin(async move {
            self.done.cancel();
            if let Some(reader) = self.reader.lock().await.take() {
                let _ = reader.await;
            }
            // The channel drop closes it; the exclusive auto-delete
            // queue disappears with it.
            if let Some(channel) = self.channel.take() {
                channel
                    .close(200, lapin::types::ShortString::from("unsubscribed"))
                    .await
                    .map_err(|e| BrokerError::Failed(format!("rabbitmq close: {e}")))?;
            }
            Ok(())
        })
    }
}

impl Drop for RabbitmqSubscriber {
    fn drop(&mut self) {
        // Best-effort: cancelling the reader and dropping the channel
        // tears down the exclusive queue without awaiting the close
        // frames.
        self.done.cancel();
        if let Ok(mut reader) = self.reader.try_lock() {
            if let Some(reader) = reader.take() {
                reader.abort();
            }
        }
        self.channel.take();
    }
}
