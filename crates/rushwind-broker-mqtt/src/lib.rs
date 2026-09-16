//! MQTT engine for the RushWind broker, over `rumqttc`.
//!
//! # The wire behavior
//!
//! Publishes default to QoS 1 without
//! the retain flag and carry **only the payload** — headers and
//! metadata have no MQTT 3.1.1 carrier, so they are dropped.
//! Subscriptions default to QoS 1 and follow
//! MQTT topic filters (`+` single-level, `#` multi-level); incoming
//! publishes are routed to every handler whose filter matches.
//!
//! # Connection lifecycle
//!
//! `connect` spawns the event-loop pump; the loop drives rumqttc's
//! automatic reconnection and, on every successful CONNACK, re-arms
//! every live subscription, as if resubscribed on connect.
//! Publishing while
//! disconnected fails with [`BrokerError::NotConnected`]. A re-`connect` starts a fresh pump
//! generation; the previous pump exits.
//!
//! # Divergences
//!
//! - Deliveries dispatch concurrently (one task each); handler
//!   ordering is not guaranteed.
//! - Per-call QoS and retain options are not exposed — publishes
//!   use the defaults (QoS 1, no retain).
//! - Dropping a [`Subscriber`] removes its filter best-effort; the
//!   explicit [`Subscriber::unsubscribe`] is the reliable path.
//!
//! # Testing
//!
//! Live conformance tests run against a real broker (EMQX works) via
//! the `live` feature; there is no embedded MQTT broker for CI's unit
//! lanes.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use rushwind_broker::{BoxFuture, Broker, BrokerError, Event, Handler, Message, Subscriber};
use tokio::sync::{mpsc, Mutex, Notify};

struct Shared {
    /// The subscriptions' topic filters and their handlers, re-armed
    /// on every reconnect.
    subscriptions: Mutex<HashMap<String, Handler>>,
    connected: Mutex<bool>,
    /// Wakes the pump when a subscription changes so reconnect
    /// backoffs cannot strand a fresh subscription.
    rearm: Notify,
}

/// An MQTT-backed broker over one rumqttc event loop.
pub struct MqttBroker {
    mqtt_options: rumqttc::MqttOptions,
    client: Mutex<Option<rumqttc::AsyncClient>>,
    shared: Arc<Shared>,
    /// The live pump generation; older pumps exit when it moves on.
    generation: Arc<AtomicU64>,
    /// Per-generation pump shutdown.
    shutdown: Mutex<Option<Arc<Notify>>>,
}

/// The MQTT engine's connection settings.
#[derive(Debug, Clone)]
pub struct MqttOptions {
    /// The broker address as `host:port`.
    pub addr: String,
    /// The MQTT client id. Default: a random `rushwind-mqtt-…` id.
    pub client_id: String,
    /// The username, when the broker demands authentication.
    pub username: Option<String>,
    /// The password.
    pub password: Option<String>,
    /// The keep-alive interval. Default: 30 s.
    pub keep_alive: Duration,
}

impl MqttOptions {
    /// Settings for `addr` with a random client id.
    pub fn new(addr: impl Into<String>) -> Self {
        Self {
            addr: addr.into(),
            client_id: random_client_id(),
            username: None,
            password: None,
            keep_alive: Duration::from_secs(30),
        }
    }
}

impl MqttBroker {
    /// Builds the broker; call [`Broker::connect`] to go live.
    pub fn new(options: MqttOptions) -> Self {
        Self {
            mqtt_options: to_rumqtt_options(&options),
            client: Mutex::new(None),
            shared: Arc::new(Shared {
                subscriptions: Mutex::new(HashMap::new()),
                connected: Mutex::new(false),
                rearm: Notify::new(),
            }),
            generation: Arc::new(AtomicU64::new(0)),
            shutdown: Mutex::new(None),
        }
    }

    /// Constructs from the bootstrap factory's settings wire shape:
    /// `addr` (required), `client_id`, `username`, `password`.
    pub fn from_settings(settings: serde_json::Value) -> Result<Self, BrokerError> {
        let settings: MqttSettings = serde_json::from_value(settings)
            .map_err(|e| BrokerError::Failed(format!("settings parse: {e}")))?;
        let mut options = MqttOptions::new(settings.addr);
        if let Some(client_id) = settings.client_id {
            options.client_id = client_id;
        }
        options.username = settings.username;
        options.password = settings.password;
        Ok(Self::new(options))
    }
}

/// The bootstrap factory's settings wire shape for
/// [`MqttBroker::from_settings`].
#[derive(serde::Deserialize)]
pub struct MqttSettings {
    /// The broker address as `host:port`.
    pub addr: String,
    /// The MQTT client id. Default: a random id.
    pub client_id: Option<String>,
    /// The username.
    pub username: Option<String>,
    /// The password.
    pub password: Option<String>,
}

fn to_rumqtt_options(options: &MqttOptions) -> rumqttc::MqttOptions {
    let mut mqtt = rumqttc::MqttOptions::new(
        &options.client_id,
        host_of(&options.addr),
        port_of(&options.addr),
    );
    mqtt.set_keep_alive(options.keep_alive);
    if let Some(username) = &options.username {
        mqtt.set_credentials(
            username.clone(),
            options.password.clone().unwrap_or_default(),
        );
    }
    mqtt
}

fn fresh_client(mqtt: &rumqttc::MqttOptions) -> (rumqttc::AsyncClient, rumqttc::EventLoop) {
    rumqttc::AsyncClient::new(mqtt.clone(), 64)
}

fn host_of(addr: &str) -> String {
    addr.rsplit_once(':')
        .map(|(host, _)| host.to_string())
        .unwrap_or_else(|| addr.to_string())
}

fn port_of(addr: &str) -> u16 {
    addr.rsplit_once(':')
        .and_then(|(_, port)| port.parse().ok())
        .unwrap_or(1883)
}

/// A random `rushwind-mqtt-…` client id.
fn random_client_id() -> String {
    let mut bytes = [0u8; 8];
    let _ = getrandom::fill(&mut bytes);
    format!(
        "rushwind-mqtt-{}",
        bytes
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>()
    )
}

/// MQTT topic-filter matching: `+` matches one level, `#` matches the
/// remaining levels.
fn topic_matches(filter: &str, topic: &str) -> bool {
    let mut filter_levels = filter.split('/');
    let mut topic_levels = topic.split('/');
    loop {
        match (filter_levels.next(), topic_levels.next()) {
            (Some("#"), _) => return true,
            (Some("+"), Some(_)) => continue,
            (Some(f), Some(t)) if f == t => continue,
            (None, None) => return true,
            _ => return false,
        }
    }
}

impl Broker for MqttBroker {
    fn name(&self) -> &'static str {
        "mqtt"
    }

    fn connect(&self) -> BoxFuture<'_, Result<(), BrokerError>> {
        Box::pin(async move {
            // Retire the previous pump generation.
            if let Some(previous) = self.shutdown.lock().await.take() {
                previous.notify_waiters();
            }
            let generation = self.generation.fetch_add(1, Ordering::SeqCst) + 1;

            let (client, mut eventloop) = fresh_client(&self.mqtt_options);
            *self.client.lock().await = Some(client.clone());

            let shutdown = Arc::new(Notify::new());
            *self.shutdown.lock().await = Some(Arc::clone(&shutdown));

            let shared = Arc::clone(&self.shared);
            let generations = Arc::clone(&self.generation);
            tokio::spawn(async move {
                let mut backoff_secs = 1u64;
                loop {
                    if generations.load(Ordering::SeqCst) != generation {
                        return;
                    }
                    let reconnect = tokio::select! {
                        _ = shutdown.notified() => return,
                        _ = shared.rearm.notified() => false,
                        result = eventloop.poll() => match result {
                            Ok(event) => {
                                match event {
                                    rumqttc::Event::Incoming(rumqttc::Packet::ConnAck(_)) => {
                                        // Re-arm every live subscription —
                                        // resubscribe on connect.
                                        let filters = shared
                                            .subscriptions
                                            .lock()
                                            .await
                                            .keys()
                                            .cloned()
                                            .collect::<Vec<_>>();
                                        for filter in filters {
                                            let _ = client
                                                .subscribe(&filter, rumqttc::QoS::AtLeastOnce)
                                                .await;
                                        }
                                        *shared.connected.lock().await = true;
                                    }
                                    rumqttc::Event::Incoming(rumqttc::Packet::Publish(publish)) => {
                                        let topic = publish.topic.clone();
                                        let payload = publish.payload.to_vec();
                                        let handlers: Vec<Handler> = {
                                            let subs = shared.subscriptions.lock().await;
                                            subs.iter()
                                                .filter(|(filter, _)| {
                                                    topic_matches(filter, &topic)
                                                })
                                                .map(|(_, handler)| Arc::clone(handler))
                                                .collect()
                                        };
                                        for handler in handlers {
                                            let event = Event::new(
                                                topic.clone(),
                                                Message::from_payload(payload.clone()),
                                            );
                                            tokio::spawn(handler(event));
                                        }
                                    }
                                    _ => {}
                                }
                                false
                            }
                            Err(_) => {
                                *shared.connected.lock().await = false;
                                true
                            }
                        },
                    };
                    if reconnect {
                        // rumqttc's poll retries the connection on the
                        // next call; the backoff just spaces the polls.
                        tokio::time::sleep(Duration::from_secs(backoff_secs)).await;
                        backoff_secs = (backoff_secs * 2).min(30);
                    } else {
                        backoff_secs = 1;
                    }
                }
            });

            // The pump's first polls drive the connect; wait for the
            // CONNACK so callers can publish straight away.
            for _ in 0..100 {
                if *self.shared.connected.lock().await {
                    return Ok(());
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            Err(BrokerError::Failed("mqtt connect timed out".to_string()))
        })
    }

    fn disconnect(&self) -> BoxFuture<'_, Result<(), BrokerError>> {
        Box::pin(async move {
            *self.shared.connected.lock().await = false;
            if let Some(client) = self.client.lock().await.as_ref() {
                client
                    .disconnect()
                    .await
                    .map_err(|e| BrokerError::Failed(format!("mqtt disconnect: {e}")))?;
            }
            Ok(())
        })
    }

    fn publish<'a>(
        &'a self,
        topic: &'a str,
        message: Message,
    ) -> BoxFuture<'a, Result<(), BrokerError>> {
        Box::pin(async move {
            let Some(client) = self.client.lock().await.clone() else {
                return Err(BrokerError::NotConnected);
            };
            if !*self.shared.connected.lock().await {
                return Err(BrokerError::NotConnected);
            }
            // Publishes are body-only at QoS 1 without
            // retain; headers and metadata have no MQTT 3.1.1 carrier.
            client
                .publish(topic, rumqttc::QoS::AtLeastOnce, false, message.payload)
                .await
                .map_err(|e| BrokerError::Failed(format!("mqtt publish: {e}")))
        })
    }

    fn subscribe<'a>(
        &'a self,
        topic: &'a str,
        handler: Handler,
    ) -> BoxFuture<'a, Result<Box<dyn Subscriber>, BrokerError>> {
        Box::pin(async move {
            self.shared
                .subscriptions
                .lock()
                .await
                .insert(topic.to_string(), Arc::clone(&handler));
            if let (Some(client), true) = (
                self.client.lock().await.clone(),
                *self.shared.connected.lock().await,
            ) {
                client
                    .subscribe(topic, rumqttc::QoS::AtLeastOnce)
                    .await
                    .map_err(|e| BrokerError::Failed(format!("mqtt subscribe {topic}: {e}")))?;
            }
            self.shared.rearm.notify_waiters();
            Ok(Box::new(MqttSubscriber {
                topic: topic.to_string(),
                client: self.client.lock().await.clone(),
                shared: Arc::clone(&self.shared),
            }) as Box<dyn Subscriber>)
        })
    }
}

/// The MQTT subscription handle.
struct MqttSubscriber {
    topic: String,
    client: Option<rumqttc::AsyncClient>,
    shared: Arc<Shared>,
}

impl Subscriber for MqttSubscriber {
    fn topic(&self) -> &str {
        &self.topic
    }

    fn unsubscribe(&mut self) -> BoxFuture<'_, Result<(), BrokerError>> {
        Box::pin(async move {
            self.shared.subscriptions.lock().await.remove(&self.topic);
            if let (Some(client), true) = (self.client.clone(), *self.shared.connected.lock().await)
            {
                client.unsubscribe(&self.topic).await.map_err(|e| {
                    BrokerError::Failed(format!("mqtt unsubscribe {}: {e}", self.topic))
                })?;
            }
            Ok(())
        })
    }
}

impl Drop for MqttSubscriber {
    fn drop(&mut self) {
        if let Ok(mut subscriptions) = self.shared.subscriptions.try_lock() {
            subscriptions.remove(&self.topic);
        }
    }
}

/// A test helper: pairs an unbounded event channel with a handler
/// that forwards every delivery into it.
pub fn event_channel() -> (
    tokio::sync::mpsc::UnboundedReceiver<Event>,
    impl Fn(Event) -> BoxFuture<'static, Result<(), BrokerError>> + Send + Sync,
) {
    let (tx, rx) = mpsc::unbounded_channel::<Event>();
    let forward = move |event: Event| {
        let tx = tx.clone();
        Box::pin(async move {
            let _ = tx.send(event);
            Ok::<(), BrokerError>(())
        }) as BoxFuture<'static, Result<(), BrokerError>>
    };
    (rx, forward)
}
