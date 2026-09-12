//! MQTT consumer bridge for the RushWind lifecycle.
//!
//! [`MqttBridge`] subscribes to topics on an **external** broker (the
//! broker is infrastructure deployed out-of-process — the bridge never
//! embeds one) and pumps every received message into a session handler,
//! under the lifecycle's [`Server`] contract.
//!
//! # Shape
//!
//! This transport has **no session surface**: the bridge holds one
//! pre-trusted connection to the broker, whose credentials live in the
//! caller-supplied [`rumqttc::MqttOptions`] — there are no per-client
//! handshakes, so the session middleware chain (gates, admission policy)
//! has nothing to guard here. The security boundary is the broker
//! configuration, and message payloads are untrusted application input
//! (see `docs/threat-model.md`).
//!
//! # Connection semantics
//!
//! The handler is invoked **serially** on the pump loop — one message at a
//! time, in broker delivery order. That is deliberate backpressure: a slow
//! handler slows delivery instead of growing an unbounded queue; QoS 1
//! deliveries queue at the broker.
//!
//! Connection loss is not an error: the pump reconnects with a capped
//! exponential backoff, re-establishing every registered subscription on
//! each generation, forever — a broker restart must not end the
//! application. The stop signal always wins the race; when it fires, the
//! bridge returns `Cancelled` and the connection drops with the pump
//! future.
//!
//! `Server::stop` is a no-op (the connection lives inside the start
//! future and drops with it — the axum-style rule-1 case of the
//! architecture document).

#![forbid(unsafe_code)]
#![deny(missing_docs)]

use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use rumqttc::{AsyncClient, Incoming, MqttOptions};
use rushwind_transport::{Server, ServerError, ServerFuture, StopSignal};

/// Reconnect backoff floor.
const BACKOFF_BASE: Duration = Duration::from_millis(100);
/// Reconnect backoff ceiling.
const BACKOFF_CAP: Duration = Duration::from_secs(5);

/// One message delivered by the broker.
pub struct MqttMessage {
    /// The topic the message was published on.
    pub topic: String,
    /// The message payload bytes. Untrusted application input.
    pub payload: Vec<u8>,
}

/// Session-handler function type: receives each delivered message.
type MessageHandlerFn = Box<dyn Fn(MqttMessage) -> MessageFuture + Send + Sync>;

/// Message-handler future type.
type MessageFuture = Pin<Box<dyn Future<Output = ()> + Send>>;

/// Shared pump state, frozen at build time.
struct BridgeState {
    broker: SocketAddr,
    client_id: String,
    subscriptions: Vec<(String, rumqttc::QoS)>,
    handler: MessageHandlerFn,
}

/// A bridge from an external MQTT broker into a handler, under the
/// RushWind lifecycle.
pub struct MqttBridge {
    state: Arc<BridgeState>,
}

impl MqttBridge {
    /// Starts building a bridge to the broker at `broker`, identifying
    /// itself to it as `client_id`.
    pub fn builder(broker: SocketAddr, client_id: impl Into<String>) -> BridgeBuilder {
        BridgeBuilder {
            broker,
            client_id: client_id.into(),
            subscriptions: Vec::new(),
            handler: None,
        }
    }
}

/// Builder for [`MqttBridge`].
pub struct BridgeBuilder {
    broker: SocketAddr,
    client_id: String,
    subscriptions: Vec<(String, rumqttc::QoS)>,
    handler: Option<MessageHandlerFn>,
}

impl BridgeBuilder {
    /// Subscribes to `topic` at QoS 1 (at-least-once delivery).
    pub fn subscribe(self, topic: impl Into<String>) -> Self {
        self.subscribe_with_qos(topic, rumqttc::QoS::AtLeastOnce)
    }

    /// Subscribes to `topic` at an explicit QoS.
    pub fn subscribe_with_qos(mut self, topic: impl Into<String>, qos: rumqttc::QoS) -> Self {
        self.subscriptions.push((topic.into(), qos));
        self
    }

    /// Sets the handler invoked for every delivered message, serially, in
    /// broker delivery order. The handler runs until it returns or the
    /// lifecycle's stop signal fires — whichever comes first.
    pub fn session_handler<F, Fut>(mut self, handler: F) -> Self
    where
        F: Fn(MqttMessage) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        self.handler = Some(Box::new(move |message: MqttMessage| {
            Box::pin(handler(message)) as MessageFuture
        }));
        self
    }

    /// Freezes the builder into an [`MqttBridge`].
    ///
    /// Errors when no session handler was registered — there is no default
    /// message behavior.
    pub fn build(self) -> Result<MqttBridge, ServerError> {
        let handler = self
            .handler
            .ok_or_else(|| ServerError::Failed("mqtt bridge has no session handler".to_string()))?;
        Ok(MqttBridge {
            state: Arc::new(BridgeState {
                broker: self.broker,
                client_id: self.client_id,
                subscriptions: self.subscriptions,
                handler,
            }),
        })
    }
}

impl Server for MqttBridge {
    fn endpoint(&self) -> Result<String, ServerError> {
        Ok(format!("mqtt://{}", self.state.broker))
    }

    fn start(&self, stop: StopSignal) -> ServerFuture<'_> {
        let state = Arc::clone(&self.state);
        Box::pin(async move {
            let mut backoff = BACKOFF_BASE;
            loop {
                // Each generation is a fresh client/eventloop pair;
                // subscriptions are re-established per generation so a
                // broker restart cannot silently drop them.
                let options = MqttOptions::new(
                    state.client_id.clone(),
                    state.broker.ip().to_string(),
                    state.broker.port(),
                );
                let (client, mut eventloop) = AsyncClient::new(options, 16);
                for (topic, qos) in &state.subscriptions {
                    let _ = client.subscribe(topic, *qos).await;
                }

                let mut connected = false;
                loop {
                    let event = tokio::select! {
                        _ = stop.wait() => return Err(ServerError::Cancelled),
                        polled = eventloop.poll() => match polled {
                            Ok(event) => event,
                            Err(_connection_error) => break,
                        },
                    };
                    match event {
                        rumqttc::Event::Incoming(Incoming::ConnAck(_)) => {
                            // A real connection existed: the next failure
                            // starts from the base backoff again.
                            connected = true;
                            backoff = BACKOFF_BASE;
                        }
                        rumqttc::Event::Incoming(Incoming::Publish(publish)) => {
                            (state.handler)(MqttMessage {
                                topic: publish.topic,
                                payload: publish.payload.to_vec(),
                            })
                            .await;
                        }
                        _ => {}
                    }
                }

                // Connection lost (or never established): back off with
                // the stop signal always winning the race.
                if !connected {
                    backoff = (backoff * 2).min(BACKOFF_CAP);
                }
                tokio::select! {
                    _ = stop.wait() => return Err(ServerError::Cancelled),
                    _ = tokio::time::sleep(backoff) => {}
                }
            }
        })
    }

    fn stop(&self) -> ServerFuture<'_> {
        // The broker connection lives inside the start future and drops
        // with it; the bridge holds no long-lived resource of its own.
        Box::pin(async { Ok(()) })
    }
}
