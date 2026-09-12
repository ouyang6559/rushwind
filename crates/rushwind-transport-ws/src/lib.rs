//! WebSocket session routes for axum-based RushWind servers.
//!
//! [`WsRoute`] builds an axum [`MethodRouter`] that mounts a
//! session-handshake gate chain and a listener-scoped admission policy in
//! front of a session handler, and returns the route together with a
//! **session-shutdown bus**.
//!
//! # Composition
//!
//! Mount the returned router into an [`axum::Router`] with
//! `Router::route`, and serve that router through `AxumServer` (crate
//! `rushwind-transport-axum`). Register the bus with
//! `AxumServer::with_aux_shutdown` so the session handlers wind down when
//! the lifecycle's shutdown path runs; an unregistered bus means the
//! sessions terminate only when the process exits.
//!
//! # What the gates see
//!
//! Gates run at HTTP-request time, before the upgrade: they see the
//! request's header pairs and nothing else. The session socket itself —
//! including frames — is invisible to the gate chain; frame-level policy
//! is a future tier (see `docs/session-middleware.md`).
//!
//! The session handler receives axum's native
//! [`WebSocket`](axum::extract::ws::WebSocket). There is deliberately no
//! cross-transport session abstraction: the chain (gates, policy, bus) is
//! uniform across session transports, the session type stays native.
//!
//! # Admission policy
//!
//! [`SessionPolicy`] caps simultaneously live sessions per route. Handshakes
//! over the cap are refused before the upgrade; sessions that raced past
//! the pre-check are closed immediately at session start.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use axum::extract::ws::{WebSocket, WebSocketUpgrade};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, MethodRouter};
use rushwind_transport::{
    GateChain, Handshake, HandshakeGate, Rejection, ServerError, SessionPolicy, StopSignal,
};

/// Session-handler function type: receives the live native socket.
type SessionHandlerFn = Box<dyn Fn(WebSocket) -> SessionFuture + Send + Sync>;

/// Session-handler future type.
type SessionFuture = Pin<Box<dyn Future<Output = ()> + Send>>;

/// Shared per-route state: the gate chain, admission policy, session
/// handler, live-session counter, and session-shutdown bus.
struct RouteState {
    gates: GateChain,
    policy: SessionPolicy,
    session: SessionHandlerFn,
    live: Arc<AtomicUsize>,
    bus: StopSignal,
}

/// A drop guard for the live-session counter. The counted variant
/// decrements on drop; the unbounded variant is inert, so an uncapped
/// route never decrements a counter it never incremented.
enum SessionGuard {
    /// The route has no session cap; no counter bookkeeping.
    Unbounded,
    /// The route counts live sessions; the guard owns the shared counter
    /// handle and decrements it when dropped.
    Counted(Arc<AtomicUsize>),
}

impl Drop for SessionGuard {
    fn drop(&mut self) {
        if let SessionGuard::Counted(live) = self {
            live.fetch_sub(1, Ordering::Relaxed);
        }
    }
}

impl RouteState {
    /// Whether the route is currently at its configured session cap.
    /// Advisory only — the authoritative check happens atomically inside
    /// [`RouteState::admit_session`].
    fn at_capacity(&self) -> bool {
        self.policy
            .get_max_concurrent_sessions()
            .is_some_and(|cap| self.live.load(Ordering::Relaxed) >= cap)
    }

    /// Attempts to register a live session, returning a guard that
    /// decrements the counter on drop, or `None` when the cap is reached.
    fn admit_session(&self) -> Option<SessionGuard> {
        match self.policy.get_max_concurrent_sessions() {
            None => Some(SessionGuard::Unbounded),
            Some(cap) => {
                let prev = self.live.fetch_add(1, Ordering::Relaxed);
                if prev >= cap {
                    self.live.fetch_sub(1, Ordering::Relaxed);
                    None
                } else {
                    Some(SessionGuard::Counted(Arc::clone(&self.live)))
                }
            }
        }
    }
}

/// Maps a gate rejection to an HTTP response.
fn rejection_response(rej: Rejection) -> Response {
    let status = StatusCode::from_u16(rej.status).unwrap_or(StatusCode::FORBIDDEN);
    (status, rej.reason).into_response()
}

/// Builder for a gated WebSocket session route.
pub struct WsRoute {
    gates: GateChain,
    policy: SessionPolicy,
    session: Option<SessionHandlerFn>,
}

impl WsRoute {
    /// Starts building an empty WebSocket session route. Gates default to
    /// none (every handshake accepted) and the admission policy to
    /// uncapped — mount gates explicitly on auth-sensitive routes.
    pub fn new() -> Self {
        Self {
            gates: GateChain::new(),
            policy: SessionPolicy::new(),
            session: None,
        }
    }

    /// Appends a handshake gate. Gates run in registration order; the
    /// first rejection refuses the handshake.
    pub fn gate(mut self, gate: Arc<dyn HandshakeGate>) -> Self {
        self.gates = self.gates.gate(gate);
        self
    }

    /// Caps simultaneously live sessions on this route.
    pub fn max_concurrent_sessions(mut self, n: usize) -> Self {
        self.policy = self.policy.max_concurrent_sessions(n);
        self
    }

    /// Sets the session handler invoked for every accepted handshake. The
    /// handler runs until it returns, the socket closes, or the
    /// session-shutdown bus fires — whichever comes first.
    pub fn session_handler<F, Fut>(mut self, handler: F) -> Self
    where
        F: Fn(WebSocket) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        self.session = Some(Box::new(move |socket: WebSocket| {
            Box::pin(handler(socket)) as SessionFuture
        }));
        self
    }

    /// Builds the route.
    ///
    /// Returns the mountable [`MethodRouter`] together with the route's
    /// **session-shutdown bus**. Register the bus with
    /// `AxumServer::with_aux_shutdown` (crate `rushwind-transport-axum`)
    /// so the route's sessions terminate when the lifecycle's shutdown
    /// path runs; an unregistered bus means the sessions terminate only
    /// when the process exits.
    ///
    /// Errors when no session handler was registered — there is no
    /// default session behavior.
    pub fn build(self) -> Result<(MethodRouter, StopSignal), ServerError> {
        let session = self
            .session
            .ok_or_else(|| ServerError::Failed("ws route has no session handler".to_string()))?;
        let bus = StopSignal::new();
        let state = Arc::new(RouteState {
            gates: self.gates,
            policy: self.policy,
            session,
            live: Arc::new(AtomicUsize::new(0)),
            bus: bus.clone(),
        });
        let handler_state = Arc::clone(&state);
        let handler = move |upgrade: WebSocketUpgrade, headers: HeaderMap| async move {
            let handshake = Handshake {
                headers: headers
                    .iter()
                    .map(|(name, value)| {
                        (
                            name.as_str().to_string(),
                            value.to_str().unwrap_or_default().to_string(),
                        )
                    })
                    .collect(),
                remote: None,
            };
            if let Err(rej) = handler_state.gates.evaluate(&handshake) {
                return rejection_response(rej);
            }
            if handler_state.at_capacity() {
                return (StatusCode::TOO_MANY_REQUESTS, "session capacity reached").into_response();
            }
            let callback_state = Arc::clone(&handler_state);
            upgrade
                .on_upgrade(move |socket| {
                    let state = callback_state;
                    async move {
                        let _guard = match state.admit_session() {
                            Some(g) => g,
                            None => return,
                        };
                        let session_fut = (state.session)(socket);
                        tokio::select! {
                            _ = state.bus.wait() => {}
                            _ = session_fut => {}
                        }
                    }
                })
                .into_response()
        };
        Ok((get(handler), bus))
    }
}

impl Default for WsRoute {
    fn default() -> Self {
        Self::new()
    }
}
