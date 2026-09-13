//! The bridge from [`Authenticator`] engines onto the session middleware's
//! gate chain — the enforcement point `docs/session-middleware.md` and
//! `docs/threat-model.md` designate for session transports, where a
//! credential-less handshake must be refused before any session state is
//! allocated.
//!
//! [`AuthenticationGate`] runs the wrapped authenticator over each
//! handshake's header snapshot. Authentication succeeds →
//! [`GateVerdict::Continue`]; any [`AuthnError`] → a rejection whose
//! status is the error's taxonomy status and whose reason is the error's
//! stable code (the message stays server-side).
//!
//! # The gate contract applies to the engine
//!
//! Gates are synchronous and CPU-bound by contract: only
//! local-evidence engines belong here — static key sets, signature
//! verification, in-process stores. A validator or resolver callback that
//! performs IO (a database lookup, a remote introspection) violates the
//! gate contract; front it with an in-process cache, or leave it out of
//! gate chains.

use std::sync::Arc;

use rushwind_transport::{GateVerdict, Handshake, HandshakeGate, Rejection};

use crate::authenticator::Authenticator;

/// A gate that authenticates handshakes through one [`Authenticator`]
/// engine.
pub struct AuthenticationGate {
    authenticator: Arc<dyn Authenticator>,
}

impl AuthenticationGate {
    /// Wraps an authenticator engine as a session-handshake gate.
    pub fn new(authenticator: Arc<dyn Authenticator>) -> Self {
        Self { authenticator }
    }
}

impl HandshakeGate for AuthenticationGate {
    fn name(&self) -> String {
        "authn".to_string()
    }

    fn inspect(&self, handshake: &Handshake) -> GateVerdict {
        match self.authenticator.authenticate(&handshake.headers) {
            Ok(_) => GateVerdict::Continue,
            Err(err) => GateVerdict::Reject(Rejection {
                status: err.status(),
                reason: err.code().to_string(),
            }),
        }
    }
}
