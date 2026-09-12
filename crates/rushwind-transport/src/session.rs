//! Session-handshake middleware contracts.
//!
//! Session-family transports (websocket now; QUIC/http3/webtransport and
//! the MQTT consumer bridge in P2) accept long-lived connections whose
//! handshake happens **before** any authentication context exists. The
//! contracts here give every such transport a uniform, ordered gate chain
//! plus a listener-scoped admission policy — closing the gap documented in
//! the threat model: HTTP-family middleware (tower) never sees a session
//! handshake, so the gate chain is the only authn/authz point for these
//! transports.
//!
//! Design rationale, the per-transport enforcement matrix, and the
//! session-shutdown bus live in `docs/session-middleware.md` in the
//! repository root.

use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

/// A transport-neutral snapshot of one session handshake, as visible to
/// gate middleware **before** any session state is allocated.
///
/// Fields carry only what the concrete transport can reveal:
/// HTTP-family transports fill `headers` and no `remote`; raw-socket
/// transports (P2) will fill `remote` and no `headers`. Gates must treat
/// absent evidence as absence — never guess.
#[derive(Debug, Clone, Default)]
pub struct Handshake {
    /// Header pairs as presented by the client, when the transport is
    /// HTTP-family. Empty otherwise.
    pub headers: Vec<(String, String)>,
    /// The peer address, when the transport is a raw socket. `None`
    /// otherwise; HTTP-family listeners do not expose it until
    /// connect-info plumbing lands.
    pub remote: Option<SocketAddr>,
}

/// A rejection handed back by a gate: an HTTP-family transport maps
/// `status` to an HTTP status code; raw transports map it to their
/// protocol's refusal.
#[derive(Debug, Clone)]
pub struct Rejection {
    /// Status code, interpreted per transport.
    pub status: u16,
    /// Human-readable reason, for diagnostics.
    pub reason: String,
}

/// The verdict of one gate on one handshake.
#[derive(Debug, Clone)]
pub enum GateVerdict {
    /// No objection; proceed to the next gate.
    Continue,
    /// Refuse the handshake. First rejection wins and the chain stops.
    Reject(Rejection),
}

/// One gate in a session-handshake chain.
///
/// Gates are **synchronous and CPU-bound by contract**: a gate verifies
/// local evidence (signatures, allowlists, header shape) and must never
/// perform IO. Verdicts that need remote evidence (token introspection,
/// revocation checks) belong behind an in-process cache the gate reads —
/// or in a future async gate tier.
pub trait HandshakeGate: Send + Sync {
    /// The gate's name, for diagnostics and rejection records.
    fn name(&self) -> String;
    /// Inspect one handshake snapshot and return a verdict.
    fn inspect(&self, handshake: &Handshake) -> GateVerdict;
}

/// An ordered chain of gates, evaluated in registration order.
///
/// Built via [`GateChain::gate`] on the route builder of the concrete
/// transport; evaluation semantics are transport-independent.
pub struct GateChain {
    gates: Vec<Arc<dyn HandshakeGate>>,
}

impl Default for GateChain {
    fn default() -> Self {
        Self::new()
    }
}

impl GateChain {
    /// Creates an empty chain. An empty chain accepts every handshake —
    /// mount gates explicitly on auth-sensitive routes.
    pub fn new() -> Self {
        Self { gates: Vec::new() }
    }

    /// Appends a gate. Registration order is evaluation order.
    pub fn gate(mut self, gate: Arc<dyn HandshakeGate>) -> Self {
        self.gates.push(gate);
        self
    }

    /// Evaluates the chain in registration order. The first rejection
    /// wins and short-circuits; an empty chain accepts everything.
    pub fn evaluate(&self, handshake: &Handshake) -> Result<(), Rejection> {
        for gate in &self.gates {
            if let GateVerdict::Reject(rej) = gate.inspect(handshake) {
                return Err(rej);
            }
        }
        Ok(())
    }
}

/// Listener-scoped admission policy for a session route.
///
/// This is the pre-auth budget of the threat model: quotas that bound
/// resource consumption **before and during** the handshake, deliberately
/// separate from any post-auth application-level limiting. Fields exist
/// only for the controls a concrete transport can actually enforce; see
/// `docs/session-middleware.md` for the per-transport enforcement matrix.
#[derive(Default)]
pub struct SessionPolicy {
    max_concurrent_sessions: Option<usize>,
    handshake_timeout: Option<Duration>,
}

impl SessionPolicy {
    /// Creates a policy with no limits.
    pub fn new() -> Self {
        Self::default()
    }

    /// Caps the number of simultaneously live sessions on this route.
    /// Excess handshakes are refused at the pre-admission check and, for
    /// those that raced past it, closed immediately at session start.
    pub fn max_concurrent_sessions(mut self, n: usize) -> Self {
        self.max_concurrent_sessions = Some(n);
        self
    }

    /// The configured simultaneous-session cap, if any.
    pub fn get_max_concurrent_sessions(&self) -> Option<usize> {
        self.max_concurrent_sessions
    }

    /// Bounds the wall-clock budget a single handshake may consume.
    /// Transports with an observable handshake phase enforce this by
    /// racing the handshake future against the deadline and dropping it
    /// on expiry.
    pub fn handshake_timeout(mut self, d: Duration) -> Self {
        self.handshake_timeout = Some(d);
        self
    }

    /// The configured handshake deadline, if any.
    pub fn get_handshake_timeout(&self) -> Option<Duration> {
        self.handshake_timeout
    }
}

/// The shared counter behind every session cap, owned by one listener.
///
/// Admission is a single atomic step: the counter increments and the cap
/// is checked together, so racing handshakes cannot both claim the last
/// slot. Guards decrement on drop — including drop by task abort — so a
/// cap can never leak slots.
pub struct SessionCounter {
    live: Arc<AtomicUsize>,
}

impl Default for SessionCounter {
    fn default() -> Self {
        Self::new()
    }
}

impl SessionCounter {
    /// Creates a counter at zero.
    pub fn new() -> Self {
        Self {
            live: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// Advisory cap check without admission. Transport pre-checks use
    /// this where a cheap early refusal is possible; the authoritative
    /// check is [`SessionCounter::admit`].
    pub fn at_cap(&self, cap: Option<usize>) -> bool {
        cap.is_some_and(|cap| self.live.load(Ordering::Relaxed) >= cap)
    }

    /// Attempts admission under `cap`, returning a guard that decrements
    /// on drop, or `None` when the cap is reached. `None` for `cap` means
    /// uncapped: admission always succeeds and the guard is inert — an
    /// uncapped route never decrements a counter it never incremented.
    pub fn admit(&self, cap: Option<usize>) -> Option<SessionCounterGuard> {
        let live = match cap {
            None => None,
            Some(cap) => {
                let prev = self.live.fetch_add(1, Ordering::Relaxed);
                if prev >= cap {
                    self.live.fetch_sub(1, Ordering::Relaxed);
                    return None;
                }
                Some(Arc::clone(&self.live))
            }
        };
        Some(SessionCounterGuard { live })
    }
}

/// The admission guard returned by [`SessionCounter::admit`]. The counted
/// variant decrements the counter on drop; the uncapped variant is inert.
pub struct SessionCounterGuard {
    live: Option<Arc<AtomicUsize>>,
}

impl Drop for SessionCounterGuard {
    fn drop(&mut self) {
        if let Some(live) = &self.live {
            live.fetch_sub(1, Ordering::Relaxed);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    struct Recorder {
        label: &'static str,
        verdict: GateVerdict,
        log: Arc<Mutex<Vec<String>>>,
    }

    impl HandshakeGate for Recorder {
        fn name(&self) -> String {
            self.label.to_string()
        }
        fn inspect(&self, _handshake: &Handshake) -> GateVerdict {
            self.log
                .lock()
                .expect("log poisoned")
                .push(self.label.to_string());
            self.verdict.clone()
        }
    }

    fn recorder(
        label: &'static str,
        verdict: GateVerdict,
        log: &Arc<Mutex<Vec<String>>>,
    ) -> Arc<Recorder> {
        Arc::new(Recorder {
            label,
            verdict,
            log: Arc::clone(log),
        })
    }

    #[test]
    fn empty_chain_accepts_everything() {
        let chain = GateChain::new();
        assert!(chain.evaluate(&Handshake::default()).is_ok());
    }

    #[test]
    fn first_rejection_short_circuits_in_registration_order() {
        let log: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
        let chain = GateChain::new()
            .gate(recorder("a", GateVerdict::Continue, &log))
            .gate(recorder(
                "b",
                GateVerdict::Reject(Rejection {
                    status: 403,
                    reason: "denied".to_string(),
                }),
                &log,
            ))
            .gate(recorder("c", GateVerdict::Continue, &log));
        let verdict = chain.evaluate(&Handshake::default());
        assert!(verdict.is_err());
        assert_eq!(verdict.unwrap_err().status, 403);
        assert_eq!(*log.lock().expect("log poisoned"), vec!["a", "b"]);
    }

    #[test]
    fn all_continue_accepts_and_every_gate_runs() {
        let log: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
        let chain = GateChain::new()
            .gate(recorder("a", GateVerdict::Continue, &log))
            .gate(recorder("b", GateVerdict::Continue, &log))
            .gate(recorder("c", GateVerdict::Continue, &log));
        assert!(chain.evaluate(&Handshake::default()).is_ok());
        assert_eq!(*log.lock().expect("log poisoned"), vec!["a", "b", "c"]);
    }

    #[test]
    fn counter_admits_to_cap_then_refuses() {
        let counter = SessionCounter::new();
        // Held for the whole test: while the guard lives, the cap holds.
        let _held = counter
            .admit(Some(1))
            .expect("first admission must succeed");
        assert!(counter.admit(Some(1)).is_none());
        assert!(counter.at_cap(Some(1)));
    }

    #[test]
    fn counter_guard_drop_frees_slot() {
        let counter = SessionCounter::new();
        {
            let _guard = counter.admit(Some(1));
            assert!(_guard.is_some());
        }
        assert!(!counter.at_cap(Some(1)));
        assert!(counter.admit(Some(1)).is_some());
    }

    #[test]
    fn counter_uncapped_never_counts() {
        let counter = SessionCounter::new();
        for _ in 0..8 {
            assert!(counter.admit(None).is_some());
        }
        assert!(!counter.at_cap(None));
    }
}
