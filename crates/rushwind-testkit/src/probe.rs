//! Probe servers used by the conformance suite's orchestrator-semantics
//! half. These are test doubles, not a reference production server: each
//! one scripts one lifecycle behaviour and records its teardown in an
//! observable event log.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use rushwind_transport::{Server, ServerError, ServerFuture, StopSignal};
use tokio::time::sleep;

/// Behaviour script of a [`ProbeServer`].
#[derive(Clone, Copy, Debug)]
pub enum ProbeKind {
    /// Well-behaved: runs until signalled, then reports cooperative
    /// cancellation; teardown records itself.
    Normal,
    /// Returns a `Failed` error after the given delay.
    FailAfter(u64),
    /// Self-exits successfully after the given delay (must trigger cascade).
    ExitAfter(u64),
    /// Panics after the given delay; the orchestrator must isolate it and
    /// still tear down its siblings.
    PanicAfter(u64),
    /// Ignores the stop signal entirely; the orchestrator must abandon it
    /// at the drain deadline instead of blocking forever.
    IgnoreSignal,
    /// Start behaves normally, but teardown hangs; the deadline must cut it
    /// off and report `Timeout`.
    HangStop,
    /// Start behaves normally; teardown records itself after a delay.
    SlowStop(u64),
}

/// A scripted [`Server`] whose lifecycle actions are observable through an
/// event log.
pub struct ProbeServer {
    kind: ProbeKind,
    log: Arc<Mutex<Vec<&'static str>>>,
}

impl ProbeServer {
    /// Creates a probe with the given behaviour script and a fresh event
    /// log.
    pub fn new(kind: ProbeKind) -> Self {
        Self::with_log(kind, Arc::new(Mutex::new(Vec::new())))
    }

    /// Creates a probe that records its teardown into a caller-supplied
    /// event log, shared with the suite's hook recorders so that
    /// cross-component ordering (hook vs. teardown) is observable.
    pub fn with_log(kind: ProbeKind, log: Arc<Mutex<Vec<&'static str>>>) -> Self {
        Self { kind, log }
    }

    /// The event log; contains `"stop:invoked"` once teardown has run.
    ///
    /// Keep a handle **before** handing the probe to the builder — the
    /// builder consumes the `Arc`.
    pub fn log(&self) -> Arc<Mutex<Vec<&'static str>>> {
        Arc::clone(&self.log)
    }
}

impl Server for ProbeServer {
    fn endpoint(&self) -> Result<String, ServerError> {
        Ok(format!("probe://{:?}", self.kind))
    }

    fn start(&self, stop: StopSignal) -> ServerFuture<'_> {
        let kind = self.kind;
        Box::pin(async move {
            match kind {
                ProbeKind::Normal | ProbeKind::HangStop | ProbeKind::SlowStop(_) => {
                    stop.wait().await;
                    Err(ServerError::Cancelled)
                }
                ProbeKind::FailAfter(ms) => {
                    sleep(Duration::from_millis(ms)).await;
                    Err(ServerError::Failed("probe failure".to_string()))
                }
                ProbeKind::ExitAfter(ms) => {
                    sleep(Duration::from_millis(ms)).await;
                    Ok(())
                }
                ProbeKind::PanicAfter(ms) => {
                    sleep(Duration::from_millis(ms)).await;
                    panic!("probe panic");
                }
                ProbeKind::IgnoreSignal => {
                    sleep(Duration::from_secs(600)).await;
                    Err(ServerError::Cancelled)
                }
            }
        })
    }

    fn stop(&self) -> ServerFuture<'_> {
        let kind = self.kind;
        let log = Arc::clone(&self.log);
        Box::pin(async move {
            match kind {
                ProbeKind::HangStop => {
                    sleep(Duration::from_secs(600)).await;
                    Ok(())
                }
                ProbeKind::SlowStop(ms) => {
                    sleep(Duration::from_millis(ms)).await;
                    log.lock().expect("probe log poisoned").push("stop:invoked");
                    Ok(())
                }
                _ => {
                    log.lock().expect("probe log poisoned").push("stop:invoked");
                    Ok(())
                }
            }
        })
    }
}
