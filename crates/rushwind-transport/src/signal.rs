//! Cooperative shutdown signalling.

use tokio_util::sync::CancellationToken;

/// A cloneable, one-way "please stop" signal shared between the orchestrator
/// and one server.
///
/// The orchestrator owns the source; every server receives a clone and is
/// expected to race [`StopSignal::wait`] against its own work, returning
/// `Err(ServerError::Cancelled)` as soon as the signal fires. Signalling is
/// idempotent — repeated calls have no additional effect.
///
/// This is a thin wrapper around `tokio_util::sync::CancellationToken` so the
/// contract surface stays behind a RushWind type, independent of any
/// particular primitive.
#[derive(Clone, Default)]
pub struct StopSignal {
    inner: CancellationToken,
}

impl StopSignal {
    /// Creates a new, un-signalled stop signal.
    pub fn new() -> Self {
        Self {
            inner: CancellationToken::new(),
        }
    }

    /// Fires the signal. Idempotent; wakes every current and future waiter.
    pub fn signal(&self) {
        self.inner.cancel();
    }

    /// Returns `true` once the signal has fired.
    pub fn is_signalled(&self) -> bool {
        self.inner.is_cancelled()
    }

    /// Resolves once the signal has fired. A server awaiting this should
    /// promptly return `Err(ServerError::Cancelled)`.
    pub async fn wait(&self) {
        self.inner.cancelled().await;
    }
}
