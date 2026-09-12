//! Error taxonomy for transport lifecycles.

use std::fmt;

/// Errors produced by, or about, a [`crate::Server`] lifecycle method.
///
/// The orchestrator (`rushwind-core`) aggregates these into a single terminal
/// outcome: the [`ServerError::Cancelled`] variant marks a *cooperative*
/// shutdown — the normal exit of every well-behaved server — and is filtered
/// out; every other variant is propagated as the application's final error.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum ServerError {
    /// The server observed the [`crate::StopSignal`] and cooperatively
    /// returned. Never reported as an application error.
    Cancelled,
    /// A bounded lifecycle phase exceeded its deadline and was abandoned.
    Timeout,
    /// The transport failed to perform its duty (bind, accept, teardown…).
    Failed(String),
    /// The server implementation panicked inside a lifecycle method. The
    /// orchestrator isolated the panic; this is the propagated record of it.
    Panicked(String),
}

impl fmt::Display for ServerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Cancelled => write!(f, "server cancelled by shutdown signal"),
            Self::Timeout => write!(f, "lifecycle phase exceeded its deadline"),
            Self::Failed(msg) => write!(f, "server failed: {msg}"),
            Self::Panicked(msg) => write!(f, "server panicked: {msg}"),
        }
    }
}

impl std::error::Error for ServerError {}
