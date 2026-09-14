//! The error taxonomy for the script-engine domain.

use std::fmt;

/// A script-engine or script-source failure.
///
/// The two sentinel variants mirror the Go package's sentinel errors
/// (`ErrCapabilityNotSupported`, `ErrQuotaExceeded`), which Go callers
/// discriminate with `errors.Is`; here the variant itself is the
/// discrimination. Everything else rides in the message, the
/// `ConfigError`/`EncodingError` shape.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum ScriptError {
    /// The operation could not complete (I/O failure, engine refused,
    /// pool closed, unregistered factory). The message carries the cause.
    Failed(String),
    /// The engine does not implement the requested optional capability.
    ///
    /// The default [`crate::ScriptSource::watch`] returns this as the
    /// runtime form of Go's `Watcher` interface assertion, and the
    /// [`crate::MultiSource`] watch delegation skips sub-sources answering
    /// with it.
    CapabilityNotSupported,
    /// A synchronous execution exceeded its configured [`crate::Quota`]
    /// budget and was interrupted mid-run.
    QuotaExceeded,
}

impl fmt::Display for ScriptError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Failed(msg) => write!(f, "script engine: operation failed: {msg}"),
            Self::CapabilityNotSupported => write!(
                f,
                "script engine: capability not supported by this engine type"
            ),
            Self::QuotaExceeded => write!(f, "script engine: execution quota exceeded"),
        }
    }
}

impl std::error::Error for ScriptError {}
