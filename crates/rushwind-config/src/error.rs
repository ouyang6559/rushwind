//! The error taxonomy for configuration sources.

use std::fmt;

/// A configuration-source failure.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum ConfigError {
    /// The source could not complete the operation (I/O failure, backend
    /// down, malformed request). The joined message carries the cause.
    Failed(String),
    /// No fallback source resolved the key — every source answered
    /// "absent" cleanly. Carries the key.
    Unresolved(String),
    /// The source offers no watch capability. The default
    /// [`crate::Source`] capability methods return this; it is the
    /// explicit capability marker, and
    /// [`crate::FallbackSource`] skips sub-sources answering with it.
    NotWatchable,
}

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Failed(msg) => write!(f, "config operation failed: {msg}"),
            Self::Unresolved(key) => write!(f, "no source could resolve key {key:?}"),
            Self::NotWatchable => write!(f, "source does not support watching"),
        }
    }
}

impl std::error::Error for ConfigError {}
