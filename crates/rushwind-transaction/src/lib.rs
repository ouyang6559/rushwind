//! Distributed transaction contract for RushWind: the minimal surface
//! every transaction-manager engine shares, with the pattern
//! operations (saga, tcc, msg, xa) left to the concrete engine types.
//!
//! # The core shapes
//!
//! The contract is a single interface, [`TransactionClient`],
//! holding only [`TransactionClient::close`] — pattern operations
//! have incompatible signatures across engines and stay on the
//! concrete clients:
//! `rushwind-transaction-dtm` exposes saga/tcc/msg/xa as inherent
//! methods on its builder types. The `dtm` engine speaks DTM's
//! HTTP protocol directly over reqwest.
//!
//! Failure modes that reference implementations panic on surface as
//! typed errors: a TCC/XA transaction growing past 99 branches —
//! [`TransactionError::TooManyBranches`] — and payload
//! marshal failure — [`TransactionError::Encode`].

#![forbid(unsafe_code)]
#![deny(missing_docs)]

use std::future::Future;
use std::pin::Pin;

/// Future type used across the transaction contract.
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Errors surfaced by transaction engines.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum TransactionError {
    /// The DTM server rejected the operation — a non-success status
    /// or a body reporting FAILURE.
    Dtm(String),
    /// A participating branch or endpoint reported FAILURE.
    Failure(String),
    /// A branch or endpoint reported ONGOING (or HTTP 425 Too
    /// Early) — the operation's outcome is not yet decided.
    Ongoing(String),
    /// The HTTP exchange itself failed (connect, timeout, reset).
    Request(String),
    /// A payload did not serialize.
    Encode(String),
    /// A TCC/XA transaction tried to register more than 99 branches.
    TooManyBranches,
}

impl std::fmt::Display for TransactionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Dtm(msg) => write!(f, "transaction: dtm server error: {msg}"),
            Self::Failure(msg) => write!(f, "transaction: FAILURE: {msg}"),
            Self::Ongoing(msg) => write!(f, "transaction: ONGOING: {msg}"),
            Self::Request(msg) => write!(f, "transaction: request failed: {msg}"),
            Self::Encode(msg) => write!(f, "transaction: payload encode: {msg}"),
            Self::TooManyBranches => {
                write!(f, "transaction: branch id is larger than 99")
            }
        }
    }
}

impl std::error::Error for TransactionError {}

/// The transaction-manager contract.
/// The only universally shared operation is
/// [`TransactionClient::close`]; pattern operations (saga, tcc, msg,
/// xa) live on the concrete engine types. Engines must be callable
/// through shared references (`&self`).
pub trait TransactionClient: Send + Sync {
    /// Releases the client's resources. The dtm engine holds no
    /// connection pool (every call is a fresh HTTP exchange), so
    /// its close is a no-op — kept for the contract.
    fn close<'a>(&'a self) -> BoxFuture<'a, Result<(), TransactionError>>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn error_displays_readably() {
        assert_eq!(
            TransactionError::Dtm("boom".to_string()).to_string(),
            "transaction: dtm server error: boom"
        );
        assert_eq!(
            TransactionError::TooManyBranches.to_string(),
            "transaction: branch id is larger than 99"
        );
    }
}
