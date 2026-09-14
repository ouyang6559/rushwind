//! Distributed transaction contract for RushWind, extracted from the
//! Go predecessor `go-wind-plugins/transaction`: the minimal surface
//! every transaction-manager engine shares, with the pattern
//! operations (saga, tcc, msg, xa) left to the concrete engine types.
//!
//! # The Go shapes, translated
//!
//! The Go contract is a single interface, [`TransactionClient`],
//! holding only [`TransactionClient::close`] — pattern operations
//! have incompatible signatures across engines and stay on the
//! concrete clients, and the same holds here:
//! `rushwind-transaction-dtm` exposes saga/tcc/msg/xa as inherent
//! methods on its builder types. The Go plugin ships one engine
//! (`dtm`, over the dtm-labs Go SDK); the Rust engine speaks DTM's
//! HTTP protocol directly over reqwest.
//!
//! Go's panic sites become typed errors: the DTM SDK panics when a
//! TCC/XA transaction grows past 99 branches —
//! [`TransactionError::TooManyBranches`] — and panics on payload
//! marshal failure — [`TransactionError::Encode`]. (The SDK's other
//! panic, a branch id past 20 characters, needs a nested root id
//! the Go wrapper never constructs, so it has no Rust shape.)

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
    /// A participating branch or endpoint reported FAILURE — the
    /// Go `dtmcli.ErrFailure`.
    Failure(String),
    /// A branch or endpoint reported ONGOING (or HTTP 425 Too
    /// Early, the Go `StatusTooEarly`) — the Go `dtmcli.ErrOngoing`;
    /// the operation's outcome is not yet decided.
    Ongoing(String),
    /// The HTTP exchange itself failed (connect, timeout, reset).
    Request(String),
    /// A payload did not serialize — the Go `MustMarshalString`
    /// panic.
    Encode(String),
    /// A TCC/XA transaction tried to register more than 99 branches
    /// — the Go branch-id panic.
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

/// The transaction-manager contract — the Go `transaction.Client`.
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
