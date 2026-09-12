//! Contracts for RushWind transports.
//!
//! This crate intentionally contains **no implementations** — only the
//! [`Server`] lifecycle contract, the [`StopSignal`] cooperative shutdown
//! primitive, the [`Instance`] service description model, and the
//! [`ServerError`] error taxonomy. Concrete transports live in sibling
//! `rushwind-transport-*` crates and implement [`Server`] against a specific
//! protocol stack.
//!
//! Design notes — including the rationale for the boxed-future signatures on
//! [`Server`] — are documented in `docs/architecture.md` in the repository
//! root.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

pub mod error;
pub mod instance;
pub mod server;
pub mod signal;

pub use error::ServerError;
pub use instance::Instance;
pub use server::{Server, ServerFuture};
pub use signal::StopSignal;
