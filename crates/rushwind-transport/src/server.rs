//! The server lifecycle contract.

use std::future::Future;
use std::pin::Pin;

use crate::error::ServerError;
use crate::signal::StopSignal;

/// The future type returned by lifecycle methods on [`Server`].
///
/// The lifetime binds the future to the borrowed server, letting the
/// orchestrator run servers concurrently through borrowed futures without
/// requiring `'static` ownership or task spawning. See
/// `docs/architecture.md` for the rationale.
pub type ServerFuture<'a> = Pin<Box<dyn Future<Output = Result<(), ServerError>> + Send + 'a>>;

/// The one contract every RushWind transport implements.
///
/// Implementations are expected to:
///
/// - Bind and listen in [`Server::start`], run their accept or session loop
///   **racing [`StopSignal::wait`]**, and return
///   `Err(ServerError::Cancelled)` as soon as the signal fires.
/// - Release listeners, sockets and session state in [`Server::stop`]; the
///   orchestrator bounds this call with a deadline.
///
/// # Conformance
///
/// Adapter crates assert the full lifecycle contract by invoking
/// `rushwind_testkit::rushwind_conformance_suite!` from their integration-test
/// target; a transport is only conformant when the entire suite passes.
pub trait Server: Send + Sync {
    /// Returns the endpoint this server is (or will be) reachable on, in
    /// `scheme://host:port` form. Called before start.
    fn endpoint(&self) -> Result<String, ServerError>;

    /// Runs the server until the provided stop signal fires or the server
    /// self-exits. A cooperative implementation returns
    /// `Err(ServerError::Cancelled)` when the signal fires.
    fn start(&self, stop: StopSignal) -> ServerFuture<'_>;

    /// Releases all resources held by the server. Bounded by an
    /// orchestrator-enforced deadline; exceeding it yields
    /// [`ServerError::Timeout`].
    fn stop(&self) -> ServerFuture<'_>;
}
