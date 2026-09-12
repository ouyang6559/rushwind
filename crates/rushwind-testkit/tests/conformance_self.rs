//! Self-test: the conformance suite exercised end-to-end against the
//! built-in probe server — the same slot an adapter crate fills with its
//! own server factory.
//!
//! The factory is referenced through a crate-rooted path: the suite expands
//! into a nested module, where a bare identifier from the file scope does
//! not resolve.

use std::sync::Arc;

use rushwind_testkit::probe::{ProbeKind, ProbeServer};
use rushwind_transport::Server;

fn probe_server() -> Arc<dyn Server> {
    Arc::new(ProbeServer::new(ProbeKind::Normal))
}

rushwind_testkit::rushwind_conformance_suite!(crate::probe_server);
