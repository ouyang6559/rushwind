//! The per-request context every repository call receives.

use std::sync::Arc;

use crate::auditor::{AuditEntry, Auditor};
use crate::viewer::Viewer;

/// The cross-cutting request context, carried as explicit fields on every
/// repository call.
///
/// Cloning is cheap (the auditor sits behind an `Arc`).
#[derive(Clone, Default)]
pub struct QueryCtx {
    /// The tenancy scope enforced on every path.
    pub viewer: Viewer,
    /// Where audit entries go, when auditing is wanted.
    pub auditor: Option<Arc<dyn Auditor>>,
}

impl std::fmt::Debug for QueryCtx {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QueryCtx")
            .field("viewer", &self.viewer)
            .field("auditor", &self.auditor.is_some())
            .finish()
    }
}

impl QueryCtx {
    /// A context whose viewer sees everything and nothing is audited —
    /// appropriate for tests, demos, and bootstrap jobs.
    pub fn all_access() -> Self {
        Self {
            viewer: Viewer::all(),
            auditor: None,
        }
    }

    /// A context for the given viewer, without auditing.
    pub fn new(viewer: Viewer) -> Self {
        Self {
            viewer,
            auditor: None,
        }
    }

    /// Attaches an auditor, builder style.
    pub fn audited(mut self, auditor: Arc<dyn Auditor>) -> Self {
        self.auditor = Some(auditor);
        self
    }

    /// Emits one audit entry if an auditor is attached; a no-op otherwise.
    /// Engines call this after a successful (or attempted, per their own
    /// policy) operation.
    pub fn audit(&self, entry: AuditEntry) {
        if let Some(auditor) = &self.auditor {
            auditor.record(entry);
        }
    }
}
