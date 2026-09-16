//! The uniform audit trail — the `Auditor` hook.

use std::time::{SystemTime, UNIX_EPOCH};

use crate::value::Value;

/// The mutation (or read family) being audited.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AuditAction {
    /// A single insert.
    Create,
    /// A batch insert.
    BatchCreate,
    /// An update by primary key.
    Update,
    /// An insert-or-update.
    Upsert,
    /// A delete by primary key.
    Delete,
    /// A point read.
    Get,
    /// A list scan.
    List,
}

impl AuditAction {
    /// The action's name, as written into entries.
    pub fn as_str(self) -> &'static str {
        match self {
            AuditAction::Create => "create",
            AuditAction::BatchCreate => "batch_create",
            AuditAction::Update => "update",
            AuditAction::Upsert => "upsert",
            AuditAction::Delete => "delete",
            AuditAction::Get => "get",
            AuditAction::List => "list",
        }
    }
}

/// One audit record: who touched which row of which table, when, how.
#[derive(Clone, Debug, PartialEq)]
pub struct AuditEntry {
    /// What happened.
    pub action: AuditAction,
    /// The table it happened to.
    pub table: String,
    /// The affected row's primary key (`None` for table-wide actions).
    pub target: Option<Value>,
    /// The acting viewer's id, when known.
    pub actor: Option<i64>,
    /// When it happened, unix epoch milliseconds.
    pub at: i64,
}

impl AuditEntry {
    /// Stamps an entry "now" on behalf of an actor.
    pub fn now(
        action: AuditAction,
        table: impl Into<String>,
        target: Option<Value>,
        actor: Option<i64>,
    ) -> Self {
        let at = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX))
            .unwrap_or_default();
        Self {
            action,
            table: table.into(),
            target,
            actor,
            at,
        }
    }
}

/// The sink audit entries flow into.
///
/// Engines call it through [`crate::QueryCtx::audit`]; the `Noop` flavor
/// makes skipping the hook free.
pub trait Auditor: Send + Sync {
    /// Records one entry. Implementations must not block for long and must
    /// never fail the operation being audited.
    fn record(&self, entry: AuditEntry);
}

/// The do-nothing auditor.
#[derive(Clone, Copy, Debug, Default)]
pub struct NoopAuditor;

impl Auditor for NoopAuditor {
    fn record(&self, _entry: AuditEntry) {}
}
