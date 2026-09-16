//! Viewer-scoped tenancy — five data ranges.
//!
//! A [`Viewer`] rides in every [`QueryCtx`](crate::QueryCtx) and bounds what
//! the repository is allowed to see. Engines must enforce the scope on every
//! path, and a row outside the scope is reported exactly like a missing row.

use crate::filter::{FilterExpr, Op};
use crate::value::Value;

/// How much of the table the viewer may see.
///
/// The five ranges: `ALL` (everything), `UNIT` (the viewer's
/// organizational unit), `USER` (an explicit list of owner ids), `OWN`
/// (only rows the viewer owns), and `NONE` (deny
/// everything). The default is `NONE` — scopes close by default.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum DataRange {
    /// The whole table (`ALL`).
    All,
    /// Rows belonging to the viewer's organizational unit (`UNIT`).
    Unit,
    /// Rows owned by an explicit list of users (`USER`).
    User,
    /// Rows owned by the viewer (`SELF`).
    Own,
    /// Nothing (`NONE`); every query yields an empty result.
    #[default]
    None,
}

/// The outcome of resolving a [`Viewer`] against concrete scope columns.
#[derive(Clone, Debug, PartialEq)]
pub enum Scope {
    /// The viewer may not see any row: engines short-circuit to empty
    /// results without touching the store.
    Deny,
    /// No extra predicate is required.
    Unrestricted,
    /// Every read must be conjoined with this filter.
    Scoped(FilterExpr),
}

/// The actor on whose behalf a query runs.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Viewer {
    /// The data range the actor may see.
    pub range: DataRange,
    /// The actor's own id (the `OWN` range needs it).
    pub actor_id: Option<i64>,
    /// The actor's organizational unit id (the `UNIT` range needs it).
    pub unit_id: Option<i64>,
    /// The explicit owner ids of the `USER` range.
    pub subjects: Vec<i64>,
}

impl Viewer {
    /// The conventional owner column (`SELF`/`USER` scoping), shared by all
    /// engines so the same schema works across them.
    pub const OWNER_COLUMN: &'static str = "owner_id";

    /// The conventional organizational-unit column (`UNIT` scoping).
    pub const UNIT_COLUMN: &'static str = "unit_id";

    /// A viewer that sees everything — tests, bootstrap jobs, migrations.
    pub fn all() -> Self {
        Self {
            range: DataRange::All,
            ..Self::default()
        }
    }

    /// A viewer restricted to its own rows.
    pub fn own(actor_id: i64) -> Self {
        Self {
            range: DataRange::Own,
            actor_id: Some(actor_id),
            ..Self::default()
        }
    }

    /// A viewer restricted to one organizational unit.
    pub fn unit(unit_id: i64) -> Self {
        Self {
            range: DataRange::Unit,
            unit_id: Some(unit_id),
            ..Self::default()
        }
    }

    /// Resolves the viewer against the owner and unit column names of the
    /// table being queried. The returned [`Scope`] tells the engine what to
    /// conjoin (or whether to short-circuit).
    pub fn scope(&self, owner_col: &str, unit_col: &str) -> Scope {
        match self.range {
            DataRange::All => Scope::Unrestricted,
            DataRange::None => Scope::Deny,
            DataRange::Own => match self.actor_id {
                Some(id) => Scope::Scoped(FilterExpr::cond(owner_col, Op::Eq, [Value::Int(id)])),
                None => Scope::Deny,
            },
            DataRange::Unit => match self.unit_id {
                Some(id) => Scope::Scoped(FilterExpr::cond(unit_col, Op::Eq, [Value::Int(id)])),
                None => Scope::Deny,
            },
            DataRange::User => {
                if self.subjects.is_empty() {
                    Scope::Deny
                } else {
                    Scope::Scoped(FilterExpr::cond(
                        owner_col,
                        Op::In,
                        self.subjects.iter().copied().map(Value::Int),
                    ))
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn own_scope_targets_the_actor() {
        let scope = Viewer::own(7).scope("owner_id", "unit_id");
        match scope {
            Scope::Scoped(expr) => {
                assert_eq!(
                    expr.node().to_owned(),
                    crate::filter::FilterNode::Cond(crate::filter::Condition::new(
                        "owner_id",
                        Op::Eq,
                        [Value::Int(7)]
                    ))
                );
            }
            other => panic!("expected a scoped filter, got {other:#?}"),
        }
    }

    #[test]
    fn own_without_actor_id_denies() {
        let viewer = Viewer {
            range: DataRange::Own,
            ..Viewer::default()
        };
        assert_eq!(viewer.scope("owner_id", "unit_id"), Scope::Deny);
    }

    #[test]
    fn unit_scope_targets_the_unit_column() {
        let scope = Viewer::unit(3).scope("owner_id", "unit_id");
        match scope {
            Scope::Scoped(_) => {}
            other => panic!("expected a scoped filter, got {other:#?}"),
        }
    }

    #[test]
    fn user_range_with_empty_subjects_denies() {
        let viewer = Viewer {
            range: DataRange::User,
            subjects: Vec::new(),
            ..Viewer::default()
        };
        assert_eq!(viewer.scope("owner_id", "unit_id"), Scope::Deny);
    }
}
