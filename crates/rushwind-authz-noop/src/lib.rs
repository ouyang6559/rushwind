//! Noop engine for the Rust authorization contract, ported from
//! `go-wind-plugins/security/authz/noop`.
//!
//! [`is_authorized`](NoopAuthz::is_authorized) returns `true` for
//! everything; the list filters return the **empty list** — the Go
//! behavior, deliberately asymmetric: the boolean says "no objection"
//! while the filters volunteer nothing.
//!
//! A permit-all engine is a placeholder, not a policy: mount it where
//! authorization is deferred, and remember that list endpoints backed
//! by it render empty until a real engine takes over.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

use rushwind_authz::{
    Action, AuthzError, Engine, Pairs, PolicyMap, Project, Projects, Resource, RoleMap, Subject,
    Subjects,
};

/// The allow-everything authorization engine.
pub struct NoopAuthz;

impl NoopAuthz {
    /// Creates the engine.
    pub fn new() -> Self {
        Self
    }
}

impl Default for NoopAuthz {
    fn default() -> Self {
        Self::new()
    }
}

impl Engine for NoopAuthz {
    fn name(&self) -> String {
        "noop".to_string()
    }

    fn is_authorized(
        &self,
        _subject: Subject,
        _action: Action,
        _resource: Resource,
        _project: Project,
    ) -> Result<bool, AuthzError> {
        Ok(true)
    }

    fn projects_authorized(
        &self,
        _subjects: Subjects,
        _action: Action,
        _resource: Resource,
        _projects: Projects,
    ) -> Result<Projects, AuthzError> {
        Ok(vec![])
    }

    fn filter_authorized_pairs(
        &self,
        _subjects: Subjects,
        _pairs: Pairs,
    ) -> Result<Pairs, AuthzError> {
        Ok(vec![])
    }

    fn filter_authorized_projects(&self, _subjects: Subjects) -> Result<Projects, AuthzError> {
        Ok(vec![])
    }

    fn set_policies(&self, _policies: PolicyMap, _roles: RoleMap) -> Result<(), AuthzError> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rushwind_authz::{Action, Project, Resource, Subject};

    #[test]
    fn the_engine_name_is_noop() {
        let engine: Box<dyn Engine> = Box::new(NoopAuthz::new());
        assert_eq!(engine.name(), "noop");
    }

    #[test]
    fn every_check_passes() {
        let engine = NoopAuthz::new();
        assert!(engine
            .is_authorized(
                Subject::from("anyone"),
                Action::from("anything"),
                Resource::from("everything"),
                Project::from("")
            )
            .unwrap());
    }

    #[test]
    fn the_list_filters_return_empty() {
        let engine = NoopAuthz::new();
        let projects: Projects = ["p1", "p2"].iter().map(|p| Project::from(*p)).collect();
        assert!(engine
            .projects_authorized(
                vec![Subject::from("anyone")],
                Action::from("x"),
                Resource::from("y"),
                projects
            )
            .unwrap()
            .is_empty());
        assert!(engine
            .filter_authorized_projects(vec![Subject::from("anyone")])
            .unwrap()
            .is_empty());
    }

    #[test]
    fn policy_installs_are_accepted_and_ignored() {
        let engine = NoopAuthz::new();
        engine
            .set_policies(PolicyMap::new(), RoleMap::new())
            .unwrap();
        assert!(engine
            .is_authorized(
                Subject::from("anyone"),
                Action::from("anything"),
                Resource::from("everything"),
                Project::from("")
            )
            .unwrap());
    }
}
