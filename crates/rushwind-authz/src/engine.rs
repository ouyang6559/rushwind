//! The authorization engine contract.

use crate::error::AuthzError;
use crate::model::{
    Action, Pairs, PolicyMap, Project, Projects, Resource, RoleMap, Subject, Subjects,
};

/// One authorization model's decision surface.
///
/// Implementations live in their own crates (`rushwind-authz-*`), one
/// engine per crate, the registry/storage pattern. An engine is
/// [`Send`] + [`Sync`] and shareable behind an `Arc`; policy state is
/// interior-mutable so [`Engine::set_policies`] can run against a live
/// engine.
pub trait Engine: Send + Sync {
    /// The engine's registered name, e.g. `"acl"` or `"rbac"`.
    fn name(&self) -> String;

    /// The single-verdict decision: may `subject` perform `action` on
    /// `resource` under `project`.
    fn is_authorized(
        &self,
        subject: Subject,
        action: Action,
        resource: Resource,
        project: Project,
    ) -> Result<bool, AuthzError>;

    /// Filters `projects` down to those any of `subjects` may act on
    /// with this action/resource. Engines without a project concept
    /// return their whole input when the subject check passes, or an
    /// empty list when it does not — the ACL/Rbac engines' behavior.
    fn projects_authorized(
        &self,
        subjects: Subjects,
        action: Action,
        resource: Resource,
        projects: Projects,
    ) -> Result<Projects, AuthzError>;

    /// Filters `pairs` down to those any of `subjects` is authorized
    /// for.
    fn filter_authorized_pairs(
        &self,
        subjects: Subjects,
        pairs: Pairs,
    ) -> Result<Pairs, AuthzError>;

    /// Filters `projects` down to those any of `subjects` is authorized
    /// to see at all.
    fn filter_authorized_projects(&self, subjects: Subjects) -> Result<Projects, AuthzError>;

    /// Installs policy state from the interchange maps. Entries an
    /// engine does not define, or payloads that fail to deserialize
    /// into its rule types, are silently skipped rather than rejected.
    fn set_policies(&self, policies: PolicyMap, roles: RoleMap) -> Result<(), AuthzError>;
}
