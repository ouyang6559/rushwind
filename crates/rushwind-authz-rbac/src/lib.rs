//! RBAC engine for the Rust authorization contract.
//!
//! Two maps drive everything:
//!
//! - **role → permissions**: each permission a resource/action pair,
//!   each component a literal or a
//!   [`wildcard`](RbacOptions::with_wildcard)-anchored pattern;
//! - **user → roles**: the memberships. A role may itself be a member
//!   of a role — the inheritance edge; resolution is transitive with a
//!   visited set, so cycles terminate.
//!
//! A subject is authorized when **any** resolved role carries a
//! permission matching the request. There is no deny concept and no
//! default-allow: no match means denied.
//!
//! The project axis does not exist in this model.
//!
//! Evaluation cost is O(roles(user) × permissions(role)) per check with
//! the full inheritance closure resolved each time — fine for small
//! hierarchies, not a scale strategy.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

use std::collections::{HashMap, HashSet};
use std::sync::RwLock;

use rushwind_authz::{
    Action, AuthzError, Engine, Pairs, PolicyMap, Project, Projects, Resource, RoleMap, Subject,
    Subjects,
};
use serde::{Deserialize, Serialize};

/// One role's permission: a resource pattern and an action pattern.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Permission {
    /// The resource pattern.
    pub resource: String,
    /// The action pattern.
    pub action: String,
}

/// The engine's live state, replaced wholesale by policy installs.
struct Config {
    role_permissions: HashMap<String, Vec<Permission>>,
    user_roles: HashMap<String, Vec<String>>,
    wildcard: String,
}

/// Builder for [`RbacEngine`].
pub struct RbacOptions {
    role_permissions: HashMap<String, Vec<Permission>>,
    user_roles: HashMap<String, Vec<String>>,
    wildcard: Option<String>,
}

impl RbacOptions {
    /// Options with empty maps and the `*` wildcard default.
    pub fn new() -> Self {
        Self {
            role_permissions: HashMap::new(),
            user_roles: HashMap::new(),
            wildcard: None,
        }
    }

    /// Replaces the role→permission map.
    pub fn with_role_permissions(mut self, permissions: HashMap<String, Vec<Permission>>) -> Self {
        self.role_permissions = permissions;
        self
    }

    /// Adds one permission to a role.
    pub fn with_role_permission(mut self, role: &str, resource: &str, action: &str) -> Self {
        self.role_permissions
            .entry(role.to_string())
            .or_default()
            .push(Permission {
                resource: resource.to_string(),
                action: action.to_string(),
            });
        self
    }

    /// Replaces the user→role map.
    pub fn with_user_roles(mut self, user_roles: HashMap<String, Vec<String>>) -> Self {
        self.user_roles = user_roles;
        self
    }

    /// Assigns one role to a user or role, appending to existing
    /// memberships.
    pub fn with_user_role(mut self, user: &str, role: &str) -> Self {
        self.user_roles
            .entry(user.to_string())
            .or_default()
            .push(role.to_string());
        self
    }

    /// Overrides the wildcard character. Default `*`.
    pub fn with_wildcard(mut self, wildcard: &str) -> Self {
        self.wildcard = Some(wildcard.to_string());
        self
    }
}

impl Default for RbacOptions {
    fn default() -> Self {
        Self::new()
    }
}

/// The RBAC authorization engine.
pub struct RbacEngine {
    config: RwLock<Config>,
}

impl RbacEngine {
    /// Builds the engine from its options.
    pub fn new(options: RbacOptions) -> Self {
        Self {
            config: RwLock::new(Config {
                role_permissions: options.role_permissions,
                user_roles: options.user_roles,
                wildcard: options.wildcard.unwrap_or_else(|| "*".to_string()),
            }),
        }
    }

    /// Replaces the role→permission map — the typed form of the
    /// `rolePermissions` policy entry.
    pub fn set_role_permissions(&self, permissions: HashMap<String, Vec<Permission>>) {
        self.config
            .write()
            .expect("rbac config lock poisoned")
            .role_permissions = permissions;
    }

    /// Replaces the user→role map — the typed form of the `userRoles`
    /// role entry.
    pub fn set_user_roles(&self, user_roles: HashMap<String, Vec<String>>) {
        self.config
            .write()
            .expect("rbac config lock poisoned")
            .user_roles = user_roles;
    }
}

impl Engine for RbacEngine {
    fn name(&self) -> String {
        "rbac".to_string()
    }

    fn is_authorized(
        &self,
        subject: Subject,
        action: Action,
        resource: Resource,
        _project: Project,
    ) -> Result<bool, AuthzError> {
        let config = self.config.read().expect("rbac config lock poisoned");
        Ok(evaluate(
            &config,
            subject.as_str(),
            action.as_str(),
            resource.as_str(),
        ))
    }

    fn projects_authorized(
        &self,
        subjects: Subjects,
        action: Action,
        resource: Resource,
        projects: Projects,
    ) -> Result<Projects, AuthzError> {
        // The RBAC model has no project axis: the subject check decides
        // per project.
        let config = self.config.read().expect("rbac config lock poisoned");
        let mut result = vec![];
        for project in projects {
            for subject in &subjects {
                if evaluate(
                    &config,
                    subject.as_str(),
                    action.as_str(),
                    resource.as_str(),
                ) {
                    result.push(project);
                    break;
                }
            }
        }
        Ok(result)
    }

    fn filter_authorized_pairs(
        &self,
        subjects: Subjects,
        pairs: Pairs,
    ) -> Result<Pairs, AuthzError> {
        let config = self.config.read().expect("rbac config lock poisoned");
        let mut result = vec![];
        for pair in pairs {
            for subject in &subjects {
                if evaluate(
                    &config,
                    subject.as_str(),
                    pair.action.as_str(),
                    pair.resource.as_str(),
                ) {
                    result.push(pair);
                    break;
                }
            }
        }
        Ok(result)
    }

    fn filter_authorized_projects(&self, _subjects: Subjects) -> Result<Projects, AuthzError> {
        // No project axis: the empty list.
        Ok(vec![])
    }

    fn set_policies(&self, policies: PolicyMap, roles: RoleMap) -> Result<(), AuthzError> {
        // Each entry the engine defines deserializes into its typed
        // map; anything else — unknown names, malformed payloads — is
        // silently skipped.
        if let Some(payload) = policies.get("rolePermissions") {
            if let Ok(map) =
                serde_json::from_value::<HashMap<String, Vec<Permission>>>(payload.clone())
            {
                self.set_role_permissions(map);
            }
        }
        if let Some(payload) = roles.get("userRoles") {
            if let Ok(map) = serde_json::from_value::<HashMap<String, Vec<String>>>(payload.clone())
            {
                self.set_user_roles(map);
            }
        }
        Ok(())
    }
}

/// The verdict: resolve the subject's transitive role closure, then
/// look for any role whose permission table matches.
fn evaluate(config: &Config, subject: &str, action: &str, resource: &str) -> bool {
    let roles = resolve_roles(config, subject, &mut HashSet::new());
    for role in &roles {
        let Some(permissions) = config.role_permissions.get(role) else {
            continue;
        };
        for permission in permissions {
            if matches_pattern(&permission.resource, resource, &config.wildcard)
                && matches_pattern(&permission.action, action, &config.wildcard)
            {
                return true;
            }
        }
    }
    false
}

/// Role resolution: the transitive closure of a subject's
/// memberships, with a visited set so a role cycle terminates. The
/// subject itself counts as visited, so a user→role edge pointing back
/// at the user name stops there.
fn resolve_roles(config: &Config, subject: &str, visited: &mut HashSet<String>) -> Vec<String> {
    if visited.contains(subject) {
        return vec![];
    }
    visited.insert(subject.to_string());
    let Some(memberships) = config.user_roles.get(subject) else {
        return vec![];
    };
    let mut result = vec![];
    for role in memberships {
        result.push(role.clone());
        result.extend(resolve_roles(config, role, visited));
    }
    result
}

/// The wildcard matcher, identical to the ACL engine's.
fn matches_pattern(pattern: &str, value: &str, wildcard: &str) -> bool {
    if pattern == wildcard {
        return true;
    }
    if pattern == value {
        return true;
    }
    if let Some(prefix) = pattern.strip_suffix(wildcard) {
        return value.starts_with(prefix);
    }
    if let Some(suffix) = pattern.strip_prefix(wildcard) {
        return value.ends_with(suffix);
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use rushwind_authz::Pair;

    fn ask(engine: &RbacEngine, subject: &str, action: &str, resource: &str) -> bool {
        engine
            .is_authorized(
                Subject::from(subject),
                Action::from(action),
                Resource::from(resource),
                Project::from(""),
            )
            .unwrap()
    }

    #[test]
    fn the_engine_name_is_rbac() {
        let engine: Box<dyn Engine> = Box::new(RbacEngine::new(RbacOptions::new()));
        assert_eq!(engine.name(), "rbac");
    }

    #[test]
    fn a_matching_permission_allows() {
        let engine = RbacEngine::new(
            RbacOptions::new()
                .with_role_permission("reader", "doc:*", "read")
                .with_user_role("alice", "reader"),
        );
        assert!(ask(&engine, "alice", "read", "doc:1"));
    }

    #[test]
    fn a_non_matching_action_denies() {
        let engine = RbacEngine::new(
            RbacOptions::new()
                .with_role_permission("reader", "doc:*", "read")
                .with_user_role("alice", "reader"),
        );
        assert!(!ask(&engine, "alice", "write", "doc:1"));
    }

    #[test]
    fn a_subject_without_roles_denies() {
        let engine = RbacEngine::new(RbacOptions::new());
        assert!(!ask(&engine, "alice", "read", "doc:1"));
    }

    #[test]
    fn inherited_roles_compose_transitively() {
        // admin → editor → reader; alice holds admin.
        let engine = RbacEngine::new(
            RbacOptions::new()
                .with_role_permission("reader", "doc:*", "read")
                .with_role_permission("editor", "doc:*", "write")
                .with_role_permission("admin", "*", "delete")
                .with_user_role("admin", "editor")
                .with_user_role("editor", "reader")
                .with_user_role("alice", "admin"),
        );
        assert!(ask(&engine, "alice", "read", "doc:1"));
        assert!(ask(&engine, "alice", "write", "doc:1"));
        assert!(ask(&engine, "alice", "delete", "doc:1"));
    }

    #[test]
    fn role_cycles_terminate_and_direct_roles_still_apply() {
        // roleA ↔ roleB is a cycle; alice → roleA directly.
        let engine = RbacEngine::new(
            RbacOptions::new()
                .with_role_permission("roleA", "doc:1", "read")
                .with_user_role("alice", "roleA")
                .with_user_role("roleA", "roleB")
                .with_user_role("roleB", "roleA"),
        );
        assert!(ask(&engine, "alice", "read", "doc:1"));
    }

    #[test]
    fn wildcard_permissions_allow_everything() {
        let engine = RbacEngine::new(
            RbacOptions::new()
                .with_role_permission("superadmin", "*", "*")
                .with_user_role("root", "superadmin"),
        );
        assert!(ask(&engine, "root", "anything", "everything"));
    }

    #[test]
    fn multiple_roles_union_their_permissions() {
        let engine = RbacEngine::new(
            RbacOptions::new()
                .with_role_permission("reader", "doc:*", "read")
                .with_role_permission("writer", "doc:*", "write")
                .with_user_role("alice", "reader")
                .with_user_role("alice", "writer"),
        );
        assert!(ask(&engine, "alice", "read", "doc:1"));
        assert!(ask(&engine, "alice", "write", "doc:1"));
        // Neither role carries delete.
        assert!(!ask(&engine, "alice", "delete", "doc:1"));
    }

    #[test]
    fn projects_pass_through_the_subject_check() {
        let engine = RbacEngine::new(
            RbacOptions::new()
                .with_role_permission("reader", "doc:*", "read")
                .with_user_role("alice", "reader"),
        );
        let projects: Projects = ["p1", "p2", "p3"]
            .iter()
            .map(|p| Project::from(*p))
            .collect();
        let result = engine
            .projects_authorized(
                vec![Subject::from("alice")],
                Action::from("read"),
                Resource::from("doc:1"),
                projects,
            )
            .unwrap();
        assert_eq!(result.len(), 3);
    }

    #[test]
    fn projects_filter_to_empty_for_unauthorized_subjects() {
        let engine = RbacEngine::new(
            RbacOptions::new()
                .with_role_permission("reader", "doc:*", "read")
                .with_user_role("alice", "reader"),
        );
        let projects: Projects = ["p1", "p2"].iter().map(|p| Project::from(*p)).collect();
        let result = engine
            .projects_authorized(
                vec![Subject::from("bob")],
                Action::from("read"),
                Resource::from("doc:1"),
                projects,
            )
            .unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn pairs_filter_down_to_matching_pairs() {
        let engine = RbacEngine::new(
            RbacOptions::new()
                .with_role_permission("reader", "doc:*", "read")
                .with_user_role("alice", "reader"),
        );
        let pairs: Pairs = vec![
            Pair {
                resource: Resource::from("doc:1"),
                action: Action::from("read"),
            },
            Pair {
                resource: Resource::from("doc:2"),
                action: Action::from("write"),
            },
            Pair {
                resource: Resource::from("img:1"),
                action: Action::from("read"),
            },
        ];
        let result = engine
            .filter_authorized_pairs(vec![Subject::from("alice")], pairs)
            .unwrap();
        assert_eq!(result.len(), 1);
    }

    #[test]
    fn policy_installs_take_effect() {
        let engine = RbacEngine::new(RbacOptions::new());
        // Initially denied.
        assert!(!ask(&engine, "root", "delete", "anything"));

        let role_permissions: HashMap<String, Vec<Permission>> = HashMap::from([(
            "admin".to_string(),
            vec![Permission {
                resource: "*".to_string(),
                action: "*".to_string(),
            }],
        )]);
        let user_roles: HashMap<String, Vec<String>> =
            HashMap::from([("root".to_string(), vec!["admin".to_string()])]);
        let mut policies = PolicyMap::new();
        policies.insert(
            "rolePermissions".to_string(),
            serde_json::to_value(&role_permissions).expect("role permissions serialize"),
        );
        let mut roles = RoleMap::new();
        roles.insert(
            "userRoles".to_string(),
            serde_json::to_value(&user_roles).expect("user roles serialize"),
        );
        engine.set_policies(policies, roles).unwrap();

        assert!(ask(&engine, "root", "delete", "anything"));
    }

    #[test]
    fn malformed_policy_payloads_are_skipped() {
        let engine = RbacEngine::new(RbacOptions::new());
        let mut policies = PolicyMap::new();
        policies.insert(
            "rolePermissions".to_string(),
            serde_json::json!("not a map"),
        );
        let mut roles = RoleMap::new();
        roles.insert("userRoles".to_string(), serde_json::json!(17));
        engine.set_policies(policies, roles).unwrap();
        assert!(!ask(&engine, "anyone", "read", "doc:1"));
    }
}
