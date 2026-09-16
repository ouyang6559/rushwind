//! The authorization model: who may do what to which resource where.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

/// The acting principal a policy is evaluated against: a user, a service
/// identity, a group.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Subject(pub String);

impl From<&str> for Subject {
    fn from(value: &str) -> Self {
        Self(value.to_string())
    }
}

impl From<String> for Subject {
    fn from(value: String) -> Self {
        Self(value)
    }
}

impl Subject {
    /// The subject string.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// A list of subjects, as a multi-principal query input.
pub type Subjects = Vec<Subject>;

/// The operation being performed: read, write, delete, …
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Action(pub String);

impl From<&str> for Action {
    fn from(value: &str) -> Self {
        Self(value.to_string())
    }
}

impl From<String> for Action {
    fn from(value: String) -> Self {
        Self(value)
    }
}

impl Action {
    /// The action string.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// A list of actions.
pub type Actions = Vec<Action>;

/// The object an action targets: a document, a table, an API route.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Resource(pub String);

impl From<&str> for Resource {
    fn from(value: &str) -> Self {
        Self(value.to_string())
    }
}

impl From<String> for Resource {
    fn from(value: String) -> Self {
        Self(value)
    }
}

impl Resource {
    /// The resource string.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// A list of resources.
pub type Resources = Vec<Resource>;

/// A tenant scope the request runs under — the one axis the engine model
/// carries that is not subject/action/resource. Engines without a
/// project concept ignore it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Project(pub String);

impl From<&str> for Project {
    fn from(value: &str) -> Self {
        Self(value.to_string())
    }
}

impl From<String> for Project {
    fn from(value: String) -> Self {
        Self(value)
    }
}

impl Project {
    /// The project string.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// A list of projects.
pub type Projects = Vec<Project>;

/// One resource/action pair, the unit of the pair-filter query. The
/// field names are `resource` and `action`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Pair {
    /// The resource of the pair.
    pub resource: Resource,
    /// The action of the pair.
    pub action: Action,
}

/// A list of pairs.
pub type Pairs = Vec<Pair>;

/// The policy-state interchange: named entries carrying JSON payloads,
/// each engine reading the entries it defines.
///
/// Each engine deserializes the JSON shapes its own rule types define; a
/// payload that does not deserialize into an engine's rule type is
/// **silently skipped** rather than rejected.
pub type PolicyMap = HashMap<String, serde_json::Value>;

/// The role-state interchange, the role half of [`PolicyMap`].
pub type RoleMap = HashMap<String, serde_json::Value>;

#[cfg(test)]
mod tests {
    use super::*;

    /// Golden vector: the pair's JSON shape is
    /// `{"resource":...,"action":...}` — field names in declaration
    /// order.
    #[test]
    fn pair_json_shape_is_stable() {
        let pair = Pair {
            resource: Resource::from("doc:1"),
            action: Action::from("read"),
        };
        assert_eq!(
            serde_json::to_string(&pair).expect("pair serializes"),
            r#"{"resource":"doc:1","action":"read"}"#
        );
    }

    /// The exact dual: the same JSON shape parses back into the
    /// same pair.
    #[test]
    fn pair_json_round_trips() {
        let pair: Pair = serde_json::from_str(r#"{"resource":"img:2","action":"write"}"#)
            .expect("golden vector parses");
        assert_eq!(pair.resource.as_str(), "img:2");
        assert_eq!(pair.action.as_str(), "write");
    }

    /// Strictness pin: serde rejects a payload missing either field.
    /// Engines
    /// consume the interchange through deserialization and skip what
    /// fails to parse — so the strictness surfaces as a skipped payload,
    /// never a half-formed rule.
    #[test]
    fn pair_json_requires_both_fields() {
        assert!(serde_json::from_str::<Pair>(r#"{"resource":"doc:1"}"#).is_err());
        assert!(serde_json::from_str::<Pair>("{}").is_err());
    }
}
