//! Authorization contract for RushWind, extracted from the Go predecessor
//! `go-wind-plugins/security/authz`.
//!
//! The contract is one trait — [`Engine`] — over a deliberately small
//! model: [`Subject`] (who), [`Action`] and [`Resource`] (what), and
//! [`Project`] (where), plus the two multi-target queries a
//! list-filtering UI needs: which of these projects is this subject
//! authorized for, and which of these resource/action pairs survive.
//!
//! Two decision shapes, one filter shape:
//!
//! - [`Engine::is_authorized`] — one subject, one action/resource, one
//!   boolean verdict. This is the only method a request-time check
//!   needs.
//! - [`Engine::projects_authorized`] / [`Engine::filter_authorized_pairs`]
//!   / [`Engine::filter_authorized_projects`] — bulk filters that
//!   partition a candidate set into what the subject may see. These
//!   exist so a list endpoint never shows a row the subject could not
//!   have fetched individually.
//! - [`Engine::set_policies`] — installs policy state through the
//!   [`PolicyMap`]/[`RoleMap`] interchange.
//!
//! # Engine matrix (ported)
//!
//! | Crate | Model |
//! |:---|:---|
//! | `rushwind-authz-acl` | ordered allow/deny rules with wildcard matching, default-deny |
//! | `rushwind-authz-rbac` | role→permission and user→role maps with role inheritance |
//! | `rushwind-authz-noop` | allows everything, filters to nothing |
//!
//! The Go engines over external policy engines — casbin, cedar, cerbos,
//! opa, zanzibar (keto, openfga), awsiam — remain unported; each wraps a
//! remote policy service or heavyweight local library that needs its own
//! contract decisions first.
//!
//! # Divergences from the Go predecessor
//!
//! | Go | Rust |
//! |:---|:---|
//! | `SetPolicies` takes `map[string]interface{}` with engine-local Go types hidden behind runtime type assertions | [`PolicyMap`]/[`RoleMap`] carry [`serde_json::Value`] and each engine deserializes its own typed rules from the JSON shape the Go tags define — the same wire format, without the assertion |
//! | the authz claims struct rides `context.Context` from middleware to engine | no request context exists; the engine methods take the model values directly, and middleware is the application's |
//! | engines register through `init()` + a factory map | direct constructors per engine crate |

#![forbid(unsafe_code)]
#![deny(missing_docs)]

mod engine;
mod error;
mod model;

pub use engine::Engine;
pub use error::AuthzError;
pub use model::{
    Action, Actions, Pair, Pairs, PolicyMap, Project, Projects, Resource, Resources, RoleMap,
    Subject, Subjects,
};
