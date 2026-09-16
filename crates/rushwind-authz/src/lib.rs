//! Authorization contract for RushWind.
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
//! # Engine matrix
//!
//! | Crate | Model |
//! |:---|:---|
//! | `rushwind-authz-acl` | ordered allow/deny rules with wildcard matching, default-deny |
//! | `rushwind-authz-rbac` | role→permission and user→role maps with role inheritance |
//! | `rushwind-authz-noop` | allows everything, filters to nothing |
//!
//! Engines over external policy engines — casbin, cedar, cerbos,
//! opa, zanzibar (keto, openfga), awsiam — are not provided; each wraps a
//! remote policy service or heavyweight local library that needs its own
//! contract decisions first.
//!
//! # Design notes
//!
//! - [`PolicyMap`]/[`RoleMap`] carry [`serde_json::Value`] and each engine
//!   deserializes its own typed rules from the JSON shape the model tags
//!   define — one wire format shared by every engine.
//! - no request context exists; the engine methods take the model values
//!   directly, and middleware is the application's.
//! - direct constructors per engine crate, no registry step.

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
