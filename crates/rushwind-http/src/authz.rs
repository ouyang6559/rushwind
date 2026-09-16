//! The authorization bridge for the HTTP
//! family, fulfilled with the [`Engine`] contract.
//!
//! One wrapped router evaluates one action/resource permission point:
//! [`with_authorization`] reads the claims the authn bridge inserted,
//! evaluates [`Engine::is_authorized`], and lets the request through or
//! answers `403 / PERMISSION_DENIED`. No claims on the request means the
//! authn layer did not run — that is `401`, not `403`.
//!
//! This is the dynamic-RBAC shape: policies live in
//! the store, the application loads them into the engine with
//! [`Engine::set_policies`] at startup and on every policy change (the
//! engine's interior mutability makes the reset invisible to live
//! traffic), and each protected route group carries its permission
//! point.
//!
//! The project axis defaults to empty; [`with_authorization_claim`]
//! reads it from a string claim (typically the tenant).

use std::sync::Arc;

use axum::extract::Request;
use axum::middleware::Next;
use axum::response::Response;
use rushwind_authn::{AuthClaims, CLAIM_FIELD_SUBJECT};
use rushwind_authz::{Action, Engine, Project, Resource, Subject};

use crate::error::{HttpError, REASON_UNAUTHENTICATED};

/// The reason a permission-point denial surfaces under.
pub const REASON_PERMISSION_DENIED: &str = "PERMISSION_DENIED";

/// Wraps the router with one permission-point check under the empty
/// project.
pub fn with_authorization(
    router: axum::Router,
    engine: Arc<dyn Engine>,
    action: impl Into<String>,
    resource: impl Into<String>,
) -> axum::Router {
    with_authorization_for(router, engine, action, resource, String::new())
}

/// Wraps the router with one permission-point check under a fixed
/// project.
pub fn with_authorization_for(
    router: axum::Router,
    engine: Arc<dyn Engine>,
    action: impl Into<String>,
    resource: impl Into<String>,
    project: impl Into<String>,
) -> axum::Router {
    let action = Action(action.into());
    let resource = Resource(resource.into());
    let project = Project(project.into());
    router.layer(axum::middleware::from_fn(
        move |req: Request, next: Next| {
            let engine = Arc::clone(&engine);
            let action = action.clone();
            let resource = resource.clone();
            let project = project.clone();
            async move { authorize(&engine, &action, &resource, &project, req, next).await }
        },
    ))
}

/// Wraps the router with one permission-point check whose project comes
/// from a string claim of the request's credential (typically
/// the tenant) — an absent claim evaluates under the empty
/// project.
pub fn with_authorization_claim(
    router: axum::Router,
    engine: Arc<dyn Engine>,
    action: impl Into<String>,
    resource: impl Into<String>,
    project_claim: impl Into<String>,
) -> axum::Router {
    let action = Action(action.into());
    let resource = Resource(resource.into());
    let project_claim = project_claim.into();
    router.layer(axum::middleware::from_fn(
        move |req: Request, next: Next| {
            let engine = Arc::clone(&engine);
            let action = action.clone();
            let resource = resource.clone();
            let project_claim = project_claim.clone();
            async move {
                let Some(claims) = req.extensions().get::<AuthClaims>() else {
                    return Err(unauthenticated());
                };
                let project = claims.get_string(&project_claim).unwrap_or_default();
                authorize(&engine, &action, &resource, &Project(project), req, next).await
            }
        },
    ))
}

async fn authorize(
    engine: &Arc<dyn Engine>,
    action: &Action,
    resource: &Resource,
    project: &Project,
    req: Request,
    next: Next,
) -> Result<Response, HttpError> {
    let Some(claims) = req.extensions().get::<AuthClaims>() else {
        return Err(unauthenticated());
    };
    // The subject default matches the claim bag's own semantics: a
    // credential without `sub` is the empty subject, which no policy
    // grants.
    let subject = claims.get_string(CLAIM_FIELD_SUBJECT).unwrap_or_default();
    match engine.is_authorized(
        Subject(subject),
        action.clone(),
        resource.clone(),
        project.clone(),
    ) {
        Ok(true) => Ok(next.run(req).await),
        Ok(false) => Err(HttpError::forbidden(
            REASON_PERMISSION_DENIED,
            "the subject is not authorized for this action",
        )),
        // The taxonomy anchors its errors to 403 — engine failures
        // deny too; the taxonomy's stable code rides
        // as the reason so the log can tell the two denials apart.
        Err(error) => Err(HttpError::new(
            axum_code_for(error.status()),
            error.code(),
            error.to_string(),
        )),
    }
}

fn axum_code_for(status: u16) -> crate::error::Code {
    if status == 401 {
        crate::error::Code::Unauthorized
    } else {
        crate::error::Code::Forbidden
    }
}

fn unauthenticated() -> HttpError {
    HttpError::unauthorized(REASON_UNAUTHENTICATED, "authentication required")
}
