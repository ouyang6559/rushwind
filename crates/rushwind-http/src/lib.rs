//! The HTTP edge for RushWind.
//!
//! `rushwind-transport-axum` puts a [`Router`](axum::Router) into the
//! lifecycle; this crate is everything a business router needs *around*
//! its routes: the one error envelope every handler returns, the
//! request-scoped middleware stack, and the bridges that put the
//! security contracts (`rushwind-authn` / `rushwind-authz`) on the
//! request path — the enforcement point both contract crates deliberately
//! leave to the HTTP family.
//!
//! # The error envelope
//!
//! [`HttpError`] is the one error envelope: a gRPC-aligned
//! [`Code`], a stable machine-readable `reason` (the i18n key the frontend
//! substitutes — there is no server-side i18n by design), a human
//! `message`, and optional structured `details`. It
//! implements [`IntoResponse`](axum::response::IntoResponse), so handlers
//! return `Result<T, HttpError>` and every rejection renders the same
//! JSON shape with the mapped HTTP status:
//!
//! ```json
//! {"code": "NOT_FOUND", "reason": "USER_NOT_FOUND", "message": "no such user"}
//! ```
//!
//! Source taxonomies convert with [`From`]: [`AuthnError`](rushwind_authn::AuthnError)
//! (401/500 by its status anchor) and [`StorageError`](rushwind_storage::StorageError)
//! (variant → code) are built in.
//!
//! # The middleware stack
//!
//! Each middleware is a `with_*` free function wrapping a [`Router`] —
//! the [`CrudApi`](https://docs.rs/rushwind-storage-axum) assembly style.
//! The wrapper composes them in the canonical order:
//!
//! | order | wrapper | middleware |
//! |:---|:---|:---|
//! | 1 | [`with_recovery`] | `recovery` — panic → `500` envelope, detail only in the log |
//! | 2 | [`with_request_id`] | `requestid` — echoes or mints `x-request-id`, inserts [`RequestId`] |
//! | 3 | [`with_logging`] | `logging` — one `rushwind.http` span per request |
//! | 4 | [`with_cors`] | `cors` — preflight + response headers from [`CorsOptions`] |
//! | 5 | [`with_timeout`] | `timeout` — budget exceeded → `504` envelope |
//!
//! [`HttpEdge`] chains the stack in that order with sane defaults.
//!
//! # The authn/authz bridges
//!
//! [`with_authn`] runs any [`Authenticator`](rushwind_authn::Authenticator)
//! against the request headers and inserts the [`AuthClaims`](rushwind_authn::AuthClaims)
//! (plus an [`AuthContext`] subject view) into the request extensions;
//! the [`Authenticated`] extractor hands them to handlers. [`with_authorization`]
//! evaluates one action/resource permission point per wrapped router via
//! any [`Engine`](rushwind_authz::Engine) — the dynamic-RBAC shape: load
//! policies from the store with
//! [`set_policies`](rushwind_authz::Engine::set_policies), wrap the
//! protected subtree, reset policies on change.
//!
//! Route whitelists (public exemptions such as login/captcha) are
//! assembly, not middleware: layer the public routes separately and
//! [`merge`](axum::Router::merge) them into the protected subtree.
//!
//! # The domain mounts
//!
//! [`mount_health`] (feature `health`) serves `/healthz` + `/readyz` from
//! the [`Health`](rushwind_health::Health) aggregator; [`mount_metrics`]
//! (feature `metrics`) serves `/metrics` from the Prometheus reporter.
//!
//! # Deliberately not here
//!
//! The `ratelimit`/`circuitbreaker` bridges (both contract
//! domains exist; per-route keying is a policy decision the application
//! owns today), `codec`/`crypto`/`metadata`/`validate` (DTO-level, the
//! application's shape), and client-side `retry` (see `rushwind-retry`).
//!
//! # Testing
//!
//! Every wrapper is exercised over `tower::ServiceExt::oneshot` — no
//! listening socket anywhere.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

mod authn;
mod authz;
mod cors;
mod cors_compat;
mod edge;
mod error;
mod logging;
mod mount;
mod recovery;
mod request_id;
mod timeout;

pub use authn::{with_authn, AuthContext, Authenticated, OptionalAuthenticated};
pub use authz::{
    with_authorization, with_authorization_claim, with_authorization_for, REASON_PERMISSION_DENIED,
};
pub use cors::{with_cors, CorsOptions};
pub use cors_compat::with_cors_compat;
pub use edge::HttpEdge;
pub use error::{Code, ErrorEnvelope, HttpError, HttpResult};
pub use error::{
    REASON_DEADLINE_EXCEEDED, REASON_INVALID_QUERY, REASON_PANIC, REASON_UNAUTHENTICATED,
};
pub use logging::with_logging;
#[cfg(feature = "health")]
pub use mount::{mount_health, LIVENESS_PATH, READINESS_PATH};
#[cfg(feature = "metrics")]
pub use mount::{mount_metrics, METRICS_PATH};
pub use recovery::with_recovery;
pub use request_id::{
    generate_request_id, with_request_id, with_request_id_generator, RequestId, HEADER_X_REQUEST_ID,
};
pub use timeout::with_timeout;
