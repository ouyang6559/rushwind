//! The authentication bridge for the HTTP
//! family, fulfilled with the [`Authenticator`] contract.
//!
//! [`with_authn`] collects the request's header pairs (the exact carrier
//! [`Authenticator::authenticate`] reads), validates the credential, and
//! inserts the resulting [`AuthClaims`] — plus an [`AuthContext`] view —
//! into the request extensions. Failures render the error envelope: the
//! taxonomy's 401s pass through as `401` with the engine's stable code
//! as the reason.
//!
//! The [`Authenticated`] extractor pulls the claims into a handler
//! parameter; [`OptionalAuthenticated`] tolerates anonymous callers.
//!
//! # Whitelists are assembly
//!
//! Login/captcha/refresh-style public routes stay outside the auth
//! chain. Here the protected subtree gets the layer; the public subtree
//! does not, and the two merge:
//!
//! ```ignore
//! let public = Router::new().route("/login", post(login));
//! let protected = Router::new()
//!     .route("/users", get(list_users))
//!     .layer(axum::middleware::from_fn(...)); // or with_authn(protected, auth)
//! let app = protected.merge(public);
//! ```

use axum::extract::Request;
use axum::middleware::Next;
use rushwind_authn::{AuthClaims, Authenticator};
use std::sync::Arc;

use crate::error::{HttpError, REASON_UNAUTHENTICATED};

/// The authenticated request's identity, inserted by [`with_authn`]:
/// the subject string plus the full claim bag.
#[derive(Debug, Clone)]
pub struct AuthContext {
    /// The `sub` claim, or the empty string when the credential
    /// carries none.
    pub subject: String,
    /// The full claim bag, also inserted standalone so the
    /// [`Authenticated`] extractor and application code can pick it up.
    pub claims: AuthClaims,
}

/// Wraps the router with credential validation. Every request must
/// carry a credential the authenticator accepts.
pub fn with_authn(router: axum::Router, authenticator: Arc<dyn Authenticator>) -> axum::Router {
    router.layer(axum::middleware::from_fn(
        move |mut req: Request, next: Next| {
            let authenticator = Arc::clone(&authenticator);
            async move {
                let headers: Vec<(String, String)> = req
                    .headers()
                    .iter()
                    .map(|(name, value)| {
                        (
                            name.to_string(),
                            value.to_str().unwrap_or_default().to_owned(),
                        )
                    })
                    .collect();
                match authenticator.authenticate(&headers) {
                    Ok(claims) => {
                        let subject = claims.get_subject().unwrap_or_default();
                        req.extensions_mut().insert(AuthContext {
                            subject,
                            claims: claims.clone(),
                        });
                        req.extensions_mut().insert(claims);
                        Ok(next.run(req).await)
                    }
                    // The taxonomy anchors its own statuses; From keeps
                    // 401s as 401s and turns configuration failures
                    // into envelope 500s.
                    Err(error) => Err(HttpError::from(error)),
                }
            }
        },
    ))
}

/// A handler extractor for the claims [`with_authn`] inserted.
/// Rejects with `401 / AUTHN_UNAUTHENTICATED` when the layer did not
/// run or did not accept the credential.
#[derive(Debug, Clone)]
pub struct Authenticated(pub AuthClaims);

impl<S: Send + Sync> axum::extract::FromRequestParts<S> for Authenticated {
    type Rejection = HttpError;

    async fn from_request_parts(
        parts: &mut axum::http::request::Parts,
        _state: &S,
    ) -> Result<Self, Self::Rejection> {
        match parts.extensions.get::<AuthClaims>() {
            Some(claims) => Ok(Self(claims.clone())),
            None => Err(HttpError::unauthorized(
                REASON_UNAUTHENTICATED,
                "authentication required",
            )),
        }
    }
}

/// A handler extractor that tolerates anonymous callers: `None` when no
/// claims are present, never a rejection.
#[derive(Debug, Clone, Default)]
pub struct OptionalAuthenticated(pub Option<AuthClaims>);

impl<S: Send + Sync> axum::extract::FromRequestParts<S> for OptionalAuthenticated {
    type Rejection = std::convert::Infallible;

    async fn from_request_parts(
        parts: &mut axum::http::request::Parts,
        _state: &S,
    ) -> Result<Self, Self::Rejection> {
        Ok(Self(parts.extensions.get::<AuthClaims>().cloned()))
    }
}
