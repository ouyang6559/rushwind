//! The authentication contract.
//!
//! One trait, one credential scheme per engine crate, re-expressed around a
//! header slice instead of a request context.

use crate::claims::AuthClaims;
use crate::error::AuthnError;
use crate::headers::auth_from_headers;

/// One credential scheme's validate-and-mint surface.
///
/// Engines live in their own crates (`rushwind-authn-*`), each implementing
/// this trait for one scheme. The extraction half has a default — the
/// `Authorization`-header shape most schemes ride — and [`authenticate`]
/// composes extraction with validation in one uniform call.
///
/// [`authenticate`]: Authenticator::authenticate
pub trait Authenticator: Send + Sync {
    /// The credential scheme this engine consumes from the
    /// `Authorization` header, e.g. [`crate::SCHEME_BEARER`] or
    /// [`crate::SCHEME_BASIC`].
    ///
    /// Engines whose credentials ride a different header return an empty
    /// string and override [`Authenticator::extract_token`] instead.
    fn scheme(&self) -> &'static str;

    /// Extracts the raw credential from a request's header pairs.
    ///
    /// The default looks at the `Authorization` header whose scheme
    /// matches [`Self::scheme`]. The
    /// granular extraction errors are documented on
    /// [`auth_from_headers`]; [`Authenticator::authenticate`] collapses
    /// them.
    fn extract_token(&self, headers: &[(String, String)]) -> Result<String, AuthnError> {
        auth_from_headers(headers, self.scheme())
    }

    /// Validates a raw credential and returns the claims it carries.
    fn authenticate_token(&self, token: &str) -> Result<AuthClaims, AuthnError>;

    /// Mints a credential carrying the given claims.
    fn create_identity(&self, claims: &AuthClaims) -> Result<String, AuthnError>;

    /// Authenticates one request: extraction, then validation.
    ///
    /// This composes the halves — including the collapse of every
    /// extraction failure to [`AuthnError::MissingBearerToken`], whatever
    /// the granular extraction error was.
    fn authenticate(&self, headers: &[(String, String)]) -> Result<AuthClaims, AuthnError> {
        let token = match self.extract_token(headers) {
            Ok(token) => token,
            Err(_) => return Err(AuthnError::MissingBearerToken),
        };
        self.authenticate_token(&token)
    }
}
