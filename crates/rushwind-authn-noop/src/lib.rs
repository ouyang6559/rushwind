//! Noop engine for the Rust authentication contract.
//!
//! Every credential authenticates to an empty claim bag; every mint
//! produces an empty credential. Useful as a placeholder where an
//! [`Authenticator`] is required but authentication is deferred, and as
//! the control specimen against which real engines' rejections stand out.
//!
//! An empty claim bag carries no `sub`: downstream authz that keys on the
//! subject sees an anonymous identity, not a bypass.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

use rushwind_authn::{AuthClaims, Authenticator, AuthnError, SCHEME_BEARER};

/// The accept-everything authenticator.
pub struct NoopAuthenticator;

impl NoopAuthenticator {
    /// Creates the engine.
    pub fn new() -> Self {
        Self
    }
}

impl Default for NoopAuthenticator {
    fn default() -> Self {
        Self::new()
    }
}

impl Authenticator for NoopAuthenticator {
    fn scheme(&self) -> &'static str {
        SCHEME_BEARER
    }

    fn authenticate_token(&self, _token: &str) -> Result<AuthClaims, AuthnError> {
        Ok(AuthClaims::new())
    }

    fn create_identity(&self, _claims: &AuthClaims) -> Result<String, AuthnError> {
        Ok(String::new())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rushwind_authn::{AuthnError, HEADER_AUTHORIZE, SCHEME_BEARER};

    #[test]
    fn every_token_authenticates_to_empty_claims() {
        let auth = NoopAuthenticator::new();
        for token in ["anything", "", "garbage.token.here"] {
            let claims = auth.authenticate_token(token).unwrap();
            assert_eq!(claims.get_subject().unwrap(), "");
        }
    }

    #[test]
    fn mint_produces_an_empty_credential() {
        let auth = NoopAuthenticator::new();
        assert_eq!(auth.create_identity(&AuthClaims::new()).unwrap(), "");
    }

    #[test]
    fn extraction_still_applies_to_the_request_path() {
        // The noop engine validates nothing, but extraction is the
        // trait's default: a header-less request still fails with the
        // collapsed missing-credential error.
        let auth = NoopAuthenticator::new();
        assert_eq!(
            auth.authenticate(&[]).unwrap_err(),
            AuthnError::MissingBearerToken
        );
        let headers = vec![(
            HEADER_AUTHORIZE.to_string(),
            format!("{SCHEME_BEARER} anything"),
        )];
        assert!(auth.authenticate(&headers).unwrap().0.is_empty());
    }
}
