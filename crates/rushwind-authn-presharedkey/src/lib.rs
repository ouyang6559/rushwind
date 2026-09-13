//! Preshared-key engine for the Rust authentication contract, ported
//! from `go-wind-plugins/security/authn/presharedkey`.
//!
//! One static set of opaque bearer keys. Validation is set membership,
//! nothing more — an authenticated key yields an **empty claim bag**: no
//! subject, no scopes, no per-key identity. Where per-key identity
//! matters, use the apikey engine's claim attachment instead.
//!
//! Minting ([`create_identity`](PresharedKeyAuthenticator::create_identity))
//! draws one key from the set uniformly at random — the shape a key
//! distributor hands to one of N clients. An empty configuration mints
//! the empty credential.
//!
//! Divergence: the Go engine's empty-configuration rejection is a plain
//! Go error outside the taxonomy; here it is
//! [`AuthnError::Unauthenticated`] — same outcome, typed surface.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

use std::collections::HashSet;

use rushwind_authn::{AuthClaims, Authenticator, AuthnError, SCHEME_BEARER};

/// Builder for [`PresharedKeyAuthenticator`].
#[derive(Default)]
pub struct PresharedKeyOptions {
    keys: HashSet<String>,
}

impl PresharedKeyOptions {
    /// Options with an empty key set — the engine rejects everything.
    pub fn new() -> Self {
        Self::default()
    }

    /// Replaces the static key set.
    pub fn with_keys(mut self, keys: &[&str]) -> Self {
        self.keys = keys.iter().map(|k| (*k).to_string()).collect();
        self
    }
}

/// A preshared-key authenticator: set membership over opaque bearer keys.
pub struct PresharedKeyAuthenticator {
    options: PresharedKeyOptions,
}

impl PresharedKeyAuthenticator {
    /// Builds the engine from its options.
    pub fn new(options: PresharedKeyOptions) -> Self {
        Self { options }
    }

    /// The Go getRandomKey: a uniformly random key from the set, or the
    /// empty string for an empty configuration.
    fn random_key(&self) -> String {
        let count = self.options.keys.len();
        if count == 0 {
            return String::new();
        }
        let Ok(index) = random_index(count) else {
            return String::new();
        };
        self.options
            .keys
            .iter()
            .nth(index)
            .cloned()
            .unwrap_or_default()
    }
}

impl Authenticator for PresharedKeyAuthenticator {
    fn scheme(&self) -> &'static str {
        SCHEME_BEARER
    }

    fn authenticate_token(&self, token: &str) -> Result<AuthClaims, AuthnError> {
        // The Go engine's two failure modes — empty configuration and
        // unknown key — both reject.
        if self.options.keys.is_empty() {
            return Err(AuthnError::Unauthenticated);
        }
        if !self.options.keys.contains(token) {
            return Err(AuthnError::Unauthenticated);
        }
        // A valid key authenticates to empty claims: this engine carries
        // no per-key identity.
        Ok(AuthClaims::new())
    }

    fn create_identity(&self, _claims: &AuthClaims) -> Result<String, AuthnError> {
        // The Go mint: a random key from the set; the empty string when
        // the set is empty — no error in either case.
        Ok(self.random_key())
    }
}

/// A uniform random index below `bound`, from the OS CSPRNG.
fn random_index(bound: usize) -> Result<usize, getrandom::Error> {
    let mut bytes = [0u8; 8];
    getrandom::fill(&mut bytes)?;
    let value = u64::from_le_bytes(bytes);
    Ok((value % bound as u64) as usize)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rushwind_authn::{AuthnError, HEADER_AUTHORIZE, SCHEME_BEARER};

    fn one_key() -> PresharedKeyOptions {
        PresharedKeyOptions::new().with_keys(&["shared-secret-1"])
    }

    #[test]
    fn configured_keys_authenticate_to_empty_claims() {
        let auth = PresharedKeyAuthenticator::new(
            PresharedKeyOptions::new().with_keys(&["key-1", "key-2"]),
        );
        for key in ["key-1", "key-2"] {
            let claims = auth.authenticate_token(key).unwrap();
            assert_eq!(claims.get_subject().unwrap(), "");
        }
    }

    #[test]
    fn unknown_keys_are_unauthenticated() {
        let auth = PresharedKeyAuthenticator::new(one_key());
        assert_eq!(
            auth.authenticate_token("bad-key").unwrap_err(),
            AuthnError::Unauthenticated
        );
        // The empty token is not a member either.
        assert_eq!(
            auth.authenticate_token("").unwrap_err(),
            AuthnError::Unauthenticated
        );
    }

    #[test]
    fn missing_configuration_is_unauthenticated() {
        let auth = PresharedKeyAuthenticator::new(PresharedKeyOptions::new());
        assert_eq!(
            auth.authenticate_token("any-key").unwrap_err(),
            AuthnError::Unauthenticated
        );
    }

    #[test]
    fn authenticate_collapses_missing_credentials() {
        let auth = PresharedKeyAuthenticator::new(one_key());
        assert_eq!(
            auth.authenticate(&[]).unwrap_err(),
            AuthnError::MissingBearerToken
        );
    }

    #[test]
    fn authenticate_extracts_and_validates() {
        let auth = PresharedKeyAuthenticator::new(one_key());
        let headers = vec![(
            HEADER_AUTHORIZE.to_string(),
            format!("{SCHEME_BEARER} shared-secret-1"),
        )];
        assert!(auth.authenticate(&headers).unwrap().0.is_empty());
    }

    #[test]
    fn mint_draws_from_the_configured_set() {
        let auth = PresharedKeyAuthenticator::new(
            PresharedKeyOptions::new().with_keys(&["k1", "k2", "k3"]),
        );
        for _ in 0..16 {
            let minted = auth.create_identity(&AuthClaims::new()).unwrap();
            assert!(["k1", "k2", "k3"].contains(&minted.as_str()));
        }
    }

    #[test]
    fn mint_from_an_empty_configuration_is_the_empty_credential() {
        // The Go mint with no keys: ("", nil) — faithfully silly.
        let auth = PresharedKeyAuthenticator::new(PresharedKeyOptions::new());
        assert_eq!(auth.create_identity(&AuthClaims::new()).unwrap(), "");
    }
}
