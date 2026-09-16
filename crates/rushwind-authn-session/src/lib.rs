//! Session engine for the Rust authentication contract.
//!
//! Credentials are opaque session IDs carried in a dedicated header —
//! `X-Session-Id` by default, renameable via
//! [`with_session_id_header`](SessionOptions::with_session_id_header).
//! Validation is a store lookup: the session ID resolves to its claim
//! bag or it does not. Minting creates a fresh session holding the
//! claims and returns its ID.
//!
//! The store is [`SessionStore`] — a trait with an in-process
//! [`MemoryStore`] default, the seam a Redis or database-backed
//! implementation plugs into.
//!
//! # Design notes
//!
//! - the ID rides the header — extraction reads it via the
//!   [`Authenticator::extract_token`] override.
//! - session IDs are 16 bytes from the OS CSPRNG, hex-encoded to a
//!   32-character shape — a session ID is a bearer credential, so an
//!   unpredictable source matters.
//! - each engine gets its own fresh [`MemoryStore`] — no hidden shared
//!   state.
//!
//! Sessions minted here never expire on their own: a store with TTL
//! semantics is the deployer's choice, and [`SessionStore::delete`] is
//! the logout path.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use rushwind_authn::{AuthClaims, Authenticator, AuthnError};

/// The default session-ID header name.
pub const DEFAULT_SESSION_ID_HEADER: &str = "X-Session-Id";

/// The storage half of the session engine: sessions by ID.
///
/// Implementations are shared (`Arc<dyn SessionStore>`), so mutability
/// is interior — a Redis or database-backed store marshals over its
/// client handle, [`MemoryStore`] over a lock.
pub trait SessionStore: Send + Sync {
    /// Retrieves the claims stored for a session ID. `None` when the
    /// session does not exist or has expired.
    fn get(&self, session_id: &str) -> Option<AuthClaims>;

    /// Stores claims under a session ID and returns the ID in effect.
    /// An empty `session_id` asks the store to generate one.
    fn set(&self, session_id: &str, claims: &AuthClaims) -> Result<String, AuthnError>;

    /// Removes a session — the logout path.
    fn delete(&self, session_id: &str);
}

/// An in-process session store behind a lock, the development and
/// testing default. Sessions live exactly as long as the store does.
#[derive(Default)]
pub struct MemoryStore {
    sessions: RwLock<HashMap<String, AuthClaims>>,
}

impl MemoryStore {
    /// Creates an empty store.
    pub fn new() -> Self {
        Self::default()
    }
}

impl SessionStore for MemoryStore {
    fn get(&self, session_id: &str) -> Option<AuthClaims> {
        self.sessions
            .read()
            .expect("session store lock poisoned")
            .get(session_id)
            .cloned()
    }

    fn set(&self, session_id: &str, claims: &AuthClaims) -> Result<String, AuthnError> {
        let id = if session_id.is_empty() {
            generate_session_id()
        } else {
            session_id.to_string()
        };
        self.sessions
            .write()
            .expect("session store lock poisoned")
            .insert(id.clone(), claims.clone());
        Ok(id)
    }

    fn delete(&self, session_id: &str) {
        self.sessions
            .write()
            .expect("session store lock poisoned")
            .remove(session_id);
    }
}

/// A fresh session ID: 16 bytes from the OS CSPRNG, hex-encoded to 32
/// characters.
fn generate_session_id() -> String {
    let mut bytes = [0u8; 16];
    getrandom::fill(&mut bytes).expect("the OS CSPRNG does not fail");
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(32);
    for byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0xf) as usize] as char);
    }
    out
}

/// Builder for [`SessionAuthenticator`].
pub struct SessionOptions {
    store: Option<Arc<dyn SessionStore>>,
    session_id_header: String,
}

impl Default for SessionOptions {
    fn default() -> Self {
        Self {
            store: None,
            session_id_header: DEFAULT_SESSION_ID_HEADER.to_string(),
        }
    }
}

impl SessionOptions {
    /// Options with a fresh [`MemoryStore`] and the default header name.
    pub fn new() -> Self {
        Self::default()
    }

    /// Installs a session store implementation, replacing the default
    /// in-process one.
    pub fn with_store(mut self, store: Arc<dyn SessionStore>) -> Self {
        self.store = Some(store);
        self
    }

    /// Overrides the header name carrying the session ID.
    pub fn with_session_id_header(mut self, name: &str) -> Self {
        self.session_id_header = name.to_string();
        self
    }
}

/// A session authenticator: opaque IDs against a session store.
pub struct SessionAuthenticator {
    store: Arc<dyn SessionStore>,
    session_id_header: String,
}

impl SessionAuthenticator {
    /// Builds the engine from its options. An unspecified store is a
    /// fresh, engine-private [`MemoryStore`] — never a shared global.
    pub fn new(options: SessionOptions) -> Self {
        Self {
            store: options
                .store
                .unwrap_or_else(|| Arc::new(MemoryStore::new())),
            session_id_header: options.session_id_header,
        }
    }

    /// The store backing this engine.
    pub fn store(&self) -> &Arc<dyn SessionStore> {
        &self.store
    }
}

impl Authenticator for SessionAuthenticator {
    fn scheme(&self) -> &'static str {
        // Session IDs ride their own header, not the Authorization
        // header; extraction is overridden below and the scheme is
        // unused.
        ""
    }

    /// The session engine's credential is its ID in the configured
    /// header.
    fn extract_token(&self, headers: &[(String, String)]) -> Result<String, AuthnError> {
        let id = headers
            .iter()
            .find(|(name, _)| name.eq_ignore_ascii_case(&self.session_id_header))
            .map(|(_, value)| value.as_str())
            .unwrap_or("");
        if id.is_empty() {
            return Err(AuthnError::MissingBearerToken);
        }
        Ok(id.to_string())
    }

    fn authenticate_token(&self, token: &str) -> Result<AuthClaims, AuthnError> {
        // An empty ID is the missing-credential error; a
        // store miss is unauthenticated.
        if token.is_empty() {
            return Err(AuthnError::MissingBearerToken);
        }
        self.store.get(token).ok_or(AuthnError::Unauthenticated)
    }

    fn create_identity(&self, claims: &AuthClaims) -> Result<String, AuthnError> {
        // Minting: a new session holding the claims, its ID
        // returned.
        self.store.set("", claims)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rushwind_authn::AuthnError;

    fn subject_claims(subject: &str) -> AuthClaims {
        let mut map = serde_json::Map::new();
        map.insert(
            rushwind_authn::CLAIM_FIELD_SUBJECT.to_string(),
            serde_json::Value::String(subject.to_string()),
        );
        AuthClaims(map)
    }

    fn engine() -> SessionAuthenticator {
        SessionAuthenticator::new(SessionOptions::new())
    }

    fn id_headers(name: &str, id: &str) -> Vec<(String, String)> {
        vec![(name.to_string(), id.to_string())]
    }

    #[test]
    fn memory_store_round_trips_generated_ids() {
        let store = MemoryStore::new();
        let id = store.set("", &subject_claims("alice")).unwrap();
        assert_eq!(id.len(), 32);
        assert!(id.chars().all(|c| c.is_ascii_hexdigit()));
        assert_eq!(store.get(&id).unwrap().get_subject().unwrap(), "alice");
    }

    #[test]
    fn memory_store_honors_explicit_ids() {
        let store = MemoryStore::new();
        let id = store.set("fixed-id", &subject_claims("bob")).unwrap();
        assert_eq!(id, "fixed-id");
        assert_eq!(store.get("fixed-id").unwrap().get_subject().unwrap(), "bob");
    }

    #[test]
    fn memory_store_misses_and_deletes() {
        let store = MemoryStore::new();
        assert!(store.get("no-such-session").is_none());
        let id = store.set("", &subject_claims("carol")).unwrap();
        store.delete(&id);
        assert!(store.get(&id).is_none());
        // Deleting an absent session is a no-op.
        store.delete("never-existed");
    }

    #[test]
    fn minted_sessions_authenticate() {
        let auth = engine();
        let id = auth.create_identity(&subject_claims("dave")).unwrap();
        let claims = auth.authenticate_token(&id).unwrap();
        assert_eq!(claims.get_subject().unwrap(), "dave");
    }

    #[test]
    fn unknown_or_empty_ids_are_rejected() {
        let auth = engine();
        assert_eq!(
            auth.authenticate_token("no-such-session").unwrap_err(),
            AuthnError::Unauthenticated
        );
        assert_eq!(
            auth.authenticate_token("").unwrap_err(),
            AuthnError::MissingBearerToken
        );
    }

    #[test]
    fn authenticate_reads_the_default_header() {
        let auth = engine();
        let id = auth.create_identity(&subject_claims("erin")).unwrap();
        let claims = auth
            .authenticate(&id_headers(DEFAULT_SESSION_ID_HEADER, &id))
            .unwrap();
        assert_eq!(claims.get_subject().unwrap(), "erin");
    }

    #[test]
    fn authenticate_reads_a_renamed_header_case_insensitively() {
        let auth = SessionAuthenticator::new(
            SessionOptions::new().with_session_id_header("X-Alt-Session"),
        );
        let id = auth.create_identity(&subject_claims("frank")).unwrap();
        let claims = auth
            .authenticate(&id_headers("x-alt-session", &id))
            .unwrap();
        assert_eq!(claims.get_subject().unwrap(), "frank");
    }

    #[test]
    fn authenticate_without_the_header_is_missing_credentials() {
        let auth = engine();
        // No headers at all, and a header with the wrong name and an
        // empty value: all collapse to the missing-credential error.
        assert_eq!(
            auth.authenticate(&[]).unwrap_err(),
            AuthnError::MissingBearerToken
        );
        assert_eq!(
            auth.authenticate(&[("X-Other".to_string(), "id".to_string())])
                .unwrap_err(),
            AuthnError::MissingBearerToken
        );
        assert_eq!(
            auth.authenticate(&[(DEFAULT_SESSION_ID_HEADER.to_string(), String::new())])
                .unwrap_err(),
            AuthnError::MissingBearerToken
        );
    }

    #[test]
    fn stores_are_per_engine_not_global() {
        // Each engine gets its own store, so a session minted by one
        // engine is invisible to another.
        let a = engine();
        let b = engine();
        let id = a.create_identity(&subject_claims("gina")).unwrap();
        assert!(b.authenticate_token(&id).is_err());
    }

    #[test]
    fn custom_stores_plugin_in() {
        struct Static;
        impl SessionStore for Static {
            fn get(&self, id: &str) -> Option<AuthClaims> {
                (id == "static-id").then(|| subject_claims("static"))
            }
            fn set(&self, _id: &str, _claims: &AuthClaims) -> Result<String, AuthnError> {
                Err(AuthnError::Unauthenticated)
            }
            fn delete(&self, _id: &str) {}
        }
        let auth = SessionAuthenticator::new(SessionOptions::new().with_store(Arc::new(Static)));
        assert_eq!(
            auth.authenticate_token("static-id")
                .unwrap()
                .get_subject()
                .unwrap(),
            "static"
        );
        assert_eq!(
            auth.authenticate_token("other").unwrap_err(),
            AuthnError::Unauthenticated
        );
        assert_eq!(
            auth.create_identity(&subject_claims("x")).unwrap_err(),
            AuthnError::Unauthenticated
        );
    }
}
