//! JWT engine for the Rust authentication contract, over the `jsonwebtoken` crate.
//!
//! Tokens ride as bearer credentials:
//!
//! ```text
//! Authorization: Bearer <jwt>
//! ```
//!
//! Minting signs the claim bag with the configured algorithm and key;
//! validation verifies the signature and the time-window claims. The
//! validation profile:
//!
//! - `exp` and `nbf` are validated **when present**, with zero leeway;
//! - `exp` is not required — a token without one parses;
//! - `aud`/`iss` are not validated;
//! - the token's `alg` must equal the configured algorithm.
//!
//! Keys arrive through the builder: a symmetric secret
//! ([`with_key`](JwtOptions::with_key), both halves), or an asymmetric
//! pair through typed keys or the PEM helpers — private PEMs mint,
//! public PEMs verify.
//!
//! # Design notes
//!
//! - `ES512`: the underlying `jsonwebtoken` library has no ES512 —
//!   [`with_algorithm`](JwtOptions::with_algorithm) rejects it as
//!   [`AuthnError::UnsupportedSigningMethod`].
//! - an unknown algorithm name is rejected at builder time, not surfaced
//!   later.
//! - a PEM parse failure rejects with
//!   [`GetKeyFailed`](AuthnError::GetKeyFailed) at builder time — fail
//!   loud, not a silently unconfigured engine.
//! - library error variants map where the underlying library classifies
//!   them; the mapping below pins what this engine returns.
//!
//! The error mapping from `jsonwebtoken`: `ExpiredSignature` →
//! [`TokenExpired`](AuthnError::TokenExpired), `InvalidSignature` →
//! [`SignTokenFailed`](AuthnError::SignTokenFailed), `InvalidAlgorithm`
//! → [`UnsupportedSigningMethod`](AuthnError::UnsupportedSigningMethod),
//! everything else → [`InvalidToken`](AuthnError::InvalidToken).

#![forbid(unsafe_code)]
#![deny(missing_docs)]

use jsonwebtoken::errors::ErrorKind;
use jsonwebtoken::{Algorithm, DecodingKey, EncodingKey, Header, Validation};

use rushwind_authn::{AuthClaims, Authenticator, AuthnError, SCHEME_BEARER};

/// The algorithm-name table, with
/// `ES512` absent (unsupported by the underlying library).
fn parse_algorithm(name: &str) -> Option<Algorithm> {
    Some(match name {
        "HS256" => Algorithm::HS256,
        "HS384" => Algorithm::HS384,
        "HS512" => Algorithm::HS512,
        "RS256" => Algorithm::RS256,
        "RS384" => Algorithm::RS384,
        "RS512" => Algorithm::RS512,
        "PS256" => Algorithm::PS256,
        "PS384" => Algorithm::PS384,
        "PS512" => Algorithm::PS512,
        "ES256" => Algorithm::ES256,
        "ES384" => Algorithm::ES384,
        "EdDSA" => Algorithm::EdDSA,
        _ => return None,
    })
}

/// Builder for [`JwtAuthenticator`].
pub struct JwtOptions {
    algorithm: Algorithm,
    encoding_key: Option<EncodingKey>,
    decoding_key: Option<DecodingKey>,
}

impl std::fmt::Debug for JwtOptions {
    /// Key material is never printed; only the algorithm is shown.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JwtOptions")
            .field("algorithm", &self.algorithm)
            .finish_non_exhaustive()
    }
}

impl Default for JwtOptions {
    /// The default: HS256, no keys.
    fn default() -> Self {
        Self {
            algorithm: Algorithm::HS256,
            encoding_key: None,
            decoding_key: None,
        }
    }
}

impl JwtOptions {
    /// Options with the HS256 default and no keys.
    pub fn new() -> Self {
        Self::default()
    }

    /// Selects the signing algorithm by name. Unknown names — and `none`,
    /// and `ES512`, which the underlying library lacks — reject with
    /// [`AuthnError::UnsupportedSigningMethod`].
    pub fn with_algorithm(mut self, name: &str) -> Result<Self, AuthnError> {
        self.algorithm = parse_algorithm(name).ok_or(AuthnError::UnsupportedSigningMethod)?;
        Ok(self)
    }

    /// A symmetric secret used for both minting and verification. Only
    /// meaningful for the HS family; an asymmetric algorithm with a
    /// secret-shaped key fails at sign time.
    pub fn with_key(mut self, secret: &[u8]) -> Self {
        self.encoding_key = Some(EncodingKey::from_secret(secret));
        self.decoding_key = Some(DecodingKey::from_secret(secret));
        self
    }

    /// A typed key for minting.
    pub fn with_encoding_key(mut self, key: EncodingKey) -> Self {
        self.encoding_key = Some(key);
        self
    }

    /// A typed key for verification.
    pub fn with_decoding_key(mut self, key: DecodingKey) -> Self {
        self.decoding_key = Some(key);
        self
    }

    /// Parses a PEM-encoded RSA private key for minting. A parse failure
    /// rejects with [`AuthnError::GetKeyFailed`] instead of leaving the
    /// engine silently unconfigured.
    pub fn with_rsa_private_key_from_pem(self, pem: &[u8]) -> Result<Self, AuthnError> {
        let key = EncodingKey::from_rsa_pem(pem).map_err(|_| AuthnError::GetKeyFailed)?;
        Ok(self.with_encoding_key(key))
    }

    /// Parses a PEM-encoded RSA public key for verification.
    pub fn with_rsa_public_key_from_pem(self, pem: &[u8]) -> Result<Self, AuthnError> {
        let key = DecodingKey::from_rsa_pem(pem).map_err(|_| AuthnError::GetKeyFailed)?;
        Ok(self.with_decoding_key(key))
    }

    /// Parses a PEM-encoded EC private key for minting.
    pub fn with_ec_private_key_from_pem(self, pem: &[u8]) -> Result<Self, AuthnError> {
        let key = EncodingKey::from_ec_pem(pem).map_err(|_| AuthnError::GetKeyFailed)?;
        Ok(self.with_encoding_key(key))
    }

    /// Parses a PEM-encoded EC public key for verification.
    pub fn with_ec_public_key_from_pem(self, pem: &[u8]) -> Result<Self, AuthnError> {
        let key = DecodingKey::from_ec_pem(pem).map_err(|_| AuthnError::GetKeyFailed)?;
        Ok(self.with_decoding_key(key))
    }

    /// Parses a PEM-encoded Ed25519 private key for minting.
    pub fn with_ed25519_private_key_from_pem(self, pem: &[u8]) -> Result<Self, AuthnError> {
        let key = EncodingKey::from_ed_pem(pem).map_err(|_| AuthnError::GetKeyFailed)?;
        Ok(self.with_encoding_key(key))
    }

    /// Parses a PEM-encoded Ed25519 public key for verification.
    pub fn with_ed25519_public_key_from_pem(self, pem: &[u8]) -> Result<Self, AuthnError> {
        let key = DecodingKey::from_ed_pem(pem).map_err(|_| AuthnError::GetKeyFailed)?;
        Ok(self.with_decoding_key(key))
    }
}

/// A JWT authenticator: sign-on-mint, verify-and-claims-on-validate.
pub struct JwtAuthenticator {
    options: JwtOptions,
}

impl JwtAuthenticator {
    /// Builds the engine from its options.
    pub fn new(options: JwtOptions) -> Self {
        Self { options }
    }

    /// The validation profile: exp/nbf validated when present, zero
    /// leeway, exp not required, algorithm pinned to the configured one.
    fn validation(&self) -> Validation {
        let mut validation = Validation::new(self.options.algorithm);
        validation.leeway = 0;
        validation.required_spec_claims.clear();
        validation.validate_exp = true;
        validation.validate_nbf = true;
        validation
    }
}

impl Authenticator for JwtAuthenticator {
    fn scheme(&self) -> &'static str {
        SCHEME_BEARER
    }

    fn authenticate_token(&self, token: &str) -> Result<AuthClaims, AuthnError> {
        // No decoding key configured → the keyfunc error.
        let Some(decoding_key) = &self.options.decoding_key else {
            return Err(AuthnError::MissingKeyFunc);
        };
        match jsonwebtoken::decode::<serde_json::Map<String, serde_json::Value>>(
            token,
            decoding_key,
            &self.validation(),
        ) {
            Ok(data) => Ok(AuthClaims(data.claims)),
            Err(err) => Err(match err.kind() {
                ErrorKind::ExpiredSignature => AuthnError::TokenExpired,
                ErrorKind::InvalidSignature => AuthnError::SignTokenFailed,
                ErrorKind::InvalidAlgorithm => AuthnError::UnsupportedSigningMethod,
                _ => AuthnError::InvalidToken,
            }),
        }
    }

    fn create_identity(&self, claims: &AuthClaims) -> Result<String, AuthnError> {
        // No signing key → the keyfunc error;
        // a signing failure → the sign failure.
        let Some(encoding_key) = &self.options.encoding_key else {
            return Err(AuthnError::MissingKeyFunc);
        };
        jsonwebtoken::encode(
            &Header::new(self.options.algorithm),
            &claims.0,
            encoding_key,
        )
        .map_err(|_| AuthnError::SignTokenFailed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rushwind_authn::{AuthnError, HEADER_AUTHORIZE, SCHEME_BEARER};

    // Throwaway key pairs generated with `openssl` at port time — test
    // fixtures, never used anywhere else.
    const RSA_PRIVATE_PEM: &str = "-----BEGIN PRIVATE KEY-----
MIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQDz8EnTCNH9z9OY
mR/K2V/k3CZ3chxBHUJVcm39tQ8YcYjZ60vtcj4+ngFmGrEE63Vt/KvXp6id5kp+
SVgyOJipqddF0IrXN1HFc/41xeMAQ3hz48u1ub+n386GWStNog3LK/2jn3smh4ma
yhI62iBecaRF5BZHKKVr9we+ah7UdjJXm7iFiuS++NK350QbkWCBJzun/BVT8pzS
+nWWb4Yb0bmsZY5pOVEJiv9ZO5CK7H7N+tDUMaVnUmrRk+oOEqM0D1yi6Cb0r2+G
at+Nwg9bKINfVcxJ3ozLEPP+//pRG6tNUf/gd02ZsdteJMvMB8luNShoP5g+T2Gc
0G72bqMLAgMBAAECggEABPYtOjqissvoXOE4cVLMjYYgzisnAfgLYluKey+UmAWv
J+eOSs0ZEQL7ukzurs/vOoZ6JE/HsTZ+62Sog5T9He5Tb5sXR9tbMW3zjLpyrI2y
xICsowydJlf6BmeH5vNV3n0NkqdqxNTa6qgTiNjo8aLUGYvTHC1qd/C1Wp0j9WRL
5VMraYzrnkdlnNLh/F+l2fx/6C4vAvbWovhkBiZLi0B67BDUKXvL/X6OAlrIM/MX
jhTPQ+17H3kz+M78v1sku5mVftROlUKNwryxLe6fQ8gZIIvnV7fPleKDK2xBGDvB
wwxJE3VxxNXwsNdYnxxdiy4GgocBjos0cabCuQZ3WQKBgQD7yS0fGkL87BPLpm04
fMdVHsPTxGorwHNNEk9OrPz86HnyBSMHJON9NeHIlKbtRURM7SQrUnLquDs/5UEK
OrUnsb5weoiSVqistK/+p+pnzKdZylp5y1TQIlIABM58zgytv0GZS9KVKwXXNpZS
XhqkkeyhZ+R6AIJwkhdpi5LQ5QKBgQD4BX0+03/CVg99tiinUmyqeGgJFCQGKbUp
YTcn9d6GmVDVLxXt8+AC0piZ4c/foHBnbSfQ9cpvN8QeDb93ekaclzBBiinrgZO4
ZI9MtXwCgociKQt5vqcP39hK/MzzJMqfS/2OYjoXDuVWZJ7mzxrq8wcZPF4su0Dw
4J/7qgGVLwKBgQC/mFB7kHJVIBfYKiaGi3zrauO9K4NXE7UpertatQw2L3lMD1ie
QRXS28OZ7HQxrTnSB8o0JSNJNTPw5TTe4dmkAP9XfAacxNDJyxz5fTFEF1lpXDAI
6g756oPXe9Dc67Z+KEF0s1vlIr3pDKLKvs0rWddk4zfbFrQrkR+7svffeQKBgGET
LPFRMLksnAWVLZZH8ZZLaFTdWDg9TNX0YfU3C7DdA0Fdm5S2FmCkcuwP8R/TGQuy
MppcCa68QfuNX/pwloClwFJ2tG+kGOBcI6ZfhjkpQ6EANaiiEZtp/qtjBQjJxrDQ
ul5nXds2jlbhLTyjpSJ+mrGq6iVR6VoeYR/Ma7ArAoGBAIqX7P/cilrl8aVpMFf2
hOoWHrnR30+yDbDaOVNAxXBTVEoo2w3hn4F5vCFCgaQ/758W6SQ2FA7m6na+Ny8o
+B0/hC+x77Qc7Q1D2aFmySMOiLjCSaddpl+aolu7yFl+8bOVT0Ltox554nrxAndv
9kEBRgW/GYltclF4DjX6fyC0
-----END PRIVATE KEY-----
";
    const RSA_PUBLIC_PEM: &str = "-----BEGIN PUBLIC KEY-----
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA8/BJ0wjR/c/TmJkfytlf
5Nwmd3IcQR1CVXJt/bUPGHGI2etL7XI+Pp4BZhqxBOt1bfyr16eoneZKfklYMjiY
qanXRdCK1zdRxXP+NcXjAEN4c+PLtbm/p9/OhlkrTaINyyv9o597JoeJmsoSOtog
XnGkReQWRyila/cHvmoe1HYyV5u4hYrkvvjSt+dEG5FggSc7p/wVU/Kc0vp1lm+G
G9G5rGWOaTlRCYr/WTuQiux+zfrQ1DGlZ1Jq0ZPqDhKjNA9cougm9K9vhmrfjcIP
WyiDX1XMSd6MyxDz/v/6URurTVH/4HdNmbHbXiTLzAfJbjUoaD+YPk9hnNBu9m6j
CwIDAQAB
-----END PUBLIC KEY-----
";
    const EC_PRIVATE_PEM: &str = "-----BEGIN PRIVATE KEY-----
MIGHAgEAMBMGByqGSM49AgEGCCqGSM49AwEHBG0wawIBAQQgxtuBALm+6LY7zzzv
rlXjgKtkuuN+aFXYY3WnJFF9ivShRANCAATDGyLKIv3my4JqXEZo3QYhKW01SbYJ
we+pPqbhanxW0m79Ab8aCs1vnd/OVwuFoTpz8l9AYRTR8czwUqmCD7g0
-----END PRIVATE KEY-----
";
    const EC_PUBLIC_PEM: &str = "-----BEGIN PUBLIC KEY-----
MFkwEwYHKoZIzj0CAQYIKoZIzj0DAQcDQgAEwxsiyiL95suCalxGaN0GISltNUm2
CcHvqT6m4Wp8VtJu/QG/GgrNb53fzlcLhaE6c/JfQGEU0fHM8FKpgg+4NA==
-----END PUBLIC KEY-----
";
    const ED25519_PRIVATE_PEM: &str = "-----BEGIN PRIVATE KEY-----
MC4CAQAwBQYDK2VwBCIEIFoBsLuwbbvUSohnokxBq46t0TbJreUa4uiqCL9Q2XAz
-----END PRIVATE KEY-----
";
    const ED25519_PUBLIC_PEM: &str = "-----BEGIN PUBLIC KEY-----
MCowBQYDK2VwAyEAzwQGegWNA/iKUVekymVdpsE8TorV48DjLobsnYP7VBI=
-----END PUBLIC KEY-----
";

    fn subject_claims(subject: &str) -> AuthClaims {
        let mut map = serde_json::Map::new();
        map.insert(
            rushwind_authn::CLAIM_FIELD_SUBJECT.to_string(),
            serde_json::Value::String(subject.to_string()),
        );
        AuthClaims(map)
    }

    fn hs_engine(key: &[u8]) -> JwtAuthenticator {
        JwtAuthenticator::new(JwtOptions::new().with_key(key))
    }

    fn auth_headers(token: &str) -> Vec<(String, String)> {
        vec![(
            HEADER_AUTHORIZE.to_string(),
            format!("{SCHEME_BEARER} {token}"),
        )]
    }

    #[test]
    fn hs256_is_the_default_algorithm() {
        // No with_algorithm call: the engine mints and verifies under
        // HS256, the default algorithm.
        let auth = hs_engine(b"default-secret");
        let token = auth.create_identity(&subject_claims("alice")).unwrap();
        let claims = auth.authenticate_token(&token).unwrap();
        assert_eq!(claims.get_subject().unwrap(), "alice");
    }

    #[test]
    fn hs512_round_trips() {
        let options = JwtOptions::new()
            .with_algorithm("HS512")
            .unwrap()
            .with_key(b"hs512-secret");
        let auth = JwtAuthenticator::new(options);
        let token = auth.create_identity(&subject_claims("bob")).unwrap();
        let claims = auth.authenticate_token(&token).unwrap();
        assert_eq!(claims.get_subject().unwrap(), "bob");
    }

    #[test]
    fn unknown_algorithm_names_reject_at_build_time() {
        for name in ["ES512", "none", "bogus", ""] {
            assert_eq!(
                JwtOptions::new().with_algorithm(name).unwrap_err(),
                AuthnError::UnsupportedSigningMethod,
                "algorithm {name}"
            );
        }
    }

    #[test]
    fn minting_without_a_key_is_missing_key_func() {
        let auth = JwtAuthenticator::new(JwtOptions::new());
        assert_eq!(
            auth.create_identity(&subject_claims("alice")).unwrap_err(),
            AuthnError::MissingKeyFunc
        );
    }

    #[test]
    fn verifying_without_a_key_is_missing_key_func() {
        let auth = JwtAuthenticator::new(JwtOptions::new());
        assert_eq!(
            auth.authenticate_token("x.y.z").unwrap_err(),
            AuthnError::MissingKeyFunc
        );
    }

    #[test]
    fn minted_claims_round_trip_with_custom_fields() {
        let auth = hs_engine(b"round-trip");
        let mut map = serde_json::Map::new();
        map.insert(
            rushwind_authn::CLAIM_FIELD_SUBJECT.to_string(),
            serde_json::Value::String("carol".to_string()),
        );
        map.insert("custom".to_string(), serde_json::Value::from(42));
        let token = auth.create_identity(&AuthClaims(map)).unwrap();
        let claims = auth.authenticate_token(&token).unwrap();
        assert_eq!(claims.get_subject().unwrap(), "carol");
        assert_eq!(claims.get_int("custom").unwrap(), 42);
    }

    #[test]
    fn tokens_without_exp_parse() {
        // required_spec_claims is cleared: a token with no exp parses,
        // exp is validated only when present.
        let auth = hs_engine(b"no-exp");
        let token = auth.create_identity(&subject_claims("dave")).unwrap();
        let claims = auth.authenticate_token(&token).unwrap();
        assert_eq!(claims.get_subject().unwrap(), "dave");
    }

    #[test]
    fn malformed_tokens_are_invalid() {
        let auth = hs_engine(b"k");
        for token in ["", "not-a-jwt", "aaa.bbb", "x.y.z.w"] {
            assert_eq!(
                auth.authenticate_token(token).unwrap_err(),
                AuthnError::InvalidToken,
                "token {token:?}"
            );
        }
    }

    #[test]
    fn foreign_keys_fail_the_signature_check() {
        let minter = hs_engine(b"key-a");
        let verifier = hs_engine(b"key-b");
        let token = minter.create_identity(&subject_claims("eve")).unwrap();
        assert_eq!(
            verifier.authenticate_token(&token).unwrap_err(),
            AuthnError::SignTokenFailed
        );
    }

    #[test]
    fn expired_tokens_expire() {
        let auth = hs_engine(b"exp-key");
        let mut map = serde_json::Map::new();
        map.insert(
            rushwind_authn::CLAIM_FIELD_EXPIRATION_TIME.to_string(),
            serde_json::Value::from(1),
        );
        let token = auth.create_identity(&AuthClaims(map)).unwrap();
        assert_eq!(
            auth.authenticate_token(&token).unwrap_err(),
            AuthnError::TokenExpired
        );
    }

    #[test]
    fn not_yet_valid_tokens_reject() {
        // nbf in the future; the variant is library-dependent, the
        // rejection is not.
        let auth = hs_engine(b"nbf-key");
        let mut map = serde_json::Map::new();
        map.insert(
            rushwind_authn::CLAIM_FIELD_NOT_BEFORE.to_string(),
            serde_json::Value::from(4_102_444_800u64),
        );
        let token = auth.create_identity(&AuthClaims(map)).unwrap();
        assert!(auth.authenticate_token(&token).is_err());
    }

    #[test]
    fn algorithm_mismatches_are_unsupported() {
        // Minted under HS256, verified by an HS384 engine: the alg
        // pinned in validation differs from the token's.
        let minter = hs_engine(b"shared");
        let verifier = JwtAuthenticator::new(
            JwtOptions::new()
                .with_algorithm("HS384")
                .unwrap()
                .with_key(b"shared"),
        );
        let token = minter.create_identity(&subject_claims("x")).unwrap();
        assert_eq!(
            verifier.authenticate_token(&token).unwrap_err(),
            AuthnError::UnsupportedSigningMethod
        );
    }

    #[test]
    fn authenticate_extracts_and_validates() {
        let auth = hs_engine(b"header-key");
        let token = auth.create_identity(&subject_claims("frank")).unwrap();
        let claims = auth.authenticate(&auth_headers(&token)).unwrap();
        assert_eq!(claims.get_subject().unwrap(), "frank");
    }

    #[test]
    fn authenticate_collapses_missing_credentials() {
        let auth = hs_engine(b"header-key");
        assert_eq!(
            auth.authenticate(&[]).unwrap_err(),
            AuthnError::MissingBearerToken
        );
    }

    #[test]
    fn pem_parse_failures_reject_at_build_time() {
        assert_eq!(
            JwtOptions::new()
                .with_rsa_private_key_from_pem(b"not a pem")
                .unwrap_err(),
            AuthnError::GetKeyFailed
        );
        assert_eq!(
            JwtOptions::new()
                .with_ec_public_key_from_pem(b"not a pem")
                .unwrap_err(),
            AuthnError::GetKeyFailed
        );
        assert_eq!(
            JwtOptions::new()
                .with_ed25519_private_key_from_pem(b"not a pem")
                .unwrap_err(),
            AuthnError::GetKeyFailed
        );
    }

    #[test]
    fn rsa_pem_pair_round_trips() {
        let options = JwtOptions::new()
            .with_algorithm("RS256")
            .unwrap()
            .with_rsa_private_key_from_pem(RSA_PRIVATE_PEM.as_bytes())
            .unwrap()
            .with_rsa_public_key_from_pem(RSA_PUBLIC_PEM.as_bytes())
            .unwrap();
        let auth = JwtAuthenticator::new(options);
        let token = auth.create_identity(&subject_claims("rsa")).unwrap();
        let claims = auth.authenticate_token(&token).unwrap();
        assert_eq!(claims.get_subject().unwrap(), "rsa");
    }

    #[test]
    fn ps256_round_trips_on_the_rsa_pair() {
        let options = JwtOptions::new()
            .with_algorithm("PS256")
            .unwrap()
            .with_rsa_private_key_from_pem(RSA_PRIVATE_PEM.as_bytes())
            .unwrap()
            .with_rsa_public_key_from_pem(RSA_PUBLIC_PEM.as_bytes())
            .unwrap();
        let auth = JwtAuthenticator::new(options);
        let token = auth.create_identity(&subject_claims("ps")).unwrap();
        let claims = auth.authenticate_token(&token).unwrap();
        assert_eq!(claims.get_subject().unwrap(), "ps");
    }

    #[test]
    fn ec_pem_pair_round_trips() {
        let options = JwtOptions::new()
            .with_algorithm("ES256")
            .unwrap()
            .with_ec_private_key_from_pem(EC_PRIVATE_PEM.as_bytes())
            .unwrap()
            .with_ec_public_key_from_pem(EC_PUBLIC_PEM.as_bytes())
            .unwrap();
        let auth = JwtAuthenticator::new(options);
        let token = auth.create_identity(&subject_claims("ec")).unwrap();
        let claims = auth.authenticate_token(&token).unwrap();
        assert_eq!(claims.get_subject().unwrap(), "ec");
    }

    #[test]
    fn ed25519_pem_pair_round_trips() {
        let options = JwtOptions::new()
            .with_algorithm("EdDSA")
            .unwrap()
            .with_ed25519_private_key_from_pem(ED25519_PRIVATE_PEM.as_bytes())
            .unwrap()
            .with_ed25519_public_key_from_pem(ED25519_PUBLIC_PEM.as_bytes())
            .unwrap();
        let auth = JwtAuthenticator::new(options);
        let token = auth.create_identity(&subject_claims("ed")).unwrap();
        let claims = auth.authenticate_token(&token).unwrap();
        assert_eq!(claims.get_subject().unwrap(), "ed");
    }

    #[test]
    fn the_public_pem_alone_cannot_mint() {
        let options = JwtOptions::new()
            .with_rsa_public_key_from_pem(RSA_PUBLIC_PEM.as_bytes())
            .unwrap();
        let auth = JwtAuthenticator::new(options);
        assert_eq!(
            auth.create_identity(&subject_claims("x")).unwrap_err(),
            AuthnError::MissingKeyFunc
        );
    }
}
