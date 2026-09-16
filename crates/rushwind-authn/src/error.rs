//! The error taxonomy for authentication: one variant per failure mode
//! across all engines.
//!
//! Every variant carries an HTTP status (the semantic anchor of the
//! taxonomy) and a stable machine-readable
//! code, exposed through [`AuthnError::status`] and [`AuthnError::code`].
//! The status feeds [`crate::AuthenticationGate`]'s rejections; the code
//! is the stable surface for diagnostics — messages may change, codes do
//! not.
//!
//! Several variants (`InvalidJwtId`, `NoAtHash`, …) have no producing
//! engine yet: they are reserved for the future `oidc`/`mtls`/`oauth2`
//! engines and keep the taxonomy whole for middleware written against it.

use std::fmt;

/// An authentication failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum AuthnError {
    /// A claim value has an unexpected type.
    InvalidType,
    /// The `jti` claim is malformed.
    InvalidJwtId,
    /// The `sub` claim is malformed or missing where required.
    InvalidSubject,
    /// The `aud` claim is malformed.
    InvalidAudience,
    /// The `iss` claim is malformed.
    InvalidIssuer,
    /// The `exp` claim is malformed.
    InvalidExpiration,
    /// The `nbf` claim is malformed.
    InvalidNotBefore,
    /// The `iat` claim is malformed.
    InvalidIssuedAt,
    /// The claim payload is not a claim object.
    InvalidClaims,
    /// The credential is structurally invalid (malformed encoding,
    /// missing separators, undecodable payload).
    InvalidToken,
    /// No credential was presented where one is required.
    MissingBearerToken,
    /// The credential was presented but does not authenticate the
    /// subject.
    Unauthenticated,
    /// The credential is expired or not yet valid.
    TokenExpired,
    /// The signing method is not one this authenticator accepts.
    UnsupportedSigningMethod,
    /// No keying material is configured for sign or verify.
    MissingKeyFunc,
    /// Signing failed with the configured key.
    SignTokenFailed,
    /// Key retrieval or parsing failed.
    GetKeyFailed,
    /// An OIDC ID token lacks its access-token hash.
    NoAtHash,
    /// An OIDC ID token's access-token hash does not match.
    InvalidAtHash,
    /// The `Authorization` header is present but not in
    /// `<scheme> <token>` form.
    BadAuthorization,
}

impl AuthnError {
    /// The HTTP status anchored to this error: 401 for
    /// authentication failures, 500 for configuration failures.
    pub const fn status(&self) -> u16 {
        match self {
            Self::InvalidType
            | Self::UnsupportedSigningMethod
            | Self::MissingKeyFunc
            | Self::SignTokenFailed
            | Self::GetKeyFailed => 500,
            _ => 401,
        }
    }

    /// The stable machine-readable code for this error.
    pub const fn code(&self) -> &'static str {
        match self {
            Self::InvalidType => "AUTHN_INVALID_TYPE",
            Self::InvalidJwtId => "AUTHN_INVALID_JWT_ID",
            Self::InvalidSubject => "AUTHN_INVALID_SUBJECT",
            Self::InvalidAudience => "AUTHN_INVALID_AUDIENCE",
            Self::InvalidIssuer => "AUTHN_INVALID_ISSUER",
            Self::InvalidExpiration => "AUTHN_INVALID_EXPIRATION",
            Self::InvalidNotBefore => "AUTHN_INVALID_NOT_BEFORE",
            Self::InvalidIssuedAt => "AUTHN_INVALID_ISSUED_AT",
            Self::InvalidClaims => "AUTHN_INVALID_CLAIMS",
            Self::InvalidToken => "AUTHN_INVALID_TOKEN",
            Self::MissingBearerToken => "AUTHN_MISSING_BEARER_TOKEN",
            Self::Unauthenticated => "AUTHN_UNAUTHENTICATED",
            Self::TokenExpired => "AUTHN_TOKEN_EXPIRED",
            Self::UnsupportedSigningMethod => "AUTHN_UNSUPPORTED_SIGNING_METHOD",
            Self::MissingKeyFunc => "AUTHN_MISSING_KEY_FUNC",
            Self::SignTokenFailed => "AUTHN_SIGN_TOKEN_FAILED",
            Self::GetKeyFailed => "AUTHN_GET_KEY_FAILED",
            Self::NoAtHash => "AUTHN_NO_AT_HASH",
            Self::InvalidAtHash => "AUTHN_INVALID_AT_HASH",
            Self::BadAuthorization => "AUTHN_BAD_AUTHORIZATION",
        }
    }
}

impl fmt::Display for AuthnError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let msg = match self {
            Self::InvalidType => "invalid type",
            Self::InvalidJwtId => "invalid jwt id",
            Self::InvalidSubject => "invalid subject",
            Self::InvalidAudience => "invalid audience",
            Self::InvalidIssuer => "invalid issuer",
            Self::InvalidExpiration => "invalid expiration",
            Self::InvalidNotBefore => "invalid not before",
            Self::InvalidIssuedAt => "invalid issued at",
            Self::InvalidClaims => "invalid claims",
            Self::InvalidToken => "invalid bearer token",
            Self::MissingBearerToken => "missing bearer token",
            Self::Unauthenticated => "unauthenticated",
            Self::TokenExpired => "token expired",
            Self::UnsupportedSigningMethod => "unsupported signing method",
            Self::MissingKeyFunc => "missing key func",
            Self::SignTokenFailed => "sign token failed",
            Self::GetKeyFailed => "get key failed",
            Self::NoAtHash => "id token did not have an access token hash",
            Self::InvalidAtHash => "access token hash does not match value in ID token",
            Self::BadAuthorization => "bad authorization string",
        };
        write!(f, "{}: {}", self.code(), msg)
    }
}

impl std::error::Error for AuthnError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn taxonomy_statuses_are_stable() {
        for (err, status) in [
            (AuthnError::InvalidType, 500),
            (AuthnError::UnsupportedSigningMethod, 500),
            (AuthnError::MissingKeyFunc, 500),
            (AuthnError::SignTokenFailed, 500),
            (AuthnError::GetKeyFailed, 500),
            (AuthnError::InvalidJwtId, 401),
            (AuthnError::InvalidToken, 401),
            (AuthnError::MissingBearerToken, 401),
            (AuthnError::Unauthenticated, 401),
            (AuthnError::TokenExpired, 401),
            (AuthnError::BadAuthorization, 401),
        ] {
            assert_eq!(err.status(), status, "{err:?}");
        }
    }

    #[test]
    fn codes_are_stable_reason_strings() {
        assert_eq!(AuthnError::Unauthenticated.code(), "AUTHN_UNAUTHENTICATED");
        assert_eq!(
            AuthnError::BadAuthorization.code(),
            "AUTHN_BAD_AUTHORIZATION"
        );
    }

    #[test]
    fn display_leads_with_the_code() {
        assert_eq!(
            AuthnError::TokenExpired.to_string(),
            "AUTHN_TOKEN_EXPIRED: token expired"
        );
    }
}
