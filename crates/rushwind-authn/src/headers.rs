//! The `Authorization` header surface and the credential extraction built
//! on it.
//!
//! HTTP presents header names and scheme values in either case, so the
//! name and scheme comparisons here are
//! case-insensitive — they accept both shapes.

use crate::error::AuthnError;

/// The request header carrying credential material.
pub const HEADER_AUTHORIZE: &str = "Authorization";
/// The bearer credential scheme.
pub const SCHEME_BEARER: &str = "Bearer";
/// The RFC 7617 basic credential scheme.
pub const SCHEME_BASIC: &str = "Basic";
/// The digest credential scheme. No engine consumes it yet.
pub const SCHEME_DIGEST: &str = "Digest";

/// Locates the `Authorization` header among a request's header pairs,
/// case-insensitively.
fn authorization_header(headers: &[(String, String)]) -> Option<&str> {
    headers
        .iter()
        .find(|(name, _)| name.eq_ignore_ascii_case(HEADER_AUTHORIZE))
        .map(|(_, value)| value.as_str())
        .filter(|value| !value.is_empty())
}

/// Extracts the credential token from the `Authorization` header.
///
/// Errors map as follows:
///
/// - missing or empty header → [`AuthnError::Unauthenticated`];
/// - present but not `<scheme> <token>` → [`AuthnError::BadAuthorization`];
/// - scheme mismatch → [`AuthnError::Unauthenticated`].
///
/// Every engine's authenticate path collapses these to
/// [`AuthnError::MissingBearerToken`]; see
/// [`crate::Authenticator::authenticate`].
pub fn auth_from_headers(
    headers: &[(String, String)],
    expected_scheme: &str,
) -> Result<String, AuthnError> {
    let Some(value) = authorization_header(headers) else {
        return Err(AuthnError::Unauthenticated);
    };
    let Some((scheme, token)) = value.split_once(' ') else {
        return Err(AuthnError::BadAuthorization);
    };
    if !scheme.eq_ignore_ascii_case(expected_scheme) {
        return Err(AuthnError::Unauthenticated);
    }
    Ok(token.to_string())
}

/// Formats a scheme-qualified `Authorization` value. Placing the returned
/// value on a client request's headers is the caller's job.
pub fn format_authorization(scheme: &str, token: &str) -> String {
    format!("{scheme} {token}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extracts_bearer_tokens_case_insensitively() {
        let headers = vec![("authorization".to_string(), "Bearer abc.def".to_string())];
        assert_eq!(
            auth_from_headers(&headers, SCHEME_BEARER).unwrap(),
            "abc.def"
        );
    }

    #[test]
    fn extracts_basic_tokens() {
        let headers = vec![(
            "Authorization".to_string(),
            "Basic dXNlcjpwYXNz".to_string(),
        )];
        assert_eq!(
            auth_from_headers(&headers, SCHEME_BASIC).unwrap(),
            "dXNlcjpwYXNz"
        );
    }

    #[test]
    fn missing_header_is_unauthenticated() {
        assert_eq!(
            auth_from_headers(&[], SCHEME_BEARER).unwrap_err(),
            AuthnError::Unauthenticated
        );
        // A header name that is not Authorization at all.
        let headers = vec![("X-Other".to_string(), "Bearer x".to_string())];
        assert_eq!(
            auth_from_headers(&headers, SCHEME_BEARER).unwrap_err(),
            AuthnError::Unauthenticated
        );
    }

    #[test]
    fn malformed_header_is_bad_authorization() {
        let headers = vec![("Authorization".to_string(), "Bearer".to_string())];
        assert_eq!(
            auth_from_headers(&headers, SCHEME_BEARER).unwrap_err(),
            AuthnError::BadAuthorization
        );
    }

    #[test]
    fn scheme_mismatch_is_unauthenticated() {
        let headers = vec![("Authorization".to_string(), "Basic abc".to_string())];
        assert_eq!(
            auth_from_headers(&headers, SCHEME_BEARER).unwrap_err(),
            AuthnError::Unauthenticated
        );
    }

    #[test]
    fn format_authorization_joins_with_a_space() {
        assert_eq!(format_authorization(SCHEME_BEARER, "tok"), "Bearer tok");
    }
}
