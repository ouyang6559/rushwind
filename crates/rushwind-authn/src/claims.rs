//! The claim bag and its typed accessors.
//!
//! [`AuthClaims`] is an untyped JSON object carried by a credential, with
//! typed getters layered on top. The getters share one uniform parsing
//! contract — including its permissive parts:
//!
//! - a missing key yields the type's zero value, never an error;
//! - `null` yields a zero value for the numeric getters but
//!   [`AuthnError::InvalidType`] for the string getters (a null short-
//!   circuits the numeric getters to zero but is an invalid type for the
//!   string getters);
//! - numeric getters convert through the JSON number representation with
//!   unchecked-cast wrapping semantics (Rust's float-to-int casts
//!   saturate).
//!
//! The date getters flatten numeric dates to raw Unix seconds.

use serde_json::Value;

use crate::error::AuthnError;

/// The JWT issuer claim: the principal that issued the token.
pub const CLAIM_FIELD_ISSUER: &str = "iss";
/// The JWT subject claim: the principal the token is about.
pub const CLAIM_FIELD_SUBJECT: &str = "sub";
/// The JWT audience claim: the recipients the token is intended for.
pub const CLAIM_FIELD_AUDIENCE: &str = "aud";
/// The JWT expiration claim: Unix seconds after which the token is invalid.
pub const CLAIM_FIELD_EXPIRATION_TIME: &str = "exp";
/// The JWT not-before claim: Unix seconds before which the token is invalid.
pub const CLAIM_FIELD_NOT_BEFORE: &str = "nbf";
/// The JWT issued-at claim: Unix seconds at which the token was issued.
pub const CLAIM_FIELD_ISSUED_AT: &str = "iat";
/// The JWT ID claim: a unique identifier for the token.
pub const CLAIM_FIELD_JWT_ID: &str = "jti";
/// The OAuth scope claim: the access scope of the token
/// ([RFC 6749 § 3.3](https://datatracker.ietf.org/doc/html/rfc6749#section-3.3)).
pub const CLAIM_FIELD_SCOPE: &str = "scope";

/// A bag of claims attached to a credential.
///
/// The claim bag is a plain JSON object — the natural carrier for untyped
/// claims. The field is public for direct construction.
#[derive(Debug, Clone, Default)]
pub struct AuthClaims(pub serde_json::Map<String, Value>);

impl AuthClaims {
    /// An empty claim bag.
    pub fn new() -> Self {
        Self::default()
    }

    /// The `jti` claim.
    pub fn get_jwt_id(&self) -> Result<String, AuthnError> {
        self.parse_string(CLAIM_FIELD_JWT_ID)
    }

    /// The `exp` claim as Unix seconds; `None` when absent or zero (an
    /// absent or zero date carries no timestamp).
    pub fn get_expiration_time(&self) -> Result<Option<i64>, AuthnError> {
        self.parse_numeric_date(CLAIM_FIELD_EXPIRATION_TIME)
    }

    /// The `nbf` claim as Unix seconds; `None` when absent or zero.
    pub fn get_not_before(&self) -> Result<Option<i64>, AuthnError> {
        self.parse_numeric_date(CLAIM_FIELD_NOT_BEFORE)
    }

    /// The `iat` claim as Unix seconds; `None` when absent or zero.
    pub fn get_issued_at(&self) -> Result<Option<i64>, AuthnError> {
        self.parse_numeric_date(CLAIM_FIELD_ISSUED_AT)
    }

    /// The `aud` claim: a single string becomes a one-element list.
    pub fn get_audience(&self) -> Result<Vec<String>, AuthnError> {
        self.parse_claim_strings(CLAIM_FIELD_AUDIENCE)
    }

    /// The `iss` claim.
    pub fn get_issuer(&self) -> Result<String, AuthnError> {
        self.parse_string(CLAIM_FIELD_ISSUER)
    }

    /// The `sub` claim.
    pub fn get_subject(&self) -> Result<String, AuthnError> {
        self.parse_string(CLAIM_FIELD_SUBJECT)
    }

    /// The `scope` claim: a single string becomes a one-element list.
    pub fn get_scopes(&self) -> Result<Vec<String>, AuthnError> {
        self.parse_claim_strings(CLAIM_FIELD_SCOPE)
    }

    /// A string-valued claim under an arbitrary key.
    pub fn get_string(&self, key: &str) -> Result<String, AuthnError> {
        self.parse_string(key)
    }

    /// A list-valued claim under an arbitrary key; a single string becomes
    /// a one-element list.
    pub fn get_strings(&self, key: &str) -> Result<Vec<String>, AuthnError> {
        self.parse_claim_strings(key)
    }

    /// A claim-list claim under an arbitrary key; the same parsing as
    /// [`AuthClaims::get_strings`].
    pub fn get_claim_strings(&self, key: &str) -> Result<Vec<String>, AuthnError> {
        self.parse_claim_strings(key)
    }

    /// String parsing: missing key or string → value; `null` or any
    /// other JSON type → [`AuthnError::InvalidType`].
    fn parse_string(&self, key: &str) -> Result<String, AuthnError> {
        match self.0.get(key) {
            None => Ok(String::new()),
            Some(Value::String(s)) => Ok(s.clone()),
            Some(_) => Err(AuthnError::InvalidType),
        }
    }

    /// Claim-list parsing: missing key → empty list; string →
    /// one-element list; array of strings → list; array containing a
    /// non-string → [`AuthnError::InvalidType`]; any other JSON type →
    /// empty list.
    fn parse_claim_strings(&self, key: &str) -> Result<Vec<String>, AuthnError> {
        match self.0.get(key) {
            Some(Value::String(s)) => Ok(vec![s.clone()]),
            Some(Value::Array(arr)) => arr
                .iter()
                .map(|v| match v {
                    Value::String(s) => Ok(s.clone()),
                    _ => Err(AuthnError::InvalidType),
                })
                .collect(),
            _ => Ok(vec![]),
        }
    }

    /// Numeric date parsing: missing key → `None`; a JSON number →
    /// its integer part (truncated toward zero) unless zero, which
    /// yields `None`; every other JSON type falls into
    /// [`AuthnError::InvalidType`].
    fn parse_numeric_date(&self, key: &str) -> Result<Option<i64>, AuthnError> {
        match self.0.get(key) {
            None => Ok(None),
            Some(Value::Number(n)) => {
                let secs = n
                    .as_i64()
                    .or_else(|| n.as_u64().and_then(|u| i64::try_from(u).ok()))
                    .or_else(|| n.as_f64().map(|f| f.trunc() as i64));
                match secs {
                    None | Some(0) => Ok(None),
                    Some(secs) => Ok(Some(secs)),
                }
            }
            Some(_) => Err(AuthnError::InvalidType),
        }
    }

    /// Numeric parsing: `None` for a missing key or a
    /// JSON `null`, the number for a JSON
    /// number, [`AuthnError::InvalidType`] for every other JSON type.
    fn raw_number(&self, key: &str) -> Result<Option<&serde_json::Number>, AuthnError> {
        match self.0.get(key) {
            Some(Value::Number(n)) => Ok(Some(n)),
            Some(Value::Null) | None => Ok(None),
            Some(_) => Err(AuthnError::InvalidType),
        }
    }
}

macro_rules! numeric_getter {
    ($name:ident, $ty:ty) => {
        /// Numeric conversion under an arbitrary key: missing
        /// key or `null` → zero, JSON number → cast to the getter's type,
        /// any other JSON type → [`AuthnError::InvalidType`].
        pub fn $name(&self, key: &str) -> Result<$ty, AuthnError> {
            let Some(n) = self.raw_number(key)? else {
                return Ok(0 as $ty);
            };
            if let Some(i) = n.as_i64() {
                return Ok(i as $ty);
            }
            if let Some(u) = n.as_u64() {
                return Ok(u as $ty);
            }
            if let Some(f) = n.as_f64() {
                return Ok(f as $ty);
            }
            Ok(0 as $ty)
        }
    };
}

impl AuthClaims {
    numeric_getter!(get_int, i64);
    numeric_getter!(get_int8, i8);
    numeric_getter!(get_int16, i16);
    numeric_getter!(get_int32, i32);
    numeric_getter!(get_int64, i64);
    numeric_getter!(get_uint, u64);
    numeric_getter!(get_uint8, u8);
    numeric_getter!(get_uint16, u16);
    numeric_getter!(get_uint32, u32);
    numeric_getter!(get_uint64, u64);
    numeric_getter!(get_float32, f32);
    numeric_getter!(get_float64, f64);
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::error::AuthnError;

    fn claims(key: &str, value: serde_json::Value) -> AuthClaims {
        let mut map = serde_json::Map::new();
        map.insert(key.to_string(), value);
        AuthClaims(map)
    }

    #[test]
    fn string_getter_returns_value_missing_and_wraps() {
        let c = claims("k", json!("value"));
        assert_eq!(c.get_string("k").unwrap(), "value");
        // Missing key: empty string, no error.
        assert_eq!(c.get_string("absent").unwrap(), "");
        assert_eq!(c.get_subject().unwrap(), "");
    }

    #[test]
    fn string_getter_rejects_non_strings() {
        assert_eq!(
            claims("k", json!(1)).get_string("k").unwrap_err(),
            AuthnError::InvalidType
        );
        // Unlike the numeric getters, the string getter rejects null.
        assert_eq!(
            claims("k", Value::Null).get_string("k").unwrap_err(),
            AuthnError::InvalidType
        );
        assert_eq!(
            claims(CLAIM_FIELD_ISSUER, json!([1]))
                .get_issuer()
                .unwrap_err(),
            AuthnError::InvalidType
        );
    }

    #[test]
    fn claim_strings_accept_string_and_string_array() {
        let single = claims("k", json!("solo"));
        assert_eq!(single.get_claim_strings("k").unwrap(), vec!["solo"]);
        let arr = claims("k", json!(["a", "b"]));
        assert_eq!(arr.get_claim_strings("k").unwrap(), vec!["a", "b"]);
        let scopes = claims(CLAIM_FIELD_SCOPE, json!("read write"));
        assert_eq!(scopes.get_scopes().unwrap(), vec!["read write"]);
    }

    #[test]
    fn claim_strings_reject_arrays_containing_non_strings() {
        assert_eq!(
            claims("k", json!(["a", 1]))
                .get_claim_strings("k")
                .unwrap_err(),
            AuthnError::InvalidType
        );
    }

    #[test]
    fn claim_strings_tolerate_missing_and_non_arrays() {
        let c = claims("k", json!("x"));
        assert!(c.get_claim_strings("absent").unwrap().is_empty());
        // A JSON number is neither string nor string array: empty list, no error.
        assert!(claims("k", json!(1))
            .get_claim_strings("k")
            .unwrap()
            .is_empty());
        assert!(claims("k", json!(1)).get_audience().unwrap().is_empty());
    }

    #[test]
    fn numeric_getter_reads_numbers_in_every_representation() {
        assert_eq!(claims("k", json!(7)).get_int("k").unwrap(), 7);
        assert_eq!(claims("k", json!(7)).get_uint8("k").unwrap(), 7);
        assert_eq!(claims("k", json!(7.9)).get_int64("k").unwrap(), 7);
        assert_eq!(claims("k", json!(-3.5)).get_int32("k").unwrap(), -3);
        assert_eq!(claims("k", json!(2)).get_float64("k").unwrap(), 2.0);
    }

    #[test]
    fn numeric_getter_zeroes_missing_and_null() {
        let c = claims("k", json!(9));
        assert_eq!(c.get_int("absent").unwrap(), 0);
        // JSON null → zero, no error.
        assert_eq!(claims("k", Value::Null).get_int("k").unwrap(), 0);
        assert_eq!(claims("k", Value::Null).get_float64("k").unwrap(), 0.0);
    }

    #[test]
    fn numeric_getter_rejects_non_numbers() {
        assert_eq!(
            claims("k", json!("7")).get_int("k").unwrap_err(),
            AuthnError::InvalidType
        );
        assert_eq!(
            claims("k", json!([7])).get_uint16("k").unwrap_err(),
            AuthnError::InvalidType
        );
    }

    #[test]
    fn numeric_date_reads_whole_seconds_and_zeroes_zero() {
        assert_eq!(
            claims(CLAIM_FIELD_EXPIRATION_TIME, json!(1_900_000_000))
                .get_expiration_time()
                .unwrap(),
            Some(1_900_000_000)
        );
        assert_eq!(
            claims(CLAIM_FIELD_NOT_BEFORE, json!(0))
                .get_not_before()
                .unwrap(),
            None
        );
        assert_eq!(claims("k", json!(1)).get_issued_at().unwrap(), None);
        assert_eq!(
            claims(CLAIM_FIELD_EXPIRATION_TIME, json!("x"))
                .get_expiration_time()
                .unwrap_err(),
            AuthnError::InvalidType
        );
    }
}
