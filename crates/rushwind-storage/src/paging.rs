//! The three paging strategies and the result envelope.
//!
//! go-crud ships `Page` (page number + size), `Offset`, and `Token`
//! (cursor / infinite scroll) requests; the contract keeps all three. The
//! cursor codec lives here too so every engine reads and writes the same
//! opaque token format.

use crate::error::StorageError;

/// The upper bound an engine must enforce on any page size or limit.
pub const MAX_LIMIT: u32 = 1_000;

/// How a list query positions itself in the result stream.
///
/// The default is page 1 with size 20 — the same default page size go-crud's
/// proto ships.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Paging {
    /// Page-number paging: 1-based page plus page size.
    Page {
        /// 1-based page number.
        page: u32,
        /// Number of rows per page.
        size: u32,
    },
    /// Raw window paging.
    Offset {
        /// Rows to skip.
        offset: u64,
        /// Rows to return.
        limit: u32,
    },
    /// Cursor paging for infinite scroll: empty `token` starts the stream,
    /// each [`Page::next_token`] continues it. Rows always stream in primary
    /// key ascending order; combining `Token` with an explicit [`crate::Sort`]
    /// is an [`StorageError::InvalidQuery`].
    Token {
        /// The opaque cursor from the previous page, empty to start.
        token: String,
        /// Rows to return.
        limit: u32,
    },
}

impl Default for Paging {
    fn default() -> Self {
        Paging::Page { page: 1, size: 20 }
    }
}

impl Paging {
    /// The maximum number of rows this strategy may return.
    pub fn limit(&self) -> u32 {
        match self {
            Paging::Page { size, .. } | Paging::Offset { limit: size, .. } => {
                (*size).min(MAX_LIMIT)
            }
            Paging::Token { limit, .. } => (*limit).min(MAX_LIMIT),
        }
    }

    /// Validates the bounds: page/size must be positive, limits must not
    /// exceed [`MAX_LIMIT`].
    pub fn validate(&self) -> Result<(), StorageError> {
        match self {
            Paging::Page { page, size } => {
                if *page == 0 {
                    return Err(StorageError::InvalidQuery(
                        "page numbers are 1-based; page 0 is invalid".into(),
                    ));
                }
                if *size == 0 || *size > MAX_LIMIT {
                    return Err(StorageError::InvalidQuery(format!(
                        "page size must be within 1..={MAX_LIMIT}"
                    )));
                }
            }
            Paging::Offset { limit, .. } => {
                if *limit == 0 || *limit > MAX_LIMIT {
                    return Err(StorageError::InvalidQuery(format!(
                        "offset limit must be within 1..={MAX_LIMIT}"
                    )));
                }
            }
            Paging::Token { limit, .. } => {
                if *limit == 0 || *limit > MAX_LIMIT {
                    return Err(StorageError::InvalidQuery(format!(
                        "token limit must be within 1..={MAX_LIMIT}"
                    )));
                }
            }
        }
        Ok(())
    }
}

/// The list-result envelope: one page of items, the total row count of the
/// filtered set, and — for [`Paging::Token`] streams — the cursor to the
/// next page (`None` when the stream is exhausted).
#[derive(Clone, Debug, PartialEq)]
pub struct Page<T> {
    /// The rows of this page.
    pub items: Vec<T>,
    /// Total rows matching the query, independent of paging.
    pub total: u64,
    /// Continuation cursor, only produced by `Token` paging.
    pub next_token: Option<String>,
}

/// Encodes a cursor: the last-seen primary key, URL-safe-base64, unpadded.
pub fn encode_cursor(last_id: i64) -> String {
    let bytes = last_id.to_be_bytes();
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
    let mut out = String::with_capacity(11);
    for chunk in bytes.chunks(3) {
        let b = [
            chunk[0],
            *chunk.get(1).unwrap_or(&0),
            *chunk.get(2).unwrap_or(&0),
        ];
        let n = (u32::from(b[0]) << 16) | (u32::from(b[1]) << 8) | u32::from(b[2]);
        let quad = [
            (n >> 18) as usize & 63,
            (n >> 12) as usize & 63,
            (n >> 6) as usize & 63,
            n as usize & 63,
        ];
        let emit = (chunk.len() + 1).min(4);
        for idx in quad.iter().take(emit) {
            out.push(ALPHABET[*idx] as char);
        }
    }
    out
}

/// Decodes a cursor produced by [`encode_cursor`].
pub fn decode_cursor(token: &str) -> Result<i64, StorageError> {
    const ALPHABET: &str = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
    if token.is_empty() {
        return Err(StorageError::InvalidQuery("empty cursor token".into()));
    }
    let mut bytes = Vec::with_capacity(token.len() * 3 / 4);
    for chunk in token.as_bytes().chunks(4) {
        let mut n: u32 = 0;
        for (i, &c) in chunk.iter().enumerate() {
            let v = ALPHABET
                .as_bytes()
                .iter()
                .position(|&a| a == c)
                .ok_or_else(|| {
                    StorageError::InvalidQuery(format!("cursor token has byte {c:#04x}"))
                })? as u32;
            n |= v << (18 - 6 * i);
        }
        // No-pad base64: 4 chars carry 3 bytes, 3 chars carry 2, 2 carry 1.
        for i in 0..chunk.len() - 1 {
            bytes.push((n >> (16 - 8 * i)) as u8);
        }
    }
    let arr: [u8; 8] = bytes.try_into().map_err(|_| {
        StorageError::InvalidQuery("cursor token does not decode to a primary key".into())
    })?;
    Ok(i64::from_be_bytes(arr))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cursor_roundtrips_all_id_shapes() {
        for id in [0, 1, 42, 100_000, i64::MAX, i64::MIN, -1, -99_999] {
            assert_eq!(decode_cursor(&encode_cursor(id)).expect("roundtrip"), id);
        }
    }

    #[test]
    fn cursor_tokens_are_url_safe_and_unpadded() {
        let token = encode_cursor(123_456_789);
        assert!(token
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_'));
    }

    #[test]
    fn garbage_tokens_are_invalid_query_errors() {
        let err = decode_cursor("!!").expect_err("must reject");
        assert!(matches!(err, StorageError::InvalidQuery(_)));
        let err = decode_cursor("").expect_err("must reject");
        assert!(matches!(err, StorageError::InvalidQuery(_)));
    }

    #[test]
    fn paging_bounds_are_validated() {
        assert!(Paging::Page { page: 1, size: 10 }.validate().is_ok());
        assert!(Paging::Page { page: 0, size: 10 }.validate().is_err());
        assert!(Paging::Offset {
            offset: 5,
            limit: 0
        }
        .validate()
        .is_err());
        assert!(Paging::Token {
            token: String::new(),
            limit: MAX_LIMIT + 1
        }
        .validate()
        .is_err());
    }
}
