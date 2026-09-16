//! Wire-in syntax parsers for the query contract.
//!
//! Two faces, one tree:
//!
//! - **protojson** — the canonical wire format, generated from
//!   `proto/rushwind/storage/v1/query.proto`; lives in
//!   `rushwind-storage-proto` (prost types + pbjson serde), which converts
//!   into this crate's contract types.
//! - **AIP text** — Google AIP-160-style filter strings
//!   (`name = "bolt" AND age >= 3`); parsed here,
//!   dependency-free, into the same [`FilterExpr`] tree.

pub(crate) mod text;
