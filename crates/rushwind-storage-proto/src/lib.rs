//! The Protobuf wire contract for RushWind storage queries — the proto face
//! of the [`Repository`](rushwind_storage::Repository) contract.
//!
//! The single source of truth is `proto/rushwind/storage/v1/query.proto` at
//! the repository root. Everything exported from the [`v1`] module is
//! **generated** from it — types by prost, the protojson JSON mapping by
//! pbjson — so the wire format cannot drift from the contract. Services in
//! different languages exchange byte-identical messages.
//!
//! The [`wire`] module converts generated types into the engine-side
//! contract types ([`FilterExpr`](rushwind_storage::FilterExpr),
//! [`ListQuery`](rushwind_storage::ListQuery), …) with the full 29-operator
//! taxonomy mapped: the relational subset lands on contract operators, the
//! Django-ORM-derived convenience operators (`icontains`, `istarts_with`,
//! `iends_with`) are derived into `ILIKE` patterns, and the tail the Rust
//! core does not implement (regexp, JSON/array operators, search) is
//! rejected with [`StorageError::Unsupported`] at the boundary — never
//! silently dropped.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

/// Generated protobuf types for `rushwind.storage.v1`.
pub mod v1 {
    // Generated code carries no doc comments and is not held to hand-written
    // lint standards; the contract prose lives in the .proto source and in
    // this crate's wire module.
    #![allow(missing_docs)]
    #![allow(clippy::all)]

    include!(concat!(env!("OUT_DIR"), "/rushwind.storage.v1.rs"));

    include!(concat!(env!("OUT_DIR"), "/rushwind.storage.v1.serde.rs"));
}

/// A feature-immune value-model deserializer for the protojson face —
/// see the module docs for why [`wire`]'s parses route through it.
mod cleanjson;

pub mod wire;
