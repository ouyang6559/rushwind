//! Contracts for RushWind storage engines.
//!
//! This crate intentionally contains **no implementations and no
//! dependencies** — only the [`Repository`] data-access contract and the
//! vocabulary types it is expressed in: dynamic [`Value`] scalars, a [`Schema`]
//! description, [`Record`] rows, the three paging strategies ([`Paging`]), the
//! nested [`FilterExpr`] query tree, [`Sort`] ordering, the [`Viewer`] tenancy
//! scope, and the [`Auditor`] cross-cutting hook. Concrete engines live in
//! sibling `rushwind-storage-*` crates and implement [`Repository`] against a
//! specific stack — the same shape the transport line uses for protocol stacks.
//!
//! The design mirrors the Go predecessor [go-crud](https://github.com/tx7do/go-crud):
//! one generic repository interface driving many engines, protobuf-grade
//! contract types (paging / filtering / sorting / field mask), viewer-scoped
//! tenancy, and a uniform audit trail. It is **not** a port: the dynamic
//! reflection of Go becomes an explicit schema + record protocol, and the
//! method surface is expressed with boxed futures so the trait stays
//! object-safe — matching the [`Server`](https://docs.rs/rushwind-transport)
//! contract's conventions in `rushwind-transport`.
//!
//! # Contract obligations
//!
//! An implementation of [`Repository`] must:
//!
//! - Enforce the [`QueryCtx::viewer`] scope on **every** path (get, list,
//!   count, update, upsert, delete); a row outside the viewer's scope is
//!   indistinguishable from a missing row.
//! - Backfill the primary key on [`Repository::create`] and return the full
//!   stored row from every mutating call.
//! - Translate [`FilterExpr`] faithfully — including nested `All`/`Any`
//!   groups — or reject it with [`StorageError::InvalidQuery`].
//! - Honor all three [`Paging`] strategies; `Token` paging orders by primary
//!   key ascending.
//! - Emit [`AuditEntry`] records through [`QueryCtx::audit`] when an auditor
//!   is attached.
//!
//! The `rushwind-testkit` crate ships `rushwind_storage_conformance_suite!`;
//! an engine is only conformant when the entire suite passes against it.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

pub mod auditor;
pub mod context;
pub mod error;
pub mod field_mask;
pub mod filter;
pub mod paging;
pub mod record;
pub mod repository;
pub mod schema;
pub mod sorting;
mod syntax;
pub mod value;
pub mod viewer;

pub use auditor::{AuditAction, AuditEntry, Auditor, NoopAuditor};
pub use context::QueryCtx;
pub use error::StorageError;
pub use field_mask::FieldMask;
pub use filter::{Condition, FilterExpr, FilterNode, Op};
pub use paging::{decode_cursor, encode_cursor, Page, Paging, MAX_LIMIT};
pub use record::Record;
pub use repository::{ListQuery, RepoFuture, Repository};
pub use schema::{Column, ColumnKind, Schema};
pub use sorting::{Sort, SortDir, SortField};
pub use value::Value;
pub use viewer::{DataRange, Scope, Viewer};
