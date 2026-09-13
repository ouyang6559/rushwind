//! ClickHouse engine for the RushWind storage contract.
//!
//! Work in progress: the offline SQL generation layer is complete (see
//! [`sql`]) — dialect-flavored DDL (MergeTree, `ORDER BY` the primary
//! key), INSERT, mutation-synced UPDATE/DELETE, and the SELECT shape —
//! unit-tested without a server. The HTTP transport and the
//! [`Repository`](rushwind_storage::Repository) implementation land on top
//! of it.
//!
//! [`sql`]: crate::sql

#![forbid(unsafe_code)]
#![deny(missing_docs)]

// The sql layer is complete and unit-tested; its consumers (the HTTP
// transport and the Repository implementation) are the next scaffold.
#[allow(dead_code)]
pub mod sql;
