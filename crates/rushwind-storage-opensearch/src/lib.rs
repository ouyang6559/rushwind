//! The OpenSearch engine for the [`Repository`](rushwind_storage::Repository)
//! contract.
//!
//! OpenSearch is the Elasticsearch 7.10 fork, and the wire shape the
//! contract uses — `_doc` CRUD, `_bulk`, `_search` bool queries, wildcard
//! with `case_insensitive`, `_count` — is identical in both. The adapter
//! is therefore the Elasticsearch one against a different endpoint;
//! `OpenSearchRepo` is a transparent rename that keeps the "one engine
//! crate per database" promise of the workspace layout.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

/// The Elasticsearch wire adapter, reused verbatim for OpenSearch.
pub use rushwind_storage_elasticsearch::{query, ElasticRepo};

/// An [`ElasticRepo`](rushwind_storage_elasticsearch::ElasticRepo) pointed
/// at an OpenSearch endpoint.
pub type OpenSearchRepo = ElasticRepo;

/// Convenience: connects exactly like the Elasticsearch adapter.
pub async fn connect(
    endpoint: impl Into<String>,
    schema: rushwind_storage::Schema,
) -> std::sync::Arc<dyn rushwind_storage::Repository> {
    std::sync::Arc::new(ElasticRepo::new(endpoint, schema))
}
