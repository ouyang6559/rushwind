//! Observability decorator for the [`Repository`] contract.
//!
//! Every call through [`ObservedRepo`] runs inside a `tracing` span named
//! `rushwind.storage` carrying `table`, `op`, and an `outcome` field
//! (`ok` / `missing` / `error`) filled after the call lands. The decorator
//! is deliberately **tracing-only**: exporting to OpenTelemetry is a
//! subscriber decision (`tracing-opentelemetry` + your collector), not a
//! hard dependency of the storage line — RushWind ships bricks, and the
//! observability stack is the user's baseplate.
//!
//! Wrap once at assembly time:
//!
//! ```ignore
//! let repo: Arc<dyn Repository> = Arc::new(ObservedRepo::new(inner));
//! ```
//!
//! and every engine underneath — in-memory, SQL, MongoDB, plus the cache
//! and soft-delete decorators — inherits identical instrumentation.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

use std::sync::Arc;

use tracing::Instrument;

use rushwind_storage::{
    FilterExpr, ListQuery, Page, QueryCtx, Record, RepoFuture, Repository, StorageError, Value,
};

/// A [`Repository`] decorator emitting a `tracing` span per call.
pub struct ObservedRepo {
    inner: Arc<dyn Repository>,
}

impl ObservedRepo {
    /// Wraps `inner` with per-call spans.
    pub fn new(inner: Arc<dyn Repository>) -> Arc<Self> {
        Arc::new(Self { inner })
    }

    fn span(&self, op: &'static str) -> tracing::Span {
        tracing::info_span!(
            "rushwind.storage",
            table = %self.inner.schema().table,
            op,
            outcome = tracing::field::Empty,
        )
    }
}

fn label<T>(outcome: &Result<T, StorageError>) -> &'static str {
    match outcome {
        Ok(_) => "ok",
        Err(StorageError::NotFound) => "missing",
        Err(_) => "error",
    }
}

fn option_label(outcome: &Result<Option<Record>, StorageError>) -> &'static str {
    match outcome {
        Ok(Some(_)) => "ok",
        Ok(None) => "missing",
        Err(StorageError::NotFound) => "missing",
        Err(_) => "error",
    }
}

impl Repository for ObservedRepo {
    fn schema(&self) -> &rushwind_storage::Schema {
        self.inner.schema()
    }

    fn get(&self, ctx: QueryCtx, id: Value) -> RepoFuture<'_, Option<Record>> {
        let span = self.span("get");
        let span_for_record = span.clone();
        Box::pin(
            async move {
                let outcome = self.inner.get(ctx, id).await;
                span_for_record.record("outcome", option_label(&outcome));
                outcome
            }
            .instrument(span),
        )
    }

    fn list<'a>(&'a self, ctx: QueryCtx, query: &'a ListQuery) -> RepoFuture<'a, Page<Record>> {
        let span = self.span("list");
        let span_for_record = span.clone();
        Box::pin(
            async move {
                let outcome = self.inner.list(ctx, query).await;
                span_for_record.record("outcome", label(&outcome));
                outcome
            }
            .instrument(span),
        )
    }

    fn count(&self, ctx: QueryCtx, filter: Option<FilterExpr>) -> RepoFuture<'_, u64> {
        let span = self.span("count");
        let span_for_record = span.clone();
        Box::pin(
            async move {
                let outcome = self.inner.count(ctx, filter).await;
                span_for_record.record("outcome", label(&outcome));
                outcome
            }
            .instrument(span),
        )
    }

    fn create(&self, ctx: QueryCtx, row: Record) -> RepoFuture<'_, Record> {
        let span = self.span("create");
        let span_for_record = span.clone();
        Box::pin(
            async move {
                let outcome = self.inner.create(ctx, row).await;
                span_for_record.record("outcome", label(&outcome));
                outcome
            }
            .instrument(span),
        )
    }

    fn batch_create(&self, ctx: QueryCtx, rows: Vec<Record>) -> RepoFuture<'_, Vec<Record>> {
        let span = self.span("batch_create");
        let span_for_record = span.clone();
        Box::pin(
            async move {
                let outcome = self.inner.batch_create(ctx, rows).await;
                span_for_record.record("outcome", label(&outcome));
                outcome
            }
            .instrument(span),
        )
    }

    fn update(&self, ctx: QueryCtx, id: Value, patch: Record) -> RepoFuture<'_, Record> {
        let span = self.span("update");
        let span_for_record = span.clone();
        Box::pin(
            async move {
                let outcome = self.inner.update(ctx, id, patch).await;
                span_for_record.record("outcome", label(&outcome));
                outcome
            }
            .instrument(span),
        )
    }

    fn upsert(&self, ctx: QueryCtx, row: Record) -> RepoFuture<'_, Record> {
        let span = self.span("upsert");
        let span_for_record = span.clone();
        Box::pin(
            async move {
                let outcome = self.inner.upsert(ctx, row).await;
                span_for_record.record("outcome", label(&outcome));
                outcome
            }
            .instrument(span),
        )
    }

    fn delete(&self, ctx: QueryCtx, id: Value) -> RepoFuture<'_, ()> {
        let span = self.span("delete");
        let span_for_record = span.clone();
        Box::pin(
            async move {
                let outcome = self.inner.delete(ctx, id).await;
                span_for_record.record("outcome", label(&outcome));
                outcome
            }
            .instrument(span),
        )
    }
}
