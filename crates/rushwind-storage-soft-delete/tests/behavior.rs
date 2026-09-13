//! Behavioral tests for the soft-delete decorator beyond contract
//! transparency: tombstone semantics, restore/purge, and the
//! deleted-rows-are-invisible doctrine on every path.

use std::sync::Arc;

use rushwind_storage::{
    Column, ColumnKind, FilterExpr, ListQuery, QueryCtx, Record, Repository, Schema, StorageError,
    Value,
};
use rushwind_storage_memory::MemoryRepo;
use rushwind_storage_soft_delete::{SoftDeleteRepo, DELETED_AT_COLUMN};

fn schema() -> Schema {
    let mut schema = rushwind_testkit::storage_conformance::suite_schema();
    schema.columns.push(Column {
        name: DELETED_AT_COLUMN.into(),
        kind: ColumnKind::Int,
    });
    schema
}

async fn repo() -> (Arc<SoftDeleteRepo>, Arc<MemoryRepo>) {
    let inner = MemoryRepo::new(schema()).expect("schema is valid");
    let inner = Arc::new(inner);
    let decorator = SoftDeleteRepo::new(Arc::clone(&inner) as Arc<dyn Repository>)
        .expect("schema carries the tombstone column");
    (decorator, inner)
}

/// Seeds one row and returns its id.
async fn seed(repo: &SoftDeleteRepo, name: &str) -> i64 {
    let stored = repo
        .create(QueryCtx::all_access(), Record::new().set("name", name))
        .await
        .expect("create lands");
    stored.get("id").and_then(Value::as_i64).expect("int id")
}

#[tokio::test]
async fn delete_is_a_tombstone_not_a_removal() {
    let (repo, inner) = repo().await;
    let id = seed(&repo, "ghost").await;

    repo.delete(QueryCtx::all_access(), Value::Int(id))
        .await
        .expect("delete lands");

    // Invisible through the decorator, alive underneath it.
    let visible = repo
        .get(QueryCtx::all_access(), Value::Int(id))
        .await
        .expect("get succeeds");
    assert!(visible.is_none(), "a tombstoned row must be invisible");
    let raw = inner
        .get(QueryCtx::all_access(), Value::Int(id))
        .await
        .expect("inner get succeeds")
        .expect("the row is only tombstoned");
    assert!(
        matches!(raw.get(DELETED_AT_COLUMN), Some(Value::Int(_))),
        "delete must write a timestamp, got: {raw:?}"
    );
}

#[tokio::test]
async fn tombstoned_rows_leave_every_read_path() {
    let (repo, _inner) = repo().await;
    let gone = seed(&repo, "gone").await;
    let stays = seed(&repo, "stays").await;
    repo.delete(QueryCtx::all_access(), Value::Int(gone))
        .await
        .expect("delete lands");

    let count = repo
        .count(QueryCtx::all_access(), None)
        .await
        .expect("count succeeds");
    assert_eq!(count, 1, "tombstones must not be counted");

    let listed = repo
        .list(QueryCtx::all_access(), &ListQuery::page(1, 10))
        .await
        .expect("list succeeds");
    assert_eq!(listed.total, 1);
    assert_eq!(
        listed.items[0].get("id").and_then(Value::as_i64),
        Some(stays)
    );

    // Even a filter that would match the deleted row finds nothing.
    let matching = ListQuery::page(1, 10).filtered(FilterExpr::cond(
        "name",
        rushwind_storage::Op::Eq,
        [Value::Text("gone".into())],
    ));
    let page = repo
        .list(QueryCtx::all_access(), &matching)
        .await
        .expect("list succeeds");
    assert!(page.items.is_empty());
    assert_eq!(page.total, 0);
}

#[tokio::test]
async fn update_and_second_delete_on_a_tombstone_are_not_found() {
    let (repo, _inner) = repo().await;
    let id = seed(&repo, "frozen").await;
    repo.delete(QueryCtx::all_access(), Value::Int(id))
        .await
        .expect("delete lands");

    let err = repo
        .update(
            QueryCtx::all_access(),
            Value::Int(id),
            Record::new().set("age", 1),
        )
        .await
        .expect_err("mutating a tombstone must fail");
    assert_eq!(err, StorageError::NotFound);

    let err = repo
        .delete(QueryCtx::all_access(), Value::Int(id))
        .await
        .expect_err("deleting a tombstone again must fail");
    assert_eq!(err, StorageError::NotFound);
}

#[tokio::test]
async fn restore_clears_the_tombstone() {
    let (repo, _inner) = repo().await;
    let id = seed(&repo, "phoenix").await;
    repo.delete(QueryCtx::all_access(), Value::Int(id))
        .await
        .expect("delete lands");

    let restored = repo
        .restore_async(&QueryCtx::all_access(), Value::Int(id))
        .await
        .expect("restore lands");
    assert_eq!(
        restored.get(DELETED_AT_COLUMN),
        Some(&Value::Null),
        "restore must clear the tombstone"
    );
    let visible = repo
        .get(QueryCtx::all_access(), Value::Int(id))
        .await
        .expect("get succeeds");
    assert!(visible.is_some(), "a restored row is visible again");

    // Restoring an alive (or missing) row is NotFound — only tombstones
    // are restorable.
    let err = repo
        .restore_async(&QueryCtx::all_access(), Value::Int(id))
        .await
        .expect_err("restoring an alive row must fail");
    assert_eq!(err, StorageError::NotFound);
}

#[tokio::test]
async fn purge_removes_the_row_for_real() {
    let (repo, inner) = repo().await;
    let id = seed(&repo, "dust").await;
    repo.delete(QueryCtx::all_access(), Value::Int(id))
        .await
        .expect("delete lands");

    repo.purge_async(&QueryCtx::all_access(), Value::Int(id))
        .await
        .expect("purge lands");

    let raw = inner
        .get(QueryCtx::all_access(), Value::Int(id))
        .await
        .expect("inner get succeeds");
    assert!(raw.is_none(), "purge must remove the row from the engine");
}

#[tokio::test]
async fn upsert_resurrects_a_tombstoned_id_with_fresh_data() {
    let (repo, _inner) = repo().await;
    let id = seed(&repo, "old").await;
    repo.delete(QueryCtx::all_access(), Value::Int(id))
        .await
        .expect("delete lands");

    let revived = repo
        .upsert(
            QueryCtx::all_access(),
            rushwind_testkit::storage_conformance::widget_with_id(id, "new", 9, None, 1, 1),
        )
        .await
        .expect("upsert resurrects");
    assert_eq!(
        revived.get(DELETED_AT_COLUMN),
        Some(&Value::Null),
        "upsert writes into the visible world"
    );
    assert_eq!(revived.get("name").and_then(Value::as_str), Some("new"));

    let total = repo
        .count(QueryCtx::all_access(), None)
        .await
        .expect("count succeeds");
    assert_eq!(total, 1);
}

#[tokio::test]
async fn created_rows_are_forced_alive() {
    let (repo, inner) = repo().await;
    // Even a payload that tries to arrive pre-tombstoned lands alive.
    let stored = repo
        .create(
            QueryCtx::all_access(),
            Record::new()
                .set("name", "alive")
                .set(DELETED_AT_COLUMN, 123i64),
        )
        .await
        .expect("create lands");
    assert_eq!(stored.get(DELETED_AT_COLUMN), Some(&Value::Null));
    let raw = inner
        .get(
            QueryCtx::all_access(),
            stored
                .get("id")
                .and_then(Value::as_i64)
                .map(Value::Int)
                .expect("id"),
        )
        .await
        .expect("inner get")
        .expect("row exists");
    assert_eq!(raw.get(DELETED_AT_COLUMN), Some(&Value::Null));
}

#[tokio::test]
async fn list_deleted_is_the_inverse_view() {
    let (repo, _inner) = repo().await;
    let gone = seed(&repo, "deleted-one").await;
    seed(&repo, "alive-one").await;
    repo.delete(QueryCtx::all_access(), Value::Int(gone))
        .await
        .expect("delete lands");

    let deleted = repo
        .list_deleted_async(&QueryCtx::all_access(), &ListQuery::page(1, 10))
        .await
        .expect("list_deleted succeeds");
    assert_eq!(deleted.items.len(), 1);
    assert_eq!(
        deleted.items[0].get("id").and_then(Value::as_i64),
        Some(gone)
    );
}

#[tokio::test]
async fn a_schema_without_the_tombstone_column_is_rejected() {
    let inner = MemoryRepo::new(rushwind_testkit::storage_conformance::suite_schema())
        .expect("schema is valid");
    let err = SoftDeleteRepo::new(Arc::new(inner)).expect_err("no deleted_at column");
    assert!(matches!(err, StorageError::InvalidQuery(_)));
}
