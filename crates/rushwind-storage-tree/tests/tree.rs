//! Tree traversal over the in-memory reference engine: children, roots,
//! ancestor walks, subtree sweeps, cycle guards, and viewer scoping.

use std::sync::Arc;

use rushwind_storage::{
    Column, ColumnKind, QueryCtx, Record, Repository, Schema, StorageError, Value,
};
use rushwind_storage_memory::MemoryRepo;
use rushwind_storage_tree::Tree;

fn schema() -> Schema {
    let mut schema = rushwind_testkit::storage_conformance::suite_schema();
    schema.columns.push(Column {
        name: "parent_id".into(),
        kind: ColumnKind::Int,
    });
    schema
}

/// Plants the sample tree and returns the row ids in planting order:
///
/// ```text
/// 1 (root)
/// ├─ 2
/// │  └─ 4
/// └─ 3
/// ```
async fn planted() -> (Arc<MemoryRepo>, Vec<i64>) {
    let repo = Arc::new(MemoryRepo::new(schema()).expect("schema is valid"));
    let ctx = QueryCtx::all_access();
    let mut ids = Vec::new();
    for (id, parent) in [(1i64, None), (2, Some(1)), (3, Some(1)), (4, Some(2))] {
        let stored = repo
            .create(
                ctx.clone(),
                rushwind_testkit::storage_conformance::widget_with_id(id, "w", 1, None, 1, 1)
                    .set("parent_id", parent.map(Value::Int).unwrap_or(Value::Null)),
            )
            .await
            .expect("seed lands");
        ids.push(stored.get("id").and_then(Value::as_i64).expect("int id"));
    }
    (repo, ids)
}

fn tree(repo: &Arc<MemoryRepo>) -> Tree<'_> {
    Tree::new(repo.as_ref() as &dyn Repository)
}

fn id_of(row: &Record) -> i64 {
    row.get("id").and_then(Value::as_i64).expect("int id")
}

#[tokio::test]
async fn children_and_roots() {
    let (repo, ids) = planted().await;
    let ctx = QueryCtx::all_access();
    let tree = tree(&repo);

    let roots = tree.roots(&ctx).await.expect("roots succeed");
    assert_eq!(roots.len(), 1);
    assert_eq!(id_of(&roots[0]), ids[0]);

    let children = tree.children(&ctx, ids[0]).await.expect("children succeed");
    let child_ids: Vec<i64> = children.iter().map(id_of).collect();
    assert_eq!(child_ids, vec![ids[1], ids[2]]);

    // Leaves have none.
    assert!(tree
        .children(&ctx, ids[3])
        .await
        .expect("children succeed")
        .is_empty());
}

#[tokio::test]
async fn ancestors_walk_root_first() {
    let (repo, ids) = planted().await;
    let ctx = QueryCtx::all_access();
    let tree = tree(&repo);

    let chain = tree.ancestors(&ctx, ids[3]).await.expect("walk succeeds");
    let chain_ids: Vec<i64> = chain.iter().map(id_of).collect();
    assert_eq!(
        chain_ids,
        vec![ids[0], ids[1]],
        "root first, immediate parent last"
    );

    // A root has no ancestors.
    assert!(tree
        .ancestors(&ctx, ids[0])
        .await
        .expect("walk succeeds")
        .is_empty());
}

#[tokio::test]
async fn is_ancestor_is_strict() {
    let (repo, ids) = planted().await;
    let ctx = QueryCtx::all_access();
    let tree = tree(&repo);

    assert!(tree.is_ancestor(&ctx, ids[0], ids[3]).await.expect("walks"));
    assert!(tree.is_ancestor(&ctx, ids[1], ids[3]).await.expect("walks"));
    assert!(!tree.is_ancestor(&ctx, ids[3], ids[1]).await.expect("walks"));
    assert!(!tree.is_ancestor(&ctx, ids[2], ids[3]).await.expect("walks"));
    // A row is not its own ancestor.
    assert!(!tree.is_ancestor(&ctx, ids[2], ids[2]).await.expect("walks"));
}

#[tokio::test]
async fn subtree_sweeps_level_by_level() {
    let (repo, ids) = planted().await;
    let ctx = QueryCtx::all_access();
    let tree = tree(&repo);

    let whole = tree
        .subtree(&ctx, ids[0], None)
        .await
        .expect("sweep succeeds");
    let rendered: Vec<(i64, u32)> = whole
        .iter()
        .map(|node| (id_of(&node.record), node.depth))
        .collect();
    assert_eq!(rendered, vec![(ids[1], 1), (ids[2], 1), (ids[3], 2)]);

    let capped = tree
        .subtree(&ctx, ids[0], Some(1))
        .await
        .expect("capped sweep succeeds");
    assert_eq!(
        capped.len(),
        2,
        "a depth cap keeps the sweep to direct children"
    );

    let leaf = tree.subtree(&ctx, ids[3], None).await.expect("leaf sweep");
    assert!(leaf.is_empty());
}

#[tokio::test]
async fn parent_cycles_are_invalid_query_never_loops() {
    let (repo, ids) = planted().await;
    let ctx = QueryCtx::all_access();
    let tree = tree(&repo);

    // Corrupt the data: the root's parent becomes its own grandchild.
    repo.update(
        ctx.clone(),
        Value::Int(ids[0]),
        Record::new().set("parent_id", ids[3]),
    )
    .await
    .expect("corruption lands");

    let err = tree
        .ancestors(&ctx, ids[3])
        .await
        .expect_err("cycle must fail");
    assert!(matches!(err, StorageError::InvalidQuery(m) if m.contains("cycle")));
    let err = tree
        .subtree(&ctx, ids[0], None)
        .await
        .expect_err("cycle must fail");
    assert!(matches!(err, StorageError::InvalidQuery(m) if m.contains("cycle")));
}

#[tokio::test]
async fn dangling_parent_ids_end_the_walk_like_a_root() {
    let (repo, ids) = planted().await;
    let ctx = QueryCtx::all_access();
    let tree = tree(&repo);

    repo.update(
        ctx.clone(),
        Value::Int(ids[1]),
        Record::new().set("parent_id", 999),
    )
    .await
    .expect("dangling pointer lands");

    let chain = tree.ancestors(&ctx, ids[3]).await.expect("walk succeeds");
    assert_eq!(chain.len(), 1, "the missing parent ends the chain");
}

#[tokio::test]
async fn traversal_respects_the_viewer_scope() {
    let (repo, ids) = planted().await;
    // A default viewer denies everything (DataRange::None); the tree walks
    // are ordinary contract reads, so scoping holds through them.
    let denied = QueryCtx::default();
    let tree = tree(&repo);
    assert!(tree
        .children(&denied, ids[0])
        .await
        .expect("empty")
        .is_empty());
    assert!(tree
        .subtree(&denied, ids[0], None)
        .await
        .expect("empty")
        .is_empty());
    assert!(tree.roots(&denied).await.expect("empty").is_empty());
}
