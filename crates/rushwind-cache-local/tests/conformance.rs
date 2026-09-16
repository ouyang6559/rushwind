//! The cache contract conformance suite, shared by every engine: the
//! same functions run against the local engine here and against the
//! redis engine's live suite. Each function pins one slice of the
//! cache contract semantics.

#![cfg(test)]

use std::time::Duration;

use rushwind_cache::{Cache, Item};
use rushwind_cache_local::LocalCache;

fn cache() -> LocalCache {
    LocalCache::new()
}

/// Set then Get round trips the bytes; a missing key reads as None.
#[tokio::test]
async fn set_then_get() {
    let cache = cache();
    cache
        .set("user:1", b"alice".as_slice(), None)
        .await
        .expect("set must succeed");
    assert_eq!(
        cache.get("user:1").await.expect("get must succeed"),
        Some(b"alice".to_vec())
    );
    assert_eq!(
        cache.get("missing").await.expect("get must succeed"),
        None,
        "missing keys read as None"
    );
}

/// A TTL'd entry expires: reads treat it as missing, and `has` agrees.
#[tokio::test]
async fn ttl_expires_entries() {
    let cache = cache();
    cache
        .set(
            "user:ttl",
            b"gone-soon".as_slice(),
            Some(Duration::from_millis(50)),
        )
        .await
        .expect("set must succeed");
    assert!(cache.has("user:ttl").await.expect("has must succeed"));

    tokio::time::sleep(Duration::from_millis(120)).await;
    assert_eq!(
        cache.get("user:ttl").await.expect("get must succeed"),
        None,
        "expired entries read as None"
    );
    assert!(
        !cache.has("user:ttl").await.expect("has must succeed"),
        "expired keys fail has"
    );
}

/// SetNX sets only when absent — the distributed-lock primitive.
#[tokio::test]
async fn set_nx_sets_only_when_absent() {
    let cache = cache();
    let first = cache
        .set_nx("lock:demo", b"holder-1".as_slice(), None)
        .await
        .expect("set_nx must succeed");
    assert!(first, "the first SetNX acquires");

    let second = cache
        .set_nx("lock:demo", b"holder-2".as_slice(), None)
        .await
        .expect("set_nx must succeed");
    assert!(!second, "the second SetNX fails while held");
    assert_eq!(
        cache.get("lock:demo").await.expect("get must succeed"),
        Some(b"holder-1".to_vec()),
        "the original value survives"
    );
}

/// The default TTL applies when an operation passes no TTL.
#[tokio::test]
async fn default_ttl_applies() {
    let cache = LocalCache::with_options(rushwind_cache_local::CacheOptions {
        default_ttl: Some(Duration::from_millis(50)),
        ..rushwind_cache_local::CacheOptions::default()
    });
    cache
        .set("user:default", b"x".as_slice(), None)
        .await
        .expect("set must succeed");
    tokio::time::sleep(Duration::from_millis(120)).await;
    assert_eq!(
        cache.get("user:default").await.expect("get must succeed"),
        None,
        "the default TTL expires the entry"
    );
}

/// GetMulti and SetMulti align with the input order; missing keys are
/// None entries, not errors.
#[tokio::test]
async fn batches_round_trip() {
    let cache = cache();
    cache
        .set_multi(&[
            Item {
                key: "batch:a".to_string(),
                value: b"1".to_vec(),
                ttl: None,
            },
            Item {
                key: "batch:b".to_string(),
                value: b"2".to_vec(),
                ttl: None,
            },
        ])
        .await
        .expect("set_multi must succeed");

    let values = cache
        .get_multi(&[
            "batch:a".to_string(),
            "batch:missing".to_string(),
            "batch:b".to_string(),
        ])
        .await
        .expect("get_multi must succeed");
    assert_eq!(values.len(), 3);
    assert_eq!(values[0].as_deref(), Some(&b"1"[..]));
    assert_eq!(values[1], None);
    assert_eq!(values[2].as_deref(), Some(&b"2"[..]));
}

/// Delete removes the entry; deleting a missing key is a no-op.
#[tokio::test]
async fn delete_removes_and_is_a_noop_when_missing() {
    let cache = cache();
    cache
        .set("user:del", b"x".as_slice(), None)
        .await
        .expect("set must succeed");
    cache.delete("user:del").await.expect("delete must succeed");
    assert_eq!(cache.get("user:del").await.expect("get must succeed"), None);
    cache
        .delete("user:never-existed")
        .await
        .expect("delete of a missing key is a no-op");
}

/// Reaching the capacity evicts — expired entries first, then the
/// oldest insertions — without losing freshly written entries.
#[tokio::test]
async fn capacity_evicts() {
    let cache = LocalCache::with_options(rushwind_cache_local::CacheOptions {
        capacity: 8,
        ..rushwind_cache_local::CacheOptions::default()
    });
    for index in 0..20u32 {
        cache
            .set(&format!("evict:{index}"), b"x".as_slice(), None)
            .await
            .expect("set must succeed");
    }
    // The newest entries survive eviction.
    assert_eq!(
        cache.get("evict:19").await.expect("get must succeed"),
        Some(b"x".to_vec())
    );
    let count = cache
        .get_multi(
            (0..20u32)
                .map(|i| format!("evict:{i}"))
                .collect::<Vec<_>>()
                .as_slice(),
        )
        .await
        .expect("get_multi must succeed")
        .into_iter()
        .flatten()
        .count();
    assert!(count <= 8, "the cache holds at most the capacity: {count}");
}

/// Close clears the local engine's entries.
#[tokio::test]
async fn close_clears() {
    let cache = cache();
    cache
        .set("user:close", b"x".as_slice(), None)
        .await
        .expect("set must succeed");
    cache.close().await.expect("close must succeed");
    assert_eq!(
        cache.get("user:close").await.expect("get must succeed"),
        None,
        "close clears the local entries"
    );
}
