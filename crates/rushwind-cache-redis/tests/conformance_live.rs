//! Live conformance against a real Redis, driven by CI's service
//! container. Compile-gated behind the `live` feature; the URL comes
//! from `CACHE_REDIS_URL`. The functions mirror the local engine's
//! conformance suite so both engines pin the same contract.

#![cfg(feature = "live")]

use std::time::Duration;

use rushwind_cache::{Cache, Item};
use rushwind_cache_redis::RedisCache;

async fn cache() -> RedisCache {
    RedisCache::connect_with(
        &std::env::var("CACHE_REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".to_string()),
        "rushwind-test:",
    )
    .await
    .expect("redis connects")
}

/// Set then Get round trips the bytes; a missing key reads as None.
#[tokio::test]
async fn set_then_get() {
    let cache = cache().await;
    cache
        .set("user:1", b"alice".as_slice(), None)
        .await
        .expect("set must succeed");
    assert_eq!(
        cache.get("user:1").await.expect("get must succeed"),
        Some(b"alice".to_vec())
    );
    assert_eq!(cache.get("missing").await.expect("get must succeed"), None);
    // The key prefix namespaces the key: the raw Redis key differs.
    cache.delete("user:1").await.expect("delete must succeed");
}

/// A TTL'd entry expires; SET with EX carries it.
#[tokio::test]
async fn ttl_expires_entries() {
    let cache = cache().await;
    cache
        .set(
            "user:ttl",
            b"gone-soon".as_slice(),
            Some(Duration::from_millis(200)),
        )
        .await
        .expect("set must succeed");
    assert!(cache.has("user:ttl").await.expect("has must succeed"));
    tokio::time::sleep(Duration::from_millis(400)).await;
    assert_eq!(
        cache.get("user:ttl").await.expect("get must succeed"),
        None,
        "expired entries read as None"
    );
}

/// SetNX sets only when absent — Redis's native atomic SET NX.
#[tokio::test]
async fn set_nx_sets_only_when_absent() {
    let cache = cache().await;
    let first = cache
        .set_nx(
            "lock:live",
            b"holder-1".as_slice(),
            Some(Duration::from_secs(30)),
        )
        .await
        .expect("set_nx must succeed");
    assert!(first, "the first SetNX acquires");
    let second = cache
        .set_nx(
            "lock:live",
            b"holder-2".as_slice(),
            Some(Duration::from_secs(30)),
        )
        .await
        .expect("set_nx must succeed");
    assert!(!second, "the second SetNX fails while held");
    cache
        .delete("lock:live")
        .await
        .expect("delete must succeed");
}

/// GetMulti uses native MGET; SetMulti pipelines the SETs; missing
/// keys read as None entries aligned with the input.
#[tokio::test]
async fn batches_round_trip() {
    let cache = cache().await;
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

    // Prefixed keys are visible through a raw connection — the key
    // prefix namespaces them, live.
    let raw = redis::Client::open(
        std::env::var("CACHE_REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".to_string()),
    )
    .expect("raw client opens");
    let mut raw_connection = raw
        .get_multiplexed_async_connection()
        .await
        .expect("raw connection");
    let raw_value: Option<String> = redis::cmd("GET")
        .arg("rushwind-test:batch:a")
        .query_async(&mut raw_connection)
        .await
        .expect("raw get works");
    assert_eq!(
        raw_value.as_deref(),
        Some("1"),
        "the prefix namespaces keys"
    );
}
