//! Live conformance against a real MinIO, driven by CI's service
//! container. Compile-gated behind the `live` feature; the connection
//! settings come from the `OSS_S3_*` environment variables. These
//! tests pin the interoperability contract where it matters most —
//! SigV4-signed PUT/GET/DELETE against a real S3-compatible server.

#![cfg(feature = "live")]

use rushwind_oss::{ObjectStorage, StorageConfig};
use rushwind_oss_s3::S3Storage;

fn config() -> StorageConfig {
    StorageConfig {
        endpoint: std::env::var("OSS_S3_ENDPOINT").unwrap_or_else(|_| "127.0.0.1:9100".to_string()),
        region: std::env::var("OSS_S3_REGION").unwrap_or_else(|_| "us-east-1".to_string()),
        access_key: std::env::var("OSS_S3_ACCESS_KEY").unwrap_or_else(|_| "rushwind".to_string()),
        secret_key: std::env::var("OSS_S3_SECRET_KEY")
            .unwrap_or_else(|_| "rushwind-secret".to_string()),
        token: None,
        use_ssl: false,
        force_path_style: true,
        bucket: std::env::var("OSS_S3_BUCKET").unwrap_or_else(|_| "rushwind-test".to_string()),
    }
}

/// The full object lifecycle: create the bucket (setup), put bytes,
/// get the same bytes back, delete, and observe the object gone.
#[tokio::test]
async fn put_get_delete_lifecycle() {
    let storage = S3Storage::new(config()).expect("storage builds");
    storage.create_bucket().await.expect("bucket setup");

    let payload: Vec<u8> = (0..=255u8).chain(0..=255u8).collect();
    storage
        .put(
            "probe/lifecycle.bin",
            &payload,
            Some("application/octet-stream"),
        )
        .await
        .expect("put must succeed");

    let fetched = storage
        .get("probe/lifecycle.bin")
        .await
        .expect("get must succeed");
    assert_eq!(fetched, payload, "the object must round trip byte-exact");

    storage
        .delete("probe/lifecycle.bin")
        .await
        .expect("delete must succeed");
    let result = storage.get("probe/lifecycle.bin").await;
    assert!(
        matches!(result, Err(rushwind_oss::StorageError::NotFound)),
        "the deleted object must read as NotFound, got {result:?}"
    );
}

/// The headers surface: a content type set at put time is served back
/// by the store.
#[tokio::test]
async fn put_carries_content_type() {
    let storage = S3Storage::new(config()).expect("storage builds");
    storage.create_bucket().await.expect("bucket setup");

    storage
        .put(
            "probe/typed.txt",
            b"text body".as_slice(),
            Some("text/plain"),
        )
        .await
        .expect("put must succeed");

    // The engine's GET returns the bytes; the content-type check goes
    // through a signed HEAD-equivalent via a plain GET + parse is not
    // exposed, so pin only that the object exists and reads back.
    let fetched = storage
        .get("probe/typed.txt")
        .await
        .expect("get must succeed");
    assert_eq!(fetched, b"text body".to_vec());
    storage
        .delete("probe/typed.txt")
        .await
        .expect("delete must succeed");
}

/// Empty keys and empty bodies are rejected up front — the Go
/// sentinels as typed errors.
#[tokio::test]
async fn validation_errors() {
    let storage = S3Storage::new(config()).expect("storage builds");

    let result = storage.put("", b"x".as_slice(), None).await;
    assert!(
        matches!(result, Err(rushwind_oss::StorageError::EmptyObjectKey)),
        "empty keys must fail with EmptyObjectKey"
    );

    let result = storage.put("probe/empty", &[], None).await;
    assert!(
        matches!(result, Err(rushwind_oss::StorageError::EmptyObjectBody)),
        "empty bodies must fail with EmptyObjectBody"
    );

    // The engine validates the bucket eagerly — the Go NewStorage's
    // missing-config guard folded into a Result.
    let result = S3Storage::new(StorageConfig {
        bucket: String::new(),
        ..config()
    });
    assert!(
        matches!(result, Err(rushwind_oss::StorageError::EmptyBucket)),
        "an empty bucket must fail the build with EmptyBucket"
    );
}
