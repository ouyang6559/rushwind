//! Object storage contract for RushWind, extracted from the Go
//! predecessor `go-wind-plugins/oss`: put/get/delete of byte objects
//! in an S3-compatible bucket.
//!
//! # The Go shapes, translated
//!
//! The Go domain ships two thin clients — `s3` (aws-sdk-go-v2) and
//! `minio` (minio-go) — both speaking the same S3-compatible API.
//! Rust keeps one contract, [`ObjectStorage`], and one engine shape:
//! an S3-compatible client covers MinIO transparently (MinIO **is**
//! an S3-compatible server), so the two Go backends collapse into
//! `rushwind-oss-s3`.
//!
//! Go's nil-check error sentinels (`ErrNilClient`, `ErrNilObjectBody`)
//! have no Rust equivalent — the type system makes nil impossible;
//! the remaining sentinels live on as [`StorageError`] variants
//! (`EmptyBucket`, `EmptyObjectKey`) plus `NotFound` for a missing
//! object read.
//!
//! # Engines
//!
//! - `rushwind-oss-s3` — SigV4-signed REST over reqwest; works
//!   against AWS S3, MinIO, and any S3-compatible endpoint.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

use std::future::Future;
use std::pin::Pin;

/// Future type used across the oss contract.
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Errors surfaced by object storage engines.
#[derive(Debug)]
#[non_exhaustive]
pub enum StorageError {
    /// The engine could not complete the operation.
    Failed(String),
    /// The object key does not exist.
    NotFound,
    /// The configured bucket is empty — the Go `ErrEmptyBucket`.
    EmptyBucket,
    /// The object key is empty — the Go `ErrEmptyObjectKey`.
    EmptyObjectKey,
    /// The object body is empty where content was required — the Go
    /// `ErrNilObjectBody`.
    EmptyObjectBody,
}

impl std::fmt::Display for StorageError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Failed(msg) => write!(f, "oss operation failed: {msg}"),
            Self::NotFound => write!(f, "oss: object not found"),
            Self::EmptyBucket => write!(f, "oss: bucket is empty"),
            Self::EmptyObjectKey => write!(f, "oss: object key is empty"),
            Self::EmptyObjectBody => write!(f, "oss: object body is empty"),
        }
    }
}

impl std::error::Error for StorageError {}

/// The connection settings — the Go `s3.Config`, byte-compatible in
/// shape.
#[derive(Debug, Clone, Default, serde::Deserialize)]
pub struct StorageConfig {
    /// The S3-compatible endpoint host (without scheme), e.g.
    /// `127.0.0.1:9100`.
    pub endpoint: String,
    /// The SigV4 region (e.g. `us-east-1`).
    pub region: String,
    /// The access key id.
    pub access_key: String,
    /// The secret access key.
    pub secret_key: String,
    /// The optional session token (STS).
    pub token: Option<String>,
    /// Whether to speak HTTPS. Default: false (HTTP).
    pub use_ssl: bool,
    /// Whether to address the bucket in the path
    /// (`http://host/bucket/key`) instead of the virtual host
    /// (`http://bucket.host/key`). Required for MinIO and local
    /// endpoints. Default: false.
    pub force_path_style: bool,
    /// The bucket every operation targets.
    pub bucket: String,
}

/// The object storage contract — the Go `s3.Storage` surface.
/// Engines must be callable through shared references (`&self`).
pub trait ObjectStorage: Send + Sync {
    /// Stores the object bytes under `key`, with an optional content
    /// type — the Go `PutObject`.
    fn put<'a>(
        &'a self,
        key: &'a str,
        body: &'a [u8],
        content_type: Option<&'a str>,
    ) -> BoxFuture<'a, Result<(), StorageError>>;

    /// Reads the object bytes for `key`; `NotFound` when the object
    /// does not exist — the Go `GetObject`.
    fn get<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<Vec<u8>, StorageError>>;

    /// Removes the object; a no-op when missing. An addition beyond
    /// the Go surface.
    fn delete<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<(), StorageError>>;

    /// Probes the engine with a timeout bound for liveness checks —
    /// the default implementation is a no-op.
    fn health_check(&self) -> BoxFuture<'_, Result<(), StorageError>> {
        Box::pin(async move { Ok(()) })
    }
}
