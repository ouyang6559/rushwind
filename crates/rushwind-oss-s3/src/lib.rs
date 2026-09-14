//! S3 engine for the RushWind oss contract — SigV4 query-signed REST
//! over reqwest, using `rusty-s3` for the signing. Covers AWS S3,
//! MinIO, and any S3-compatible endpoint; the Go domain's `s3` and
//! `minio` clients collapse into this one engine because MinIO **is**
//! an S3-compatible server.
//!
//! # The wire behavior
//!
//! Publishes are PUT requests, reads GET, removals DELETE — the Go
//! `PutObject`/`GetObject` surface plus a `delete` addition. Query-
//! string signing (the presigned-URL form) keeps the request headers
//! out of the signature; the payload integrity is still enforced by
//! the required `x-amz-content-sha256` signing input rusty-s3 emits.
//!
//! Path-style addressing (`http://host/bucket/key`) suits MinIO and
//! local endpoints; virtual-host style (`http://bucket.host/key`)
//! suits AWS. Session tokens (`x-amz-security-token`) are supported
//! through rusty-s3's credentials.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

use std::time::Duration;

use rushwind_oss::BoxFuture;
use rushwind_oss::{ObjectStorage, StorageConfig, StorageError};
use rusty_s3::{Bucket, Credentials};
use rusty_s3::{S3Action, UrlStyle};

struct Inner {
    http: reqwest::Client,
    bucket: Bucket,
    credentials: Credentials,
}

/// An S3-compatible object storage engine.
pub struct S3Storage {
    inner: Arc<Inner>,
}

use std::sync::Arc;

/// How long a signed URL stays valid. Query signing has no session;
/// every request is signed fresh, so this only bounds clock skew.
const SIGNATURE_VALIDITY: Duration = Duration::from_secs(3600);

impl S3Storage {
    /// Builds an engine from the connection settings, validating the
    /// bucket — the Go `NewStorage` with the nil-config guard folded
    /// into a `Result`.
    pub fn new(config: StorageConfig) -> Result<Self, StorageError> {
        if config.bucket.is_empty() {
            return Err(StorageError::EmptyBucket);
        }
        let scheme = if config.use_ssl { "https" } else { "http" };
        let endpoint = format!("{scheme}://{}", config.endpoint.trim_end_matches('/'))
            .parse()
            .map_err(|e| StorageError::Failed(format!("s3 endpoint parse: {e}")))?;
        let url_style = if config.force_path_style {
            UrlStyle::Path
        } else {
            UrlStyle::VirtualHost
        };
        let bucket = Bucket::new(
            endpoint,
            url_style,
            config.bucket.clone(),
            config.region.clone(),
        )
        .map_err(|e| StorageError::Failed(format!("s3 bucket url: {e}")))?;
        let credentials = match &config.token {
            Some(token) => Credentials::new_with_token(
                config.access_key.clone(),
                config.secret_key.clone(),
                token.clone(),
            ),
            None => Credentials::new(config.access_key.clone(), config.secret_key.clone()),
        };
        Ok(Self {
            inner: Arc::new(Inner {
                http: reqwest::Client::new(),
                bucket,
                credentials,
            }),
        })
    }

    /// Constructs from the bootstrap factory's settings wire shape —
    /// the Go `s3.Config` fields, camelCase keys.
    pub fn from_settings(settings: serde_json::Value) -> Result<Self, StorageError> {
        let config: StorageConfig = serde_json::from_value(settings)
            .map_err(|e| StorageError::Failed(format!("settings parse: {e}")))?;
        Self::new(config)
    }

    /// Creates the bucket — a setup convenience beyond the Go
    /// surface, useful against empty MinIO instances.
    pub async fn create_bucket(&self) -> Result<(), StorageError> {
        let action = self.inner.bucket.create_bucket(&self.inner.credentials);
        let url = action.sign(SIGNATURE_VALIDITY);
        let response = reqwest::Client::new()
            .put(url)
            .send()
            .await
            .map_err(|e| StorageError::Failed(format!("s3 create bucket: {e}")))?;
        // 409 (already exists) is a successful no-op.
        if response.status().is_success() || response.status().as_u16() == 409 {
            Ok(())
        } else {
            Err(StorageError::Failed(format!(
                "s3 create bucket: HTTP {}",
                response.status().as_u16()
            )))
        }
    }

    fn validate_key(key: &str) -> Result<(), StorageError> {
        if key.is_empty() {
            Err(StorageError::EmptyObjectKey)
        } else {
            Ok(())
        }
    }
}

impl ObjectStorage for S3Storage {
    fn put<'a>(
        &'a self,
        key: &'a str,
        body: &'a [u8],
        content_type: Option<&'a str>,
    ) -> BoxFuture<'a, Result<(), StorageError>> {
        Box::pin(async move {
            Self::validate_key(key)?;
            if body.is_empty() {
                return Err(StorageError::EmptyObjectBody);
            }
            let mut action = self
                .inner
                .bucket
                .put_object(Some(&self.inner.credentials), key);
            if let Some(content_type) = content_type {
                action
                    .headers_mut()
                    .insert("content-type", content_type.to_string());
            }
            let url = action.sign(SIGNATURE_VALIDITY);
            let mut request = self.inner.http.put(url).body(body.to_vec());
            if let Some(content_type) = content_type {
                request = request.header(reqwest::header::CONTENT_TYPE, content_type);
            }
            let response = request
                .send()
                .await
                .map_err(|e| StorageError::Failed(format!("s3 put {key}: {e}")))?;
            if !response.status().is_success() {
                return Err(StorageError::Failed(format!(
                    "s3 put {key}: HTTP {}",
                    response.status().as_u16()
                )));
            }
            Ok(())
        })
    }

    fn get<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<Vec<u8>, StorageError>> {
        Box::pin(async move {
            Self::validate_key(key)?;
            let action = self
                .inner
                .bucket
                .get_object(Some(&self.inner.credentials), key);
            let url = action.sign(SIGNATURE_VALIDITY);
            let response = self
                .inner
                .http
                .get(url)
                .send()
                .await
                .map_err(|e| StorageError::Failed(format!("s3 get {key}: {e}")))?;
            if response.status().as_u16() == 404 {
                return Err(StorageError::NotFound);
            }
            if !response.status().is_success() {
                return Err(StorageError::Failed(format!(
                    "s3 get {key}: HTTP {}",
                    response.status().as_u16()
                )));
            }
            response
                .bytes()
                .await
                .map(|bytes| bytes.to_vec())
                .map_err(|e| StorageError::Failed(format!("s3 get {key}: {e}")))
        })
    }

    fn delete<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<(), StorageError>> {
        Box::pin(async move {
            Self::validate_key(key)?;
            let action = self
                .inner
                .bucket
                .delete_object(Some(&self.inner.credentials), key);
            let url = action.sign(SIGNATURE_VALIDITY);
            let response = self
                .inner
                .http
                .delete(url)
                .send()
                .await
                .map_err(|e| StorageError::Failed(format!("s3 delete {key}: {e}")))?;
            if !response.status().is_success() {
                return Err(StorageError::Failed(format!(
                    "s3 delete {key}: HTTP {}",
                    response.status().as_u16()
                )));
            }
            Ok(())
        })
    }
}

// The ObjectStorage methods need the health_check default removed in
// favor of a head_bucket probe when the engine has a bucket.
impl S3Storage {
    /// Head-bucket probe — the setup/liveness convenience pairing
    /// with [`S3Storage::create_bucket`].
    pub async fn head_bucket(&self) -> Result<(), StorageError> {
        let action = self.inner.bucket.head_bucket(Some(&self.inner.credentials));
        let url = action.sign(SIGNATURE_VALIDITY);
        let response = reqwest::Client::new()
            .head(url)
            .send()
            .await
            .map_err(|e| StorageError::Failed(format!("s3 head bucket: {e}")))?;
        if response.status().is_success() {
            Ok(())
        } else {
            Err(StorageError::Failed(format!(
                "s3 head bucket: HTTP {}",
                response.status().as_u16()
            )))
        }
    }
}
