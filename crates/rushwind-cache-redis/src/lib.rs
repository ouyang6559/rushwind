//! Redis engine for the RushWind cache contract — the Go
//! `go-wind-plugins/cache/redis` ported onto the `redis` crate.
//!
//! # The wire behavior
//!
//! Identical to the Go adapter's: GET / SET / SETNX / DEL / EXISTS
//! for the single-key operations, native MGET for
//! [`Cache::get_multi`] (one round-trip regardless of key count) and
//! a pipelined batch of SETs for [`Cache::set_multi`]. Values are raw
//! bytes; serialization is the caller's business. An optional key
//! prefix namespaces every key — the Go `WithKeyPrefix`.
//!
//! TTL mapping: `Some(ttl)` becomes SET's EX; `None` stores without
//! expiry — the Go zero-duration semantics. On SetNX the Go adapter
//! forwards the zero duration too (Redis SET NX without EX = a
//! lock held until explicitly deleted), and this port preserves
//! that.
//!
//! # Testing
//!
//! Live conformance tests run against a real Redis via the `live`
//! feature; there is no embedded Redis for CI's unit lanes.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

use std::sync::Arc;
use std::time::Duration;

use redis::AsyncCommands;
use rushwind_cache::{BoxFuture, Cache, CacheError, Item};

struct Inner {
    connection: redis::aio::ConnectionManager,
    key_prefix: String,
}

/// A Redis-backed cache engine over a multiplexed connection.
pub struct RedisCache {
    inner: Arc<Inner>,
}

impl RedisCache {
    /// Connects to Redis at `url` (e.g. `redis://127.0.0.1:6379`)
    /// with no key prefix.
    pub async fn connect(url: &str) -> Result<Self, CacheError> {
        Self::connect_with(url, "").await
    }

    /// Connects with a key prefix prepended to every cache key.
    pub async fn connect_with(url: &str, key_prefix: &str) -> Result<Self, CacheError> {
        let client = redis::Client::open(url.to_string())
            .map_err(|e| CacheError::Failed(format!("redis client open: {e}")))?;
        let connection = redis::aio::ConnectionManager::new(client)
            .await
            .map_err(|e| CacheError::Failed(format!("redis connect: {e}")))?;
        Ok(Self {
            inner: Arc::new(Inner {
                connection,
                key_prefix: key_prefix.to_string(),
            }),
        })
    }

    /// Constructs from the bootstrap factory's settings wire shape:
    /// `url` (required), `key_prefix` (optional).
    pub async fn from_settings(settings: serde_json::Value) -> Result<Self, CacheError> {
        let settings: RedisSettings = serde_json::from_value(settings)
            .map_err(|e| CacheError::Failed(format!("settings parse: {e}")))?;
        Self::connect_with(&settings.url, &settings.key_prefix.unwrap_or_default()).await
    }

    fn key(&self, key: &str) -> String {
        format!("{}{}", self.inner.key_prefix, key)
    }
}

impl Cache for RedisCache {
    fn get<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<Option<Vec<u8>>, CacheError>> {
        Box::pin(async move {
            let mut connection = self.inner.connection.clone();
            let value: Option<Vec<u8>> = connection
                .get(self.key(key))
                .await
                .map_err(|e| CacheError::Failed(format!("redis get: {e}")))?;
            Ok(value)
        })
    }

    fn set<'a>(
        &'a self,
        key: &'a str,
        value: &'a [u8],
        ttl: Option<Duration>,
    ) -> BoxFuture<'a, Result<(), CacheError>> {
        Box::pin(async move {
            let mut connection = self.inner.connection.clone();
            let options = match ttl {
                Some(ttl) => redis::SetOptions::default()
                    .with_expiration(redis::SetExpiry::PX(ttl.as_millis() as u64)),
                None => redis::SetOptions::default(),
            };
            let _: () =
                redis::AsyncCommands::set_options(&mut connection, self.key(key), value, options)
                    .await
                    .map_err(|e| CacheError::Failed(format!("redis set: {e}")))?;
            Ok(())
        })
    }

    fn set_nx<'a>(
        &'a self,
        key: &'a str,
        value: &'a [u8],
        ttl: Option<Duration>,
    ) -> BoxFuture<'a, Result<bool, CacheError>> {
        Box::pin(async move {
            let set_options = |ttl: Option<Duration>| {
                let options =
                    redis::SetOptions::default().conditional_set(redis::ExistenceCheck::NX);
                match ttl {
                    Some(ttl) => {
                        options.with_expiration(redis::SetExpiry::PX(ttl.as_millis() as u64))
                    }
                    None => options,
                }
            };
            let mut connection = self.inner.connection.clone();
            // The Go adapter forwards the zero duration: SET NX without
            // EX holds the lock until explicitly deleted.
            let set: Option<()> = redis::AsyncCommands::set_options(
                &mut connection,
                self.key(key),
                value,
                set_options(ttl),
            )
            .await
            .map_err(|e| CacheError::Failed(format!("redis set_nx: {e}")))?;
            Ok(set.is_some())
        })
    }

    fn delete<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<(), CacheError>> {
        Box::pin(async move {
            let mut connection = self.inner.connection.clone();
            let _: () = connection
                .del(self.key(key))
                .await
                .map_err(|e| CacheError::Failed(format!("redis del: {e}")))?;
            Ok(())
        })
    }

    fn has<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<bool, CacheError>> {
        Box::pin(async move {
            let mut connection = self.inner.connection.clone();
            let exists: i64 = connection
                .exists(self.key(key))
                .await
                .map_err(|e| CacheError::Failed(format!("redis exists: {e}")))?;
            Ok(exists > 0)
        })
    }

    fn get_multi<'a>(
        &'a self,
        keys: &'a [String],
    ) -> BoxFuture<'a, Result<Vec<Option<Vec<u8>>>, CacheError>> {
        Box::pin(async move {
            if keys.is_empty() {
                return Ok(Vec::new());
            }
            let prefixed: Vec<String> = keys.iter().map(|key| self.key(key)).collect();
            let mut connection = self.inner.connection.clone();
            let values: Vec<Option<Vec<u8>>> = connection
                .mget(prefixed)
                .await
                .map_err(|e| CacheError::Failed(format!("redis mget: {e}")))?;
            Ok(values)
        })
    }

    fn set_multi<'a>(&'a self, items: &'a [Item]) -> BoxFuture<'a, Result<(), CacheError>> {
        Box::pin(async move {
            if items.is_empty() {
                return Ok(());
            }
            let mut connection = self.inner.connection.clone();
            let mut pipe = redis::pipe();
            for item in items {
                let options = match item.ttl {
                    Some(ttl) => redis::SetOptions::default()
                        .with_expiration(redis::SetExpiry::PX(ttl.as_millis() as u64)),
                    None => redis::SetOptions::default(),
                };
                pipe.set_options(self.key(&item.key), item.value.as_slice(), options);
            }
            let _: () = pipe
                .query_async(&mut connection)
                .await
                .map_err(|e| CacheError::Failed(format!("redis set_multi: {e}")))?;
            Ok(())
        })
    }

    fn close(&self) -> BoxFuture<'_, Result<(), CacheError>> {
        // The Go redis Close does not close the client; the
        // connection manager drops with the engine.
        Box::pin(async move { Ok(()) })
    }
}

/// The bootstrap factory's settings wire shape for
/// [`RedisCache::from_settings`].
#[derive(serde::Deserialize)]
pub struct RedisSettings {
    /// The Redis connection URL.
    pub url: String,
    /// The prefix prepended to every cache key.
    pub key_prefix: Option<String>,
}
