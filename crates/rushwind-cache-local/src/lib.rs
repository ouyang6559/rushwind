//! In-process KV cache engine for the RushWind cache contract — a
//! hand-rolled TTL map.
//!
//! # The engine's behavior
//!
//! Entries carry a TTL; reads (`get`/`has`/`get_multi`) check expiry
//! lazily and treat an expired entry as missing, matching FreeCache's
//! read-time expiration. When the capacity is reached, expired
//! entries are evicted first, then the oldest insertion — an
//! approximation of FreeCache's ring-buffer eviction.
//!
//! [`Cache::set_nx`] is a check-then-insert under one process-wide
//! lock (FreeCache exposes no native SetNX), closing the race window
//! without FreeCache's segment spin-lock.
//!
//! # Divergences
//!
//! - Capacity counts entries, not bytes (FreeCache pre-allocates a
//!   byte-sized ring buffer).
//! - Sub-second TTLs are honored; FreeCache rounds to whole seconds.
//! - No hit/miss/eviction counters.
//!
//! # Testing
//!
//! The engine is in-process, so its conformance suite (round trip,
//! expiry, SetNX, batches, eviction) runs as ordinary unit tests.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use rushwind_cache::{BoxFuture, Cache, CacheError, Item};

struct Entry {
    value: Vec<u8>,
    expires_at: Option<Instant>,
    inserted_at: Instant,
}

struct Shared {
    entries: Mutex<HashMap<String, Entry>>,
    default_ttl: Option<Duration>,
    capacity: usize,
}

/// An in-process KV cache engine.
pub struct LocalCache {
    shared: Arc<Shared>,
}

use std::sync::Arc;

impl LocalCache {
    /// Builds a cache with the default capacity (65,536 entries) and
    /// no default TTL.
    pub fn new() -> Self {
        Self::with_options(CacheOptions::default())
    }

    /// Builds a cache with explicit options.
    pub fn with_options(options: CacheOptions) -> Self {
        Self {
            shared: Arc::new(Shared {
                entries: Mutex::new(HashMap::new()),
                default_ttl: options.default_ttl,
                capacity: options.capacity,
            }),
        }
    }

    /// Constructs from the bootstrap factory's settings wire shape:
    /// `default_ttl_ms` and `capacity` are optional.
    pub fn from_settings(settings: serde_json::Value) -> Result<Self, CacheError> {
        let settings: LocalSettings = serde_json::from_value(settings)
            .map_err(|e| CacheError::Failed(format!("settings parse: {e}")))?;
        Ok(Self::with_options(CacheOptions {
            default_ttl: settings.default_ttl_ms.map(Duration::from_millis),
            capacity: settings.capacity.unwrap_or(DEFAULT_CAPACITY),
        }))
    }

    fn resolve_ttl(&self, ttl: Option<Duration>) -> Option<Duration> {
        ttl.or(self.shared.default_ttl)
    }
}

impl Default for LocalCache {
    fn default() -> Self {
        Self::new()
    }
}

/// The local engine's settings.
#[derive(Debug, Clone)]
pub struct CacheOptions {
    /// The TTL applied when an operation passes no TTL. Default:
    /// entries never expire.
    pub default_ttl: Option<Duration>,
    /// The maximum number of entries before eviction. Default:
    /// 65,536.
    pub capacity: usize,
}

impl Default for CacheOptions {
    fn default() -> Self {
        Self {
            default_ttl: None,
            capacity: DEFAULT_CAPACITY,
        }
    }
}

/// The default capacity, in entries.
const DEFAULT_CAPACITY: usize = 65_536;

/// The bootstrap factory's settings wire shape for
/// [`LocalCache::from_settings`].
#[derive(serde::Deserialize)]
pub struct LocalSettings {
    /// The default TTL, in milliseconds.
    pub default_ttl_ms: Option<u64>,
    /// The maximum number of entries.
    pub capacity: Option<usize>,
}

impl Cache for LocalCache {
    fn get<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<Option<Vec<u8>>, CacheError>> {
        Box::pin(async move {
            let mut entries = self.shared.entries.lock().unwrap();
            Ok(read_entry(&mut entries, key))
        })
    }

    fn set<'a>(
        &'a self,
        key: &'a str,
        value: &'a [u8],
        ttl: Option<Duration>,
    ) -> BoxFuture<'a, Result<(), CacheError>> {
        Box::pin(async move {
            let ttl = self.resolve_ttl(ttl);
            let mut entries = self.shared.entries.lock().unwrap();
            insert_entry(&mut entries, self.shared.capacity, key, value.to_vec(), ttl);
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
            let ttl = self.resolve_ttl(ttl);
            let mut entries = self.shared.entries.lock().unwrap();
            if read_entry(&mut entries, key).is_some() {
                return Ok(false);
            }
            insert_entry(&mut entries, self.shared.capacity, key, value.to_vec(), ttl);
            Ok(true)
        })
    }

    fn delete<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<(), CacheError>> {
        Box::pin(async move {
            self.shared.entries.lock().unwrap().remove(key);
            Ok(())
        })
    }

    fn has<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<bool, CacheError>> {
        Box::pin(async move {
            let mut entries = self.shared.entries.lock().unwrap();
            Ok(read_entry(&mut entries, key).is_some())
        })
    }

    fn get_multi<'a>(
        &'a self,
        keys: &'a [String],
    ) -> BoxFuture<'a, Result<Vec<Option<Vec<u8>>>, CacheError>> {
        Box::pin(async move {
            let mut entries = self.shared.entries.lock().unwrap();
            Ok(keys
                .iter()
                .map(|key| read_entry(&mut entries, key))
                .collect())
        })
    }

    fn set_multi<'a>(&'a self, items: &'a [Item]) -> BoxFuture<'a, Result<(), CacheError>> {
        Box::pin(async move {
            let ttl = self.shared.default_ttl;
            let mut entries = self.shared.entries.lock().unwrap();
            for item in items {
                let resolved = item.ttl.or(ttl);
                insert_entry(
                    &mut entries,
                    self.shared.capacity,
                    &item.key,
                    item.value.clone(),
                    resolved,
                );
            }
            Ok(())
        })
    }

    fn close(&self) -> BoxFuture<'_, Result<(), CacheError>> {
        Box::pin(async move {
            self.shared.entries.lock().unwrap().clear();
            Ok(())
        })
    }
}

/// Reads an entry, dropping it when expired — FreeCache's read-time
/// expiration.
fn read_entry(entries: &mut HashMap<String, Entry>, key: &str) -> Option<Vec<u8>> {
    if entries
        .get(key)
        .and_then(|entry| entry.expires_at.map(|at| Instant::now() >= at))
        .unwrap_or(false)
    {
        entries.remove(key);
        return None;
    }
    entries.get(key).map(|entry| entry.value.clone())
}

/// Inserts an entry, evicting expired entries first and then the
/// oldest insertion when the capacity is reached.
fn insert_entry(
    entries: &mut HashMap<String, Entry>,
    capacity: usize,
    key: &str,
    value: Vec<u8>,
    ttl: Option<Duration>,
) {
    if entries.len() >= capacity && !entries.contains_key(key) {
        // Free the expired ones first; fall back to the oldest
        // insertion.
        let now = Instant::now();
        entries.retain(|_, entry| !entry.expires_at.map(|at| now >= at).unwrap_or(false));
        while entries.len() >= capacity {
            let oldest = entries
                .iter()
                .min_by_key(|(_, entry)| entry.inserted_at)
                .map(|(key, _)| key.clone());
            match oldest {
                Some(oldest) => {
                    entries.remove(&oldest);
                }
                None => break,
            }
        }
    }
    entries.insert(
        key.to_string(),
        Entry {
            value,
            expires_at: ttl.map(|ttl| Instant::now() + ttl),
            inserted_at: Instant::now(),
        },
    );
}
