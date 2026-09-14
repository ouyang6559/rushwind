//! The caching source wrapper: an in-memory cache in front of a remote
//! source, cutting network round-trips for hot-path script loading.
//!
//! A key's first load fetches from the remote and populates the cache;
//! later loads serve from it. When the remote offers
//! [`ScriptSource::watch`], the first fetch also opens a watch stream
//! for the key and subsequent loads consult it: a pending change tick
//! evicts the entry (the cache refuses, the next load refetches), the
//! Go invalidation-goroutine behavior. An optional TTL bounds entry
//! freshness by wall clock instead.
//!
//! Divergence from the Go predecessor: the invalidation goroutine per
//! watched key becomes a lazy drain — each load polls the stored
//! stream's already-pending signal without blocking, so eviction is
//! observable by the next load rather than immediately, with no task
//! boundaries; `Close` is `Drop` (the stored streams die with the
//! source, and the remote arc releases); the nil-remote constructor
//! check is dropped (`SharedScriptSource` cannot be null).

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use futures::future::FutureExt;

use crate::source::memory::MemSource;
use crate::source::{ScriptSource, SharedScriptSource, SignalStream};
use crate::{BoxFuture, ScriptError};

/// One cached key's bookkeeping: the fetch timestamp consulted by the
/// TTL, and the invalidation stream opened on the first fetch when the
/// remote can watch.
#[derive(Default)]
struct CachedEntry {
    ts: Option<Instant>,
    stream: Option<Box<dyn SignalStream>>,
}

/// A caching wrapper over a remote source.
pub struct CachedSource {
    remote: SharedScriptSource,
    cache: MemSource,
    ttl: Option<Duration>,
    state: Mutex<HashMap<String, CachedEntry>>,
}

impl CachedSource {
    /// Creates the wrapper over `remote`.
    pub fn new(remote: SharedScriptSource) -> Self {
        Self {
            remote,
            cache: MemSource::new(),
            ttl: None,
            state: Mutex::new(HashMap::new()),
        }
    }

    /// Sets the entry TTL: once a fetched entry is older than `ttl`,
    /// the next load refetches from the remote regardless of watch
    /// status. The default is no TTL.
    pub fn with_ttl(mut self, ttl: Duration) -> Self {
        self.ttl = Some(ttl);
        self
    }

    /// Evicts one key, forcing its next load to refetch from the
    /// remote.
    pub fn invalidate(&self, key: &str) {
        self.cache.delete(key);
        let mut state = self.state.lock().expect("cached source lock");
        if let Some(entry) = state.get_mut(key) {
            entry.ts = None;
        }
    }

    /// Evicts every key the source has loaded.
    pub fn invalidate_all(&self) {
        let keys: Vec<String> = {
            let state = self.state.lock().expect("cached source lock");
            state.keys().cloned().collect()
        };
        for key in keys {
            self.invalidate(&key);
        }
    }
}

impl ScriptSource for CachedSource {
    fn load<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<String, ScriptError>> {
        Box::pin(async move {
            // Phase one, under the lock: drain any invalidation signal
            // already pending on the stored stream — a tick evicts the
            // entry, an ended stream drops off — then the freshness
            // check. The drain polls without blocking: it sees a
            // signal only if one is already there.
            let (fresh, was_loaded) = {
                let mut state = self.state.lock().expect("cached source lock");
                let mut fresh = false;
                if let Some(entry) = state.get_mut(key) {
                    if let Some(stream) = entry.stream.as_mut() {
                        match stream.next().now_or_never() {
                            Some(Some(())) => {
                                self.cache.delete(key);
                                entry.ts = None;
                            }
                            Some(None) => {
                                entry.stream = None;
                            }
                            None => {}
                        }
                    }
                    fresh = self
                        .ttl
                        .map_or(true, |ttl| entry.ts.is_some_and(|t| t.elapsed() <= ttl));
                }
                (fresh, state.contains_key(key))
            };

            if fresh {
                if let Ok(code) = self.cache.load(key).await {
                    return Ok(code);
                }
            }

            // Miss: fetch from the remote, repopulate the cache, and
            // on the key's first fetch open the invalidation watch
            // when the remote offers one.
            let code = match self.remote.load(key).await {
                Ok(code) => code,
                Err(err) => {
                    return Err(ScriptError::Failed(format!(
                        "cached source: remote load {key:?}: {err}"
                    )))
                }
            };
            self.cache.set(key, &code);
            // The invalidation watch opens only on a key's first
            // fetch, the Go loaded-gate; a remote that cannot watch
            // (or whose watch fails) contributes no stream and later
            // loads rely on the TTL or manual invalidation.
            let watch_probe = if was_loaded {
                None
            } else {
                self.remote.watch(key).await.ok()
            };

            let mut state = self.state.lock().expect("cached source lock");
            if was_loaded {
                if let Some(entry) = state.get_mut(key) {
                    entry.ts = Some(Instant::now());
                }
            } else {
                let entry = state.entry(key.to_string()).or_default();
                entry.ts = Some(Instant::now());
                if let Some(stream) = watch_probe {
                    entry.stream = Some(stream);
                }
            }
            Ok(code)
        })
    }

    fn watch<'a>(
        &'a self,
        key: &'a str,
    ) -> BoxFuture<'a, Result<Box<dyn SignalStream>, ScriptError>> {
        Box::pin(async move {
            match self.remote.watch(key).await {
                Ok(stream) => Ok(stream),
                Err(ScriptError::CapabilityNotSupported) => Err(ScriptError::Failed(
                    "cached source: remote does not implement watching".to_string(),
                )),
                Err(err) => Err(err),
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    /// A programmable remote: a mutable code answer, a load counter,
    /// no watch capability.
    struct RemoteFixture {
        code: Mutex<String>,
        load_calls: Arc<AtomicUsize>,
    }

    impl RemoteFixture {
        fn with_code(code: &str) -> Arc<Self> {
            Arc::new(Self {
                code: Mutex::new(code.to_string()),
                load_calls: Arc::new(AtomicUsize::new(0)),
            })
        }

        fn set_code(&self, code: &str) {
            *self.code.lock().expect("fixture code lock") = code.to_string();
        }
    }

    impl ScriptSource for RemoteFixture {
        fn load<'a>(&'a self, _key: &'a str) -> BoxFuture<'a, Result<String, ScriptError>> {
            self.load_calls.fetch_add(1, Ordering::SeqCst);
            let code = self.code.lock().expect("fixture code lock").clone();
            Box::pin(async move { Ok(code) })
        }
    }

    /// A remote that always fails.
    struct FailingRemote;

    impl ScriptSource for FailingRemote {
        fn load<'a>(&'a self, _key: &'a str) -> BoxFuture<'a, Result<String, ScriptError>> {
            Box::pin(async { Err(ScriptError::Failed("remote down".to_string())) })
        }
    }

    fn shared<T: ScriptSource + 'static>(source: Arc<T>) -> SharedScriptSource {
        source
    }

    #[tokio::test]
    async fn a_miss_fetches_the_remote_and_a_hit_does_not() {
        let remote = RemoteFixture::with_code("v1");
        let calls = remote.load_calls.clone();
        let cached = CachedSource::new(shared(remote));
        assert_eq!(cached.load("k").await.expect("first load"), "v1");
        assert_eq!(cached.load("k").await.expect("second load"), "v1");
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn a_failing_remote_fails_the_load() {
        let cached = CachedSource::new(shared(Arc::new(FailingRemote)));
        assert!(matches!(
            cached.load("k").await,
            Err(ScriptError::Failed(msg)) if msg.contains("cached source: remote load")
        ));
    }

    #[tokio::test]
    async fn invalidate_forces_a_refetch() {
        let remote = RemoteFixture::with_code("v1");
        let calls = remote.load_calls.clone();
        let cached = CachedSource::new(shared(remote.clone()));
        assert_eq!(cached.load("k").await.expect("first load"), "v1");
        calls.store(0, Ordering::SeqCst);
        remote.set_code("v2");
        cached.invalidate("k");
        assert_eq!(
            cached.load("k").await.expect("post-invalidate refetch"),
            "v2"
        );
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn invalidate_all_forces_refetches_for_every_key() {
        let remote = RemoteFixture::with_code("v1");
        let calls = remote.load_calls.clone();
        let cached = CachedSource::new(shared(remote));
        assert_eq!(cached.load("k1").await.expect("k1 first"), "v1");
        assert_eq!(cached.load("k2").await.expect("k2 first"), "v1");
        calls.store(0, Ordering::SeqCst);
        cached.invalidate_all();
        assert_eq!(cached.load("k1").await.expect("k1 refetch"), "v1");
        assert_eq!(cached.load("k2").await.expect("k2 refetch"), "v1");
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn an_expired_entry_refetches_the_changed_remote() {
        let remote = RemoteFixture::with_code("v1");
        let cached = CachedSource::new(shared(remote.clone())).with_ttl(Duration::from_millis(50));
        assert_eq!(cached.load("k").await.expect("first load"), "v1");
        tokio::time::sleep(Duration::from_millis(80)).await;
        remote.set_code("v2");
        assert_eq!(cached.load("k").await.expect("expired refetch"), "v2");
    }

    #[tokio::test]
    async fn an_unexpired_entry_serves_from_the_cache() {
        let remote = RemoteFixture::with_code("v1");
        let calls = remote.load_calls.clone();
        let cached = CachedSource::new(shared(remote)).with_ttl(Duration::from_secs(5));
        assert_eq!(cached.load("k").await.expect("first load"), "v1");
        assert_eq!(cached.load("k").await.expect("cached load"), "v1");
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn a_watch_signal_evicts_the_entry_for_the_next_load() {
        let remote = Arc::new(MemSource::new());
        remote.set("k", "v1");
        let cached = CachedSource::new(remote.clone());
        assert_eq!(cached.load("k").await.expect("first load"), "v1");
        // The watch signal lands; the next load drains it, evicts, and
        // refetches the changed value.
        tokio::time::sleep(Duration::from_millis(50)).await;
        remote.set("k", "v2");
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(cached.load("k").await.expect("post-signal refetch"), "v2");
    }

    #[tokio::test]
    async fn watch_delegates_to_the_remote() {
        let remote = Arc::new(MemSource::new());
        remote.set("k", "v1");
        let cached = CachedSource::new(remote.clone());
        let mut stream = cached.watch("k").await.expect("watch delegates");
        remote.set("k", "v2");
        tokio::time::timeout(Duration::from_secs(2), stream.next())
            .await
            .expect("delegated stream signals")
            .expect("stream alive");
    }

    #[tokio::test]
    async fn watch_fails_when_the_remote_cannot_watch() {
        let cached = CachedSource::new(shared(RemoteFixture::with_code("x")));
        assert!(matches!(
            cached.watch("k").await,
            Err(ScriptError::Failed(msg)) if msg.contains("does not implement")
        ));
    }

    #[tokio::test]
    async fn dropping_the_source_drops_its_watch_streams_and_the_remote_survives() {
        let remote = Arc::new(MemSource::new());
        remote.set("k", "v1");
        let cached = CachedSource::new(remote.clone());
        assert_eq!(cached.load("k").await.expect("first load"), "v1");
        drop(cached);
        // The remote is an independent object and stays functional.
        assert_eq!(remote.load("k").await.expect("remote alive"), "v1");
    }

    #[tokio::test]
    async fn concurrent_loads_of_one_key_all_answer() {
        let remote = Arc::new(MemSource::new());
        remote.set("k", "v1");
        let cached = Arc::new(CachedSource::new(remote.clone()));
        let loads: Vec<_> = (0..8)
            .map(|_| {
                let cached = cached.clone();
                async move { cached.load("k").await }
            })
            .collect();
        let results = futures::future::join_all(loads).await;
        for result in results {
            assert_eq!(result.expect("load"), "v1");
        }
    }

    #[tokio::test]
    async fn keys_cache_independently() {
        let remote = Arc::new(MemSource::new());
        remote.set("k1", "v1a");
        remote.set("k2", "v2a");
        let cached = CachedSource::new(remote.clone());
        assert_eq!(cached.load("k1").await.expect("k1"), "v1a");
        assert_eq!(cached.load("k2").await.expect("k2"), "v2a");
        // An invalidation of one key leaves the other's entry alone.
        cached.invalidate("k1");
        remote.set("k1", "v1b");
        assert_eq!(cached.load("k1").await.expect("k1 invalidated"), "v1b");
        assert_eq!(cached.load("k2").await.expect("k2 untouched"), "v2a");
    }
}
