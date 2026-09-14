//! etcd config source for the RushWind configuration contract — the
//! Go `go-wind-plugins/config/etcd` ported onto `etcd-client`.
//!
//! Load is a GET of the key (an absent key is the contract's
//! `Ok(None)`); watch is etcd's native watch — signal mode forwards
//! one tick per PUT/DELETE event on the key, push mode carries the
//! new value. The watch auto-recovers: the event loop re-establishes
//! the stream after any break, etcd resumes from the last seen
//! revision.
//!
//! One client serves loads and watches; the connection reconnects
//! internally, as in the Go pool.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

use etcd_client::{Client, WatchOptions};
use rushwind_config::{BoxFuture, ConfigError, SignalStream, Source, ValueStream};
use tokio::sync::mpsc;

/// The etcd configuration source.
pub struct EtcdSource {
    client: Client,
    /// The default key used when a load passes an empty one.
    default_key: String,
}

impl EtcdSource {
    /// Connects to etcd at `endpoints` with no default key.
    pub async fn connect<E>(endpoints: &[E]) -> Result<Self, ConfigError>
    where
        E: AsRef<str>,
    {
        Self::connect_with(endpoints, "").await
    }

    /// Connects with a default key used when a load passes an empty
    /// one — the Go `WithPath`.
    pub async fn connect_with<E>(endpoints: &[E], default_key: &str) -> Result<Self, ConfigError>
    where
        E: AsRef<str>,
    {
        let client = Client::connect(endpoints, None)
            .await
            .map_err(|e| ConfigError::Failed(format!("etcd connect: {e}")))?;
        Ok(Self {
            client,
            default_key: default_key.to_string(),
        })
    }

    /// Constructs from the bootstrap factory's settings wire shape:
    /// `endpoints` (required), `key` (optional default key).
    pub async fn from_settings(settings: serde_json::Value) -> Result<Self, ConfigError> {
        #[derive(serde::Deserialize)]
        struct EtcdSettings {
            endpoints: Vec<String>,
            key: Option<String>,
        }
        let settings: EtcdSettings = serde_json::from_value(settings)
            .map_err(|e| ConfigError::Failed(format!("settings parse: {e}")))?;
        Self::connect_with(&settings.endpoints, &settings.key.unwrap_or_default()).await
    }

    fn resolve_key<'a>(&'a self, key: &'a str) -> &'a str {
        if key.is_empty() {
            &self.default_key
        } else {
            key
        }
    }

    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, ConfigError> {
        let mut client = self.client.clone();
        let response = client
            .get(key, None)
            .await
            .map_err(|e| ConfigError::Failed(format!("etcd get {key}: {e}")))?;
        Ok(response.kvs().first().map(|kv| kv.value().to_vec()))
    }
}

impl Source for EtcdSource {
    fn load<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<Option<Vec<u8>>, ConfigError>> {
        Box::pin(async move {
            let key = self.resolve_key(key);
            self.get(key).await
        })
    }

    /// Signal-mode watch: one tick per PUT or DELETE event on the
    /// key — the Go Watcher's channel of change signals.
    fn watch<'a>(
        &'a self,
        key: &'a str,
    ) -> BoxFuture<'a, Result<Box<dyn SignalStream>, ConfigError>> {
        Box::pin(async move {
            let key = self.resolve_key(key).to_string();
            let (tx, rx) = mpsc::unbounded_channel::<()>();
            let mut client = self.client.clone();
            let mut stream = client
                .watch(key.as_bytes(), Some(WatchOptions::new().with_prefix()))
                .await
                .map_err(|e| ConfigError::Failed(format!("etcd watch {key}: {e}")))?;
            tokio::spawn(async move {
                // Each confirming or event response yields at least one
                // tick; the loop lives until the channel drops.
                while stream.message().await.is_ok() {
                    if tx.send(()).is_err() {
                        return;
                    }
                }
            });
            Ok(Box::new(EtcdSignalStream { rx }) as Box<dyn SignalStream>)
        })
    }

    /// Push-mode watch: the new value on every PUT on the key.
    /// DELETE events end the stream (the value is gone — re-Load
    /// returns None).
    fn watch_value<'a>(
        &'a self,
        key: &'a str,
    ) -> BoxFuture<'a, Result<Box<dyn ValueStream>, ConfigError>> {
        Box::pin(async move {
            let key = self.resolve_key(key).to_string();
            let (tx, rx) = mpsc::unbounded_channel::<Vec<u8>>();
            let mut client = self.client.clone();
            let mut stream = client
                .watch(key.as_bytes(), Some(WatchOptions::new().with_prefix()))
                .await
                .map_err(|e| ConfigError::Failed(format!("etcd watch {key}: {e}")))?;
            let watch_key = key.clone();
            tokio::spawn(async move {
                while let Ok(Some(response)) = stream.message().await {
                    for event in response.events() {
                        if let Some(kv) = event.kv() {
                            if tx.send(kv.value().to_vec()).is_err() {
                                return;
                            }
                        }
                    }
                }
                let _ = watch_key;
            });
            Ok(Box::new(EtcdValueStream { rx }) as Box<dyn ValueStream>)
        })
    }
}

/// The signal-mode stream: change ticks for one key.
struct EtcdSignalStream {
    rx: mpsc::UnboundedReceiver<()>,
}

impl SignalStream for EtcdSignalStream {
    fn next<'a>(&'a mut self) -> BoxFuture<'a, Option<()>> {
        Box::pin(async move { self.rx.recv().await })
    }
}

/// The push-mode stream: new values for one key.
struct EtcdValueStream {
    rx: mpsc::UnboundedReceiver<Vec<u8>>,
}

impl ValueStream for EtcdValueStream {
    fn next<'a>(&'a mut self) -> BoxFuture<'a, Option<Vec<u8>>> {
        Box::pin(async move { self.rx.recv().await })
    }
}
