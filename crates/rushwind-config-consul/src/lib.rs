//! Consul KV config source for the RushWind configuration contract —
//! the Go `go-wind-plugins/config/consul` ported onto reqwest.
//!
//! Load is a GET of the Consul KV path (`/v1/kv/{path}`): 200 carries
//! the value (base64 in the JSON envelope, decoded here), 404 is the
//! contract's `Ok(None)` "absent" answer. Watch is the Consul
//! blocking-query pattern: the same GET re-issued with `index` from
//! the previous response plus a wait time — the server holds the
//! request until the value changes or the wait lapses, then the new
//! value is pushed.
//!
//! One shared multiplexed connection serves loads and watches, as the
//! Go `api.Client` pool does.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

use std::sync::Arc;
use std::time::Duration;

use serde::Deserialize;

use rushwind_config::{BoxFuture, ConfigError, Source, ValueStream};
use tokio::sync::mpsc;

/// The default blocking-query wait — the Go watch plan's cadence.
const BLOCKING_WAIT: &str = "55s";

struct Inner {
    http: reqwest::Client,
    base: String,
    path: String,
}

/// One KV envelope entry from the Consul JSON response.
#[derive(Debug, Deserialize)]
struct KvEntry {
    #[serde(rename = "Value")]
    value: Option<String>,
}

/// The Consul KV configuration source.
pub struct ConsulSource {
    inner: Arc<Inner>,
}

impl Inner {
    fn kv_url(&self, path: &str) -> String {
        format!("{}/{}", self.base, path)
    }
}

impl ConsulSource {
    fn kv_url(&self, path: &str) -> String {
        self.inner.kv_url(path)
    }

    /// Builds the engine from the Consul address (e.g.
    /// `http://127.0.0.1:8500`) and the KV path. The Go "path
    /// invalid" guard when the path is empty.
    pub fn new(addr: &str, path: &str) -> Result<Self, ConfigError> {
        if path.is_empty() {
            return Err(ConfigError::Failed("consul: path invalid".to_string()));
        }
        Ok(Self {
            inner: Arc::new(Inner {
                http: reqwest::Client::new(),
                base: format!("{}/v1/kv", addr.trim_end_matches('/')),
                path: path.to_string(),
            }),
        })
    }

    /// Constructs from the bootstrap factory's settings wire shape:
    /// `addr` and `path` (both required).
    pub fn from_settings(settings: serde_json::Value) -> Result<Self, ConfigError> {
        #[derive(serde::Deserialize)]
        struct ConsulSettings {
            addr: String,
            path: String,
        }
        let settings: ConsulSettings = serde_json::from_value(settings)
            .map_err(|e| ConfigError::Failed(format!("settings parse: {e}")))?;
        Self::new(&settings.addr, &settings.path)
    }

    /// One KV read: the raw value when present, `None` when the key
    /// is absent (404) — the contract's "absent" answer.
    async fn get_raw(&self, path: &str) -> Result<Option<Vec<u8>>, ConfigError> {
        let url = self.kv_url(path);
        let response = self
            .inner
            .http
            .get(&url)
            .send()
            .await
            .map_err(|e| ConfigError::Failed(format!("consul get {path}: {e}")))?;
        let status = response.status().as_u16();
        if status == 404 {
            return Ok(None);
        }
        if !response.status().is_success() {
            return Err(ConfigError::Failed(format!(
                "consul get {path}: HTTP {status}"
            )));
        }
        let text = response
            .text()
            .await
            .map_err(|e| ConfigError::Failed(format!("consul get {path}: {e}")))?;
        let entries: Vec<KvEntry> = serde_json::from_str(&text)
            .map_err(|e| ConfigError::Failed(format!("consul get {path}: parse: {e}")))?;
        let Some(entry) = entries.first() else {
            return Ok(None);
        };
        let Some(value) = &entry.value else {
            return Ok(None);
        };
        // Consul returns the value base64-encoded in the JSON
        // envelope.
        Ok(Some(decode_base64(value)?))
    }

    /// Push-mode watch: spawns the Consul blocking-query loop — the
    /// same GET re-issued with `index` from the previous response plus
    /// a wait, pushing each new value as the key changes. The loop
    /// lives until the watcher is dropped.
    fn watch_value<'a>(
        &'a self,
        key: &'a str,
    ) -> BoxFuture<'a, Result<Box<dyn ValueStream>, ConfigError>> {
        Box::pin(async move {
            let path = if key.is_empty() {
                self.inner.path.clone()
            } else {
                key.to_string()
            };
            let (tx, rx) = mpsc::unbounded_channel::<Vec<u8>>();
            let inner = Arc::clone(&self.inner);
            tokio::spawn(async move {
                blocking_watch(&inner, path, tx).await;
            });
            Ok(Box::new(ConsulValueStream { rx }) as Box<dyn ValueStream>)
        })
    }
}

/// The permanent blocking-query loop: one GET per pass with `index`
/// from the last response and a wait, decoding each delivered value
/// and forwarding it to the watcher's channel. Failures back off one
/// second.
async fn blocking_watch(inner: &Inner, path: String, tx: mpsc::UnboundedSender<Vec<u8>>) {
    let mut index: u64 = 0;
    loop {
        let url = format!("{}?index={index}&wait={BLOCKING_WAIT}", inner.kv_url(&path));
        let response = inner.http.get(&url).send().await;
        let Ok(response) = response else {
            tokio::time::sleep(Duration::from_secs(1)).await;
            continue;
        };
        let status = response.status().as_u16();
        let new_index = response
            .headers()
            .get("X-Consul-Index")
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(index);
        if status == 404 {
            index = new_index;
            continue;
        }
        if !response.status().is_success() {
            tokio::time::sleep(Duration::from_secs(1)).await;
            continue;
        }
        let text = response.text().await.unwrap_or_default();
        let parsed: Option<Vec<KvEntry>> = serde_json::from_str(&text).ok();
        let Some(entries) = parsed else {
            continue;
        };
        index = new_index;
        for entry in entries {
            let Some(value) = &entry.value else {
                continue;
            };
            if let Ok(decoded) = decode_base64(value) {
                if tx.send(decoded).is_err() {
                    return;
                }
            }
        }
    }
}

use rushwind_config::SignalStream;

fn decode_base64(value: &str) -> Result<Vec<u8>, ConfigError> {
    use base64::Engine as _;
    base64::engine::general_purpose::STANDARD
        .decode(value)
        .map_err(|e| ConfigError::Failed(format!("consul value decode: {e}")))
}

impl Source for ConsulSource {
    fn load<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<Option<Vec<u8>>, ConfigError>> {
        Box::pin(async move {
            let path = if key.is_empty() {
                &self.inner.path
            } else {
                key
            };
            self.get_raw(path).await
        })
    }

    fn watch_value<'a>(
        &'a self,
        key: &'a str,
    ) -> BoxFuture<'a, Result<Box<dyn ValueStream>, ConfigError>> {
        Box::pin(async move {
            let path = if key.is_empty() {
                self.inner.path.clone()
            } else {
                key.to_string()
            };
            let (tx, rx) = mpsc::unbounded_channel::<Vec<u8>>();
            let inner = Arc::clone(&self.inner);
            tokio::spawn(async move {
                blocking_watch(&inner, path, tx).await;
            });
            Ok(Box::new(ConsulValueStream { rx }) as Box<dyn ValueStream>)
        })
    }

    fn watch<'a>(
        &'a self,
        key: &'a str,
    ) -> BoxFuture<'a, Result<Box<dyn SignalStream>, ConfigError>> {
        Box::pin(async move {
            let value_stream = self.watch_value(key).await?;
            Ok(Box::new(SignalFromValues { value_stream }) as Box<dyn SignalStream>)
        })
    }
}

/// A signal stream adapted from the push-value stream: each delivered
/// value is a change tick.
struct SignalFromValues {
    value_stream: Box<dyn ValueStream>,
}

impl SignalStream for SignalFromValues {
    fn next<'a>(&'a mut self) -> BoxFuture<'a, Option<()>> {
        Box::pin(async move { self.value_stream.next().await.map(|_| ()) })
    }
}

/// The push-mode stream: values from the blocking-query loop.
struct ConsulValueStream {
    rx: mpsc::UnboundedReceiver<Vec<u8>>,
}

impl ValueStream for ConsulValueStream {
    fn next<'a>(&'a mut self) -> BoxFuture<'a, Option<Vec<u8>>> {
        Box::pin(async move { self.rx.recv().await })
    }
}
