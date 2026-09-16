//! Polaris adapter for the RushWind registry contract — registration
//! and discovery over the polaris v1 HTTP client API, the officially
//! documented path for languages without a polaris SDK. This adapter
//! speaks protobuf-JSON to the server.
//!
//! # Registration
//!
//! Each endpoint registers as its own polaris instance under the
//! service name `{name}{scheme}` — concatenation, no separator — with
//! the rush-wind round-trip data smuggled
//! through instance metadata (`kind` = the endpoint scheme, `version`)
//! merged over any registration metadata. No health-check object is
//! attached, so polaris never expires the instance and there is no
//! heartbeat machinery; [`Registrar::deregister`] is the removal
//! path, and the [`RegistrationHandle`] is inert.
//!
//! # Discovery
//!
//! [`Discovery::get_service`] POSTs a Discover (INSTANCE) request and
//! rebuilds the healthy instances — endpoints from `kind`/host/port,
//! version from the metadata.
//! [`Discovery::watch`] polls the same discover request and wakes its
//! watchers whenever the healthy instance list changes; the delivered
//! shape is the full current
//! snapshot on every change, seeded from the initial state.
//!
//! # Behavior notes
//!
//! - The wire `metadata` is dropped on the rebuild beyond the
//!   identity fields: the Rust [`Instance`] has no metadata field.
//! - Instance weight is registered as 100 rather than 0, so a
//!   default-constructed registry produces usable instances.
//!
//! # Testing
//!
//! Live conformance tests run against a real polaris server via the
//! `live` feature; there is no embedded polaris for CI's unit lanes.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use rushwind_registry::{
    BoxFuture, Discovery, Registrar, Registration, RegistrationHandle, RegistryError, Watcher,
};
use rushwind_transport::Instance;
use serde::Deserialize;
use tokio::sync::watch;

/// The polaris response code for success.
const CODE_SUCCESS: u32 = 200_000;
/// The default namespace.
const DEFAULT_NAMESPACE: &str = "default";
/// The discovery poll cadence, in seconds.
const POLL_SECS: u64 = 5;

struct Inner {
    http: reqwest::Client,
    base: String,
    namespace: String,
    weight: u32,
    /// Watched services, created on first [`Discovery::watch`].
    sets: Mutex<HashMap<String, Arc<ServiceSet>>>,
}

/// The cache of one watched service: the latest broadcast instance
/// list, held forever by the poll task.
struct ServiceSet {
    cache: watch::Sender<Vec<Instance>>,
}

/// A polaris-backed registry: registration and discovery over the v1
/// HTTP client API.
pub struct PolarisRegistry {
    inner: Arc<Inner>,
}

impl PolarisRegistry {
    /// Connects to the polaris HTTP endpoint at `addr` (e.g.
    /// `http://127.0.0.1:8090`) with the default namespace.
    pub fn connect(addr: &str) -> Result<Self, RegistryError> {
        Self::connect_with(addr, DEFAULT_NAMESPACE)
    }

    /// Connects with an explicit namespace.
    pub fn connect_with(addr: &str, namespace: &str) -> Result<Self, RegistryError> {
        Ok(Self {
            inner: Arc::new(Inner {
                http: reqwest::Client::new(),
                base: addr.trim_end_matches('/').to_string(),
                namespace: namespace.to_string(),
                weight: 100,
                sets: Mutex::new(HashMap::new()),
            }),
        })
    }

    /// Constructs from the bootstrap factory's settings wire shape:
    /// `addr` (required), `namespace` (default `default`).
    pub fn from_settings(settings: serde_json::Value) -> Result<Self, RegistryError> {
        let settings: PolarisSettings = serde_json::from_value(settings)
            .map_err(|e| RegistryError::Failed(format!("settings parse: {e}")))?;
        Self::connect_with(
            &settings.addr,
            settings.namespace.as_deref().unwrap_or(DEFAULT_NAMESPACE),
        )
    }
}

impl Inner {
    /// POSTs a protobuf-JSON body to a `/v1` client endpoint and
    /// checks the response envelope.
    async fn post(
        &self,
        path: &str,
        body: serde_json::Value,
    ) -> Result<serde_json::Value, RegistryError> {
        let url = format!("{}/v1/{}", self.base, path);
        let response = self
            .http
            .post(&url)
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .body(body.to_string())
            .send()
            .await
            .map_err(|e| RegistryError::Failed(format!("polaris {path}: {e}")))?;
        let text = response
            .text()
            .await
            .map_err(|e| RegistryError::Failed(format!("polaris {path}: {e}")))?;
        let value: serde_json::Value = serde_json::from_str(&text)
            .map_err(|e| RegistryError::Failed(format!("polaris {path}: parse: {e}")))?;
        let code = value
            .get("code")
            .and_then(|code| code.as_u64())
            .unwrap_or_default() as u32;
        if code != CODE_SUCCESS {
            let info = value
                .get("info")
                .and_then(|info| info.as_str())
                .unwrap_or_default();
            return Err(RegistryError::Failed(format!(
                "polaris {path}: code {code}: {info}"
            )));
        }
        Ok(value)
    }
}

impl Registrar for PolarisRegistry {
    fn register<'a>(
        &'a self,
        registration: Registration,
    ) -> BoxFuture<'a, Result<RegistrationHandle, RegistryError>> {
        Box::pin(async move {
            for endpoint in &registration.instance.endpoints {
                let Some((scheme, host, port)) = split_endpoint(endpoint) else {
                    return Err(RegistryError::Failed(format!("endpoint parse: {endpoint}")));
                };
                // The rush-wind round-trip data rides in the instance
                // metadata, merged over any registration metadata.
                let mut metadata: HashMap<String, String> = registration
                    .metadata
                    .as_ref()
                    .map(|meta| meta.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
                    .unwrap_or_default();
                metadata.insert("kind".to_string(), scheme.to_string());
                metadata.insert("version".to_string(), registration.instance.version.clone());
                let body = serde_json::json!({
                    "service": format!("{}{}", registration.instance.name, scheme),
                    "namespace": self.inner.namespace,
                    "host": host,
                    "port": port,
                    "weight": self.inner.weight,
                    "version": registration.instance.version,
                    "metadata": metadata,
                });
                self.inner.post("RegisterInstance", body).await?;
            }
            // No health-check object is attached, so polaris never
            // expires the instance and no heartbeat task is needed.
            Ok(RegistrationHandle::from_cancel(|| {}))
        })
    }

    fn deregister<'a>(
        &'a self,
        registration: Registration,
    ) -> BoxFuture<'a, Result<(), RegistryError>> {
        Box::pin(async move {
            for endpoint in &registration.instance.endpoints {
                let Some((scheme, host, port)) = split_endpoint(endpoint) else {
                    return Err(RegistryError::Failed(format!("endpoint parse: {endpoint}")));
                };
                let body = serde_json::json!({
                    "service": format!("{}{}", registration.instance.name, scheme),
                    "namespace": self.inner.namespace,
                    "host": host,
                    "port": port,
                });
                self.inner.post("DeregisterInstance", body).await?;
            }
            Ok(())
        })
    }
}

impl Discovery for PolarisRegistry {
    fn get_service<'a>(
        &'a self,
        service_name: &'a str,
    ) -> BoxFuture<'a, Result<Vec<Instance>, RegistryError>> {
        Box::pin(async move {
            let instances = self
                .inner
                .post(
                    "Discover",
                    discover_body(service_name, &self.inner.namespace),
                )
                .await?;
            Ok(rebuild(&instances, service_name))
        })
    }

    fn watch<'a>(
        &'a self,
        service_name: &'a str,
    ) -> BoxFuture<'a, Result<Box<dyn Watcher>, RegistryError>> {
        Box::pin(async move {
            let (set, spawned) = {
                let mut sets = self.inner.sets.lock().expect("service sets poisoned");
                match sets.entry(service_name.to_string()) {
                    std::collections::hash_map::Entry::Occupied(entry) => {
                        (Arc::clone(entry.get()), false)
                    }
                    std::collections::hash_map::Entry::Vacant(entry) => {
                        let (cache, _rx) = watch::channel(Vec::new());
                        let set = Arc::new(ServiceSet { cache });
                        entry.insert(Arc::clone(&set));
                        (set, true)
                    }
                }
            };
            if spawned {
                let inner = Arc::clone(&self.inner);
                let name = service_name.to_string();
                let poll_set = Arc::clone(&set);
                tokio::spawn(async move {
                    poll_loop(inner, name, poll_set).await;
                });
            }
            Ok(Box::new(PolarisWatcher {
                rx: set.cache.subscribe(),
                stopped: false,
            }) as Box<dyn Watcher>)
        })
    }
}

/// The polaris-backed [`Watcher`]: a receiver on the watched
/// service's broadcast cache. There is no establishment shortcut —
/// the first change from the polled view provides the first
/// snapshot.
struct PolarisWatcher {
    rx: watch::Receiver<Vec<Instance>>,
    stopped: bool,
}

impl Watcher for PolarisWatcher {
    fn next<'a>(&'a mut self) -> BoxFuture<'a, Result<Vec<Instance>, RegistryError>> {
        Box::pin(async move {
            if self.stopped {
                return Err(RegistryError::Failed("watcher stopped".to_string()));
            }
            if self.rx.changed().await.is_err() {
                return Err(RegistryError::Failed("watcher stopped".to_string()));
            }
            Ok(self.rx.borrow_and_update().clone())
        })
    }

    fn stop(&mut self) {
        self.stopped = true;
    }
}

impl Drop for PolarisWatcher {
    fn drop(&mut self) {
        self.stop();
    }
}

/// The permanent poll loop: one discover per cadence, each changed
/// healthy-instance list replacing the service cache and waking its
/// watchers. Failed polls skip the pass.
async fn poll_loop(inner: Arc<Inner>, service_name: String, set: Arc<ServiceSet>) {
    let mut ticker = tokio::time::interval(Duration::from_secs(POLL_SECS));
    ticker.tick().await;
    loop {
        ticker.tick().await;
        let result = inner
            .post("Discover", discover_body(&service_name, &inner.namespace))
            .await;
        if let Ok(instances) = result {
            let _ = set.cache.send(rebuild(&instances, &service_name));
        }
    }
}

/// The Discover request body — the protobuf-JSON shape: the enum name
/// INSTANCE and the service as an object.
fn discover_body(service_name: &str, namespace: &str) -> serde_json::Value {
    serde_json::json!({
        "type": "INSTANCE",
        "service": {
            "name": service_name,
            "namespace": namespace,
        },
    })
}

/// The rush-wind rebuild of a discover response: healthy instances
/// only, identity from the smuggled metadata, the endpoint URL from
/// `kind`/host/port. Everything else polaris carries has no Rust
/// [`Instance`] field and is dropped.
fn rebuild(response: &serde_json::Value, service_name: &str) -> Vec<Instance> {
    response
        .get("instances")
        .and_then(|instances| instances.as_array())
        .map(|instances| {
            instances
                .iter()
                .filter(|instance| {
                    instance
                        .get("healthy")
                        .and_then(|healthy| healthy.as_bool())
                        != Some(false)
                })
                .filter_map(|instance| {
                    let id = instance
                        .get("id")
                        .and_then(|value| value.as_str())
                        .unwrap_or_default();
                    let host = instance
                        .get("host")
                        .and_then(|value| value.as_str())
                        .unwrap_or_default();
                    let port = instance
                        .get("port")
                        .and_then(|value| value.as_u64())
                        .unwrap_or_default();
                    let metadata: HashMap<String, String> = instance
                        .get("metadata")
                        .and_then(|value| serde_json::from_value(value.clone()).ok())
                        .unwrap_or_default();
                    if id.is_empty() || host.is_empty() {
                        return None;
                    }
                    let kind = metadata.get("kind").cloned().unwrap_or_default();
                    Some(Instance {
                        id: id.to_string(),
                        name: service_name.to_string(),
                        version: metadata.get("version").cloned().unwrap_or_default(),
                        endpoints: vec![format!("{kind}://{host}:{port}")],
                    })
                })
                .collect()
        })
        .unwrap_or_default()
}

/// Splits an endpoint URL into `(scheme, host, port)`, with the port
/// defaulting to 0 when absent or unparseable.
fn split_endpoint(endpoint: &str) -> Option<(&str, &str, i64)> {
    let separator = endpoint.find("://")?;
    let scheme = &endpoint[..separator];
    let rest = &endpoint[separator + 3..];
    let end = rest.find(['/', '?', '#']).unwrap_or(rest.len());
    let hostport = &rest[..end];
    let (host, port) = match hostport.rsplit_once(':') {
        Some((host, port)) => (host, port.parse::<i64>().unwrap_or(0)),
        None => (hostport, 0),
    };
    Some((scheme, host, port))
}

/// The bootstrap factory's settings wire shape for
/// [`PolarisRegistry::from_settings`].
#[derive(Deserialize)]
pub struct PolarisSettings {
    /// The polaris HTTP endpoint address (e.g. `http://127.0.0.1:8090`).
    pub addr: String,
    /// The polaris namespace. Default: `default`.
    pub namespace: Option<String>,
}
