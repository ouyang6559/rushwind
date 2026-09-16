//! Nacos adapter for the RushWind registry contract — registration and
//! discovery over the
//! `nacos-sdk` naming client.
//!
//! # Registration
//!
//! Each endpoint registers as its own nacos instance under the
//! service name `{name}.{scheme}`, in the configured cluster and group
//! (defaults `DEFAULT` and `DEFAULT_GROUP`). The rush-wind round-trip
//! data rides in the instance metadata: `kind` (the endpoint's URL
//! scheme) and `version`. All instances are ephemeral and the SDK's
//! own connection machinery keeps them alive; there is no per-handle
//! heartbeat to abort, so the [`RegistrationHandle`] is inert and
//! [`Registrar::deregister`] is the only removal path.
//!
//! # Discovery
//!
//! [`Discovery::get_service`] is healthy-only with
//! subscription off. [`Discovery::watch`] subscribes
//! through the SDK; its push notifications signal the watcher, whose
//! every [`Watcher::next`] call — the first one included — re-reads the
//! SDK's
//! pushed instance cache and rebuilds the instance list from the
//! `kind`/`version` metadata.
//!
//! # Behavior notes
//!
//! - A stopped watcher's SDK subscription is not withdrawn; its
//!   signals are dropped once the watcher is gone.
//! - The `weight` metadata has no Rust-side
//!   [`Instance`] field and is dropped on the rebuild.
//!
//! # Testing
//!
//! Live conformance tests run against a real nacos server via the
//! `live` feature; there is no embedded nacos for CI's unit lanes.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

use std::collections::HashMap;
use std::sync::Arc;

use nacos_sdk::api::naming::{
    NamingChangeEvent, NamingEventListener, NamingServiceBuilder, ServiceInstance,
};
use rushwind_registry::{
    BoxFuture, Discovery, Registrar, Registration, RegistrationHandle, RegistryError, Watcher,
};
use rushwind_transport::Instance;
use serde::Deserialize;
use tokio::sync::mpsc;

struct Inner {
    naming: nacos_sdk::api::naming::NamingService,
    cluster: String,
    group: String,
    kind: String,
    weight: f64,
}

/// Options for the nacos registrar.
#[derive(Debug, Clone)]
pub struct NacosOptions {
    /// The nacos cluster instances register under. Default `DEFAULT`.
    pub cluster: String,
    /// The nacos group. Default `DEFAULT_GROUP`.
    pub group: String,
    /// The endpoint scheme assumed when an instance's metadata carries
    /// no `kind`. Default `grpc`.
    pub kind: String,
    /// The registration weight. Default `100`.
    pub weight: f64,
}

impl Default for NacosOptions {
    fn default() -> Self {
        Self {
            cluster: "DEFAULT".to_string(),
            group: nacos_sdk::api::constants::DEFAULT_GROUP.to_string(),
            kind: "grpc".to_string(),
            weight: 100.0,
        }
    }
}

/// A nacos-backed registry: registration and discovery over the SDK's
/// naming client.
pub struct NacosRegistry {
    inner: Arc<Inner>,
}

impl NacosRegistry {
    /// Connects to the nacos server at `addr` (e.g.
    /// `127.0.0.1:8848`) with default options.
    pub async fn connect(addr: &str) -> Result<Self, RegistryError> {
        Self::connect_with(addr, NacosOptions::default()).await
    }

    /// Connects with explicit options.
    pub async fn connect_with(addr: &str, options: NacosOptions) -> Result<Self, RegistryError> {
        let naming = NamingServiceBuilder::new(
            nacos_sdk::api::props::ClientProps::new().server_addr(addr.to_string()),
        )
        .build()
        .await
        .map_err(|e| RegistryError::Failed(format!("nacos connect {addr}: {e}")))?;
        Ok(Self {
            inner: Arc::new(Inner {
                naming,
                cluster: options.cluster,
                group: options.group,
                kind: options.kind,
                weight: options.weight,
            }),
        })
    }

    /// Constructs from the bootstrap factory's settings wire shape:
    /// `addr` (required). All nacos-side knobs stay at their
    /// defaults.
    pub async fn from_settings(settings: serde_json::Value) -> Result<Self, RegistryError> {
        let settings: NacosSettings = serde_json::from_value(settings)
            .map_err(|e| RegistryError::Failed(format!("settings parse: {e}")))?;
        Self::connect_with(&settings.addr, NacosOptions::default()).await
    }
}

/// The bootstrap factory's settings wire shape for
/// [`NacosRegistry::from_settings`].
#[derive(Deserialize)]
pub struct NacosSettings {
    /// The nacos server address.
    pub addr: String,
}

impl Registrar for NacosRegistry {
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
                // metadata: the endpoint's scheme and the version,
                // merged over any registration metadata. A `weight`
                // entry overrides the configured weight.
                let mut metadata: HashMap<String, String> = registration
                    .metadata
                    .as_ref()
                    .map(|meta| meta.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
                    .unwrap_or_default();
                let mut weight = self.inner.weight;
                if let Some(weight_override) = registration
                    .metadata
                    .as_ref()
                    .and_then(|meta| meta.get("weight"))
                    .and_then(|value| value.parse::<f64>().ok())
                {
                    weight = weight_override;
                }
                metadata.insert("kind".to_string(), scheme.to_string());
                metadata.insert("version".to_string(), registration.instance.version.clone());
                let instance = ServiceInstance {
                    ip: host.to_string(),
                    port: port as i32,
                    weight,
                    healthy: true,
                    enabled: true,
                    ephemeral: true,
                    cluster_name: Some(self.inner.cluster.clone()),
                    service_name: None,
                    instance_id: None,
                    metadata,
                };
                let service_name = format!("{}.{}", registration.instance.name, scheme);
                self.inner
                    .naming
                    .register_instance(
                        service_name.clone(),
                        Some(self.inner.group.clone()),
                        instance,
                    )
                    .await
                    .map_err(|e| {
                        RegistryError::Failed(format!(
                            "nacos register {service_name} {endpoint}: {e}"
                        ))
                    })?;
            }
            // Nacos instances are kept alive by the SDK's connection
            // machinery, not by a per-registration task.
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
                let instance = ServiceInstance {
                    ip: host.to_string(),
                    port: port as i32,
                    cluster_name: Some(self.inner.cluster.clone()),
                    ephemeral: true,
                    ..ServiceInstance::default()
                };
                let service_name = format!("{}.{}", registration.instance.name, scheme);
                self.inner
                    .naming
                    .deregister_instance(
                        service_name.clone(),
                        Some(self.inner.group.clone()),
                        instance,
                    )
                    .await
                    .map_err(|e| {
                        RegistryError::Failed(format!(
                            "nacos deregister {service_name} {endpoint}: {e}"
                        ))
                    })?;
            }
            Ok(())
        })
    }
}

impl Discovery for NacosRegistry {
    fn get_service<'a>(
        &'a self,
        service_name: &'a str,
    ) -> BoxFuture<'a, Result<Vec<Instance>, RegistryError>> {
        Box::pin(async move {
            // Healthy-only, no
            // subscription.
            let instances = self
                .inner
                .naming
                .select_instances(
                    service_name.to_string(),
                    Some(self.inner.group.clone()),
                    vec![self.inner.cluster.clone()],
                    false,
                    true,
                )
                .await
                .map_err(|e| RegistryError::Failed(format!("nacos select {service_name}: {e}")))?;
            Ok(rebuild(&instances, &self.inner.kind))
        })
    }

    fn watch<'a>(
        &'a self,
        service_name: &'a str,
    ) -> BoxFuture<'a, Result<Box<dyn Watcher>, RegistryError>> {
        Box::pin(async move {
            // The SDK subscription: push notifications signal the
            // watcher; the initial push populates the cache the first
            // next() reads.
            let (tx, rx) = mpsc::unbounded_channel::<()>();
            let listener = Arc::new(SignalListener { tx });
            self.inner
                .naming
                .subscribe(
                    service_name.to_string(),
                    Some(self.inner.group.clone()),
                    vec![self.inner.cluster.clone()],
                    listener,
                )
                .await
                .map_err(|e| {
                    RegistryError::Failed(format!("nacos subscribe {service_name}: {e}"))
                })?;
            Ok(Box::new(NacosWatcher {
                naming: self.inner.naming.clone(),
                service_name: service_name.to_string(),
                group: self.inner.group.clone(),
                cluster: self.inner.cluster.clone(),
                kind: self.inner.kind.clone(),
                first: true,
                signal: Some(rx),
            }) as Box<dyn Watcher>)
        })
    }
}

/// The nacos-backed [`Watcher`]: signals from the SDK subscription,
/// each answered with a rebuild of the pushed instance cache. The
/// first [`Watcher::next`] reads the cache immediately.
struct NacosWatcher {
    naming: nacos_sdk::api::naming::NamingService,
    service_name: String,
    group: String,
    cluster: String,
    kind: String,
    first: bool,
    signal: Option<mpsc::UnboundedReceiver<()>>,
}

impl Watcher for NacosWatcher {
    fn next<'a>(&'a mut self) -> BoxFuture<'a, Result<Vec<Instance>, RegistryError>> {
        Box::pin(async move {
            if self.first {
                self.first = false;
                return pushed_snapshot(
                    &self.naming,
                    &self.service_name,
                    &self.group,
                    &self.cluster,
                    &self.kind,
                )
                .await;
            }
            let Some(signal) = self.signal.as_mut() else {
                return Err(RegistryError::Failed("watcher stopped".to_string()));
            };
            if signal.recv().await.is_none() {
                return Err(RegistryError::Failed("watcher stopped".to_string()));
            }
            pushed_snapshot(
                &self.naming,
                &self.service_name,
                &self.group,
                &self.cluster,
                &self.kind,
            )
            .await
        })
    }

    fn stop(&mut self) {
        self.signal = None;
    }
}

impl Drop for NacosWatcher {
    fn drop(&mut self) {
        self.stop();
    }
}

/// The SDK push-notification listener: every notification signals the
/// watcher's channel.
struct SignalListener {
    tx: mpsc::UnboundedSender<()>,
}

impl NamingEventListener for SignalListener {
    fn event(&self, _event: Arc<NamingChangeEvent>) {
        let _ = self.tx.send(());
    }
}

/// Reads the SDK's pushed instance cache and rebuilds the rush-wind
/// instance list — endpoint URLs from `kind`/`ip`/`port`, version from
/// the metadata.
async fn pushed_snapshot(
    naming: &nacos_sdk::api::naming::NamingService,
    service_name: &str,
    group: &str,
    cluster: &str,
    kind: &str,
) -> Result<Vec<Instance>, RegistryError> {
    let instances = naming
        .get_all_instances(
            service_name.to_string(),
            Some(group.to_string()),
            vec![cluster.to_string()],
            true,
        )
        .await
        .map_err(|e| RegistryError::Failed(format!("nacos get_all {service_name}: {e}")))?;
    Ok(rebuild(&instances, kind))
}

/// The rush-wind rebuild of SDK instances: `kind`/`version` from the
/// instance metadata (the registered round-trip data), the endpoint
/// URL from `kind`://`ip`:`port`. Everything else the SDK carries —
/// weight included — has no [`Instance`] field and is dropped.
fn rebuild(instances: &[ServiceInstance], default_kind: &str) -> Vec<Instance> {
    instances
        .iter()
        .map(|instance| Instance {
            id: instance.instance_id.clone().unwrap_or_default(),
            name: instance.service_name.clone().unwrap_or_default(),
            version: instance
                .metadata
                .get("version")
                .cloned()
                .unwrap_or_default(),
            endpoints: vec![format!(
                "{}://{}:{}",
                instance
                    .metadata
                    .get("kind")
                    .cloned()
                    .unwrap_or_else(|| default_kind.to_string()),
                instance.ip,
                instance.port
            )],
        })
        .collect()
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
