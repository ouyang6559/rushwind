//! Kubernetes adapter for the RushWind registry, over the `kube` client.
//! It runs **in-cluster only**: registration patches the owning pod's
//! own labels and annotations, and discovery reads pods through the
//! API server the pod's service account can see.
//!
//! # The wire contract
//!
//! Service identity rides on the owning pod:
//!
//! - Labels `wind-service-id`, `wind-service-app`,
//!   `wind-service-version` carry the instance identity.
//! - Annotations `wind-service-metadata` (JSON object or `null`) and
//!   `wind-service-protocols` (a JSON map from container port to
//!   endpoint scheme) carry the rush-wind round-trip data.
//!
//! [`Registrar::register`] strategic-merge-patches those onto the pod
//! named by `HOSTNAME` in the namespace the service account file
//! names; [`Registrar::deregister`] blanks the labels and resets both
//! annotations to `{}`.
//!
//! # Discovery
//!
//! [`Discovery::get_service`] lists pods labeled `wind-service-app ==
//! name`, keeps the `Running` ones, and rebuilds instances from the
//! labels plus the annotations — endpoints from the pod IP and every
//! container port, the scheme from the protocol map when it names the
//! port and from the container-port name prefix or the IP protocol
//! otherwise.
//!
//! [`Discovery::watch`] runs a `kube` watcher on the same label
//! selector; **every** event it emits — the initialization series
//! included —
//! triggers a fresh full-list read that updates the service cache and
//! wakes its watchers. A ten-minute ticker provides periodic
//! resync announcements.
//!
//! # Behavior notes
//!
//! - `Start()` is fused into [`Discovery::watch`]: the watcher runs
//!   from creation, not from a separate informer start.
//! - A failed re-list skips the broadcast instead of aborting.
//! - The wire `metadata` annotation is parsed and dropped on the
//!   rebuild: the Rust [`Instance`] has no metadata field.
//!
//! # Testing
//!
//! The `live` suite runs only inside a real cluster and skips itself
//! everywhere else; there is no embedded Kubernetes for CI's unit
//! lanes.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::StreamExt;
use k8s_openapi::api::core::v1::Pod;
use kube::api::{ListParams, Patch, PatchParams};
use kube::{Api, Client, Config};
use rushwind_registry::{
    BoxFuture, Discovery, Registrar, Registration, RegistrationHandle, RegistryError, Watcher,
};
use rushwind_transport::Instance;
use serde::Deserialize;
use tokio::sync::watch;

/// Label carrying the instance id.
const LABEL_SERVICE_ID: &str = "wind-service-id";
/// Label carrying the service name.
const LABEL_SERVICE_NAME: &str = "wind-service-app";
/// Label carrying the instance version.
const LABEL_SERVICE_VERSION: &str = "wind-service-version";
/// Annotation carrying the JSON metadata.
const ANNOTATION_METADATA: &str = "wind-service-metadata";
/// Annotation carrying the JSON port-to-scheme map.
const ANNOTATION_PROTOCOLS: &str = "wind-service-protocols";

/// The resync period: every tick triggers a re-list broadcast.
const RESYNC: Duration = Duration::from_secs(600);

struct Inner {
    client: Client,
    /// The namespace the listing API is scoped to; empty means all
    /// namespaces.
    namespace: String,
    /// The owning pod's namespace, from the service account file.
    /// Patches always target this.
    own_namespace: String,
    /// The owning pod's name, from `HOSTNAME`.
    own_pod_name: String,
    /// Watched services, created on first [`Discovery::watch`].
    sets: Mutex<HashMap<String, Arc<ServiceSet>>>,
}

/// The cache of one watched service: the latest broadcast instance
/// list, held forever by the watcher task.
struct ServiceSet {
    cache: watch::Sender<Vec<Instance>>,
}

/// A Kubernetes-backed registry: in-cluster pod-label registration
/// and pod-watch discovery.
pub struct KubernetesRegistry {
    inner: Arc<Inner>,
}

impl KubernetesRegistry {
    /// Connects against the in-cluster service account, listing pods
    /// across all namespaces.
    pub async fn connect() -> Result<Self, RegistryError> {
        Self::connect_with("").await
    }

    /// Connects with the listing namespace scoped to `namespace`
    /// (empty = all namespaces). The namespace the service account
    /// file names is read here, exactly once.
    pub async fn connect_with(namespace: &str) -> Result<Self, RegistryError> {
        let config = Config::incluster()
            .map_err(|e| RegistryError::Failed(format!("kubernetes in-cluster config: {e}")))?;
        let client = Client::try_from(config)
            .map_err(|e| RegistryError::Failed(format!("kubernetes client: {e}")))?;
        let own_namespace =
            std::fs::read_to_string("/var/run/secrets/kubernetes.io/serviceaccount/namespace")
                .unwrap_or_default();
        let own_pod_name = std::env::var("HOSTNAME").unwrap_or_default();
        Ok(Self {
            inner: Arc::new(Inner {
                client,
                namespace: namespace.to_string(),
                own_namespace,
                own_pod_name,
                sets: Mutex::new(HashMap::new()),
            }),
        })
    }

    /// Constructs from the bootstrap factory's settings wire shape:
    /// `namespace` (default: all namespaces).
    pub async fn from_settings(settings: serde_json::Value) -> Result<Self, RegistryError> {
        let settings: KubernetesSettings = serde_json::from_value(settings)
            .map_err(|e| RegistryError::Failed(format!("settings parse: {e}")))?;
        Self::connect_with(settings.namespace.as_deref().unwrap_or("")).await
    }
}

/// The bootstrap factory's settings wire shape for
/// [`KubernetesRegistry::from_settings`].
#[derive(Deserialize)]
pub struct KubernetesSettings {
    /// The namespace the listing API is scoped to; absent means all
    /// namespaces.
    pub namespace: Option<String>,
}

impl KubernetesRegistry {
    fn pod_api(&self, namespace: &str) -> Api<Pod> {
        if namespace.is_empty() {
            Api::all(self.inner.client.clone())
        } else {
            Api::namespaced(self.inner.client.clone(), namespace)
        }
    }
}

impl Registrar for KubernetesRegistry {
    fn register<'a>(
        &'a self,
        registration: Registration,
    ) -> BoxFuture<'a, Result<RegistrationHandle, RegistryError>> {
        Box::pin(async move {
            let protocol_map_json = build_protocol_map(&registration)?;
            let patch_body = build_patch(
                &registration.instance.id,
                &registration.instance.name,
                &registration.instance.version,
                serde_json::to_string(&registration.metadata).unwrap_or_default(),
                &protocol_map_json,
            );
            self.patch_own_pod(patch_body).await?;
            // No keepalive machinery: the labels and annotations stay
            // until overwritten or the pod goes away.
            Ok(RegistrationHandle::from_cancel(|| {}))
        })
    }

    fn deregister<'a>(
        &'a self,
        _registration: Registration,
    ) -> BoxFuture<'a, Result<(), RegistryError>> {
        Box::pin(async move {
            // Deregistration: a registration patch with every
            // field blanked and both annotations reset to `{}`.
            let patch_body = build_patch("", "", "", "{}".to_string(), "{}");
            self.patch_own_pod(patch_body).await
        })
    }
}

impl KubernetesRegistry {
    async fn patch_own_pod(&self, patch_body: serde_json::Value) -> Result<(), RegistryError> {
        let api = self.pod_api(&self.inner.own_namespace);
        api.patch(
            &self.inner.own_pod_name,
            &PatchParams::default(),
            &Patch::Strategic(patch_body),
        )
        .await
        .map(|_| ())
        .map_err(|e| {
            RegistryError::Failed(format!(
                "kubernetes patch {} / {}: {e}",
                self.inner.own_namespace, self.inner.own_pod_name
            ))
        })
    }
}

impl Discovery for KubernetesRegistry {
    fn get_service<'a>(
        &'a self,
        service_name: &'a str,
    ) -> BoxFuture<'a, Result<Vec<Instance>, RegistryError>> {
        Box::pin(async move {
            let api = self.pod_api(&self.inner.namespace);
            list_instances(&api, service_name).await
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

            // Only a newly created set runs a watcher, started with
            // the watch creation.
            if spawned {
                let api = self.pod_api(&self.inner.namespace);
                let name = service_name.to_string();
                let watcher_set = Arc::clone(&set);
                tokio::spawn(async move {
                    watcher_task(api, name, watcher_set).await;
                });
            }

            Ok(Box::new(KubernetesWatcher {
                rx: set.cache.subscribe(),
                stopped: false,
            }) as Box<dyn Watcher>)
        })
    }
}

/// The watcher task: a `kube` watcher on the service's label selector
/// plus a ten-minute resync ticker. Every event —
/// initialization included —
/// and every tick trigger a fresh full-list read that replaces the
/// service cache and wakes its watchers. Failed reads skip the
/// broadcast.
async fn watcher_task(api: Api<Pod>, service_name: String, set: Arc<ServiceSet>) {
    let selector = format!("{LABEL_SERVICE_NAME}={service_name}");
    let stream = kube_runtime::watcher::watcher(
        api.clone(),
        kube_runtime::watcher::Config::default().labels(&selector),
    );
    tokio::pin!(stream);
    let mut resync = tokio::time::interval(RESYNC);
    loop {
        let refresh = tokio::select! {
            _ = resync.tick() => true,
            event = stream.next() => event.is_some(),
        };
        if !refresh {
            break;
        }
        if let Ok(instances) = list_instances(&api, &service_name).await {
            let _ = set.cache.send(instances);
        }
    }
}

/// The Kubernetes-backed [`Watcher`]: a receiver on the watched
/// service's broadcast cache. There is no
/// establishment shortcut — the initialization events of the
/// underlying watcher provide the first snapshot.
struct KubernetesWatcher {
    rx: watch::Receiver<Vec<Instance>>,
    stopped: bool,
}

impl Watcher for KubernetesWatcher {
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

impl Drop for KubernetesWatcher {
    fn drop(&mut self) {
        self.stop();
    }
}

/// Builds the strategic-merge patch body: the
/// identity labels and the two annotations, with `""`/`{}` values for
/// the deregistration shape.
fn build_patch(
    id: &str,
    name: &str,
    version: &str,
    metadata_json: String,
    protocol_map_json: &str,
) -> serde_json::Value {
    serde_json::json!({
        "metadata": {
            "labels": {
                LABEL_SERVICE_ID: id,
                LABEL_SERVICE_NAME: name,
                LABEL_SERVICE_VERSION: version,
            },
            "annotations": {
                ANNOTATION_METADATA: metadata_json,
                ANNOTATION_PROTOCOLS: protocol_map_json,
            },
        }
    })
}

/// Builds the port-to-scheme map from the registration's endpoints; an
/// endpoint without a port writes the empty-string key, and an
/// unparseable endpoint aborts the registration.
fn build_protocol_map(registration: &Registration) -> Result<String, RegistryError> {
    let mut map = HashMap::new();
    for endpoint in &registration.instance.endpoints {
        let Some((scheme, _host, port)) = split_endpoint(endpoint) else {
            return Err(RegistryError::Failed(format!("endpoint parse: {endpoint}")));
        };
        let key = if port == 0 {
            String::new()
        } else {
            port.to_string()
        };
        map.insert(key, scheme.to_string());
    }
    Ok(serde_json::to_string(&map).unwrap_or_default())
}

/// The instance rebuild from a pod: identity from the
/// labels, endpoints from the pod IP and every container port with the
/// protocol-map / port-name-prefix / IP-protocol fallback chain.
/// Non-`Running` pods skip (`Ok(None)`), and malformed annotations
/// propagate the parse error.
fn rebuild_instance(pod: &Pod) -> Result<Option<Instance>, RegistryError> {
    let Some(status) = &pod.status else {
        return Ok(None);
    };
    if status.phase.as_deref() != Some("Running") {
        return Ok(None);
    }
    let pod_ip = status.pod_ip.clone().unwrap_or_default();
    let labels = pod.metadata.labels.as_ref();
    let get_label = |key: &str| {
        labels
            .and_then(|map| map.get(key))
            .cloned()
            .unwrap_or_default()
    };
    let annotations = pod.metadata.annotations.as_ref();
    let annotation = |key: &str| -> &str {
        annotations
            .and_then(|map| map.get(key))
            .map(|value| value.as_str())
            .unwrap_or("")
    };
    let protocol_map = parse_object_string(annotation(ANNOTATION_PROTOCOLS))?;
    parse_object_string(annotation(ANNOTATION_METADATA))?;

    let mut endpoints = Vec::new();
    if let Some(spec) = &pod.spec {
        for container in &spec.containers {
            if let Some(ports) = &container.ports {
                for port in ports {
                    let port_number = port.container_port;
                    let mut protocol = protocol_map
                        .get(&port_number.to_string())
                        .cloned()
                        .unwrap_or_default();
                    if protocol.is_empty() {
                        let port_name = port.name.as_deref().unwrap_or("");
                        if !port_name.is_empty() {
                            protocol = port_name.split('-').next().unwrap_or_default().to_string();
                        } else {
                            protocol = port.protocol.clone().unwrap_or_default();
                        }
                    }
                    endpoints.push(format!("{protocol}://{pod_ip}:{port_number}"));
                }
            }
        }
    }

    Ok(Some(Instance {
        id: get_label(LABEL_SERVICE_ID),
        name: get_label(LABEL_SERVICE_NAME),
        version: get_label(LABEL_SERVICE_VERSION),
        endpoints,
    }))
}

/// Lists the pods labeled for `service_name` and rebuilds their
/// instances, with a Running-only filter.
async fn list_instances(
    api: &Api<Pod>,
    service_name: &str,
) -> Result<Vec<Instance>, RegistryError> {
    let selector = format!("{LABEL_SERVICE_NAME}={service_name}");
    let list_params = ListParams::default().labels(&selector);
    let pods = api
        .list(&list_params)
        .await
        .map_err(|e| RegistryError::Failed(format!("kubernetes list {selector}: {e}")))?;
    let mut instances = Vec::new();
    for pod in &pods.items {
        if let Some(instance) = rebuild_instance(pod)? {
            instances.push(instance);
        }
    }
    Ok(instances)
}

/// The empty-sentinel gate plus the JSON object parse
/// behind it: empty sentinel strings yield an empty map, parse errors
/// propagate.
fn parse_object_string(value: &str) -> Result<HashMap<String, String>, RegistryError> {
    if matches!(value, "" | "{}" | "null" | "nil" | "[]") {
        return Ok(HashMap::new());
    }
    serde_json::from_str(value)
        .map_err(|e| RegistryError::Failed(format!("kubernetes annotation parse: {e}")))
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
