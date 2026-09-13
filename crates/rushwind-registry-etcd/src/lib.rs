//! Etcd adapter for the RushWind registry contract — registration and
//! discovery, ported from `go-wind-plugins/registry/etcd`.
//!
//! Registration announces instances using the **extracted go-wind wire
//! contract**: key `{namespace}/{name}/{id}` (namespace defaults to
//! [`DEFAULT_NAMESPACE`]), value = the go-`json.Marshal`-compatible
//! instance JSON from [`rushwind_registry::registry_json`], lease-granted
//! with a TTL (default 15 s) and kept alive by a self-healing background
//! task that re-grants and re-puts on loss — mirroring the Go registrar's
//! `heartBeat` goroutine.
//!
//! Discovery is the Go `Discovery`/`watcher` pair:
//! [`Discovery::get_service`] reads a service's instance list through the
//! KV API, and [`Discovery::watch`] establishes a prefix watch whose every
//! response — including the establishment progress notification —
//! triggers a full snapshot re-read. A stream that dies, is canceled
//! server-side, or hits a compaction boundary is rebuilt after a
//! one-second backoff, mirroring the Go watcher's `reWatch` path.
//!
//! # Cancellation
//!
//! [`Registrar::register`] returns a [`RegistrationHandle`]. Dropping it
//! aborts the keepalive task; the lease then expires within its TTL and
//! etcd removes the key — a best-effort eventual removal, because a Drop
//! cannot await a revoke. [`Registrar::deregister`] deletes the key
//! immediately instead. Both are safe; pick per call site.
//!
//! Watchers stop on drop: the watch stream is dropped, which ends the
//! gRPC stream and releases the server-side watcher.
//!
//! # Testing
//!
//! The pure parts (key layout, wire JSON) are golden-tested against Go
//! output in `rushwind-registry`. The gRPC wrapper in this crate is
//! intentionally thin; live conformance tests exercise it against a real
//! etcd via the `live` feature (no embedded etcd exists for CI).

#![forbid(unsafe_code)]
#![deny(missing_docs)]

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use rushwind_registry::{
    registry_json, registry_key, registry_parse, service_prefix, BoxFuture, Discovery, Registrar,
    Registration, RegistrationHandle, RegistryError, Watcher, DEFAULT_NAMESPACE,
};
use rushwind_transport::Instance;
use serde::Deserialize;

/// The keepalive refresh cadence: a third of the TTL, so several refresh
/// attempts fit before expiry.
fn refresh_interval(ttl: i64) -> Duration {
    Duration::from_secs((ttl / 3).max(1) as u64)
}

/// Shared registrar internals.
struct Inner {
    /// etcd's mutating API takes `&mut self`; a shared handle is the
    /// trait-compatible shape (`Registrar` methods take `&self`).
    client: tokio::sync::Mutex<etcd_client::Client>,
    namespace: String,
    ttl: i64,
    /// Live keepalive tasks per registry key; aborting one stops the
    /// heartbeat and lets the lease expire.
    tasks: Mutex<HashMap<String, tokio::task::JoinHandle<()>>>,
}

/// An etcd-backed registry: registration and discovery over one client,
/// the port of the Go `registry.Registry` type.
pub struct EtcdRegistry {
    inner: Arc<Inner>,
}

impl EtcdRegistry {
    /// Connects to etcd at `endpoints` with the default namespace and a
    /// 15 s lease TTL.
    pub async fn connect<E>(endpoints: &[E]) -> Result<Self, RegistryError>
    where
        E: AsRef<str>,
    {
        Self::connect_with(endpoints, DEFAULT_NAMESPACE, 15).await
    }

    /// Connects with an explicit namespace and lease TTL (seconds).
    pub async fn connect_with<E>(
        endpoints: &[E],
        namespace: &str,
        ttl_seconds: i64,
    ) -> Result<Self, RegistryError>
    where
        E: AsRef<str>,
    {
        let client = etcd_client::Client::connect(endpoints, None)
            .await
            .map_err(|e| RegistryError::Failed(format!("etcd connect: {e}")))?;
        Ok(Self {
            inner: Arc::new(Inner {
                client: tokio::sync::Mutex::new(client),
                namespace: namespace.to_string(),
                ttl: ttl_seconds,
                tasks: Mutex::new(HashMap::new()),
            }),
        })
    }

    /// Constructs from the bootstrap factory's settings wire shape:
    /// `endpoints` (required), `namespace` (default
    /// [`DEFAULT_NAMESPACE`]), `ttl` seconds (default 15).
    pub async fn from_settings(settings: serde_json::Value) -> Result<Self, RegistryError> {
        let settings: EtcdSettings = serde_json::from_value(settings)
            .map_err(|e| RegistryError::Failed(format!("settings parse: {e}")))?;
        Self::connect_with(
            &settings.endpoints,
            settings.namespace.as_deref().unwrap_or(DEFAULT_NAMESPACE),
            settings.ttl.unwrap_or(15),
        )
        .await
    }
}

/// The bootstrap factory's settings wire shape for
/// [`EtcdRegistry::from_settings`].
#[derive(Deserialize)]
pub struct EtcdSettings {
    /// The etcd endpoint URLs.
    pub endpoints: Vec<String>,
    /// The key namespace. Default: [`DEFAULT_NAMESPACE`].
    pub namespace: Option<String>,
    /// The lease TTL, in seconds. Default: 15.
    pub ttl: Option<i64>,
}

impl Registrar for EtcdRegistry {
    fn register<'a>(
        &'a self,
        registration: Registration,
    ) -> BoxFuture<'a, Result<RegistrationHandle, RegistryError>> {
        Box::pin(async move {
            let key = registry_key(&self.inner.namespace, &registration.instance);
            let value = registry_json(&registration);
            let ttl = self.inner.ttl;

            // Grant + put once here, so registration errors surface to the
            // caller instead of hiding inside the keepalive task.
            let lease_id = {
                let mut client = self.inner.client.lock().await;
                grant_and_put(&mut client, &key, &value, ttl).await?
            };

            // The self-healing loop: refresh the lease; on any loss,
            // re-grant and re-put (mirroring the Go registrar's heartBeat).
            // The task owns its own client handle.
            let task_client = self.inner.client.lock().await.clone();
            let heal_key = key.clone();
            let task = tokio::spawn(async move {
                let mut client = task_client;
                let mut lease_id = lease_id;
                loop {
                    if let Ok((mut keeper, mut stream)) = client.lease_keep_alive(lease_id).await {
                        loop {
                            if keeper.keep_alive().await.is_err() {
                                break;
                            }
                            let Ok(Ok(Some(_response))) =
                                tokio::time::timeout(refresh_interval(ttl), stream.message()).await
                            else {
                                break;
                            };
                        }
                    }
                    // Lease lost or keepalive broken: re-grant and re-put,
                    // forever.
                    lease_id = match grant_and_put(&mut client, &heal_key, &value, ttl).await {
                        Ok(id) => id,
                        Err(_) => {
                            tokio::time::sleep(Duration::from_secs(1)).await;
                            continue;
                        }
                    };
                }
            });

            self.inner
                .tasks
                .lock()
                .expect("keepalive map poisoned")
                .insert(key.clone(), task);

            let inner = Arc::clone(&self.inner);
            Ok(RegistrationHandle::from_cancel(move || {
                if let Some(task) = inner
                    .tasks
                    .lock()
                    .expect("keepalive map poisoned")
                    .remove(&key)
                {
                    task.abort();
                }
            }))
        })
    }

    fn deregister<'a>(
        &'a self,
        registration: Registration,
    ) -> BoxFuture<'a, Result<(), RegistryError>> {
        Box::pin(async move {
            let key = registry_key(&self.inner.namespace, &registration.instance);
            if let Some(task) = self
                .inner
                .tasks
                .lock()
                .expect("keepalive map poisoned")
                .remove(&key)
            {
                task.abort();
            }
            let mut client = self.inner.client.lock().await;
            client
                .delete(key.as_str(), None)
                .await
                .map_err(|e| RegistryError::Failed(format!("etcd delete {key}: {e}")))?;
            Ok(())
        })
    }
}

impl Discovery for EtcdRegistry {
    fn get_service<'a>(
        &'a self,
        service_name: &'a str,
    ) -> BoxFuture<'a, Result<Vec<Instance>, RegistryError>> {
        Box::pin(async move {
            let prefix = service_prefix(&self.inner.namespace, service_name);
            let mut client = self.inner.client.lock().await;
            read_instances(&mut client, &prefix, service_name).await
        })
    }

    fn watch<'a>(
        &'a self,
        service_name: &'a str,
    ) -> BoxFuture<'a, Result<Box<dyn Watcher>, RegistryError>> {
        Box::pin(async move {
            let prefix = service_prefix(&self.inner.namespace, service_name);
            let mut client = self.inner.client.lock().await.clone();
            let stream = open_watch(&mut client, &prefix).await?;
            Ok(Box::new(EtcdWatcher {
                client,
                prefix,
                service_name: service_name.to_string(),
                first: true,
                stream: Some(stream),
            }) as Box<dyn Watcher>)
        })
    }
}

/// The etcd-backed [`Watcher`]: a prefix watch whose every response —
/// including the establishment progress notification — triggers a full
/// snapshot re-read through the KV API, mirroring the Go watcher's
/// `Next`/`getInstance` pair. A dead, canceled, or compacted stream is
/// rebuilt after a one-second backoff, mirroring its `reWatch` path.
struct EtcdWatcher {
    /// A private client handle, cloned at watch creation so snapshot
    /// reads never contend on the registry's client lock.
    client: etcd_client::Client,
    /// The watched prefix: `{namespace}/{name}`.
    prefix: String,
    /// The name filter applied to parsed instances.
    service_name: String,
    /// Whether the next snapshot is the establishment snapshot.
    first: bool,
    /// The watch stream; `None` once stopped.
    stream: Option<etcd_client::WatchStream>,
}

impl Watcher for EtcdWatcher {
    fn next<'a>(&'a mut self) -> BoxFuture<'a, Result<Vec<Instance>, RegistryError>> {
        Box::pin(async move {
            if self.first {
                self.first = false;
                return read_instances(&mut self.client, &self.prefix, &self.service_name).await;
            }
            let message = match self.stream.as_mut() {
                Some(stream) => stream.message().await,
                None => {
                    return Err(RegistryError::Failed(
                        "watcher stopped: stream already released".to_string(),
                    ))
                }
            };
            let healthy = match message {
                Ok(Some(response)) => !response.canceled() && response.compact_revision() == 0,
                _ => false,
            };
            if !healthy {
                // The stream died, was canceled server-side, or hit a
                // compaction boundary — the cases the Go watcher's channel
                // closes on. Rebuild after the backoff, then re-read.
                self.stream = None;
                tokio::time::sleep(Duration::from_secs(1)).await;
                let stream = open_watch(&mut self.client, &self.prefix).await?;
                self.stream = Some(stream);
            }
            read_instances(&mut self.client, &self.prefix, &self.service_name).await
        })
    }

    fn stop(&mut self) {
        self.stream = None;
    }
}

impl Drop for EtcdWatcher {
    fn drop(&mut self) {
        self.stop();
    }
}

/// Grants a lease and puts `value` at `key`, bound to the lease.
async fn grant_and_put(
    client: &mut etcd_client::Client,
    key: &str,
    value: &str,
    ttl: i64,
) -> Result<i64, RegistryError> {
    let grant = client
        .lease_grant(ttl, None)
        .await
        .map_err(|e| RegistryError::Failed(format!("etcd lease grant: {e}")))?;
    let lease_id = grant.id();
    client
        .put(
            key,
            value,
            Some(etcd_client::PutOptions::new().with_lease(lease_id)),
        )
        .await
        .map_err(|e| RegistryError::Failed(format!("etcd put {key}: {e}")))?;
    Ok(lease_id)
}

/// Reads the full instance list for `prefix` through the KV API, keeping
/// only entries whose parsed name matches — the prefix over-matches
/// sibling service names (`order` also covers `order-service`), so the
/// parsed-name filter is load-bearing, exactly like the Go discovery's
/// `GetService`.
async fn read_instances(
    client: &mut etcd_client::Client,
    prefix: &str,
    service_name: &str,
) -> Result<Vec<Instance>, RegistryError> {
    let response = client
        .get(prefix, Some(etcd_client::GetOptions::new().with_prefix()))
        .await
        .map_err(|e| RegistryError::Failed(format!("etcd get prefix {prefix}: {e}")))?;
    let mut instances = Vec::new();
    for kv in response.kvs() {
        let value = kv.value_str().map_err(|e| {
            RegistryError::Failed(format!("etcd get prefix {prefix}: non-utf8 value: {e}"))
        })?;
        let instance = registry_parse(value)?;
        if instance.name != service_name {
            continue;
        }
        instances.push(instance);
    }
    Ok(instances)
}

/// Establishes the prefix watch on `prefix` and requests an immediate
/// progress notification — the exact setup the Go watcher performs at
/// creation, verifying stream liveness before the first snapshot read.
async fn open_watch(
    client: &mut etcd_client::Client,
    prefix: &str,
) -> Result<etcd_client::WatchStream, RegistryError> {
    let mut stream = client
        .watch(prefix, Some(etcd_client::WatchOptions::new().with_prefix()))
        .await
        .map_err(|e| RegistryError::Failed(format!("etcd watch {prefix}: {e}")))?;
    stream.request_progress().await.map_err(|e| {
        RegistryError::Failed(format!("etcd watch {prefix}: request progress: {e}"))
    })?;
    Ok(stream)
}
