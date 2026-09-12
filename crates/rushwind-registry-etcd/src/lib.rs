//! Etcd registration-only adapter for the RushWind registry contract.
//!
//! [`EtcdRegistrar`] announces instances to etcd using the **extracted
//! go-wind wire contract**: key `{namespace}/{name}/{id}` (namespace
//! defaults to [`DEFAULT_NAMESPACE`]), value = the go-`json.Marshal`-
//! compatible instance JSON from [`rushwind_registry::registry_json`],
//! lease-granted with a TTL (default 15 s) and kept alive by a self-healing
//! background task that re-grants and re-puts on loss — mirroring the Go
//! registrar's `heartBeat` goroutine.
//!
//! # Cancellation
//!
//! [`Registrar::register`] returns a [`RegistrationHandle`]. Dropping it
//! aborts the keepalive task; the lease then expires within its TTL and
//! etcd removes the key — a best-effort eventual removal, because a Drop
//! cannot await a revoke. [`Registrar::deregister`] deletes the key
//! immediately instead. Both are safe; pick per call site.
//!
//! # Testing
//!
//! The pure parts (key layout, wire JSON) are golden-tested against Go
//! output in `rushwind-registry`. The gRPC wrapper in this crate is
//! intentionally thin; exercise it against a live etcd when deploying (no
//! embedded etcd exists for CI).

#![forbid(unsafe_code)]
#![deny(missing_docs)]

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use rushwind_registry::{
    registry_json, registry_key, BoxFuture, Registrar, Registration, RegistrationHandle,
    RegistryError, DEFAULT_NAMESPACE,
};

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

/// An etcd-backed registrar.
pub struct EtcdRegistrar {
    inner: Arc<Inner>,
}

impl EtcdRegistrar {
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
}

impl Registrar for EtcdRegistrar {
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
