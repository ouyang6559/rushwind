//! ZooKeeper adapter for the RushWind registry contract — registration
//! and discovery, ported from `go-wind-plugins/registry/zookeeper`.
//!
//! # The wire contract
//!
//! Identical to the etcd half's: the service-name and instance znodes
//! live at the go-wind layouts [`service_prefix`] and
//! [`rushwind_registry::registry_key`] under the namespace (default
//! [`DEFAULT_NAMESPACE`]), and the instance znode holds the go
//! `json.Marshal(wind.Instance)` bytes from
//! [`rushwind_registry::registry_json`]. Registration creates the two
//! persistent parent nodes and the **ephemeral** instance node; a
//! keeper task watches for session re-establishment and re-creates the
//! ephemeral node after a session flap — the Go `reRegister` loop,
//! event-driven instead of polled.
//!
//! # Discovery
//!
//! [`Discovery::get_service`] lists the service node's children and
//! parses each instance znode. [`Discovery::watch`] arms a child watch
//! on the service node — falling back to an exists watch while the
//! node is absent — re-arming after every firing; each firing delivers
//! a fresh full snapshot, the Go watcher's shape.
//!
//! # Divergences from the Go adapter
//!
//! - The session-restoration keeper dies when its registration handle
//!   drops; the Go goroutine leaks forever and can resurrect a
//!   deregistered service after a session flap.
//! - Deregister aborts the keeper alongside deleting the node; the Go
//!   version only deletes.
//!
//! # Cancellation
//!
//! Dropping the [`RegistrationHandle`] aborts the keeper task. The
//! ephemeral node itself lives until the session ends or
//! [`Registrar::deregister`] deletes it — ZooKeeper semantics; the
//! best-effort eventual removal here is session-bound, not
//! handle-bound. Watchers stop on drop.
//!
//! # Testing
//!
//! Live conformance tests run against a real ZooKeeper via the `live`
//! feature; the pure wire parts are golden-tested in
//! `rushwind-registry`.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use rushwind_registry::{
    registry_json, registry_key, registry_parse, service_prefix, BoxFuture, Discovery, Registrar,
    Registration, RegistrationHandle, RegistryError, Watcher, DEFAULT_NAMESPACE,
};
use rushwind_transport::Instance;
use serde::Deserialize;
use tokio::sync::mpsc;
use zookeeper_async::ZooKeeper;

struct Inner {
    zk: Arc<ZooKeeper>,
    namespace: String,
    /// The optional digest ACL credentials, mirroring the Go
    /// `WithDigestACL` option.
    digest: Option<(String, String)>,
    /// Live keeper tasks per instance node.
    tasks: Mutex<HashMap<String, tokio::task::JoinHandle<()>>>,
}

/// A ZooKeeper-backed registry: registration and discovery over one
/// session.
pub struct ZookeeperRegistry {
    inner: Arc<Inner>,
}

impl ZookeeperRegistry {
    /// Connects to the ensemble at `connect_string` (e.g.
    /// `127.0.0.1:2181`) with the default namespace. The session is
    /// negotiated with a five-second timeout.
    pub async fn connect(connect_string: &str) -> Result<Self, RegistryError> {
        Self::connect_with(connect_string, DEFAULT_NAMESPACE, None).await
    }

    /// Connects with an explicit namespace and optional digest ACL
    /// credentials for node creation.
    pub async fn connect_with(
        connect_string: &str,
        namespace: &str,
        digest: Option<(String, String)>,
    ) -> Result<Self, RegistryError> {
        let zk = ZooKeeper::connect(connect_string, std::time::Duration::from_secs(5), |_| {})
            .await
            .map_err(|e| {
                RegistryError::Failed(format!("zookeeper connect {connect_string}: {e}"))
            })?;
        Ok(Self {
            inner: Arc::new(Inner {
                zk: Arc::new(zk),
                namespace: namespace.to_string(),
                digest,
                tasks: Mutex::new(HashMap::new()),
            }),
        })
    }

    /// Constructs from the bootstrap factory's settings wire shape:
    /// `connect_string` (required), `namespace` (default
    /// [`DEFAULT_NAMESPACE`]), `digest_user`/`digest_password` (no
    /// digest ACL when either is absent).
    pub async fn from_settings(settings: serde_json::Value) -> Result<Self, RegistryError> {
        let settings: ZookeeperSettings = serde_json::from_value(settings)
            .map_err(|e| RegistryError::Failed(format!("settings parse: {e}")))?;
        let namespace = settings.namespace.as_deref().unwrap_or(DEFAULT_NAMESPACE);
        let digest = match (settings.digest_user, settings.digest_password) {
            (Some(user), Some(password)) => Some((user, password)),
            _ => None,
        };
        Self::connect_with(&settings.connect_string, namespace, digest).await
    }
}

/// The bootstrap factory's settings wire shape for
/// [`ZookeeperRegistry::from_settings`].
#[derive(Deserialize)]
pub struct ZookeeperSettings {
    /// The ensemble connect string.
    pub connect_string: String,
    /// The znode namespace. Default: [`DEFAULT_NAMESPACE`].
    pub namespace: Option<String>,
    /// The digest-ACL user.
    pub digest_user: Option<String>,
    /// The digest-ACL password.
    pub digest_password: Option<String>,
}

impl Registrar for ZookeeperRegistry {
    fn register<'a>(
        &'a self,
        registration: Registration,
    ) -> BoxFuture<'a, Result<RegistrationHandle, RegistryError>> {
        Box::pin(async move {
            let service_path = service_prefix(&self.inner.namespace, &registration.instance.name);
            let instance_path = registry_key(&self.inner.namespace, &registration.instance);
            let value = registry_json(&registration).into_bytes();

            // The persistent namespace and service nodes, then the
            // ephemeral instance node — the Go ensureName sequence.
            ensure_name(
                &self.inner.zk,
                &self.inner.namespace,
                &[],
                false,
                &self.inner.digest,
            )
            .await?;
            ensure_name(
                &self.inner.zk,
                &service_path,
                &[],
                false,
                &self.inner.digest,
            )
            .await?;
            ensure_name(
                &self.inner.zk,
                &instance_path,
                &value,
                true,
                &self.inner.digest,
            )
            .await?;

            // The keeper: on session re-establishment, re-create the
            // ephemeral node. The listener filters the initial
            // connection — every later Connected is a re-establishment.
            let (keeper_tx, mut keeper_rx) = mpsc::unbounded_channel::<()>();
            let established = AtomicBool::new(false);
            let listener_tx = keeper_tx.clone();
            self.inner.zk.add_listener(move |state| {
                if state == zookeeper_async::ZkState::Connected
                    && established.swap(true, Ordering::SeqCst)
                {
                    let _ = listener_tx.send(());
                }
            });
            let zk = Arc::clone(&self.inner.zk);
            let digest = self.inner.digest.clone();
            let keeper_path = instance_path.clone();
            let keeper_data = value;
            let task = tokio::spawn(async move {
                while keeper_rx.recv().await.is_some() {
                    // The Go keeper dies on the first failed re-create.
                    if ensure_name(&zk, &keeper_path, &keeper_data, true, &digest)
                        .await
                        .is_err()
                    {
                        break;
                    }
                }
            });
            self.inner
                .tasks
                .lock()
                .expect("keeper map poisoned")
                .insert(instance_path.clone(), task);

            let inner = Arc::clone(&self.inner);
            Ok(RegistrationHandle::from_cancel(move || {
                if let Some(task) = inner
                    .tasks
                    .lock()
                    .expect("keeper map poisoned")
                    .remove(&instance_path)
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
            let instance_path = registry_key(&self.inner.namespace, &registration.instance);
            if let Some(task) = self
                .inner
                .tasks
                .lock()
                .expect("keeper map poisoned")
                .remove(&instance_path)
            {
                task.abort();
            }
            self.inner
                .zk
                .delete(&instance_path, None)
                .await
                .map_err(|e| {
                    RegistryError::Failed(format!("zookeeper delete {instance_path}: {e}"))
                })?;
            Ok(())
        })
    }
}

impl Discovery for ZookeeperRegistry {
    fn get_service<'a>(
        &'a self,
        service_name: &'a str,
    ) -> BoxFuture<'a, Result<Vec<Instance>, RegistryError>> {
        Box::pin(async move {
            let prefix = service_prefix(&self.inner.namespace, service_name);
            // The Go GetService reads without a name filter: children
            // of the service node are its instances by construction.
            read_instances(&self.inner.zk, &prefix, service_name, false).await
        })
    }

    fn watch<'a>(
        &'a self,
        service_name: &'a str,
    ) -> BoxFuture<'a, Result<Box<dyn Watcher>, RegistryError>> {
        Box::pin(async move {
            let prefix = service_prefix(&self.inner.namespace, service_name);

            // The re-arm loop: children watch on the service node,
            // exists watch while it is absent — each firing is
            // forwarded to the watcher and the watch re-arms, the Go
            // watcher goroutine's shape. The loop dies when the
            // watcher's channel is gone.
            let (loop_tx, mut loop_rx) = mpsc::unbounded_channel::<zookeeper_async::WatchedEvent>();
            let (signal_tx, signal_rx) = mpsc::unbounded_channel::<WatchSignal>();
            let zk = Arc::clone(&self.inner.zk);
            let loop_prefix = prefix.clone();
            tokio::spawn(async move {
                loop {
                    let armed = zk
                        .get_children_w(&loop_prefix, {
                            let loop_tx = loop_tx.clone();
                            move |event| {
                                let _ = loop_tx.send(event);
                            }
                        })
                        .await;
                    match armed {
                        Err(zookeeper_async::ZkError::NoNode) => {
                            // The service node is absent: watch for its
                            // creation instead.
                            if zk
                                .exists_w(&loop_prefix, {
                                    let loop_tx = loop_tx.clone();
                                    move |event| {
                                        let _ = loop_tx.send(event);
                                    }
                                })
                                .await
                                .is_err()
                            {
                                let _ = signal_tx.send(WatchSignal::Fatal(format!(
                                    "zookeeper watch {loop_prefix}: exists arm failed"
                                )));
                                break;
                            }
                        }
                        Err(e) => {
                            let _ = signal_tx.send(WatchSignal::Fatal(format!(
                                "zookeeper watch {loop_prefix}: arm failed: {e}"
                            )));
                            break;
                        }
                        Ok(_) => {}
                    }
                    match loop_rx.recv().await {
                        Some(event) => {
                            if signal_tx.send(WatchSignal::Event(event)).is_err() {
                                break;
                            }
                        }
                        None => break,
                    }
                }
            });

            Ok(Box::new(ZookeeperWatcher {
                zk: Arc::clone(&self.inner.zk),
                prefix,
                service_name: service_name.to_string(),
                first: true,
                signal: Some(signal_rx),
            }) as Box<dyn Watcher>)
        })
    }
}

/// The ZooKeeper-backed [`Watcher`]: a channel fed by the re-arm loop.
/// The first [`Watcher::next`] call is the establishment snapshot;
/// subsequent calls block on the loop's firings and re-read. A
/// disconnect event ends the call with an error — the Go
/// `ErrWatcherStopped` shape.
struct ZookeeperWatcher {
    zk: Arc<ZooKeeper>,
    prefix: String,
    service_name: String,
    first: bool,
    signal: Option<mpsc::UnboundedReceiver<WatchSignal>>,
}

impl Watcher for ZookeeperWatcher {
    fn next<'a>(&'a mut self) -> BoxFuture<'a, Result<Vec<Instance>, RegistryError>> {
        Box::pin(async move {
            if self.first {
                self.first = false;
                return read_instances(&self.zk, &self.prefix, &self.service_name, true).await;
            }
            let Some(signal) = self.signal.as_mut() else {
                return Err(RegistryError::Failed("watcher stopped".to_string()));
            };
            let Some(message) = signal.recv().await else {
                return Err(RegistryError::Failed("watcher stopped".to_string()));
            };
            match message {
                WatchSignal::Fatal(error) => Err(RegistryError::Failed(error)),
                WatchSignal::Event(event) => match event.keeper_state {
                    zookeeper_async::KeeperState::Disconnected => Err(RegistryError::Failed(
                        "watcher stopped: connection lost".to_string(),
                    )),
                    state @ (zookeeper_async::KeeperState::Expired
                    | zookeeper_async::KeeperState::AuthFailed) => Err(RegistryError::Failed(
                        format!("zookeeper watch: session state {state}"),
                    )),
                    _ => read_instances(&self.zk, &self.prefix, &self.service_name, true).await,
                },
            }
        })
    }

    fn stop(&mut self) {
        self.signal = None;
    }
}

impl Drop for ZookeeperWatcher {
    fn drop(&mut self) {
        self.stop();
    }
}

/// The re-arm loop's messages to its watcher.
enum WatchSignal {
    /// A watch fired.
    Event(zookeeper_async::WatchedEvent),
    /// Arming the watch failed fatally; the loop is dead.
    Fatal(String),
}

/// The Go `ensureName`: create `path` when absent, with the ephemeral
/// variant first deleting a leftover node at the path — the Go
/// restart-race handling.
async fn ensure_name(
    zk: &ZooKeeper,
    path: &str,
    data: &[u8],
    ephemeral: bool,
    digest: &Option<(String, String)>,
) -> Result<(), RegistryError> {
    let stat = zk
        .exists(path, false)
        .await
        .map_err(|e| RegistryError::Failed(format!("zookeeper exists {path}: {e}")))?;
    let exists = match stat {
        Some(stat) if ephemeral => {
            // The Go restart-race handling: a leftover node at an
            // ephemeral path is deleted before recreation.
            match zk.delete(path, Some(stat.version)).await {
                Err(zookeeper_async::ZkError::NoNode) => {}
                Err(e) => {
                    return Err(RegistryError::Failed(format!(
                        "zookeeper delete {path}: {e}"
                    )))
                }
                Ok(()) => {}
            }
            false
        }
        Some(_) => true,
        None => false,
    };
    if !exists {
        let acl: Vec<zookeeper_async::Acl> = match digest {
            Some((user, password)) => vec![zookeeper_async::Acl::new(
                zookeeper_async::Permission::ALL,
                "digest",
                format!("{user}:{password}"),
            )],
            None => zookeeper_async::Acl::open_unsafe().clone(),
        };
        let mode = if ephemeral {
            zookeeper_async::CreateMode::Ephemeral
        } else {
            zookeeper_async::CreateMode::Persistent
        };
        match zk.create(path, data.to_vec(), acl, mode).await {
            Err(zookeeper_async::ZkError::NodeExists) => {
                // Concurrent creation won the race. The Go original
                // fails here; the node exists either way, so this is
                // an idempotent completion, not a divergence.
                match zk
                    .exists(path, false)
                    .await
                    .map_err(|e| RegistryError::Failed(format!("zookeeper exists {path}: {e}")))?
                {
                    Some(_) => {}
                    None => {
                        return Err(RegistryError::Failed(format!(
                            "zookeeper create {path}: node vanished mid-race"
                        )))
                    }
                }
            }
            Err(e) => {
                return Err(RegistryError::Failed(format!(
                    "zookeeper create {path}: {e}"
                )))
            }
            Ok(_) => {}
        }
    }
    Ok(())
}

/// Reads the instance list for `prefix` — each child znode's data
/// parsed as the go-wind wire JSON. `filter` applies the Go watcher's
/// parsed-name filter; the registry-side read never filters.
async fn read_instances(
    zk: &ZooKeeper,
    prefix: &str,
    service_name: &str,
    filter: bool,
) -> Result<Vec<Instance>, RegistryError> {
    let children = zk
        .get_children(prefix, false)
        .await
        .map_err(|e| RegistryError::Failed(format!("zookeeper children {prefix}: {e}")))?;
    let mut instances = Vec::new();
    for child in children {
        let path = format!("{prefix}/{child}");
        let (data, _stat) = zk
            .get_data(&path, false)
            .await
            .map_err(|e| RegistryError::Failed(format!("zookeeper get {path}: {e}")))?;
        let instance = registry_parse(&String::from_utf8_lossy(&data))?;
        if filter && instance.name != service_name {
            continue;
        }
        instances.push(instance);
    }
    Ok(instances)
}
