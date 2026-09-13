//! Consul adapter for the RushWind registry contract — registration and
//! discovery, ported from `go-wind-plugins/registry/consul` and speaking
//! the same agent HTTP API the hashicorp Go client speaks.
//!
//! # Registration
//!
//! [`Registrar::register`] PUTs the go-wind registration shape to
//! `/v1/agent/service/register`: instance identity as the service
//! registration, version in a `version=` tag, endpoints as tagged
//! addresses holding the full endpoint URL strings keyed by scheme.
//! Health-check registration follows the Go registrar: per-endpoint TCP
//! checks and a TTL check (`service:{id}`, TTL twice the check interval),
//! each with `DeregisterCriticalServiceAfter`, plus a background
//! heartbeat task that keeps the TTL check passing and re-registers the
//! whole service when a heartbeat PUT fails — the Go
//! `heartBeat`-goroutine behavior, heartbeat failure included.
//!
//! # Discovery
//!
//! [`Discovery::get_service`] reads the current passing-only health view
//! (`/v1/health/service/{name}?passing=1`). [`Discovery::watch`] starts,
//! per newly watched service, a background loop over consul's **blocking
//! queries** (`index` + `wait=55000ms`, cut to the 10 s client timeout —
//! exactly the Go resolver's shape): each observed index change carrying
//! a non-empty instance list updates the service's cache and wakes its
//! watchers. Empty lists — removals — never wake watchers, matching the
//! Go fanout.
//!
//! # Divergences from the Go adapter
//!
//! - Single datacenter: the Go `MULTI` datacenter mode is not ported.
//! - The Go resolver/service-check function hooks are not ported; the
//!   default resolver is baked in.
//! - Watched services live forever: the poll loop holds the service
//!   cache permanently, as the Go resolve goroutines do.
//!
//! # Cancellation
//!
//! [`RegistrationHandle`] drops abort the heartbeat task: the TTL check
//! then goes critical and consul deregisters the service after its
//! `DeregisterCriticalServiceAfter` — a best-effort eventual removal,
//! the consul analogue of lease expiry on the etcd side.
//! [`Registrar::deregister`] deletes the service immediately.
//! Watchers stop on drop.
//!
//! # Testing
//!
//! Live conformance tests run against a real consul via the `live`
//! feature; there is no embedded consul for CI's unit lanes.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

use std::collections::{hash_map::Entry, BTreeMap, HashMap};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use rushwind_registry::{
    BoxFuture, Discovery, Registrar, Registration, RegistrationHandle, RegistryError, Watcher,
};
use rushwind_transport::Instance;
use serde::{Deserialize, Serialize};
use tokio::sync::watch;

/// The Go resolver's blocking-query wait, in milliseconds.
const BLOCKING_WAIT: &str = "55000ms";
/// The Go client's per-request body for a passing TTL update: its
/// `UpdateTTL` normalizes the "pass" status to "passing" before
/// sending, so the wire value is "passing".
const PASSING_UPDATE: &str = r#"{"Status":"passing","Output":"pass"}"#;

/// Options mirroring the Go registrar's option surface.
#[derive(Debug, Clone)]
pub struct ConsulOptions {
    /// Whether per-endpoint TCP checks are registered. Default `true`.
    pub enable_health_check: bool,
    /// Whether a TTL check is registered and kept passing by a
    /// heartbeat task. Default `true`.
    pub heartbeat: bool,
    /// The check interval in seconds; the TTL check's TTL is twice
    /// this. Default `10`.
    pub healthcheck_interval_secs: u64,
    /// The checks' `DeregisterCriticalServiceAfter`, in seconds.
    /// Default `600`.
    pub deregister_critical_after_secs: u64,
    /// The per-request timeout for health queries. Default 10 s.
    pub timeout: Duration,
}

impl Default for ConsulOptions {
    fn default() -> Self {
        Self {
            enable_health_check: true,
            heartbeat: true,
            healthcheck_interval_secs: 10,
            deregister_critical_after_secs: 600,
            timeout: Duration::from_secs(10),
        }
    }
}

/// The cache of one watched service: the latest broadcast instance
/// list. The poll task holds this forever once a service is watched —
/// the Go adapter's permanent resolve goroutines.
struct ServiceSet {
    /// The latest instance snapshot; empty until first broadcast.
    cache: watch::Sender<Vec<Instance>>,
}

struct Inner {
    http: reqwest::Client,
    base: String,
    options: ConsulOptions,
    /// Watched services, created on first [`Discovery::watch`].
    sets: Mutex<HashMap<String, Arc<ServiceSet>>>,
    /// Live heartbeat tasks per `{name}/{id}`; aborting one stops the
    /// heartbeat and lets the TTL check go critical.
    tasks: Mutex<HashMap<String, tokio::task::JoinHandle<()>>>,
}

/// A consul-backed registry: registration and discovery over the agent
/// HTTP API.
pub struct ConsulRegistry {
    inner: Arc<Inner>,
}

impl ConsulRegistry {
    /// Connects to the consul agent at `addr` (e.g.
    /// `http://127.0.0.1:8500`) with default options.
    pub fn connect(addr: &str) -> Result<Self, RegistryError> {
        Self::connect_with(addr, ConsulOptions::default())
    }

    /// Connects with explicit options.
    pub fn connect_with(addr: &str, options: ConsulOptions) -> Result<Self, RegistryError> {
        Ok(Self {
            inner: Arc::new(Inner {
                http: reqwest::Client::new(),
                base: addr.trim_end_matches('/').to_string(),
                options,
                sets: Mutex::new(HashMap::new()),
                tasks: Mutex::new(HashMap::new()),
            }),
        })
    }

    /// Constructs from the bootstrap factory's settings wire shape:
    /// `addr` (required). Every health-check knob stays at the Go
    /// defaults.
    pub fn from_settings(settings: serde_json::Value) -> Result<Self, RegistryError> {
        let settings: ConsulSettings = serde_json::from_value(settings)
            .map_err(|e| RegistryError::Failed(format!("settings parse: {e}")))?;
        Self::connect_with(&settings.addr, ConsulOptions::default())
    }
}

/// The bootstrap factory's settings wire shape for
/// [`ConsulRegistry::from_settings`].
#[derive(Deserialize)]
pub struct ConsulSettings {
    /// The consul agent address (e.g. `http://127.0.0.1:8500`).
    pub addr: String,
}

impl Registrar for ConsulRegistry {
    fn register<'a>(
        &'a self,
        registration: Registration,
    ) -> BoxFuture<'a, Result<RegistrationHandle, RegistryError>> {
        Box::pin(async move {
            let payload = build_registration(&self.inner.options, &registration)?;
            put_register(&self.inner.http, &self.inner.base, &payload).await?;

            // The heartbeat task: initial pass after 1 s, then one per
            // interval; a failing pass re-registers the service after a
            // short random backoff — the Go heartBeat goroutine.
            let task_key = format!(
                "{}/{}",
                registration.instance.name, registration.instance.id
            );
            let task = if self.inner.options.heartbeat {
                let http = self.inner.client();
                let base = self.inner.base.clone();
                let check_id = format!("service:{}", registration.instance.id);
                let interval = Duration::from_secs(self.inner.options.healthcheck_interval_secs);
                let payload = payload.clone();
                Some(tokio::spawn(async move {
                    heartbeat_loop(http, base, payload, check_id, interval).await;
                }))
            } else {
                None
            };
            match task {
                Some(task) => {
                    self.inner
                        .tasks
                        .lock()
                        .expect("heartbeat map poisoned")
                        .insert(task_key.clone(), task);
                    let inner = Arc::clone(&self.inner);
                    Ok(RegistrationHandle::from_cancel(move || {
                        if let Some(task) = inner
                            .tasks
                            .lock()
                            .expect("heartbeat map poisoned")
                            .remove(&task_key)
                        {
                            task.abort();
                        }
                    }))
                }
                None => Ok(RegistrationHandle::from_cancel(|| {})),
            }
        })
    }

    fn deregister<'a>(
        &'a self,
        registration: Registration,
    ) -> BoxFuture<'a, Result<(), RegistryError>> {
        Box::pin(async move {
            let task_key = format!(
                "{}/{}",
                registration.instance.name, registration.instance.id
            );
            if let Some(task) = self
                .inner
                .tasks
                .lock()
                .expect("heartbeat map poisoned")
                .remove(&task_key)
            {
                task.abort();
            }
            put_deregister(
                &self.inner.http,
                &self.inner.base,
                &registration.instance.id,
            )
            .await
        })
    }
}

impl Discovery for ConsulRegistry {
    fn get_service<'a>(
        &'a self,
        service_name: &'a str,
    ) -> BoxFuture<'a, Result<Vec<Instance>, RegistryError>> {
        Box::pin(async move {
            // The Go adapter serves a watched service from its cache
            // when one has been broadcast.
            let cached = {
                let sets = self.inner.sets.lock().expect("service sets poisoned");
                sets.get(service_name)
                    .map(|set| set.cache.borrow().clone())
                    .filter(|cached| !cached.is_empty())
            };
            if let Some(cached) = cached {
                return Ok(cached);
            }
            // One-shot immediate fetch (index 0): the current state.
            let (instances, _) = fetch_instances(
                &self.inner.http,
                &self.inner.base,
                service_name,
                None,
                self.inner.options.timeout,
            )
            .await?;
            if instances.is_empty() {
                return Err(RegistryError::Failed(format!(
                    "service {service_name} not found in registry"
                )));
            }
            Ok(instances)
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
                    Entry::Occupied(entry) => (Arc::clone(entry.get()), false),
                    Entry::Vacant(entry) => {
                        let (cache, _rx) = watch::channel(Vec::new());
                        let set = Arc::new(ServiceSet { cache });
                        entry.insert(Arc::clone(&set));
                        (set, true)
                    }
                }
            };

            // Only a newly created set resolves: the Go adapter runs the
            // initial fetch inline (failure aborts the watch) and then
            // spawns the permanent blocking-poll loop.
            if spawned {
                let (instances, index) = fetch_instances(
                    &self.inner.http,
                    &self.inner.base,
                    service_name,
                    None,
                    self.inner.options.timeout,
                )
                .await?;
                if !instances.is_empty() {
                    let _ = set.cache.send(instances);
                }
                let http = self.inner.client();
                let base = self.inner.base.clone();
                let name = service_name.to_string();
                let timeout = self.inner.options.timeout;
                let poll_set = Arc::clone(&set);
                tokio::spawn(async move {
                    poll_loop(http, base, name, timeout, poll_set, index).await;
                });
            }

            Ok(Box::new(ConsulWatcher {
                rx: set.cache.subscribe(),
                first: true,
                stopped: false,
            }) as Box<dyn Watcher>)
        })
    }
}

impl Inner {
    fn client(&self) -> reqwest::Client {
        self.http.clone()
    }
}

/// The consul-backed [`Watcher`]: a receiver on the watched service's
/// broadcast cache. The first [`Watcher::next`] returns the current
/// cache immediately when a snapshot is already broadcast (the Go
/// watcher's creation push), and blocks on broadcasts otherwise.
struct ConsulWatcher {
    rx: watch::Receiver<Vec<Instance>>,
    first: bool,
    stopped: bool,
}

impl Watcher for ConsulWatcher {
    fn next<'a>(&'a mut self) -> BoxFuture<'a, Result<Vec<Instance>, RegistryError>> {
        Box::pin(async move {
            if self.stopped {
                return Err(RegistryError::Failed("watcher stopped".to_string()));
            }
            if self.first {
                self.first = false;
                let cached = self.rx.borrow_and_update().clone();
                if !cached.is_empty() {
                    return Ok(cached);
                }
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

impl Drop for ConsulWatcher {
    fn drop(&mut self) {
        self.stop();
    }
}

/// The permanent blocking-poll loop: one query per second (the Go
/// resolver's ticker cadence), each holding on the last-seen index
/// until consul reports a change or the client timeout cuts it.
/// Errors back off one second, as the Go loop does.
async fn poll_loop(
    http: reqwest::Client,
    base: String,
    service_name: String,
    timeout: Duration,
    set: Arc<ServiceSet>,
    mut index: u64,
) {
    loop {
        tokio::time::sleep(Duration::from_secs(1)).await;
        let (instances, next_index) =
            match fetch_instances(&http, &base, &service_name, Some(index), timeout).await {
                Ok(result) => result,
                Err(_) => {
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    continue;
                }
            };
        if !instances.is_empty() && next_index != index {
            let _ = set.cache.send(instances);
        }
        index = next_index;
    }
}

/// The TTL heartbeat loop: one passing update after a second, then one
/// per interval; a failing update re-registers the service after a
/// short random backoff — the Go heartBeat goroutine, minus its
/// context-cancellation deregistration (the Rust cancellation story is
/// task abort plus `DeregisterCriticalServiceAfter`).
async fn heartbeat_loop(
    http: reqwest::Client,
    base: String,
    payload: String,
    check_id: String,
    interval: Duration,
) {
    tokio::time::sleep(Duration::from_secs(1)).await;
    let _ = put_pass(&http, &base, &check_id).await;
    loop {
        tokio::time::sleep(interval).await;
        if put_pass(&http, &base, &check_id).await.is_err() {
            let backoff = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.subsec_nanos() % 5)
                .unwrap_or(0);
            tokio::time::sleep(Duration::from_secs(backoff as u64)).await;
            let _ = put_register(&http, &base, &payload).await;
        }
    }
}

/// Splits an endpoint URL into `(scheme, host, port)`, with the port
/// defaulting to 0 — the Go register path's `url.Parse` +
/// ignored-error `ParseUint` shape.
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

/// Builds the `/v1/agent/service/register` body the Go registrar
/// sends, semantics intact: identity fields, the version tag, tagged
/// addresses holding full endpoint URLs, and the TCP + TTL checks per
/// the options.
fn build_registration(
    options: &ConsulOptions,
    registration: &Registration,
) -> Result<String, RegistryError> {
    let mut tagged_addresses: HashMap<String, WireServiceAddress> = HashMap::new();
    let mut hostports: Vec<(String, i64)> = Vec::new();
    for endpoint in &registration.instance.endpoints {
        let Some((scheme, host, port)) = split_endpoint(endpoint) else {
            return Err(RegistryError::Failed(format!("endpoint parse: {endpoint}")));
        };
        tagged_addresses.insert(
            scheme.to_string(),
            WireServiceAddress {
                Address: endpoint.clone(),
                Port: port,
            },
        );
        hostports.push((host.to_string(), port));
    }

    let mut checks: Vec<WireCheck> = Vec::new();
    if options.enable_health_check {
        for (host, port) in &hostports {
            checks.push(WireCheck {
                TCP: Some(format!("{host}:{port}")),
                Interval: Some(format!("{}s", options.healthcheck_interval_secs)),
                DeregisterCriticalServiceAfter: Some(format!(
                    "{}s",
                    options.deregister_critical_after_secs
                )),
                Timeout: Some("5s".to_string()),
                ..WireCheck::default()
            });
        }
    }
    if options.heartbeat {
        checks.push(WireCheck {
            CheckID: Some(format!("service:{}", registration.instance.id)),
            TTL: Some(format!("{}s", options.healthcheck_interval_secs * 2)),
            DeregisterCriticalServiceAfter: Some(format!(
                "{}s",
                options.deregister_critical_after_secs
            )),
            ..WireCheck::default()
        });
    }

    let wire = WireRegistration {
        ID: registration.instance.id.clone(),
        Name: registration.instance.name.clone(),
        Tags: vec![format!("version={}", registration.instance.version)],
        Address: hostports
            .first()
            .map(|(host, _)| host.clone())
            .filter(|host| !host.is_empty()),
        Port: hostports.first().map(|(_, port)| *port).filter(|p| *p != 0),
        TaggedAddresses: (!tagged_addresses.is_empty()).then_some(tagged_addresses),
        Meta: registration.metadata.as_ref(),
        Checks: (!checks.is_empty()).then_some(checks),
    };
    // Infallible for this shape: strings, arrays and maps only.
    Ok(serde_json::to_string(&wire).expect("consul wire serialization is infallible"))
}

/// PUT /v1/agent/service/register.
async fn put_register(
    http: &reqwest::Client,
    base: &str,
    payload: &str,
) -> Result<(), RegistryError> {
    let url = format!("{base}/v1/agent/service/register");
    let response = http
        .put(&url)
        .header(reqwest::header::CONTENT_TYPE, "application/json")
        .body(payload.to_string())
        .send()
        .await
        .map_err(|e| RegistryError::Failed(format!("consul register: {e}")))?;
    if !response.status().is_success() {
        return Err(RegistryError::Failed(format!(
            "consul register: HTTP {}",
            response.status()
        )));
    }
    Ok(())
}

/// PUT /v1/agent/service/deregister/{id}.
async fn put_deregister(http: &reqwest::Client, base: &str, id: &str) -> Result<(), RegistryError> {
    let url = format!("{base}/v1/agent/service/deregister/{id}");
    let response = http
        .put(&url)
        .send()
        .await
        .map_err(|e| RegistryError::Failed(format!("consul deregister {id}: {e}")))?;
    if !response.status().is_success() {
        return Err(RegistryError::Failed(format!(
            "consul deregister {id}: HTTP {}",
            response.status()
        )));
    }
    Ok(())
}

/// PUT /v1/agent/check/update/{check-id} with the Go client's passing
/// body — the TTL heartbeat.
async fn put_pass(http: &reqwest::Client, base: &str, check_id: &str) -> Result<(), RegistryError> {
    let url = format!("{base}/v1/agent/check/update/{check_id}");
    let response = http
        .put(&url)
        .header(reqwest::header::CONTENT_TYPE, "application/json")
        .body(PASSING_UPDATE.to_string())
        .send()
        .await
        .map_err(|e| RegistryError::Failed(format!("consul check update {check_id}: {e}")))?;
    if !response.status().is_success() {
        return Err(RegistryError::Failed(format!(
            "consul check update {check_id}: HTTP {}",
            response.status()
        )));
    }
    Ok(())
}

/// GET /v1/health/service/{name} — the Go client's health view, with
/// `passing=1` and `wait` always set, `index` only for blocking
/// queries. Returns the resolved instance list and the response's
/// `X-Consul-Index`.
async fn fetch_instances(
    http: &reqwest::Client,
    base: &str,
    service_name: &str,
    index: Option<u64>,
    timeout: Duration,
) -> Result<(Vec<Instance>, u64), RegistryError> {
    let url = match index {
        Some(index) => format!(
            "{base}/v1/health/service/{service_name}?index={index}&passing=1&wait={BLOCKING_WAIT}"
        ),
        None => format!("{base}/v1/health/service/{service_name}?passing=1&wait={BLOCKING_WAIT}"),
    };
    let response = http
        .get(&url)
        .timeout(timeout)
        .send()
        .await
        .map_err(|e| RegistryError::Failed(format!("consul health {service_name}: {e}")))?;
    if !response.status().is_success() {
        return Err(RegistryError::Failed(format!(
            "consul health {service_name}: HTTP {}",
            response.status()
        )));
    }
    let next_index = response
        .headers()
        .get("X-Consul-Index")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(0);
    let body = response
        .text()
        .await
        .map_err(|e| RegistryError::Failed(format!("consul health {service_name}: {e}")))?;
    let entries: Vec<WireEntry> = serde_json::from_str(&body)
        .map_err(|e| RegistryError::Failed(format!("consul health {service_name}: parse: {e}")))?;
    Ok((resolve_entries(entries), next_index))
}

/// The Go default resolver: version from the `version=` tag, endpoints
/// from tagged addresses (the lan/wan interface addresses excluded),
/// with the bare address/port fallback.
fn resolve_entries(entries: Vec<WireEntry>) -> Vec<Instance> {
    entries
        .into_iter()
        .map(|entry| {
            let service = entry.Service;
            let version = service
                .Tags
                .as_deref()
                .unwrap_or(&[])
                .iter()
                .filter_map(|tag| tag.split_once('='))
                .find(|(key, _)| *key == "version")
                .map(|(_, value)| value.to_string())
                .unwrap_or_default();
            let mut endpoints: Vec<String> = service
                .TaggedAddresses
                .as_ref()
                .map(|addresses| {
                    addresses
                        .iter()
                        .filter(|(scheme, _)| {
                            !matches!(
                                scheme.as_str(),
                                "lan_ipv4" | "wan_ipv4" | "lan_ipv6" | "wan_ipv6"
                            )
                        })
                        .map(|(_, address)| address.Address.clone())
                        .collect()
                })
                .unwrap_or_default();
            if endpoints.is_empty() {
                let address = service.Address.clone().unwrap_or_default();
                let port = service.Port.unwrap_or(0);
                if !address.is_empty() && port != 0 {
                    endpoints.push(format!("http://{address}:{port}"));
                }
            }
            Instance {
                id: service.ID,
                name: service.Service,
                version,
                endpoints,
            }
        })
        .collect()
}

/// The wire shape of `/v1/agent/service/register`, matching the Go
/// `api.AgentServiceRegistration` marshal semantics: fields the Go
/// client omits when empty are omitted here too.
#[derive(Serialize)]
#[allow(non_snake_case)]
struct WireRegistration<'a> {
    ID: String,
    Name: String,
    Tags: Vec<String>,
    Address: Option<String>,
    Port: Option<i64>,
    TaggedAddresses: Option<HashMap<String, WireServiceAddress>>,
    Meta: Option<&'a BTreeMap<String, String>>,
    Checks: Option<Vec<WireCheck>>,
}

/// The Go `api.ServiceAddress` shape: the full endpoint URL string and
/// its parsed port.
#[derive(Serialize, Deserialize)]
#[allow(non_snake_case)]
struct WireServiceAddress {
    Address: String,
    Port: i64,
}

/// The Go `api.AgentServiceCheck` shape, restricted to the fields the
/// registrar sets.
#[derive(Default, Serialize)]
#[allow(non_snake_case)]
struct WireCheck {
    TCP: Option<String>,
    Interval: Option<String>,
    Timeout: Option<String>,
    DeregisterCriticalServiceAfter: Option<String>,
    CheckID: Option<String>,
    TTL: Option<String>,
}

/// One entry of the health-view response.
#[derive(Deserialize)]
#[allow(non_snake_case)]
struct WireEntry {
    Service: WireService,
}

/// The service half of a health-view entry: the fields the resolver
/// reads.
#[derive(Deserialize)]
#[allow(non_snake_case)]
struct WireService {
    ID: String,
    Service: String,
    Tags: Option<Vec<String>>,
    Address: Option<String>,
    Port: Option<i64>,
    TaggedAddresses: Option<HashMap<String, WireServiceAddress>>,
}
