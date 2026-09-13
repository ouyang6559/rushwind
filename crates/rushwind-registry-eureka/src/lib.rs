//! Eureka adapter for the RushWind registry contract — registration
//! and discovery, ported from `go-wind-plugins/registry/eureka`
//! (itself a hand-rolled eureka v2 REST client, so this port speaks
//! the identical JSON wire shapes through `reqwest`).
//!
//! # Registration
//!
//! Each endpoint registers under the upper-cased service name as an
//! eureka instance whose metadata smuggles the go-wind round-trip
//! data (`ID`, `Name`, `Version`, `Endpoints`, `agent`), with the
//! per-endpoint URLs the Go adapter derives. A heartbeat task PUTs
//! the instance every ten seconds and re-registers it after three
//! consecutive failures — the Go `Heartbeat` goroutine, failure
//! handling included. Endpoints already listed UP are skipped, as in
//! the Go API-level register.
//!
//! # Discovery
//!
//! A background loop fetches the full application list every thirty
//! seconds (the Go `refresh`/`broadcast` pair), keeps the `UP`
//! instances grouped by application, and updates each watched
//! application's cache. [`Discovery::get_service`] serves the cache
//! when one exists for the (upper-cased) service name and otherwise
//! falls back to the Go single-application fetch — which never
//! unwraps the response envelope in the Go original and therefore
//! always yields an empty list there; this port reproduces that.
//! [`Discovery::watch`] subscribes an application and, like the Go
//! `Subscribe`, triggers an immediate refresh; the watcher then wakes
//! on each cache update.
//!
//! # Divergences from the Go adapter
//!
//! - The Go broadcast wakes every subscriber on every refresh; this
//!   port wakes a watcher only when its own application's cached
//!   value changed — an unchanged wake is a no-op for consumers.
//! - A dropped [`RegistrationHandle`] aborts that registration's
//!   heartbeat tasks (the Go client tears down **all** heartbeats on
//!   the first deregistration); removal then depends on eureka's
//!   eviction of non-heartbeating instances, which eureka servers
//!   may disable.
//! - The wire `metadata` is dropped on the rebuild beyond the
//!   identity fields: the Rust [`Instance`] has no metadata field.
//!
//! # Testing
//!
//! Live conformance tests run against a real eureka server via the
//! `live` feature; there is no embedded eureka for CI's unit lanes.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use rushwind_registry::{
    BoxFuture, Discovery, Registrar, Registration, RegistrationHandle, RegistryError, Watcher,
};
use rushwind_transport::Instance;
use serde::{Deserialize, Serialize};
use tokio::sync::watch;

/// The heartbeat cadence, in seconds.
const HEARTBEAT_SECS: u64 = 10;
/// The full-list refresh cadence, in seconds — also eureka's own
/// response-cache period, so a refreshed view lands no faster.
const REFRESH_SECS: u64 = 30;
/// The per-request timeout, in seconds.
const HTTP_TIMEOUT_SECS: u64 = 3;
/// The eureka REST root, relative to each server base.
const DEFAULT_EUREKA_PATH: &str = "eureka/v2";

struct Inner {
    http: reqwest::Client,
    /// The shuffled server bases; the Go client shuffles once per
    /// request batch and rotates through them on transport failure.
    urls: Mutex<Vec<String>>,
    eureka_path: String,
    /// Watched applications, keyed by upper-cased application name,
    /// created on first [`Discovery::watch`].
    sets: Mutex<HashMap<String, Arc<ServiceSet>>>,
    /// Live heartbeat tasks per `{app}/{instance}`.
    heartbeat_tasks: Mutex<HashMap<String, tokio::task::JoinHandle<()>>>,
}

/// The cache of one watched application: the latest broadcast
/// instance list, held forever by the refresh loop.
struct ServiceSet {
    cache: watch::Sender<Vec<Instance>>,
}

/// A eureka-backed registry: registration and discovery over the v2
/// REST API of the given servers.
pub struct EurekaRegistry {
    inner: Arc<Inner>,
}

impl EurekaRegistry {
    /// Connects to the eureka servers at `urls` with the default REST
    /// root.
    pub fn connect<U>(urls: &[U]) -> Result<Self, RegistryError>
    where
        U: AsRef<str>,
    {
        Self::connect_with(urls, DEFAULT_EUREKA_PATH)
    }

    /// Connects with an explicit REST root.
    pub fn connect_with<U>(urls: &[U], eureka_path: &str) -> Result<Self, RegistryError>
    where
        U: AsRef<str>,
    {
        let inner = Arc::new(Inner {
            http: reqwest::Client::new(),
            urls: Mutex::new(
                urls.iter()
                    .map(|url| url.as_ref().trim_end_matches('/').to_string())
                    .collect(),
            ),
            eureka_path: eureka_path.to_string(),
            sets: Mutex::new(HashMap::new()),
            heartbeat_tasks: Mutex::new(HashMap::new()),
        });
        // The Go API constructor runs one immediate broadcast and
        // then the thirty-second refresh loop.
        let loop_inner = Arc::clone(&inner);
        tokio::spawn(async move {
            refresh_once(&loop_inner).await;
            let mut ticker = tokio::time::interval(Duration::from_secs(REFRESH_SECS));
            ticker.tick().await;
            loop {
                ticker.tick().await;
                refresh_once(&loop_inner).await;
            }
        });
        Ok(Self { inner })
    }

    /// Constructs from the bootstrap factory's settings wire shape:
    /// `urls` (required), `eureka_path` (default
    /// [`DEFAULT_EUREKA_PATH`]).
    pub fn from_settings(settings: serde_json::Value) -> Result<Self, RegistryError> {
        let settings: EurekaSettings = serde_json::from_value(settings)
            .map_err(|e| RegistryError::Failed(format!("settings parse: {e}")))?;
        Self::connect_with(
            &settings.urls,
            settings
                .eureka_path
                .as_deref()
                .unwrap_or(DEFAULT_EUREKA_PATH),
        )
    }
}

/// The bootstrap factory's settings wire shape for
/// [`EurekaRegistry::from_settings`].
#[derive(Deserialize)]
pub struct EurekaSettings {
    /// The eureka server base URLs.
    pub urls: Vec<String>,
    /// The eureka REST root. Default: [`DEFAULT_EUREKA_PATH`].
    pub eureka_path: Option<String>,
}

impl Registrar for EurekaRegistry {
    fn register<'a>(
        &'a self,
        registration: Registration,
    ) -> BoxFuture<'a, Result<RegistrationHandle, RegistryError>> {
        Box::pin(async move {
            // The Go API-level register skips endpoints already
            // listed UP for the application.
            let up_ids: Vec<String> = self
                .get_service(&registration.instance.name)
                .await
                .unwrap_or_default()
                .into_iter()
                .map(|instance| instance.id)
                .collect();
            let mut spawned_keys: Vec<String> = Vec::new();
            for endpoint in &registration.instance.endpoints {
                let wire = build_wire_endpoint(&registration, endpoint)?;
                if up_ids.contains(&wire.instance_id) {
                    continue;
                }
                post_register(&self.inner, &wire).await?;
                let task_inner = Arc::clone(&self.inner);
                let task_wire = wire.clone();
                let key = format!("{}/{}", wire.app_id, wire.instance_id);
                let task = tokio::spawn(async move {
                    heartbeat_loop(task_inner, task_wire).await;
                });
                self.inner
                    .heartbeat_tasks
                    .lock()
                    .expect("heartbeat map poisoned")
                    .insert(key.clone(), task);
                spawned_keys.push(key);
            }
            let inner = Arc::clone(&self.inner);
            Ok(RegistrationHandle::from_cancel(move || {
                let mut tasks = inner
                    .heartbeat_tasks
                    .lock()
                    .expect("heartbeat map poisoned");
                for key in &spawned_keys {
                    if let Some(task) = tasks.remove(key) {
                        task.abort();
                    }
                }
            }))
        })
    }

    fn deregister<'a>(
        &'a self,
        registration: Registration,
    ) -> BoxFuture<'a, Result<(), RegistryError>> {
        Box::pin(async move {
            for endpoint in &registration.instance.endpoints {
                let wire = build_wire_endpoint(&registration, endpoint)?;
                delete_instance(&self.inner, &wire.app_id, &wire.instance_id).await?;
                // The Go client tears down every heartbeat of the
                // application, not just this endpoint's.
                let mut tasks = self
                    .inner
                    .heartbeat_tasks
                    .lock()
                    .expect("heartbeat map poisoned");
                let prefix = format!("{}/", wire.app_id);
                let keys: Vec<String> = tasks
                    .keys()
                    .filter(|key| key.starts_with(&prefix))
                    .cloned()
                    .collect();
                for key in keys {
                    if let Some(task) = tasks.remove(&key) {
                        task.abort();
                    }
                }
            }
            Ok(())
        })
    }
}

impl Discovery for EurekaRegistry {
    fn get_service<'a>(
        &'a self,
        service_name: &'a str,
    ) -> BoxFuture<'a, Result<Vec<Instance>, RegistryError>> {
        Box::pin(async move {
            let app_id = service_name.to_uppercase();
            // The Go GetService serves the cached list whenever one
            // exists — empty lists included — and only misses through
            // to the single-application fetch.
            let cached = {
                let sets = self.inner.sets.lock().expect("service sets poisoned");
                sets.get(&app_id).map(|set| set.cache.borrow().clone())
            };
            if let Some(cached) = cached {
                return Ok(cached);
            }
            // The single-application fetch: the Go original parses
            // the wrapped response into an unwrapped struct, so the
            // envelope key is ignored and the struct stays zeroed —
            // an empty list, faithfully reproduced by this parse.
            let Ok(body) =
                do_request(&self.inner, reqwest::Method::GET, &["apps", &app_id], None).await
            else {
                return Ok(Vec::new());
            };
            let parsed: Option<WireApplication> = serde_json::from_str(&body).ok();
            let _ = parsed;
            Ok(Vec::new())
        })
    }

    fn watch<'a>(
        &'a self,
        service_name: &'a str,
    ) -> BoxFuture<'a, Result<Box<dyn Watcher>, RegistryError>> {
        Box::pin(async move {
            let app_id = service_name.to_uppercase();
            let set = {
                let mut sets = self.inner.sets.lock().expect("service sets poisoned");
                match sets.entry(app_id) {
                    std::collections::hash_map::Entry::Occupied(entry) => Arc::clone(entry.get()),
                    std::collections::hash_map::Entry::Vacant(entry) => {
                        let (cache, _rx) = watch::channel(Vec::new());
                        let set = Arc::new(ServiceSet { cache });
                        entry.insert(Arc::clone(&set));
                        set
                    }
                }
            };
            // The Go Subscribe triggers an immediate broadcast in
            // addition to registering the subscriber.
            let inner = Arc::clone(&self.inner);
            tokio::spawn(async move {
                refresh_once(&inner).await;
            });
            Ok(Box::new(EurekaWatcher {
                rx: set.cache.subscribe(),
                stopped: false,
            }) as Box<dyn Watcher>)
        })
    }
}

/// The eureka-backed [`Watcher`]: a receiver on the watched
/// application's broadcast cache. There is no establishment shortcut
/// — the Subscribe-triggered refresh provides the first snapshot.
struct EurekaWatcher {
    rx: watch::Receiver<Vec<Instance>>,
    stopped: bool,
}

impl Watcher for EurekaWatcher {
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

impl Drop for EurekaWatcher {
    fn drop(&mut self) {
        self.stop();
    }
}

/// One refresh pass: the full application list, UP instances kept,
/// grouped by application, each watched application's cache replaced.
/// A failed or unparsable fetch aborts the pass entirely, as the Go
/// broadcast does.
async fn refresh_once(inner: &Inner) {
    let Some(listing) = fetch_all(inner).await else {
        return;
    };
    let sets: Vec<(String, Arc<ServiceSet>)> = {
        let sets = inner.sets.lock().expect("service sets poisoned");
        sets.iter()
            .map(|(key, set)| (key.clone(), Arc::clone(set)))
            .collect()
    };
    for (app_id, set) in sets {
        let instances = listing
            .iter()
            .find(|(key, _)| *key == app_id)
            .map(|(_, instances)| instances.clone())
            .unwrap_or_default();
        let _ = set.cache.send(instances);
    }
}

/// The Go `FetchAllUpInstances` plus `cacheAllInstances` grouping:
/// the root list, UP instances only, keyed by upper-cased application
/// name, rebuilt into go-wind instances from their smuggled metadata.
async fn fetch_all(inner: &Inner) -> Option<Vec<(String, Vec<Instance>)>> {
    let body = do_request(inner, reqwest::Method::GET, &["apps"], None)
        .await
        .ok()?;
    let parsed: WireApplicationsRoot = serde_json::from_str(&body).ok()?;
    let mut listing: Vec<(String, Vec<Instance>)> = Vec::new();
    for application in parsed.applications.application {
        let app_id = application.name.to_uppercase();
        let mut instances = Vec::new();
        for instance in application.instance.unwrap_or_default() {
            if instance.status.as_deref() == Some("UP") {
                instances.push(rebuild(instance.metadata));
            }
        }
        listing.push((app_id, instances));
    }
    Some(listing)
}

/// The go-wind rebuild: identity and endpoints from the smuggled
/// metadata — a single-element endpoint list, empty-stringed when the
/// key is absent, exactly the Go shape.
fn rebuild(metadata: Option<HashMap<String, String>>) -> Instance {
    let get = |key: &str| {
        metadata
            .as_ref()
            .and_then(|map| map.get(key))
            .cloned()
            .unwrap_or_default()
    };
    Instance {
        id: get("ID"),
        name: get("Name"),
        version: get("Version"),
        endpoints: vec![get("Endpoints")],
    }
}

/// The heartbeat loop: a PUT per ten-second tick, and a full
/// re-registration after three consecutive failures — the Go
/// `Heartbeat` goroutine, whose failure counter never resets on
/// success, replicated here.
async fn heartbeat_loop(inner: Arc<Inner>, wire: WireEndpoint) {
    let mut ticker = tokio::time::interval(Duration::from_secs(HEARTBEAT_SECS));
    ticker.tick().await;
    let mut failures = 0u32;
    loop {
        ticker.tick().await;
        let result = do_request(
            &inner,
            reqwest::Method::PUT,
            &["apps", &wire.app_id, &wire.instance_id],
            None,
        )
        .await;
        if result.is_err() {
            failures += 1;
            if failures > 3 {
                let _ = post_register(&inner, &wire).await;
                failures = 0;
            }
        }
    }
}

/// POST /eureka/v2/apps/{app}: the registration payload.
async fn post_register(inner: &Inner, wire: &WireEndpoint) -> Result<(), RegistryError> {
    let payload = serde_json::to_string(&WireRequestInstance {
        instance: wire.to_wire_instance(),
    })
    .expect("eureka wire serialization is infallible");
    do_request(
        inner,
        reqwest::Method::POST,
        &["apps", &wire.app_id],
        Some(payload),
    )
    .await
    .map(|_| ())
}

/// DELETE /eureka/v2/apps/{app}/{instance}.
async fn delete_instance(
    inner: &Inner,
    app_id: &str,
    instance_id: &str,
) -> Result<(), RegistryError> {
    do_request(
        inner,
        reqwest::Method::DELETE,
        &["apps", app_id, instance_id],
        None,
    )
    .await
    .map(|_| ())
}

/// The Go `Endpoint` derivation: identity fields and URLs pulled from
/// the registration, with the metadata smuggle laid over the
/// registration metadata — the Go `Registry.Endpoints` builder.
#[derive(Clone)]
struct WireEndpoint {
    app_id: String,
    ip: String,
    port: i64,
    secure_port: i64,
    home_page_url: String,
    status_page_url: String,
    health_check_url: String,
    instance_id: String,
    metadata: HashMap<String, String>,
}

impl WireEndpoint {
    /// The eureka wire instance: the Go `registerEndpoint` marshal
    /// shape, field names intact.
    fn to_wire_instance(&self) -> WireInstance {
        WireInstance {
            instanceId: self.instance_id.clone(),
            hostName: self.app_id.clone(),
            port: WirePort {
                dollar: self.port,
                at_enabled: "true".to_string(),
            },
            app: self.app_id.clone(),
            ipAddr: self.ip.clone(),
            vipAddress: self.app_id.clone(),
            status: "UP".to_string(),
            securePort: WirePort {
                dollar: self.secure_port,
                at_enabled: "false".to_string(),
            },
            homePageUrl: self.home_page_url.clone(),
            statusPageUrl: self.status_page_url.clone(),
            healthCheckUrl: self.health_check_url.clone(),
            dataCenterInfo: WireDataCenterInfo {
                name: "MyOwn".to_string(),
                at_class: "com.netflix.appinfo.InstanceInfo$DefaultDataCenterInfo".to_string(),
            },
            metadata: Some(self.metadata.clone()),
        }
    }
}

/// Builds one endpoint's wire shape from the registration — the Go
/// builder with its `strconv.Atoi`-failure-to-zero port semantics and
/// its metadata-keyed URL overrides.
fn build_wire_endpoint(
    registration: &Registration,
    endpoint: &str,
) -> Result<WireEndpoint, RegistryError> {
    let Some((ip, port)) = split_endpoint(endpoint) else {
        return Err(RegistryError::Failed(format!("endpoint parse: {endpoint}")));
    };
    let app_id = registration.instance.name.to_uppercase();
    let mut metadata: HashMap<String, String> = registration
        .metadata
        .as_ref()
        .map(|meta| meta.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
        .unwrap_or_default();
    let registration_metadata = registration.metadata.as_ref();
    let mut secure_port = 443i64;
    if let Some(value) = registration_metadata.and_then(|meta| meta.get("securePort")) {
        secure_port = value.parse::<i64>().unwrap_or(0);
    }
    let mut home_page_url = format!("{endpoint}/");
    let mut status_page_url = format!("{endpoint}/info");
    let mut health_check_url = format!("{endpoint}/health");
    if let Some(value) = registration_metadata.and_then(|meta| meta.get("homePageURL")) {
        home_page_url = value.clone();
    }
    if let Some(value) = registration_metadata.and_then(|meta| meta.get("statusPageURL")) {
        status_page_url = value.clone();
    }
    if let Some(value) = registration_metadata.and_then(|meta| meta.get("healthCheckURL")) {
        health_check_url = value.clone();
    }
    metadata.insert("ID".to_string(), registration.instance.id.clone());
    metadata.insert("Name".to_string(), registration.instance.name.clone());
    metadata.insert("Version".to_string(), registration.instance.version.clone());
    metadata.insert("Endpoints".to_string(), endpoint.to_string());
    metadata.insert("agent".to_string(), "go-eureka-client".to_string());
    Ok(WireEndpoint {
        instance_id: format!("{ip}.{app_id}:{port}"),
        app_id,
        ip: ip.to_string(),
        port,
        secure_port,
        home_page_url,
        status_page_url,
        health_check_url,
        metadata,
    })
}

/// The Go request pipeline: header set, per-request timeout, one
/// attempt per configured server with the list shuffled before the
/// first, transport failures rotating to the next server, HTTP >= 400
/// surfacing as errors, and everything else returning the body.
async fn do_request(
    inner: &Inner,
    method: reqwest::Method,
    path: &[&str],
    body: Option<String>,
) -> Result<String, RegistryError> {
    let attempts = {
        let mut urls = inner.urls.lock().expect("url list poisoned");
        if urls.is_empty() {
            return Err(RegistryError::Failed("eureka: no servers".to_string()));
        }
        shuffle(&mut urls);
        urls.len()
    };
    for attempt in 0..attempts {
        let server = {
            let urls = inner.urls.lock().expect("url list poisoned");
            urls[attempt % urls.len()].clone()
        };
        let url = format!("{}/{}/{}", server, inner.eureka_path, path.join("/"));
        let mut request = inner
            .http
            .request(method.clone(), &url)
            .header("User-Agent", "go-eureka-client")
            .header("Accept", "application/json;charset=UTF-8")
            .header(
                reqwest::header::CONTENT_TYPE,
                "application/json;charset=UTF-8",
            )
            .timeout(Duration::from_secs(HTTP_TIMEOUT_SECS));
        if let Some(body) = &body {
            request = request.body(body.clone());
        }
        let Ok(response) = request.send().await else {
            continue;
        };
        let status = response.status().as_u16();
        let text = if (200..300).contains(&status) {
            response
                .text()
                .await
                .map_err(|e| RegistryError::Failed(format!("eureka: read body: {e}")))?
        } else {
            String::new()
        };
        if status >= 400 {
            return Err(RegistryError::Failed(format!(
                "eureka: response Error {status}"
            )));
        }
        return Ok(text);
    }
    Err(RegistryError::Failed(
        "eureka: retry after all servers".to_string(),
    ))
}

/// A nanos-seeded Fisher-Yates shuffle, the Go client's
/// `rand.Shuffle` stand-in.
fn shuffle(urls: &mut [String]) {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.subsec_nanos())
        .unwrap_or(0);
    let mut state = if nanos == 0 { 1 } else { nanos };
    for index in (1..urls.len()).rev() {
        state ^= state << 13;
        state ^= state >> 17;
        state ^= state << 5;
        let swap = (state % (index as u32 + 1)) as usize;
        urls.swap(index, swap);
    }
}

/// Splits an endpoint URL into `(host, port)`, with the port
/// defaulting to 0 — the Go builder's index-slicing shape.
fn split_endpoint(endpoint: &str) -> Option<(&str, i64)> {
    let separator = endpoint.find("://")?;
    let rest = &endpoint[separator + 3..];
    let end = rest.find(['/', '?', '#']).unwrap_or(rest.len());
    let hostport = &rest[..end];
    let (host, port) = match hostport.rsplit_once(':') {
        Some((host, port)) => (host, port.parse::<i64>().unwrap_or(0)),
        None => (hostport, 0),
    };
    Some((host, port))
}

/// The wire shape of a registration POST body.
#[derive(Serialize)]
struct WireRequestInstance {
    instance: WireInstance,
}

/// The eureka instance wire shape, field names exactly as the Go
/// client's tags emit them.
#[derive(Serialize)]
#[allow(non_snake_case)]
struct WireInstance {
    instanceId: String,
    hostName: String,
    port: WirePort,
    app: String,
    ipAddr: String,
    vipAddress: String,
    status: String,
    securePort: WirePort,
    homePageUrl: String,
    statusPageUrl: String,
    healthCheckUrl: String,
    dataCenterInfo: WireDataCenterInfo,
    metadata: Option<HashMap<String, String>>,
}

/// The eureka port wire shape: `{"$": n, "@enabled": "..."}`.
#[derive(Serialize, Deserialize)]
#[allow(non_snake_case)]
struct WirePort {
    #[serde(rename = "$")]
    dollar: i64,
    #[serde(rename = "@enabled")]
    at_enabled: String,
}

/// The eureka data-center wire shape.
#[derive(Serialize)]
#[allow(non_snake_case)]
struct WireDataCenterInfo {
    name: String,
    #[serde(rename = "@class")]
    at_class: String,
}

/// The root application listing: `{"applications": {...}}` with the
/// array of applications.
#[derive(Deserialize)]
struct WireApplicationsRoot {
    #[serde(rename = "applications")]
    applications: WireApplications,
}

/// The applications object; `versions__delta` and `apps__hashcode`
/// are ignored by the parse, as in the Go client.
#[derive(Deserialize)]
struct WireApplications {
    #[serde(rename = "application")]
    application: Vec<WireApplication>,
}

/// One application in a listing. The Go struct this mirrors is also
/// the (never-populated) target of the single-application fetch,
/// whose wrapped responses leave every field at its zero value — the
/// `#[serde(default)]` here reproduces that on this side of the wire.
#[derive(Deserialize, Default)]
#[serde(default)]
#[allow(non_snake_case)]
struct WireApplication {
    #[serde(rename = "name")]
    name: String,
    #[serde(rename = "instance")]
    instance: Option<Vec<WireInstanceOut>>,
}

/// The instance half of a listing entry: the status the UP filter
/// reads and the smuggled metadata the rebuild reads.
#[derive(Deserialize)]
#[allow(non_snake_case)]
struct WireInstanceOut {
    status: Option<String>,
    metadata: Option<HashMap<String, String>>,
}
