//! ServiceComb service-center adapter for the RushWind registry
//! contract — registration, heartbeats, and WebSocket watch over the
//! `/v4/{project}/registry` API.
//!
//! # Registration
//!
//! The microservice definition (rush-wind framework identity included)
//! is created-or-fetched on first register; older service centers
//! answer a duplicate create with a 400 carrying the
//! already-exists error code (400010), which falls back to the
//! existence lookup — current ones answer 200 with the same id. The
//! instance then registers under that service with the rush-wind
//! endpoints, hostname, and version, its identity being the
//! registration's id or a fresh random one, and a heartbeat task PUTs
//! the instance every thirty seconds; failures are ignored and the
//! loop keeps ticking.
//!
//! # Discovery
//!
//! [`Discovery::get_service`] returns the instance list for
//! `{appId}/{serviceName}` under the
//! configured environment, rebuilt with the quirk that
//! the instance version field carries the **service** id. The find
//! path serves a view service-center caches for roughly thirty
//! seconds, so a registration or deregistration lands in discovery
//! only after that cache's next refresh. No
//! health-check object is attached to instances, so service-center
//! never expires them; the
//! heartbeats are belt-and-braces.
//!
//! [`Discovery::watch`] first re-queries the target's instances, then
//! opens the
//! WebSocket watcher on the registry's own service id — the
//! per-process identity set at register time — and forwards each
//! matching event as a one-instance snapshot. A broken stream re-dials
//! with exponential
//! backoff capped at thirty seconds.
//!
//! # Behavior notes
//!
//! - The self service id is stored on the fetch-existing path too.
//! - A stopped watcher drops its events.
//! - The `X-ConsumerId` header is sent empty; the Properties bag is
//!   parsed and dropped on the rebuild — no Rust [`Instance`] field.
//!
//! # Testing
//!
//! Live conformance tests run against a real service-center via the
//! `live` feature; there is no embedded service-center for CI's unit
//! lanes.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

use std::sync::Mutex;
use std::time::Duration;

use rushwind_registry::{
    BoxFuture, Discovery, Registrar, Registration, RegistrationHandle, RegistryError, Watcher,
};
use rushwind_transport::Instance;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

/// The heartbeat cadence, in seconds.
const HEARTBEAT_SECS: u64 = 30;
/// The watch re-dial backoff cap, in seconds.
const WATCH_BACKOFF_CAP_SECS: u64 = 30;
/// The default tenancy project segment of the registry path.
const DEFAULT_PROJECT: &str = "default";
/// The framework identity the registration declares.
const FRAMEWORK_NAME: &str = "wind";
const FRAMEWORK_VERSION: &str = "v2";
/// The service-center error code for a duplicate microservice.
const ERR_SERVICE_ALREADY_EXISTS: &str = "400010";

struct Inner {
    http: reqwest::Client,
    base: String,
    project: String,
    app_id: String,
    environment: String,
    /// The process's microservice name, remembered from the first
    /// registration — the create call's hint.
    service_name: Mutex<Option<String>>,
    /// The process's microservice version, same rule.
    service_version: Mutex<Option<String>>,
    /// The registry's own service id, set when a registration creates
    /// or resolves the owning microservice — the watch target.
    self_service_id: Mutex<Option<String>>,
    /// Live heartbeat tasks per instance id.
    heartbeat_tasks: Mutex<HashMap<String, tokio::task::JoinHandle<()>>>,
}

/// A ServiceComb service-center-backed registry: registration,
/// heartbeats, and WebSocket watch over the v4 registry API.
pub struct ServicecombRegistry {
    inner: Arc<Inner>,
}

/// Options for the service-center adapter, environment-derived by
/// default.
#[derive(Debug, Clone)]
pub struct ServicecombOptions {
    /// The service-center application id. Default: the
    /// `CAS_APPLICATION_NAME` environment variable, else `default`.
    pub app_id: String,
    /// The service-center environment. Default: the
    /// `CAS_ENVIRONMENT_ID` environment variable, else empty.
    pub environment: String,
}

impl Default for ServicecombOptions {
    fn default() -> Self {
        Self {
            app_id: std::env::var("CAS_APPLICATION_NAME").unwrap_or_else(|_| "default".to_string()),
            environment: std::env::var("CAS_ENVIRONMENT_ID").unwrap_or_default(),
        }
    }
}

impl ServicecombRegistry {
    /// Connects to service-center at `addr` (e.g.
    /// `http://127.0.0.1:30100`) with environment-derived default
    /// options.
    pub fn connect(addr: &str) -> Result<Self, RegistryError> {
        Self::connect_with(addr, ServicecombOptions::default())
    }

    /// Connects with explicit options.
    pub fn connect_with(addr: &str, options: ServicecombOptions) -> Result<Self, RegistryError> {
        Ok(Self {
            inner: Arc::new(Inner {
                http: reqwest::Client::new(),
                base: addr.trim_end_matches('/').to_string(),
                project: DEFAULT_PROJECT.to_string(),
                app_id: options.app_id,
                environment: options.environment,
                service_name: Mutex::new(None),
                service_version: Mutex::new(None),
                self_service_id: Mutex::new(None),
                heartbeat_tasks: Mutex::new(HashMap::new()),
            }),
        })
    }

    /// Constructs from the bootstrap factory's settings wire shape:
    /// `addr` (required), `app_id` and `environment` (default to the
    /// standard environment variables, then their fallbacks).
    pub fn from_settings(settings: serde_json::Value) -> Result<Self, RegistryError> {
        let settings: ServicecombSettings = serde_json::from_value(settings)
            .map_err(|e| RegistryError::Failed(format!("settings parse: {e}")))?;
        let mut options = ServicecombOptions::default();
        if let Some(app_id) = settings.app_id {
            options.app_id = app_id;
        }
        if let Some(environment) = settings.environment {
            options.environment = environment;
        }
        Self::connect_with(&settings.addr, options)
    }

    fn registry_root(&self) -> String {
        self.inner.registry_root()
    }

    /// Issues a request with the default headers under the caller's.
    fn request(&self, method: reqwest::Method, url: String) -> reqwest::RequestBuilder {
        self.inner.request(method, url)
    }

    /// Creates the owning microservice when absent and returns its id
    /// — create first, then the existence lookup on a duplicate
    /// rejection. The id is stored as the
    /// watch target on both paths, keeping the identity just resolved.
    async fn ensure_service(&self) -> Result<String, RegistryError> {
        let url = format!("{}/microservices", self.registry_root());
        let payload = serde_json::json!({
            "service": {
                "appId": self.inner.app_id,
                "serviceName": self.service_name_hint(),
                "version": self.instance_version_hint(),
                "framework": {
                    "name": FRAMEWORK_NAME,
                    "version": FRAMEWORK_VERSION,
                },
            }
        });
        let response = self
            .request(reqwest::Method::POST, url)
            .body(payload.to_string())
            .send()
            .await
            .map_err(|e| RegistryError::Failed(format!("servicecomb create service: {e}")))?;
        let status = response.status().as_u16();
        let body = response.text().await.unwrap_or_default();
        if (200..300).contains(&status) {
            let parsed: WireCreated = serde_json::from_str(&body)
                .map_err(|e| RegistryError::Failed(format!("servicecomb create parse: {e}")))?;
            let service_id = parsed.service_id;
            *self
                .inner
                .self_service_id
                .lock()
                .expect("service id poisoned") = Some(service_id.clone());
            return Ok(service_id);
        }
        if status == 400 && body.contains(ERR_SERVICE_ALREADY_EXISTS) {
            // The older service centers reject a duplicate create;
            // resolve through the existence lookup.
            let service_id = self.resolve_service_id().await?;
            *self
                .inner
                .self_service_id
                .lock()
                .expect("service id poisoned") = Some(service_id.clone());
            return Ok(service_id);
        }
        Err(RegistryError::Failed(format!(
            "servicecomb create service: HTTP {status}: {body}"
        )))
    }

    /// ONE microservice is registered per process, named
    /// after the first registration's service name; the hints keep
    /// that name/version available for the create call.
    fn service_name_hint(&self) -> String {
        self.inner
            .service_name
            .lock()
            .expect("service name poisoned")
            .clone()
            .unwrap_or_else(|| "wind-service".to_string())
    }

    fn instance_version_hint(&self) -> String {
        self.inner
            .service_version
            .lock()
            .expect("service version poisoned")
            .clone()
            .unwrap_or_else(|| "1.0.0".to_string())
    }

    /// The existence lookup for an already-created microservice.
    async fn resolve_service_id(&self) -> Result<String, RegistryError> {
        let url = format!(
            "{}/existence?type=microservice&appId={}&serviceName={}&version={}&env={}",
            self.registry_root(),
            urlencode(&self.inner.app_id),
            urlencode(&self.service_name_hint()),
            urlencode(&self.instance_version_hint()),
            urlencode(&self.inner.environment),
        );
        let response = self
            .request(reqwest::Method::GET, url)
            .send()
            .await
            .map_err(|e| RegistryError::Failed(format!("servicecomb existence: {e}")))?;
        let body = response.text().await.unwrap_or_default();
        let parsed: WireCreated = serde_json::from_str(&body)
            .map_err(|e| RegistryError::Failed(format!("servicecomb existence parse: {e}")))?;
        Ok(parsed.service_id)
    }
}

impl Inner {
    fn registry_root(&self) -> String {
        format!("{}/v4/{}/registry", self.base, self.project)
    }

    /// The default request headers — the sc-client's set.
    fn default_headers(&self) -> [(reqwest::header::HeaderName, String); 3] {
        [
            (
                reqwest::header::CONTENT_TYPE,
                "application/json".to_string(),
            ),
            (reqwest::header::USER_AGENT, "go-client".to_string()),
            (
                reqwest::header::HeaderName::from_static("x-domain-name"),
                "default".to_string(),
            ),
        ]
    }

    fn request(&self, method: reqwest::Method, url: String) -> reqwest::RequestBuilder {
        let mut request = self.http.request(method, url);
        for (name, value) in self.default_headers() {
            request = request.header(name, value);
        }
        request
    }
}

impl Registrar for ServicecombRegistry {
    fn register<'a>(
        &'a self,
        registration: Registration,
    ) -> BoxFuture<'a, Result<RegistrationHandle, RegistryError>> {
        Box::pin(async move {
            // The process's microservice is named after
            // its first registration; remember the name and version
            // for the create call.
            {
                let mut name = self
                    .inner
                    .service_name
                    .lock()
                    .expect("service name poisoned");
                if name.is_none() {
                    *name = Some(registration.instance.name.clone());
                }
            }
            {
                let mut version = self
                    .inner
                    .service_version
                    .lock()
                    .expect("service version poisoned");
                if version.is_none() {
                    *version = Some(registration.instance.version.clone());
                }
            }

            let service_id = self.ensure_service().await?;
            let instance_id = if registration.instance.id.is_empty() {
                random_instance_id()
            } else {
                registration.instance.id.clone()
            };
            let payload = serde_json::json!({
                "instance": {
                    "instanceId": instance_id,
                    "serviceId": service_id,
                    "endpoints": registration.instance.endpoints,
                    "hostName": instance_id,
                    "version": registration.instance.version,
                    "properties": {
                        "appId": self.inner.app_id,
                        "environment": self.inner.environment,
                    },
                }
            });
            let url = format!(
                "{}/microservices/{}/instances",
                self.registry_root(),
                service_id
            );
            let response = self
                .request(reqwest::Method::POST, url)
                .body(payload.to_string())
                .send()
                .await
                .map_err(|e| {
                    RegistryError::Failed(format!("servicecomb register instance: {e}"))
                })?;
            if !response.status().is_success() {
                let status = response.status().as_u16();
                let body = response.text().await.unwrap_or_default();
                return Err(RegistryError::Failed(format!(
                    "servicecomb register instance: HTTP {status}: {body}"
                )));
            }

            // Heartbeat: a PUT per thirty seconds;
            // failures are ignored and the loop continues.
            let task_inner = Arc::clone(&self.inner);
            let heartbeat_service_id = service_id.clone();
            let heartbeat_instance_id = instance_id.clone();
            let task = tokio::spawn(async move {
                heartbeat_loop(task_inner, heartbeat_service_id, heartbeat_instance_id).await;
            });
            self.inner
                .heartbeat_tasks
                .lock()
                .expect("heartbeat map poisoned")
                .insert(instance_id.clone(), task);

            let inner = Arc::clone(&self.inner);
            let deregister_instance_id = instance_id.clone();
            Ok(RegistrationHandle::from_cancel(move || {
                if let Some(task) = inner
                    .heartbeat_tasks
                    .lock()
                    .expect("heartbeat map poisoned")
                    .remove(&deregister_instance_id)
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
            // Deregistration resolves the service id fresh.
            let service_id = self.resolve_service_id().await?;
            let instance_id = registration.instance.id.clone();
            if let Some(task) = self
                .inner
                .heartbeat_tasks
                .lock()
                .expect("heartbeat map poisoned")
                .remove(&instance_id)
            {
                task.abort();
            }
            let url = format!(
                "{}/microservices/{}/instances/{}",
                self.registry_root(),
                service_id,
                urlencode(&instance_id)
            );
            let response = self
                .request(reqwest::Method::DELETE, url)
                .send()
                .await
                .map_err(|e| RegistryError::Failed(format!("servicecomb deregister: {e}")))?;
            if !response.status().is_success() {
                let status = response.status().as_u16();
                let body = response.text().await.unwrap_or_default();
                return Err(RegistryError::Failed(format!(
                    "servicecomb deregister: HTTP {status}: {body}"
                )));
            }
            Ok(())
        })
    }
}

impl Discovery for ServicecombRegistry {
    fn get_service<'a>(
        &'a self,
        service_name: &'a str,
    ) -> BoxFuture<'a, Result<Vec<Instance>, RegistryError>> {
        Box::pin(async move {
            let instances =
                find_instances(&self.inner, "", &self.inner.app_id, service_name).await?;
            // The rebuild quirk: version carries the SERVICE id, name the
            // queried name.
            Ok(instances
                .into_iter()
                .map(|wire| Instance {
                    id: wire.instance_id.unwrap_or_default(),
                    name: service_name.to_string(),
                    version: wire.service_id.unwrap_or_default(),
                    endpoints: wire.endpoints.unwrap_or_default(),
                })
                .collect())
        })
    }

    fn watch<'a>(
        &'a self,
        service_name: &'a str,
    ) -> BoxFuture<'a, Result<Box<dyn Watcher>, RegistryError>> {
        Box::pin(async move {
            // The self id is the watch target; watching before any
            // registration of this process has none to watch through.
            let Some(self_service_id) = self
                .inner
                .self_service_id
                .lock()
                .expect("service id poisoned")
                .clone()
            else {
                return Err(RegistryError::Failed(
                    "servicecomb watch: the registry has not registered a service".to_string(),
                ));
            };
            // The dependency-establishment query.
            find_instances(
                &self.inner,
                &self_service_id,
                &self.inner.app_id,
                service_name,
            )
            .await?;

            let (signal_tx, signal_rx) = tokio::sync::mpsc::unbounded_channel::<Instance>();
            let task_inner = Arc::clone(&self.inner);
            let watch_name = service_name.to_string();
            tokio::spawn(async move {
                watch_loop(task_inner, self_service_id, watch_name, signal_tx).await;
            });
            Ok(Box::new(ServicecombWatcher {
                signal: Some(signal_rx),
                stopped: false,
            }) as Box<dyn Watcher>)
        })
    }
}

/// The ServiceComb-backed [`Watcher`]: per-instance events from the
/// WebSocket watch, each delivered as a one-instance snapshot.
struct ServicecombWatcher {
    signal: Option<tokio::sync::mpsc::UnboundedReceiver<Instance>>,
    stopped: bool,
}

impl Watcher for ServicecombWatcher {
    fn next<'a>(&'a mut self) -> BoxFuture<'a, Result<Vec<Instance>, RegistryError>> {
        Box::pin(async move {
            if self.stopped {
                return Err(RegistryError::Failed("watcher stopped".to_string()));
            }
            let Some(signal) = self.signal.as_mut() else {
                return Err(RegistryError::Failed("watcher stopped".to_string()));
            };
            match signal.recv().await {
                Some(instance) => Ok(vec![instance]),
                None => Err(RegistryError::Failed("watcher stopped".to_string())),
            }
        })
    }

    fn stop(&mut self) {
        self.stopped = true;
        self.signal = None;
    }
}

impl Drop for ServicecombWatcher {
    fn drop(&mut self) {
        self.stop();
    }
}

/// The watch loop: dial the WebSocket watcher, forward every matching
/// event, and re-dial with a doubling backoff capped at thirty
/// seconds after every break. The loop
/// ends when its watcher is gone.
async fn watch_loop(
    inner: Arc<Inner>,
    self_service_id: String,
    service_name: String,
    signal_tx: tokio::sync::mpsc::UnboundedSender<Instance>,
) {
    let ws_url = format!(
        "{}/microservices/{}/watcher",
        inner.registry_root(),
        urlencode(&self_service_id)
    )
    .replacen("http://", "ws://", 1)
    .replacen("https://", "wss://", 1);
    let mut backoff_secs = 1u64;
    loop {
        let dial = tokio_tungstenite::connect_async(&ws_url).await;
        let Ok((stream, _response)) = dial else {
            tokio::time::sleep(Duration::from_secs(backoff_secs)).await;
            backoff_secs = (backoff_secs * 2).min(WATCH_BACKOFF_CAP_SECS);
            continue;
        };
        backoff_secs = 1;
        let mut stream = stream;
        // Read events until the stream breaks; a matching event is
        // rebuilt and forwarded, ending the loop when the watcher is
        // gone.
        while let Some(message) = futures::StreamExt::next(&mut stream).await {
            let Ok(message) = message else {
                break;
            };
            let tokio_tungstenite::tungstenite::Message::Text(text) = message else {
                continue;
            };
            let Ok(event) = serde_json::from_str::<WireChangedEvent>(&text) else {
                continue;
            };
            if event.key.service_name.as_deref() != Some(service_name.as_str()) {
                continue;
            }
            let Some(wire) = event.instance else {
                continue;
            };
            let instance = Instance {
                id: wire.instance_id.unwrap_or_default(),
                name: event.key.service_name.unwrap_or_default(),
                version: event.key.version.unwrap_or_default(),
                endpoints: wire.endpoints.unwrap_or_default(),
            };
            if signal_tx.send(instance).is_err() {
                return;
            }
        }
        tokio::time::sleep(Duration::from_secs(backoff_secs)).await;
        backoff_secs = (backoff_secs * 2).min(WATCH_BACKOFF_CAP_SECS);
    }
}

/// The instance list for the
/// application/service pair under the environment.
async fn find_instances(
    inner: &Inner,
    consumer_id: &str,
    app_id: &str,
    service_name: &str,
) -> Result<Vec<WireInstance>, RegistryError> {
    let url = format!(
        "{}/instances?appId={}&serviceName={}&version=",
        inner.registry_root(),
        urlencode(app_id),
        urlencode(service_name)
    );
    let mut request = inner.http.get(url);
    for (name, value) in [
        (
            reqwest::header::CONTENT_TYPE,
            "application/json".to_string(),
        ),
        (reqwest::header::USER_AGENT, "go-client".to_string()),
        (
            reqwest::header::HeaderName::from_static("x-domain-name"),
            "default".to_string(),
        ),
        (
            reqwest::header::HeaderName::from_static("x-consumerid"),
            consumer_id.to_string(),
        ),
    ] {
        request = request.header(name, value);
    }
    let response = request
        .send()
        .await
        .map_err(|e| RegistryError::Failed(format!("servicecomb find instances: {e}")))?;
    let status = response.status().as_u16();
    let body = response.text().await.unwrap_or_default();
    if !(200..300).contains(&status) {
        return Err(RegistryError::Failed(format!(
            "servicecomb find instances: HTTP {status}: {body}"
        )));
    }
    let parsed: WireInstances = serde_json::from_str(&body)
        .map_err(|e| RegistryError::Failed(format!("servicecomb find instances parse: {e}")))?;
    Ok(parsed.instances.unwrap_or_default())
}

/// The heartbeat loop: a PUT per thirty-second tick; failures are
/// swallowed.
async fn heartbeat_loop(inner: Arc<Inner>, service_id: String, instance_id: String) {
    let mut ticker = tokio::time::interval(Duration::from_secs(HEARTBEAT_SECS));
    ticker.tick().await;
    loop {
        ticker.tick().await;
        let url = format!(
            "{}/microservices/{}/instances/{}/heartbeat",
            inner.registry_root(),
            urlencode(&service_id),
            urlencode(&instance_id)
        );
        let _ = inner.request(reqwest::Method::PUT, url).send().await;
    }
}

/// A fresh random instance identifier — 16 CSPRNG bytes, hex-formatted.
fn random_instance_id() -> String {
    let mut bytes = [0u8; 16];
    let _ = getrandom::fill(&mut bytes);
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn urlencode(value: &str) -> String {
    let mut encoded = String::new();
    for byte in value.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                encoded.push(byte as char)
            }
            _ => encoded.push_str(&format!("%{byte:02X}")),
        }
    }
    encoded
}

/// The bootstrap factory's settings wire shape for
/// [`ServicecombRegistry::from_settings`].
#[derive(Deserialize)]
pub struct ServicecombSettings {
    /// The service-center address (e.g. `http://127.0.0.1:30100`).
    pub addr: String,
    /// The application id. Default: the `CAS_APPLICATION_NAME`
    /// environment variable, then `default`.
    pub app_id: Option<String>,
    /// The environment. Default: the `CAS_ENVIRONMENT_ID`
    /// environment variable, then empty.
    pub environment: Option<String>,
}

/// The microservice create/existence response: the resolved id.
#[derive(Deserialize)]
#[allow(non_snake_case)]
struct WireCreated {
    #[serde(rename = "serviceId")]
    service_id: String,
}

/// The instance-list response.
#[derive(Deserialize)]
struct WireInstances {
    #[serde(rename = "instances")]
    instances: Option<Vec<WireInstance>>,
}

/// The instance wire shape, restricted to the fields the adapter
/// reads and writes.
#[derive(Serialize, Deserialize)]
#[allow(non_snake_case)]
struct WireInstance {
    #[serde(rename = "instanceId", skip_serializing_if = "Option::is_none")]
    instance_id: Option<String>,
    #[serde(rename = "serviceId", skip_serializing_if = "Option::is_none")]
    service_id: Option<String>,
    #[serde(rename = "endpoints", skip_serializing_if = "Option::is_none")]
    endpoints: Option<Vec<String>>,
    #[serde(rename = "hostName", skip_serializing_if = "Option::is_none")]
    host_name: Option<String>,
    #[serde(rename = "version", skip_serializing_if = "Option::is_none")]
    version: Option<String>,
    #[serde(rename = "properties", skip_serializing_if = "Option::is_none")]
    properties: Option<HashMap<String, String>>,
}

/// The watch event: the service key plus the changed instance.
#[derive(Deserialize)]
#[allow(non_snake_case)]
struct WireChangedEvent {
    #[serde(rename = "key")]
    key: WireChangedEventKey,
    #[serde(rename = "instance")]
    instance: Option<WireInstance>,
}

/// The watch event's service key.
#[derive(Deserialize)]
#[allow(non_snake_case)]
struct WireChangedEventKey {
    #[serde(rename = "serviceName")]
    service_name: Option<String>,
    #[serde(rename = "version")]
    version: Option<String>,
}
