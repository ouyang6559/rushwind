//! Config-driven assembly for RushWind applications.
//!
//! [`Bootstrap`] turns a YAML document into a running-shaped
//! application: every engine family the document names is assembled
//! from its registered factories and exposed on the [`Bootstrapped`]
//! result for the application's own machinery and on the [`RouteInput`]
//! every route pack and server factory receives. Handlers, schemas,
//! gates, and job bodies stay in code — configuration picks which
//! registered pieces mount, it never contains logic.
//!
//! # The assembly matrix
//!
//! Every engine family follows one shape: a config section names a
//! factory from the registry the application stocked, the factory turns
//! engine-specific settings into the engine, and the assembled instance
//! lands on [`Bootstrapped`] and [`RouteInput`]. Engine crates whose
//! constructors carry a `from_settings` wire shape parse the settings
//! node directly inside the factory closure; the rest hand-assemble in
//! the closure over their builder options.
//!
//! | Family | Config section | Factory registration | Exposure |
//! |:---|:---|:---|:---|
//! | storage | `storage` | [`Bootstrap::storage_factory`] | [`Bootstrapped::repository`] |
//! | registry | `registry` | [`Bootstrap::registry_factory`] | [`Bootstrapped::discovery`] (the announcement rides [`Bootstrapped::registration`]) |
//! | authn | `authn.<instance>.engine` | [`Bootstrap::authn_factory`] | [`Bootstrapped::authn`] |
//! | authz | `authz.<instance>.engine` | [`Bootstrap::authz_factory`] | [`Bootstrapped::authz`] |
//! | broker | `brokers.<instance>.engine` | [`Bootstrap::broker_factory`] | [`Bootstrapped::brokers`] |
//! | cache | `caches.<instance>.engine` | [`Bootstrap::cache_factory`] | [`Bootstrapped::caches`] |
//! | circuit breaker | `circuitbreakers.<instance>.engine` | [`Bootstrap::circuitbreaker_factory`] | [`Bootstrapped::circuitbreakers`] |
//! | rate limiter | `limiters.<instance>.engine` | [`Bootstrap::limiter_factory`] | [`Bootstrapped::limiters`] |
//! | AI chat | `ai.engine` | [`Bootstrap::ai_factory`] | [`Bootstrapped::ai`] |
//! | object storage | `oss.engine` | [`Bootstrap::oss_factory`] | [`Bootstrapped::object_storage`] |
//! | metrics | `metrics.engine` | [`Bootstrap::metrics_factory`] | [`Bootstrapped::metrics`] |
//! | config sources | `config_sources[].engine` | [`Bootstrap::config_source_factory`] | [`Bootstrapped::config`] (multiple sources compose into the contract's priority fallback) |
//! | script engines | `scripts.<name>.engine` | the script domain's own factory registry (engines self-register via their `register()` functions) | [`Bootstrapped::scripts`] (a name-keyed [`Manager`] closed by a shutdown sweep) |
//!
//! The metrics family additionally carries the built-in Prometheus
//! engine under this crate's `metrics` feature — the one engine whose
//! concrete type the `/metrics` scrape mount needs.
//!
//! # The HTTP edge
//!
//! The built-in `http` server kind assembles through
//! [`HttpEdge`](rushwind_http::HttpEdge): the per-server `edge` block
//! toggles the request-id, logging, and recovery middlewares (defaults
//! on), sets the request budget, and configures CORS — through the
//! tower-http layer or the gorilla-compatible one when `compat` is set.
//! Listener addresses take the standard `host:port` form or the
//! host-any `":port"` form; request budgets take a second count or a
//! duration string (`"10s"`).
//! The domain mounts ride per-server `mounts` flags, each behind its
//! cargo feature: `health` serves `/healthz` + `/readyz` from the
//! aggregated health section, `metrics` serves `/metrics` from the
//! built-in Prometheus engine. A mount or section whose feature is
//! compiled out fails the assembly loudly.
//!
//! ## Per-subtree guards
//!
//! `route_packs[]` and `storage_endpoints[]` entries optionally name an
//! assembled authn instance and a permission point (an assembled authz
//! instance plus fixed action/resource axes, and a project axis that is
//! either a fixed string or the name of a credential claim). The wrap
//! composes the bridges [`with_authn`], [`with_authorization`],
//! [`with_authorization_for`], and [`with_authorization_claim`] around
//! that one subtree — the whitelisting model of the HTTP edge, where
//! public subtrees stay unwrapped and merge with the protected ones.
//! A pack's own closure receives its `route_packs[].settings` node
//! verbatim alongside the [`RouteInput`], and may wrap inner subtrees
//! further with anything the input carries.
//!
//! # Session transports
//!
//! Route packs yield a [`RouteSurface`]: the pack's router plus any
//! session-shutdown buses its sessions drain on. The ws family's
//! `WsRoute::build` yields its bus alongside the mountable route; the
//! pack forwards both and the assembler registers the bus with the
//! axum server, so a server shutdown relays into live sessions.
//!
//! Server kinds beyond `http` — the ws, quic, webtransport, h3, and
//! mqtt glue — mount through [`Bootstrap::server_factory`]: the
//! application's closure owns the TLS material, session handlers, and
//! gates (including the authn contract's `AuthenticationGate` over any
//! assembled authenticator on [`RouteInput`]) and reads its bind or
//! endpoint knobs from the settings node. The `cron` kind is built in:
//! registered jobs ( [`Bootstrap::cron_job`], application code) mount by
//! name, exactly like route packs.
//!
//! # Deliberately not wired
//!
//! - **Codecs** — the encoding domain is a process-wide registry of
//!   stateless engines with no settings; the application's `register()`
//!   one-liners are the whole integration.
//! - **Job storage** — the apalis Postgres backend is generic over the
//!   job payload type, which is application knowledge; its
//!   `from_settings` wire shape serves the application's own assembly.
//! - **Retry** — a static policy helper, nothing to instantiate.
//!
//! # A minimal document
//!
//! ```yaml
//! app:
//!   name: demo
//!   version: v0.1.0
//! storage:
//!   engine: memory
//!   settings: {}
//! servers:
//!   - kind: http
//!     bind: 127.0.0.1:8080
//!     route_packs:
//!       - name: health
//! ```
//!
//! `route_packs[].name` names a pack registered with
//! [`Bootstrap::route_pack`]; the `memory` engine names a factory
//! registered with [`Bootstrap::storage_factory`].

#![forbid(unsafe_code)]
#![deny(missing_docs)]

use std::collections::HashMap;
use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use rushwind_ai::ChatModel;
use rushwind_authn::Authenticator;
use rushwind_authz::Engine as AuthzEngine;
use rushwind_broker::Broker;
use rushwind_cache::Cache;
use rushwind_circuitbreaker::CircuitBreaker;
use rushwind_config::{FallbackSource, SharedSource, Source};
use rushwind_core::App;
use rushwind_http::{
    with_authn, with_authorization, with_authorization_claim, with_authorization_for, CorsOptions,
    HttpEdge,
};
use rushwind_metrics::Metrics;
use rushwind_oss::ObjectStorage;
use rushwind_ratelimit::Limiter;
use rushwind_registry::{Discovery, Registrar, Registration, RegistrationHandle};
use rushwind_script::Manager as ScriptManager;
use rushwind_storage::Repository;
use rushwind_storage_axum::CrudApi;
use rushwind_transport::{Instance, Server, ServerError, StopSignal};
use rushwind_transport_axum::AxumServer;
use rushwind_transport_cron::{CronJob, CronServer};
use serde::Deserialize;

#[cfg(feature = "health")]
use rushwind_health::Health;
#[cfg(feature = "health")]
use rushwind_http::mount_health;
#[cfg(feature = "metrics")]
use rushwind_http::mount_metrics;
#[cfg(feature = "metrics")]
use rushwind_metrics_prometheus::PrometheusMetrics;
#[cfg(feature = "trace")]
use rushwind_tracer::{SdkTracerProvider, TracerProviderBuilder};

/// Future type used by registered factories.
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

impl std::fmt::Debug for Bootstrapped {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Bootstrapped").finish_non_exhaustive()
    }
}

/// Everything an assembled application exposes after [`Bootstrap::build`].
pub struct Bootstrapped {
    /// The assembled application: configured servers attached, ready for
    /// [`App::run`].
    pub app: App,
    /// The configured storage engine, if the document declared one.
    pub repository: Option<Arc<dyn Repository>>,
    /// The endpoint of every configured server, in configuration order.
    pub endpoints: Vec<String>,
    /// The discovery half of the configured registry backend, when one
    /// is configured and provides it. Consumer machinery — clients,
    /// admin consoles — reads service views through it.
    pub discovery: Option<Arc<dyn Discovery>>,
    /// The handle keeping the startup announcement alive, when one was
    /// made. Dropping it begins the backend's best-effort eventual
    /// removal; the matching deregistration runs as a before-shutdown
    /// hook regardless.
    pub registration: Option<RegistrationHandle>,
    /// The assembled authn instances, keyed by their configured
    /// instance names.
    pub authn: HashMap<String, Arc<dyn Authenticator>>,
    /// The assembled authz engines, keyed by their configured instance
    /// names.
    pub authz: HashMap<String, Arc<dyn AuthzEngine>>,
    /// The assembled brokers, keyed by their configured instance names.
    pub brokers: HashMap<String, Arc<dyn Broker>>,
    /// The assembled caches, keyed by their configured instance names.
    pub caches: HashMap<String, Arc<dyn Cache>>,
    /// The assembled circuit breakers, keyed by their configured
    /// instance names.
    pub circuitbreakers: HashMap<String, Arc<dyn CircuitBreaker>>,
    /// The assembled rate limiters, keyed by their configured instance
    /// names.
    pub limiters: HashMap<String, Arc<dyn Limiter>>,
    /// The assembled AI chat model, when the document declared one.
    pub ai: Option<Arc<dyn ChatModel>>,
    /// The assembled object-storage engine, when the document declared
    /// one.
    pub object_storage: Option<Arc<dyn ObjectStorage>>,
    /// The assembled config source — a lone source or the priority
    /// fallback over the configured list, when the document declared
    /// any.
    pub config: Option<SharedSource>,
    /// The assembled metrics engine, when the document declared one.
    pub metrics: Option<Arc<dyn Metrics>>,
    /// The script engine manager holding the assembled, initialized
    /// script engines. A shutdown sweep closes and clears it.
    pub scripts: Option<Arc<ScriptManager>>,
    /// The assembled health aggregator, when the document declared one
    /// (feature `health`). The application registers its checkers on
    /// it.
    #[cfg(feature = "health")]
    pub health: Option<Arc<Health>>,
    /// The assembled OTLP tracer provider, when the document declared
    /// one (feature `trace`). The provider is an owned value the
    /// application hands to the layers that need tracing.
    #[cfg(feature = "trace")]
    pub tracer: Option<SdkTracerProvider>,
}

/// What a registry factory yields: the two halves of a configured
/// backend.
pub struct RegistryEndpoint {
    /// The registration half, when the backend provides one.
    pub registrar: Option<Arc<dyn Registrar>>,
    /// The discovery half, when the backend provides one.
    pub discovery: Option<Arc<dyn Discovery>>,
}

/// What a route pack yields: the pack's router plus any
/// session-shutdown buses its sessions drain on. Packs without session
/// surfaces construct this with [`RouteSurface::new`]; the ws family's
/// `WsRoute::build` yields its bus alongside the mountable route, and
/// the pack forwards both so the assembler can register the bus with
/// the server — a server shutdown then relays into live sessions.
pub struct RouteSurface {
    /// The pack's router.
    pub router: Router,
    /// Session-shutdown buses the assembler registers with the server.
    pub aux: Vec<StopSignal>,
}

impl RouteSurface {
    /// A plain router surface — no session buses.
    pub fn new(router: Router) -> Self {
        Self {
            router,
            aux: Vec::new(),
        }
    }

    /// Adds one session-shutdown bus to the surface.
    pub fn with_aux_shutdown(mut self, bus: StopSignal) -> Self {
        self.aux.push(bus);
        self
    }
}

/// What a route pack or server factory receives when building: every
/// engine the document assembled, keyed the way [`Bootstrapped`]
/// exposes them.
#[derive(Clone)]
pub struct RouteInput {
    /// The configured storage engine, if any. Shared with every pack
    /// and server on this bootstrap.
    pub repository: Option<Arc<dyn Repository>>,
    /// The assembled authn instances, if any.
    pub authn: HashMap<String, Arc<dyn Authenticator>>,
    /// The assembled authz engines, if any.
    pub authz: HashMap<String, Arc<dyn AuthzEngine>>,
    /// The assembled brokers, if any.
    pub brokers: HashMap<String, Arc<dyn Broker>>,
    /// The assembled caches, if any.
    pub caches: HashMap<String, Arc<dyn Cache>>,
    /// The assembled circuit breakers, if any.
    pub circuitbreakers: HashMap<String, Arc<dyn CircuitBreaker>>,
    /// The assembled rate limiters, if any.
    pub limiters: HashMap<String, Arc<dyn Limiter>>,
    /// The assembled AI chat model, if any.
    pub ai: Option<Arc<dyn ChatModel>>,
    /// The assembled object-storage engine, if any.
    pub object_storage: Option<Arc<dyn ObjectStorage>>,
    /// The assembled config source, if any.
    pub config: Option<SharedSource>,
    /// The assembled metrics engine, if any.
    pub metrics: Option<Arc<dyn Metrics>>,
    /// The script engine manager, if any script instances were
    /// configured.
    pub scripts: Option<Arc<ScriptManager>>,
}

/// Errors surfaced by assembly.
#[derive(Debug)]
#[non_exhaustive]
pub enum BootstrapError {
    /// The YAML document could not be parsed.
    Config(String),
    /// A `servers[].kind` with no registered factory.
    UnknownServerKind(String),
    /// A `route_packs[].name` with no registered pack.
    UnknownRoutePack(String),
    /// A `storage.engine` with no registered factory.
    UnknownStorageEngine(String),
    /// A `registry.engine` with no registered factory.
    UnknownRegistryEngine(String),
    /// A `storage_endpoints[].api` with no registered pack.
    UnknownApiPack(String),
    /// A configured engine, instance, or job name with no registered
    /// factory or no assembled instance.
    UnknownEngine {
        /// The family the name was looked up in.
        domain: String,
        /// The unknown name.
        name: String,
    },
    /// `storage_endpoints` declared without a `storage` section.
    StorageEndpointWithoutStorage,
    /// A transport server failed to construct (e.g. bind refused).
    Server(rushwind_transport::ServerError),
    /// A factory failed.
    Failed(String),
}

impl std::fmt::Display for BootstrapError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Config(msg) => write!(f, "bootstrap config: {msg}"),
            Self::UnknownServerKind(kind) => write!(f, "unknown server kind: {kind}"),
            Self::UnknownRoutePack(name) => write!(f, "unknown route pack: {name}"),
            Self::UnknownStorageEngine(engine) => {
                write!(f, "unknown storage engine: {engine}")
            }
            Self::UnknownRegistryEngine(engine) => {
                write!(f, "unknown registry engine: {engine}")
            }
            Self::UnknownApiPack(name) => write!(f, "unknown storage api pack: {name}"),
            Self::UnknownEngine { domain, name } => write!(f, "unknown {domain}: {name}"),
            Self::StorageEndpointWithoutStorage => {
                write!(f, "storage_endpoints requires a storage section")
            }
            Self::Server(e) => write!(f, "server construction failed: {e}"),
            Self::Failed(msg) => write!(f, "bootstrap failed: {msg}"),
        }
    }
}

impl std::error::Error for BootstrapError {}

impl From<rushwind_transport::ServerError> for BootstrapError {
    fn from(e: rushwind_transport::ServerError) -> Self {
        Self::Server(e)
    }
}

/// The YAML document shape.
#[derive(Debug, Default, Deserialize)]
pub struct BootstrapConfig {
    /// Application identity and lifecycle settings.
    #[serde(default)]
    pub app: AppConfig,
    /// Storage engine selection; omit for storage-less applications.
    #[serde(default)]
    pub storage: Option<StorageConfig>,
    /// Registry backend selection; omit for registry-less applications.
    #[serde(default)]
    pub registry: Option<RegistryConfig>,
    /// HTTP edge mounted over the configured storage, in order. Requires
    /// `storage`.
    #[serde(default)]
    pub storage_endpoints: Vec<StorageEndpointConfig>,
    /// Servers to assemble, in order.
    #[serde(default)]
    pub servers: Vec<ServerConfig>,
    /// Authn instances to assemble, keyed by instance name. Reference
    /// the names from `route_packs[].authn` and
    /// `storage_endpoints[].authn`.
    #[serde(default)]
    pub authn: HashMap<String, EngineConfig>,
    /// Authz engines to assemble, keyed by instance name. Reference
    /// the names from `route_packs[].authz.engine` and
    /// `storage_endpoints[].authz.engine`.
    #[serde(default)]
    pub authz: HashMap<String, EngineConfig>,
    /// Brokers to assemble, keyed by instance name.
    #[serde(default)]
    pub brokers: HashMap<String, EngineConfig>,
    /// Caches to assemble, keyed by instance name.
    #[serde(default)]
    pub caches: HashMap<String, EngineConfig>,
    /// Circuit breakers to assemble, keyed by instance name.
    #[serde(default)]
    pub circuitbreakers: HashMap<String, EngineConfig>,
    /// Rate limiters to assemble, keyed by instance name.
    #[serde(default)]
    pub limiters: HashMap<String, EngineConfig>,
    /// The AI chat model selection; omit for AI-less applications.
    #[serde(default)]
    pub ai: Option<EngineConfig>,
    /// The object-storage engine selection; omit when the application
    /// stores nothing.
    #[serde(default)]
    pub oss: Option<EngineConfig>,
    /// The metrics engine selection; omit for metrics-less
    /// applications.
    #[serde(default)]
    pub metrics: Option<EngineConfig>,
    /// Config sources in priority order — the first entry has the
    /// highest priority. Two or more entries compose into the config
    /// domain's fallback source.
    #[serde(default)]
    pub config_sources: Vec<EngineConfig>,
    /// Script engine instances to assemble into the script manager,
    /// keyed by instance name. The `engine` field names a factory in
    /// the script domain's own registry — the engine crates
    /// self-register there via their `register()` functions, which the
    /// application calls once at startup.
    #[serde(default)]
    pub scripts: HashMap<String, ScriptConfig>,
    /// The health aggregator settings (feature `health`); the section
    /// fails the assembly when the feature is compiled out.
    #[serde(default)]
    pub health: Option<serde_json::Value>,
    /// The OTLP tracer provider settings (feature `trace`); the section
    /// fails the assembly when the feature is compiled out.
    #[serde(default)]
    pub tracer: Option<serde_json::Value>,
}

/// Application identity and lifecycle settings.
#[derive(Debug, Default, Deserialize)]
pub struct AppConfig {
    /// Application name (optional).
    #[serde(default)]
    pub name: Option<String>,
    /// Application version (optional).
    #[serde(default)]
    pub version: Option<String>,
    /// Per-phase shutdown budget in seconds (default: the core's 10 s).
    #[serde(default)]
    pub stop_timeout_secs: Option<u64>,
}

/// Storage engine selection.
#[derive(Debug, Deserialize)]
pub struct StorageConfig {
    /// The registered factory name.
    pub engine: String,
    /// Engine-specific settings, passed to the factory verbatim.
    #[serde(default)]
    pub settings: serde_json::Value,
}

/// Registry backend selection.
#[derive(Debug, Deserialize)]
pub struct RegistryConfig {
    /// The registered factory name.
    pub engine: String,
    /// Engine-specific settings, passed to the factory verbatim.
    #[serde(default)]
    pub settings: serde_json::Value,
}

/// One engine-family selection: the factory name plus its verbatim
/// settings node.
#[derive(Debug, Deserialize)]
pub struct EngineConfig {
    /// The registered factory name.
    pub engine: String,
    /// Engine-specific settings, passed to the factory verbatim.
    #[serde(default)]
    pub settings: serde_json::Value,
}

/// One script-engine instance selection. Construction takes no settings
/// — the script domain's factory registry builds engines from their
/// type name alone.
#[derive(Debug, Deserialize)]
pub struct ScriptConfig {
    /// The script engine type, naming a factory in the script domain's
    /// own registry.
    pub engine: String,
}

/// One route-pack mount reference: the pack name, the pack's verbatim
/// settings node, and the optional security wraps applied to the pack's
/// whole router.
#[derive(Debug, Deserialize)]
pub struct RoutePackRef {
    /// The registered route-pack name.
    pub name: String,
    /// The assembled authn instance wrapping this pack, if any.
    #[serde(default)]
    pub authn: Option<String>,
    /// The permission point wrapping this pack, if any.
    #[serde(default)]
    pub authz: Option<AuthzRef>,
    /// Pack-specific settings, passed verbatim to the pack's closure.
    #[serde(default)]
    pub settings: serde_json::Value,
}

/// One permission point: the assembled authz instance plus the fixed
/// action/resource axes, and a project axis that is either a fixed
/// string or the name of a credential claim carrying it. The two
/// project shapes are mutually exclusive; absent both, the permission
/// point evaluates under the empty project.
#[derive(Debug, Deserialize)]
pub struct AuthzRef {
    /// The assembled authz instance name.
    pub engine: String,
    /// The action axis.
    pub action: String,
    /// The resource axis.
    pub resource: String,
    /// A fixed project axis.
    #[serde(default)]
    pub project: Option<String>,
    /// A named credential claim carrying the project axis.
    #[serde(default)]
    pub project_claim: Option<String>,
}

/// One HTTP edge mounted over the configured storage.
#[derive(Debug, Deserialize)]
pub struct StorageEndpointConfig {
    /// The path prefix the api router nests under (e.g. `/widgets`).
    pub nest: String,
    /// The registered api pack name (`"crud"` is built in).
    pub api: String,
    /// Pack-specific settings, passed verbatim.
    #[serde(default)]
    pub settings: serde_json::Value,
    /// The assembled authn instance wrapping this edge, if any.
    #[serde(default)]
    pub authn: Option<String>,
    /// The permission point wrapping this edge, if any.
    #[serde(default)]
    pub authz: Option<AuthzRef>,
}

/// One server to assemble.
#[derive(Debug, Deserialize)]
pub struct ServerConfig {
    /// The registered factory name (`http` and `cron` are built in).
    pub kind: String,
    /// Factory-specific settings, passed verbatim.
    #[serde(flatten)]
    pub settings: serde_json::Value,
}

/// The route-pack closure type: the pack's verbatim settings node plus
/// the shared assembly input.
type RoutePackFn = Box<
    dyn Fn(serde_json::Value, RouteInput) -> Result<RouteSurface, BootstrapError> + Send + Sync,
>;

/// The storage-factory closure type.
type StorageFactoryFn = Box<
    dyn Fn(serde_json::Value) -> BoxFuture<'static, Result<Arc<dyn Repository>, BootstrapError>>
        + Send
        + Sync,
>;

/// The registry-factory closure type: engine-specific settings into a
/// [`RegistryEndpoint`].
type RegistryFactoryFn = Box<
    dyn Fn(serde_json::Value) -> BoxFuture<'static, Result<RegistryEndpoint, BootstrapError>>
        + Send
        + Sync,
>;

/// The api-pack closure type: one storage HTTP edge, mounted under a
/// prefix. The built-in `"crud"` pack (backed by `rushwind-storage-axum`)
/// ships with the bootstrap; application packs register alongside it.
type ApiPackFn = Box<dyn Fn(RouteInput) -> Result<Router, BootstrapError> + Send + Sync>;

/// The server-factory closure type, for kinds beyond the built-in
/// `http` and `cron` (ws, quic, webtransport, h3, mqtt glue registers
/// here).
type ServerFactoryFn = Box<
    dyn Fn(
            serde_json::Value,
            RouteInput,
        )
            -> BoxFuture<'static, Result<Arc<dyn rushwind_transport::Server>, BootstrapError>>
        + Send
        + Sync,
>;

/// Emits one engine-family factory closure type — the boxed
/// settings-to-engine constructor every family shares.
macro_rules! factory_fn_type {
    ($alias:ident, $object:ty) => {
        type $alias = Box<
            dyn Fn(serde_json::Value) -> BoxFuture<'static, Result<Arc<$object>, BootstrapError>>
                + Send
                + Sync,
        >;
    };
}

factory_fn_type!(AuthnFactoryFn, dyn Authenticator);
factory_fn_type!(AuthzFactoryFn, dyn AuthzEngine);
factory_fn_type!(BrokerFactoryFn, dyn Broker);
factory_fn_type!(CacheFactoryFn, dyn Cache);
factory_fn_type!(CircuitBreakerFactoryFn, dyn CircuitBreaker);
factory_fn_type!(LimiterFactoryFn, dyn Limiter);
factory_fn_type!(MetricsFactoryFn, dyn Metrics);
factory_fn_type!(AiFactoryFn, dyn ChatModel);
factory_fn_type!(OssFactoryFn, dyn ObjectStorage);
factory_fn_type!(ConfigSourceFactoryFn, dyn Source);

/// Config-driven application assembler. See the crate docs.
pub struct Bootstrap {
    config: BootstrapConfig,
    route_packs: HashMap<String, RoutePackFn>,
    storage_factories: HashMap<String, StorageFactoryFn>,
    registry_factories: HashMap<String, RegistryFactoryFn>,
    server_factories: HashMap<String, ServerFactoryFn>,
    api_packs: HashMap<String, ApiPackFn>,
    cron_jobs: HashMap<String, CronJob>,
    authn_factories: HashMap<String, AuthnFactoryFn>,
    authz_factories: HashMap<String, AuthzFactoryFn>,
    broker_factories: HashMap<String, BrokerFactoryFn>,
    cache_factories: HashMap<String, CacheFactoryFn>,
    circuitbreaker_factories: HashMap<String, CircuitBreakerFactoryFn>,
    limiter_factories: HashMap<String, LimiterFactoryFn>,
    metrics_factories: HashMap<String, MetricsFactoryFn>,
    ai_factories: HashMap<String, AiFactoryFn>,
    oss_factories: HashMap<String, OssFactoryFn>,
    config_source_factories: HashMap<String, ConfigSourceFactoryFn>,
}

impl std::fmt::Debug for Bootstrap {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Bootstrap").finish_non_exhaustive()
    }
}

/// Emits one engine-family factory registration method.
macro_rules! factory_builder {
    ($method:ident, $field:ident, $object:ty, $doc:literal) => {
        #[doc = $doc]
        pub fn $method<F, Fut>(mut self, name: impl Into<String>, factory: F) -> Self
        where
            F: Fn(serde_json::Value) -> Fut + Send + Sync + 'static,
            Fut: Future<Output = Result<Arc<$object>, BootstrapError>> + Send + 'static,
        {
            self.$field.insert(
                name.into(),
                Box::new(move |settings| Box::pin(factory(settings))),
            );
            self
        }
    };
}

impl Bootstrap {
    /// Parses a YAML document.
    pub fn from_yaml_str(yaml: &str) -> Result<Self, BootstrapError> {
        let config = serde_yaml::from_str(yaml)
            .map_err(|e| BootstrapError::Config(format!("YAML parse: {e}")))?;
        Ok(Self::from_config(config))
    }

    /// Reads and parses a YAML file.
    pub fn from_yaml_path(path: impl AsRef<std::path::Path>) -> Result<Self, BootstrapError> {
        let yaml = std::fs::read_to_string(path.as_ref()).map_err(|e| {
            BootstrapError::Config(format!("read {}: {e}", path.as_ref().display()))
        })?;
        Self::from_yaml_str(&yaml)
    }

    /// Wraps an already-parsed configuration.
    pub fn from_config(config: BootstrapConfig) -> Self {
        let mut api_packs: HashMap<String, ApiPackFn> = HashMap::new();
        api_packs.insert("crud".to_string(), Box::new(crud_api_pack));
        Self {
            config,
            route_packs: HashMap::new(),
            storage_factories: HashMap::new(),
            registry_factories: HashMap::new(),
            server_factories: HashMap::new(),
            api_packs,
            cron_jobs: HashMap::new(),
            authn_factories: HashMap::new(),
            authz_factories: HashMap::new(),
            broker_factories: HashMap::new(),
            cache_factories: HashMap::new(),
            circuitbreaker_factories: HashMap::new(),
            limiter_factories: HashMap::new(),
            metrics_factories: HashMap::new(),
            ai_factories: HashMap::new(),
            oss_factories: HashMap::new(),
            config_source_factories: HashMap::new(),
        }
    }

    /// Registers a named storage-endpoint api pack, mountable from
    /// `storage_endpoints[].api`. The built-in `"crud"` pack serves the
    /// storage line's HTTP edge; application packs override it by
    /// registering the same name.
    pub fn api_pack<F>(mut self, name: impl Into<String>, pack: F) -> Self
    where
        F: Fn(RouteInput) -> Result<Router, BootstrapError> + Send + Sync + 'static,
    {
        self.api_packs.insert(name.into(), Box::new(pack));
        self
    }

    /// Registers a named route pack. The closure receives the pack's
    /// settings node from `route_packs[].settings` verbatim, alongside
    /// the shared assembly input.
    pub fn route_pack<F>(mut self, name: impl Into<String>, pack: F) -> Self
    where
        F: Fn(serde_json::Value, RouteInput) -> Result<RouteSurface, BootstrapError>
            + Send
            + Sync
            + 'static,
    {
        self.route_packs.insert(name.into(), Box::new(pack));
        self
    }

    /// Registers a named storage factory. The schema is application
    /// knowledge: capture it in the closure, read engine knobs from
    /// `settings`.
    pub fn storage_factory<F, Fut>(mut self, name: impl Into<String>, factory: F) -> Self
    where
        F: Fn(serde_json::Value) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Arc<dyn Repository>, BootstrapError>> + Send + 'static,
    {
        self.storage_factories.insert(
            name.into(),
            Box::new(move |settings| Box::pin(factory(settings))),
        );
        self
    }

    /// Registers a named registry factory, mountable from
    /// `registry.engine`. The closure turns engine-specific settings
    /// into the backend's registration and discovery halves.
    pub fn registry_factory<F, Fut>(mut self, name: impl Into<String>, factory: F) -> Self
    where
        F: Fn(serde_json::Value) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<RegistryEndpoint, BootstrapError>> + Send + 'static,
    {
        self.registry_factories.insert(
            name.into(),
            Box::new(move |settings| Box::pin(factory(settings))),
        );
        self
    }

    /// Registers a named server factory for a kind beyond the built-in
    /// `http` and `cron` (ws, quic, webtransport, h3, mqtt glue
    /// registers here).
    pub fn server_factory<F, Fut>(mut self, kind: impl Into<String>, factory: F) -> Self
    where
        F: Fn(serde_json::Value, RouteInput) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Arc<dyn rushwind_transport::Server>, BootstrapError>>
            + Send
            + 'static,
    {
        self.server_factories.insert(
            kind.into(),
            Box::new(move |settings, input| Box::pin(factory(settings, input))),
        );
        self
    }

    /// Registers a named cron job, mountable from a `cron` server's
    /// `jobs` list. The handler is application code; the name is what
    /// configuration mounts.
    pub fn cron_job(mut self, name: impl Into<String>, job: CronJob) -> Self {
        self.cron_jobs.insert(name.into(), job);
        self
    }

    factory_builder!(
        authn_factory,
        authn_factories,
        dyn Authenticator,
        "Registers a named authn factory, mountable from `authn.<instance>.engine`. The closure turns engine-specific settings into the authenticator; the assembled instance is referenceable from `route_packs[].authn` and `storage_endpoints[].authn`."
    );
    factory_builder!(
        authz_factory,
        authz_factories,
        dyn AuthzEngine,
        "Registers a named authz factory, mountable from `authz.<instance>.engine`. The closure turns engine-specific settings into the engine; the assembled instance is referenceable from the `authz.engine` field of a permission point."
    );
    factory_builder!(
        broker_factory,
        broker_factories,
        dyn Broker,
        "Registers a named broker factory, mountable from `brokers.<instance>.engine`. The closure turns engine-specific settings into the broker."
    );
    factory_builder!(
        cache_factory,
        cache_factories,
        dyn Cache,
        "Registers a named cache factory, mountable from `caches.<instance>.engine`. The closure turns engine-specific settings into the cache."
    );
    factory_builder!(
        circuitbreaker_factory,
        circuitbreaker_factories,
        dyn CircuitBreaker,
        "Registers a named circuit-breaker factory, mountable from `circuitbreakers.<instance>.engine`. The closure turns engine-specific settings into the breaker."
    );
    factory_builder!(
        limiter_factory,
        limiter_factories,
        dyn Limiter,
        "Registers a named rate-limiter factory, mountable from `limiters.<instance>.engine`. The closure turns engine-specific settings into the limiter."
    );
    factory_builder!(
        metrics_factory,
        metrics_factories,
        dyn Metrics,
        "Registers a named metrics factory, mountable from `metrics.engine`. The closure turns engine-specific settings into the metrics engine. The built-in Prometheus engine (feature `metrics`) serves when no factory is registered under its name."
    );
    factory_builder!(
        ai_factory,
        ai_factories,
        dyn ChatModel,
        "Registers a named AI chat-model factory, mountable from `ai.engine`. The closure turns engine-specific settings into the chat model."
    );
    factory_builder!(
        oss_factory,
        oss_factories,
        dyn ObjectStorage,
        "Registers a named object-storage factory, mountable from `oss.engine`. The closure turns engine-specific settings into the object-storage engine."
    );
    factory_builder!(
        config_source_factory,
        config_source_factories,
        dyn Source,
        "Registers a named config-source factory, mountable from `config_sources[].engine`. The closure turns engine-specific settings into the source; the assembled list composes into the config domain's priority fallback."
    );

    /// Assembles the application: the engines first (route packs and
    /// server factories share them through the [`RouteInput`]), then
    /// every configured server, then the [`App`] identity and lifecycle
    /// settings.
    pub async fn build(self) -> Result<Bootstrapped, BootstrapError> {
        let mut builder = App::builder();
        if let Some(name) = &self.config.app.name {
            builder = builder.name(name.clone());
        }
        if let Some(version) = &self.config.app.version {
            builder = builder.version(version.clone());
        }
        if let Some(secs) = self.config.app.stop_timeout_secs {
            builder = builder.stop_timeout(Duration::from_secs(secs));
        }

        // Storage first: route packs may share the engine.
        let repository = match &self.config.storage {
            Some(storage) => {
                let factory = self
                    .storage_factories
                    .get(&storage.engine)
                    .ok_or_else(|| BootstrapError::UnknownStorageEngine(storage.engine.clone()))?;
                Some(factory(storage.settings.clone()).await?)
            }
            None => None,
        };

        // The named engine families: every configured instance resolved
        // through its factory and keyed by its instance name.
        macro_rules! assemble_named {
            ($target:ident, $config:ident, $factories:ident, $object:ty, $domain:literal) => {
                let mut $target: HashMap<String, Arc<$object>> = HashMap::new();
                for (instance, engine) in &self.config.$config {
                    let factory = self.$factories.get(&engine.engine).ok_or_else(|| {
                        BootstrapError::UnknownEngine {
                            domain: $domain.to_string(),
                            name: engine.engine.clone(),
                        }
                    })?;
                    $target.insert(instance.clone(), factory(engine.settings.clone()).await?);
                }
            };
        }
        assemble_named!(
            authn,
            authn,
            authn_factories,
            dyn Authenticator,
            "authn engine"
        );
        assemble_named!(
            authz,
            authz,
            authz_factories,
            dyn AuthzEngine,
            "authz engine"
        );
        assemble_named!(
            brokers,
            brokers,
            broker_factories,
            dyn Broker,
            "broker engine"
        );
        assemble_named!(caches, caches, cache_factories, dyn Cache, "cache engine");
        assemble_named!(
            circuitbreakers,
            circuitbreakers,
            circuitbreaker_factories,
            dyn CircuitBreaker,
            "circuitbreaker engine"
        );
        assemble_named!(
            limiters,
            limiters,
            limiter_factories,
            dyn Limiter,
            "limiter engine"
        );

        // The AI chat model: a single instance.
        let ai = match &self.config.ai {
            Some(engine) => {
                let factory = self.ai_factories.get(&engine.engine).ok_or_else(|| {
                    BootstrapError::UnknownEngine {
                        domain: "ai engine".to_string(),
                        name: engine.engine.clone(),
                    }
                })?;
                Some(factory(engine.settings.clone()).await?)
            }
            None => None,
        };

        // The object-storage engine: a single instance.
        let object_storage = match &self.config.oss {
            Some(engine) => {
                let factory = self.oss_factories.get(&engine.engine).ok_or_else(|| {
                    BootstrapError::UnknownEngine {
                        domain: "oss engine".to_string(),
                        name: engine.engine.clone(),
                    }
                })?;
                Some(factory(engine.settings.clone()).await?)
            }
            None => None,
        };

        // The metrics engine: an application-registered factory, or the
        // built-in Prometheus engine under the `metrics` feature — the
        // one engine whose concrete type the scrape mount needs.
        let mut metrics: Option<Arc<dyn Metrics>> = None;
        #[cfg(feature = "metrics")]
        let mut prometheus_scrape: Option<Arc<PrometheusMetrics>> = None;
        if let Some(engine) = &self.config.metrics {
            match self.metrics_factories.get(&engine.engine) {
                Some(factory) => {
                    metrics = Some(factory(engine.settings.clone()).await?);
                }
                None => {
                    if engine.engine == "prometheus" {
                        #[cfg(feature = "metrics")]
                        {
                            let provider =
                                PrometheusMetrics::from_settings(engine.settings.clone()).map_err(
                                    |e| BootstrapError::Config(format!("prometheus settings: {e}")),
                                )?;
                            let provider = Arc::new(provider);
                            prometheus_scrape = Some(Arc::clone(&provider));
                            let erased: Arc<dyn Metrics> = provider;
                            metrics = Some(erased);
                        }
                        #[cfg(not(feature = "metrics"))]
                        {
                            return Err(BootstrapError::UnknownEngine {
                                domain: "metrics engine".to_string(),
                                name: engine.engine.clone(),
                            });
                        }
                    } else {
                        return Err(BootstrapError::UnknownEngine {
                            domain: "metrics engine".to_string(),
                            name: engine.engine.clone(),
                        });
                    }
                }
            }
        }

        // Config sources: a lone source stays itself; a list composes
        // into the config domain's priority fallback.
        let mut config: Option<SharedSource> = None;
        if !self.config.config_sources.is_empty() {
            let mut sources: Vec<SharedSource> = Vec::new();
            for engine in &self.config.config_sources {
                let factory = self
                    .config_source_factories
                    .get(&engine.engine)
                    .ok_or_else(|| BootstrapError::UnknownEngine {
                        domain: "config source engine".to_string(),
                        name: engine.engine.clone(),
                    })?;
                sources.push(factory(engine.settings.clone()).await?);
            }
            config = if sources.len() == 1 {
                Some(sources.swap_remove(0))
            } else {
                let fallback = FallbackSource::new(sources)
                    .map_err(|e| BootstrapError::Failed(format!("config fallback: {e}")))?;
                let erased: SharedSource = Arc::new(fallback);
                Some(erased)
            };
        }

        // Script engines: built and initialized through the script
        // domain's own factory registry and held in a name-keyed
        // manager. A shutdown sweep closes and clears the manager.
        let mut scripts: Option<Arc<ScriptManager>> = None;
        if !self.config.scripts.is_empty() {
            let manager = Arc::new(ScriptManager::new());
            for (name, script) in &self.config.scripts {
                let engine = rushwind_script::new_script_engine(&script.engine).map_err(|_| {
                    BootstrapError::UnknownEngine {
                        domain: "script engine".to_string(),
                        name: script.engine.clone(),
                    }
                })?;
                engine
                    .init()
                    .await
                    .map_err(|e| BootstrapError::Failed(format!("script init {name}: {e}")))?;
                manager
                    .register(name, engine)
                    .map_err(|e| BootstrapError::Failed(format!("script register {name}: {e}")))?;
            }
            let hook_manager = Arc::clone(&manager);
            builder = builder.before_stop(move |_budget| {
                let manager = Arc::clone(&hook_manager);
                async move {
                    let _ = manager.close_all();
                    Ok::<(), ServerError>(())
                }
            });
            scripts = Some(manager);
        }

        // The health aggregator (feature `health`).
        #[cfg(feature = "health")]
        let health = match &self.config.health {
            Some(settings) => Some(Arc::new(
                Health::from_settings(settings.clone())
                    .map_err(|e| BootstrapError::Config(format!("health settings: {e}")))?,
            )),
            None => None,
        };
        #[cfg(not(feature = "health"))]
        if self.config.health.is_some() {
            return Err(BootstrapError::Failed(
                "the health section requires building rushwind-bootstrap with feature 'health'"
                    .to_string(),
            ));
        }

        // The OTLP tracer provider (feature `trace`). An owned value the
        // application hands to the layers that need tracing.
        #[cfg(feature = "trace")]
        let tracer = match &self.config.tracer {
            Some(settings) => {
                let builder = TracerProviderBuilder::from_settings(settings.clone())
                    .map_err(|e| BootstrapError::Config(format!("tracer settings: {e}")))?;
                let provider = builder
                    .build()
                    .map_err(|e| BootstrapError::Failed(format!("tracer provider build: {e}")))?;
                Some(provider)
            }
            None => None,
        };
        #[cfg(not(feature = "trace"))]
        if self.config.tracer.is_some() {
            return Err(BootstrapError::Failed(
                "the tracer section requires building rushwind-bootstrap with feature 'trace'"
                    .to_string(),
            ));
        }

        let input = RouteInput {
            repository: repository.clone(),
            authn: authn.clone(),
            authz: authz.clone(),
            brokers: brokers.clone(),
            caches: caches.clone(),
            circuitbreakers: circuitbreakers.clone(),
            limiters: limiters.clone(),
            ai: ai.clone(),
            object_storage: object_storage.clone(),
            config: config.clone(),
            metrics: metrics.clone(),
            scripts: scripts.clone(),
        };

        // Storage endpoints: HTTP edges mounted over the configured
        // storage. They require a storage section (there is nothing to
        // mount otherwise) and merge into every http server's router.
        let mut storage_routers: Vec<(String, Router)> = Vec::new();
        for endpoint_config in &self.config.storage_endpoints {
            if repository.is_none() {
                return Err(BootstrapError::StorageEndpointWithoutStorage);
            }
            let pack = self
                .api_packs
                .get(&endpoint_config.api)
                .ok_or_else(|| BootstrapError::UnknownApiPack(endpoint_config.api.clone()))?;
            let mut edge_router = pack(input.clone())?;
            edge_router = apply_guards(
                edge_router,
                &input,
                &endpoint_config.authn,
                &endpoint_config.authz,
            )?;
            storage_routers.push((endpoint_config.nest.clone(), edge_router));
        }

        let mut endpoints = Vec::new();
        let mut servers = Vec::new();
        for server_config in &self.config.servers {
            // The built-in `http` and `cron` kinds assemble here, where
            // the pack and job registries are in scope; every other
            // kind goes through the server-factory registry.
            let (server, endpoint) = match server_config.kind.as_str() {
                "http" => {
                    let http: HttpServerConfig =
                        serde_json::from_value(server_config.settings.clone()).map_err(|e| {
                            BootstrapError::Config(format!("http server settings: {e}"))
                        })?;
                    let mut router = Router::new();
                    let mut aux: Vec<StopSignal> = Vec::new();
                    for pack_ref in &http.route_packs {
                        let pack = self.route_packs.get(&pack_ref.name).ok_or_else(|| {
                            BootstrapError::UnknownRoutePack(pack_ref.name.clone())
                        })?;
                        let mut surface = pack(pack_ref.settings.clone(), input.clone())?;
                        surface.router =
                            apply_guards(surface.router, &input, &pack_ref.authn, &pack_ref.authz)?;
                        router = router.merge(surface.router);
                        aux.extend(surface.aux);
                    }
                    for (prefix, edge_router) in &storage_routers {
                        router = router.nest(prefix, edge_router.clone());
                    }
                    // The domain mounts, each behind its feature.
                    if http.mounts.health {
                        #[cfg(feature = "health")]
                        {
                            let health = health.as_ref().ok_or_else(|| {
                                BootstrapError::Failed(
                                    "the health mount requires a health section".to_string(),
                                )
                            })?;
                            router = mount_health(router, Arc::clone(health));
                        }
                        #[cfg(not(feature = "health"))]
                        {
                            return Err(BootstrapError::Failed(
                                "the health mount requires building rushwind-bootstrap with feature 'health'"
                                    .to_string(),
                            ));
                        }
                    }
                    if http.mounts.metrics {
                        #[cfg(feature = "metrics")]
                        {
                            let scrape = prometheus_scrape.as_ref().ok_or_else(|| {
                                BootstrapError::Failed(
                                    "the metrics mount requires the built-in prometheus metrics engine"
                                        .to_string(),
                                )
                            })?;
                            router = mount_metrics(router, Arc::clone(scrape));
                        }
                        #[cfg(not(feature = "metrics"))]
                        {
                            return Err(BootstrapError::Failed(
                                "the metrics mount requires building rushwind-bootstrap with feature 'metrics'"
                                    .to_string(),
                            ));
                        }
                    }
                    // The edge stack: the configured toggles over the
                    // defaults, then the wrap.
                    let mut edge = HttpEdge::new();
                    if !http.edge.request_id {
                        edge = edge.without_request_id();
                    }
                    if !http.edge.logging {
                        edge = edge.without_logging();
                    }
                    if !http.edge.recovery {
                        edge = edge.without_recovery();
                    }
                    if let Some(timeout) = &http.edge.timeout {
                        edge = edge.with_timeout(timeout.0);
                    }
                    if let Some(cors) = &http.edge.cors {
                        let options = cors_options_from(cors);
                        edge = if cors.compat {
                            edge.with_cors_compat(options)
                        } else {
                            edge.with_cors(options)
                        };
                    }
                    router = edge.wrap(router);
                    let mut server = AxumServer::new(http.bind.0, router)?;
                    for bus in aux {
                        server = server.with_aux_shutdown(bus);
                    }
                    let endpoint = server.endpoint()?;
                    (
                        Arc::new(server) as Arc<dyn rushwind_transport::Server>,
                        endpoint,
                    )
                }
                "cron" => {
                    let cron: CronServerConfig =
                        serde_json::from_value(server_config.settings.clone()).map_err(|e| {
                            BootstrapError::Config(format!("cron server settings: {e}"))
                        })?;
                    let mut server = CronServer::new(cron.endpoint.clone());
                    for job_name in &cron.jobs {
                        let job = self.cron_jobs.get(job_name).ok_or_else(|| {
                            BootstrapError::UnknownEngine {
                                domain: "cron job".to_string(),
                                name: job_name.clone(),
                            }
                        })?;
                        // One mount per server: the spec clones, the
                        // handler is a shared arc.
                        server = server.with_job(CronJob {
                            name: job.name.clone(),
                            spec: job.spec.clone(),
                            handler: Arc::clone(&job.handler),
                        });
                    }
                    let endpoint = server.endpoint()?;
                    (
                        Arc::new(server) as Arc<dyn rushwind_transport::Server>,
                        endpoint,
                    )
                }
                kind => {
                    let factory = self
                        .server_factories
                        .get(kind)
                        .ok_or_else(|| BootstrapError::UnknownServerKind(kind.to_string()))?;
                    let server = factory(server_config.settings.clone(), input.clone()).await?;
                    let endpoint = server.endpoint()?;
                    (server, endpoint)
                }
            };
            endpoints.push(endpoint);
            servers.push(server);
        }

        // The registry backend, when configured: its two halves come
        // from the registered factory. With the registration half, a
        // complete identity (name and version), and at least one
        // assembled endpoint, the application announces itself; the
        // announcement's deregistration is wired as a before-shutdown
        // hook. Anything missing leaves the application
        // unannounced.
        let mut discovery: Option<Arc<dyn Discovery>> = None;
        let mut registration: Option<RegistrationHandle> = None;
        if let Some(registry_config) = &self.config.registry {
            let factory = self
                .registry_factories
                .get(&registry_config.engine)
                .ok_or_else(|| {
                    BootstrapError::UnknownRegistryEngine(registry_config.engine.clone())
                })?;
            let endpoint = factory(registry_config.settings.clone()).await?;
            if let (Some(name), Some(version)) = (
                self.config.app.name.clone(),
                self.config.app.version.clone(),
            ) {
                if let (Some(registrar), false) = (endpoint.registrar, endpoints.is_empty()) {
                    let instance = Instance {
                        id: random_instance_id(),
                        name,
                        version,
                        endpoints: endpoints.clone(),
                    };
                    let registration_record = Registration::new(instance);
                    let handle = registrar
                        .register(registration_record.clone())
                        .await
                        .map_err(|e| BootstrapError::Failed(format!("registry register: {e}")))?;
                    let hook_registrar = registrar;
                    let hook_record = registration_record;
                    builder = builder.before_stop(move |_budget| {
                        let registrar = Arc::clone(&hook_registrar);
                        let record = hook_record.clone();
                        async move {
                            let _ = registrar.deregister(record).await;
                            Ok::<(), ServerError>(())
                        }
                    });
                    registration = Some(handle);
                }
            }
            discovery = endpoint.discovery;
        }

        for server in servers {
            builder = builder.erased_server(server);
        }

        Ok(Bootstrapped {
            app: builder.build(),
            repository,
            endpoints,
            discovery,
            registration,
            authn,
            authz,
            brokers,
            caches,
            circuitbreakers,
            limiters,
            ai,
            object_storage,
            config,
            metrics,
            scripts,
            #[cfg(feature = "health")]
            health,
            #[cfg(feature = "trace")]
            tracer,
        })
    }
}

/// A fresh random instance identifier — 16 CSPRNG bytes, hex-formatted.
/// A degenerate all-zero id is the documented fallback when the OS
/// entropy source fails.
fn random_instance_id() -> String {
    let mut bytes = [0u8; 16];
    let _ = getrandom::fill(&mut bytes);
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

/// The default for the edge toggles: every default-on middleware on.
fn wire_true() -> bool {
    true
}

/// The per-server HTTP edge wire: the middleware toggles (defaults on),
/// the optional request budget (a second count or a duration string),
/// and the optional CORS policy. Absent fields leave the HTTP edge's
/// defaults in place.
#[derive(Debug, Deserialize)]
struct EdgeWire {
    #[serde(default = "wire_true")]
    request_id: bool,
    #[serde(default = "wire_true")]
    logging: bool,
    #[serde(default = "wire_true")]
    recovery: bool,
    #[serde(default)]
    timeout: Option<DurationWire>,
    #[serde(default)]
    cors: Option<CorsWire>,
}

impl Default for EdgeWire {
    fn default() -> Self {
        Self {
            request_id: true,
            logging: true,
            recovery: true,
            timeout: None,
            cors: None,
        }
    }
}

/// The CORS policy wire: origin, method, header, and expose lists, the
/// credentials flag, the preflight cache duration, and the compat
/// switch choosing the gorilla-parity layer over tower-http. Empty or
/// absent lists fall back to the HTTP edge's documented defaults.
#[derive(Debug, Default, Deserialize)]
#[serde(default)]
struct CorsWire {
    origins: Vec<String>,
    credentials: bool,
    methods: Vec<String>,
    headers: Vec<String>,
    expose: Vec<String>,
    max_age_secs: Option<u64>,
    compat: bool,
}

/// The per-server domain-mount wire: whether the health probes and the
/// Prometheus scrape endpoint mount. Each flag rides its cargo feature
/// and fails the assembly loudly when the feature is compiled out.
#[derive(Debug, Default, Deserialize)]
#[serde(default)]
struct MountsWire {
    health: bool,
    metrics: bool,
}

/// Settings of the built-in `http` server kind.
#[derive(Debug, Deserialize)]
struct HttpServerConfig {
    bind: BindWire,
    #[serde(default)]
    route_packs: Vec<RoutePackRef>,
    #[serde(default)]
    edge: EdgeWire,
    #[serde(default)]
    mounts: MountsWire,
}

/// Settings of the built-in `cron` server kind: the endpoint string the
/// server reports (and the announcement carries), plus the registered
/// jobs to mount.
#[derive(Debug, Deserialize)]
struct CronServerConfig {
    endpoint: String,
    #[serde(default)]
    jobs: Vec<String>,
}

/// The built-in `"crud"` api pack: the storage line's HTTP edge
/// (`rushwind-storage-axum`'s [`CrudApi`]) bound to the configured
/// repository. Expects the schema's column fields as JSON in and out.
fn crud_api_pack(input: RouteInput) -> Result<Router, BootstrapError> {
    let repo = input.repository.ok_or_else(|| {
        BootstrapError::Failed("crud api pack requires a configured storage".to_string())
    })?;
    Ok(CrudApi::new(repo).router())
}

/// Applies the configured per-subtree security wraps to one router: the
/// authorization bridge first, the authentication bridge second — later
/// `layer` calls wrap earlier ones, so the authenticator ends up
/// outermost and the claims it inserts reach the permission point
/// beneath it. The permission point's project axis is fixed,
/// claim-carried, or empty — the two explicit shapes are mutually
/// exclusive.
fn apply_guards(
    router: Router,
    input: &RouteInput,
    authn: &Option<String>,
    authz: &Option<AuthzRef>,
) -> Result<Router, BootstrapError> {
    let router = apply_guards_authz(router, input, authz)?;
    let Some(authn_instance) = authn else {
        return Ok(router);
    };
    let authenticator =
        input
            .authn
            .get(authn_instance)
            .ok_or_else(|| BootstrapError::UnknownEngine {
                domain: "authn instance".to_string(),
                name: authn_instance.clone(),
            })?;
    Ok(with_authn(router, Arc::clone(authenticator)))
}

/// The authorization half of [`apply_guards`].
fn apply_guards_authz(
    router: Router,
    input: &RouteInput,
    authz: &Option<AuthzRef>,
) -> Result<Router, BootstrapError> {
    let Some(reference) = authz else {
        return Ok(router);
    };
    let engine =
        input
            .authz
            .get(&reference.engine)
            .ok_or_else(|| BootstrapError::UnknownEngine {
                domain: "authz instance".to_string(),
                name: reference.engine.clone(),
            })?;
    Ok(match (&reference.project, &reference.project_claim) {
        (Some(_), Some(_)) => {
            return Err(BootstrapError::Config(
                "authz reference: project and project_claim are mutually exclusive".to_string(),
            ));
        }
        (Some(project), None) => with_authorization_for(
            router,
            Arc::clone(engine),
            reference.action.clone(),
            reference.resource.clone(),
            project.clone(),
        ),
        (None, Some(claim)) => with_authorization_claim(
            router,
            Arc::clone(engine),
            reference.action.clone(),
            reference.resource.clone(),
            claim.clone(),
        ),
        (None, None) => with_authorization(
            router,
            Arc::clone(engine),
            reference.action.clone(),
            reference.resource.clone(),
        ),
    })
}

/// Maps the CORS wire onto the HTTP edge's options builder.
fn cors_options_from(wire: &CorsWire) -> CorsOptions {
    let mut options = CorsOptions::default();
    for origin in &wire.origins {
        options = options.with_allow_origin(origin.clone());
    }
    options = options.with_allow_credentials(wire.credentials);
    for method in &wire.methods {
        options = options.with_allow_method(method.clone());
    }
    for header in &wire.headers {
        options = options.with_allow_header(header.clone());
    }
    for header in &wire.expose {
        options = options.with_expose_header(header.clone());
    }
    if let Some(secs) = wire.max_age_secs {
        options = options.with_max_age(Duration::from_secs(secs));
    }
    options
}

/// A listener address wire: the standard `host:port` socket-address
/// form, or the host-any `":port"` form, whose omitted host binds
/// every interface.
#[derive(Debug, Clone, Copy)]
pub struct BindWire(pub SocketAddr);

impl<'de> Deserialize<'de> for BindWire {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let text = String::deserialize(deserializer)?;
        parse_bind(&text)
            .ok_or_else(|| <D::Error as serde::de::Error>::custom(format!("bind address: {text}")))
            .map(BindWire)
    }
}

/// Parses a listener address: the standard socket-address form, or the
/// host-any `":port"` form (the address `0.0.0.0:port`).
fn parse_bind(text: &str) -> Option<SocketAddr> {
    let text = text.trim();
    if let Some(port_text) = text.strip_prefix(':') {
        let port = port_text.parse::<u16>().ok()?;
        return Some(SocketAddr::from(([0, 0, 0, 0], port)));
    }
    text.parse::<SocketAddr>().ok()
}

/// A duration wire: a plain second count or a duration string
/// (`"300s"`, `"1.5h"`). The string grammar is the `ns`, `us`, `µs`,
/// `ms`, `s`, `m`, `h` unit set of the Go standard library's duration
/// form.
#[derive(Debug, Clone, Copy)]
pub struct DurationWire(pub Duration);

impl<'de> Deserialize<'de> for DurationWire {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer
            .deserialize_any(DurationWireVisitor)
            .map(DurationWire)
    }
}

/// The duration wire's visitor: second counts or duration strings.
struct DurationWireVisitor;

impl<'de> serde::de::Visitor<'de> for DurationWireVisitor {
    type Value = Duration;

    fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "a second count or a duration string")
    }

    fn visit_u64<E>(self, secs: u64) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Duration::try_from_secs_f64(secs as f64)
            .map_err(|_| E::custom(format!("duration: {secs}s overflows")))
    }

    fn visit_i64<E>(self, secs: i64) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        if secs < 0 {
            return Err(E::custom(format!("duration: {secs}s is negative")));
        }
        self.visit_u64(secs as u64)
    }

    fn visit_f64<E>(self, secs: f64) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Duration::try_from_secs_f64(secs)
            .map_err(|_| E::custom(format!("duration: {secs}s is not representable")))
    }

    fn visit_str<E>(self, text: &str) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        parse_duration_string(text)
            .and_then(|secs| Duration::try_from_secs_f64(secs).ok())
            .ok_or_else(|| E::custom(format!("duration string: {text}")))
    }
}

/// Parses a duration string (`"300s"`, `"1.5h"`) into seconds.
fn parse_duration_string(text: &str) -> Option<f64> {
    let text = text.trim();
    if text.is_empty() {
        return None;
    }
    let (value, unit) = text.split_at(text.find(|c: char| c.is_alphabetic())?);
    let value: f64 = value.parse().ok()?;
    let secs = match unit {
        "ns" => value / 1e9,
        "us" | "µs" => value / 1e6,
        "ms" => value / 1e3,
        "s" => value,
        "m" => value * 60.0,
        "h" => value * 3600.0,
        _ => return None,
    };
    Some(secs)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rushwind_ai::AiError;
    use rushwind_authn::{AuthClaims, AuthnError};
    use rushwind_authz::{
        Action, AuthzError, Pairs, PolicyMap, Project, Projects, Resource, RoleMap, Subject,
        Subjects,
    };
    use rushwind_registry::{BoxFuture, RegistryError, Watcher};
    use rushwind_transport::Instance;
    use rushwind_transport_cron::CronSpec;
    use std::collections::HashMap;
    use std::sync::Mutex;

    /// A registry mock recording every registration and deregistration.
    #[derive(Default)]
    struct MockRegistry {
        registered: Mutex<Vec<Registration>>,
        deregistered: Mutex<Vec<Registration>>,
    }

    impl Registrar for MockRegistry {
        fn register<'a>(
            &'a self,
            registration: Registration,
        ) -> BoxFuture<'a, Result<RegistrationHandle, RegistryError>> {
            Box::pin(async move {
                self.registered.lock().unwrap().push(registration.clone());
                Ok(RegistrationHandle::from_cancel(|| {}))
            })
        }

        fn deregister<'a>(
            &'a self,
            registration: Registration,
        ) -> BoxFuture<'a, Result<(), RegistryError>> {
            Box::pin(async move {
                self.deregistered.lock().unwrap().push(registration);
                Ok(())
            })
        }
    }

    impl Discovery for MockRegistry {
        fn get_service<'a>(
            &'a self,
            _service_name: &'a str,
        ) -> BoxFuture<'a, Result<Vec<Instance>, RegistryError>> {
            Box::pin(async move { Ok(Vec::new()) })
        }

        fn watch<'a>(
            &'a self,
            _service_name: &'a str,
        ) -> BoxFuture<'a, Result<Box<dyn Watcher>, RegistryError>> {
            Box::pin(async move { Ok(Box::new(MockWatcher { first: true }) as Box<dyn Watcher>) })
        }
    }

    struct MockWatcher {
        first: bool,
    }

    impl Watcher for MockWatcher {
        fn next<'a>(&'a mut self) -> BoxFuture<'a, Result<Vec<Instance>, RegistryError>> {
            Box::pin(async move {
                if self.first {
                    self.first = false;
                    return Ok(Vec::new());
                }
                Err(RegistryError::Failed("watcher stopped".to_string()))
            })
        }

        fn stop(&mut self) {}
    }

    const CONFIG: &str = r#"
app:
  name: demo
  version: v0.1.0
registry:
  engine: mock
  settings: {}
servers:
  - kind: http
    bind: 127.0.0.1:0
"#;

    const NO_IDENTITY: &str = r#"
registry:
  engine: mock
  settings: {}
servers:
  - kind: http
    bind: 127.0.0.1:0
"#;

    /// The registry section parses.
    #[test]
    fn registry_section_parses() {
        let config: BootstrapConfig = serde_yaml::from_str(CONFIG).expect("config must parse");
        assert_eq!(
            config.registry.as_ref().expect("registry section").engine,
            "mock"
        );
    }

    /// The announcement, the exposure, and the shutdown
    /// deregistration: a configured backend receives the identity and
    /// every assembled endpoint at build time, the discovery half is
    /// exposed, and a full shutdown run deregisters through the
    /// before-stop hook.
    #[tokio::test]
    async fn announcement_exposure_and_deregistration() {
        let mock = Arc::new(MockRegistry::default());
        let factory_mock = Arc::clone(&mock);
        let bootstrapped = Bootstrap::from_yaml_str(CONFIG)
            .expect("bootstrap parses")
            .registry_factory("mock", move |_settings| {
                let registrar = Arc::clone(&factory_mock) as Arc<dyn Registrar>;
                let discovery = Arc::clone(&factory_mock) as Arc<dyn Discovery>;
                async move {
                    Ok(RegistryEndpoint {
                        registrar: Some(registrar),
                        discovery: Some(discovery),
                    })
                }
            })
            .build()
            .await
            .expect("assembly must succeed");

        {
            let registered = mock.registered.lock().unwrap();
            assert_eq!(registered.len(), 1, "exactly one announcement");
            let instance = &registered[0].instance;
            assert_eq!(instance.name, "demo");
            assert_eq!(instance.version, "v0.1.0");
            assert_eq!(instance.endpoints, bootstrapped.endpoints);
            assert!(!instance.id.is_empty());
        }
        assert!(bootstrapped.registration.is_some());
        assert!(bootstrapped.discovery.is_some());

        bootstrapped.app.stop();
        let _ = bootstrapped
            .app
            .run(rushwind_transport::StopSignal::new())
            .await;
        assert_eq!(
            mock.deregistered.lock().unwrap().len(),
            1,
            "the shutdown hook deregisters"
        );
    }

    /// Without identity there is no announcement — but the discovery
    /// half is still exposed.
    #[tokio::test]
    async fn no_identity_no_announcement() {
        let mock = Arc::new(MockRegistry::default());
        let factory_mock = Arc::clone(&mock);
        let bootstrapped = Bootstrap::from_yaml_str(NO_IDENTITY)
            .expect("bootstrap parses")
            .registry_factory("mock", move |_settings| {
                let registrar = Arc::clone(&factory_mock) as Arc<dyn Registrar>;
                let discovery = Arc::clone(&factory_mock) as Arc<dyn Discovery>;
                async move {
                    Ok(RegistryEndpoint {
                        registrar: Some(registrar),
                        discovery: Some(discovery),
                    })
                }
            })
            .build()
            .await
            .expect("assembly must succeed");

        assert!(mock.registered.lock().unwrap().is_empty());
        assert!(bootstrapped.registration.is_none());
        assert!(bootstrapped.discovery.is_some());
    }

    /// A registry section naming an unregistered engine fails the
    /// assembly.
    #[tokio::test]
    async fn unknown_registry_engine_fails() {
        let result = Bootstrap::from_yaml_str(CONFIG)
            .expect("bootstrap parses")
            .build()
            .await;
        assert!(matches!(
            result,
            Err(BootstrapError::UnknownRegistryEngine(name)) if name == "mock"
        ));
    }

    // -----------------------------------------------------------------
    // Engine-family assembly: mock engines proving every family's
    // factory registry, instance maps, and single-instance exposures.
    // -----------------------------------------------------------------

    struct MockAuthn;
    impl Authenticator for MockAuthn {
        fn scheme(&self) -> &'static str {
            "mock"
        }
        fn authenticate_token(&self, _token: &str) -> Result<AuthClaims, AuthnError> {
            Ok(AuthClaims::default())
        }
        fn create_identity(&self, _claims: &AuthClaims) -> Result<String, AuthnError> {
            Ok("mock".to_string())
        }
    }

    struct MockAuthz;
    impl rushwind_authz::Engine for MockAuthz {
        fn name(&self) -> String {
            "mock".to_string()
        }
        fn is_authorized(
            &self,
            _subject: Subject,
            _action: Action,
            _resource: Resource,
            _project: Project,
        ) -> Result<bool, AuthzError> {
            Ok(true)
        }
        fn projects_authorized(
            &self,
            _subjects: Subjects,
            _action: Action,
            _resource: Resource,
            projects: Projects,
        ) -> Result<Projects, AuthzError> {
            Ok(projects)
        }
        fn filter_authorized_pairs(
            &self,
            _subjects: Subjects,
            pairs: Pairs,
        ) -> Result<Pairs, AuthzError> {
            Ok(pairs)
        }
        fn filter_authorized_projects(&self, _subjects: Subjects) -> Result<Projects, AuthzError> {
            Ok(Vec::new())
        }
        fn set_policies(&self, _policies: PolicyMap, _roles: RoleMap) -> Result<(), AuthzError> {
            Ok(())
        }
    }

    struct MockSubscriber;
    impl rushwind_broker::Subscriber for MockSubscriber {
        fn topic(&self) -> &str {
            "mock"
        }
        fn unsubscribe(
            &mut self,
        ) -> rushwind_broker::BoxFuture<'_, Result<(), rushwind_broker::BrokerError>> {
            Box::pin(async { Ok(()) })
        }
    }

    struct MockBroker;
    impl rushwind_broker::Broker for MockBroker {
        fn name(&self) -> &'static str {
            "mock"
        }
        fn connect(
            &self,
        ) -> rushwind_broker::BoxFuture<'_, Result<(), rushwind_broker::BrokerError>> {
            Box::pin(async { Ok(()) })
        }
        fn disconnect(
            &self,
        ) -> rushwind_broker::BoxFuture<'_, Result<(), rushwind_broker::BrokerError>> {
            Box::pin(async { Ok(()) })
        }
        fn publish<'a>(
            &'a self,
            _topic: &'a str,
            _message: rushwind_broker::Message,
        ) -> rushwind_broker::BoxFuture<'a, Result<(), rushwind_broker::BrokerError>> {
            Box::pin(async { Ok(()) })
        }
        fn subscribe<'a>(
            &'a self,
            _topic: &'a str,
            _handler: rushwind_broker::Handler,
        ) -> rushwind_broker::BoxFuture<
            'a,
            Result<Box<dyn rushwind_broker::Subscriber>, rushwind_broker::BrokerError>,
        > {
            let _ = _handler;
            Box::pin(async { Ok(Box::new(MockSubscriber) as Box<dyn rushwind_broker::Subscriber>) })
        }
    }

    struct MockCache;
    impl rushwind_cache::Cache for MockCache {
        fn get<'a>(
            &'a self,
            _key: &'a str,
        ) -> rushwind_cache::BoxFuture<'a, Result<Option<Vec<u8>>, rushwind_cache::CacheError>>
        {
            Box::pin(async { Ok(None) })
        }
        fn set<'a>(
            &'a self,
            _key: &'a str,
            _value: &'a [u8],
            _ttl: Option<Duration>,
        ) -> rushwind_cache::BoxFuture<'a, Result<(), rushwind_cache::CacheError>> {
            Box::pin(async { Ok(()) })
        }
        fn set_nx<'a>(
            &'a self,
            _key: &'a str,
            _value: &'a [u8],
            _ttl: Option<Duration>,
        ) -> rushwind_cache::BoxFuture<'a, Result<bool, rushwind_cache::CacheError>> {
            Box::pin(async { Ok(true) })
        }
        fn delete<'a>(
            &'a self,
            _key: &'a str,
        ) -> rushwind_cache::BoxFuture<'a, Result<(), rushwind_cache::CacheError>> {
            Box::pin(async { Ok(()) })
        }
        fn has<'a>(
            &'a self,
            _key: &'a str,
        ) -> rushwind_cache::BoxFuture<'a, Result<bool, rushwind_cache::CacheError>> {
            Box::pin(async { Ok(false) })
        }
        fn get_multi<'a>(
            &'a self,
            _keys: &'a [String],
        ) -> rushwind_cache::BoxFuture<'a, Result<Vec<Option<Vec<u8>>>, rushwind_cache::CacheError>>
        {
            let _ = _keys;
            Box::pin(async { Ok(Vec::new()) })
        }
        fn set_multi<'a>(
            &'a self,
            _items: &'a [rushwind_cache::Item],
        ) -> rushwind_cache::BoxFuture<'a, Result<(), rushwind_cache::CacheError>> {
            let _ = _items;
            Box::pin(async { Ok(()) })
        }
        fn close(&self) -> rushwind_cache::BoxFuture<'_, Result<(), rushwind_cache::CacheError>> {
            Box::pin(async { Ok(()) })
        }
    }

    struct MockCircuitBreaker;
    impl rushwind_circuitbreaker::CircuitBreaker for MockCircuitBreaker {
        fn allow(&self) -> Result<(), rushwind_circuitbreaker::CircuitError> {
            Ok(())
        }
        fn mark_success(&self) {}
        fn mark_failure(&self) {}
        fn state(&self) -> rushwind_circuitbreaker::State {
            rushwind_circuitbreaker::State::Closed
        }
        fn close(&self) {}
    }

    struct MockLimiter;
    impl rushwind_ratelimit::Limiter for MockLimiter {
        fn allow(&self) -> bool {
            true
        }
        fn wait(
            &self,
        ) -> rushwind_ratelimit::BoxFuture<'_, Result<(), rushwind_ratelimit::RateLimitError>>
        {
            Box::pin(async { Ok(()) })
        }
        fn close(&self) {}
    }

    struct MockMetrics;
    impl rushwind_metrics::Metrics for MockMetrics {
        fn counter(&self, _name: &str, _value: f64, _labels: &[(&str, &str)]) {}
        fn histogram(&self, _name: &str, _value: f64, _labels: &[(&str, &str)]) {}
        fn gauge(&self, _name: &str, _value: f64, _labels: &[(&str, &str)]) {}
    }

    struct MockChatModel;
    impl rushwind_ai::ChatModel for MockChatModel {
        fn chat<'a>(
            &'a self,
            _request: rushwind_ai::ChatRequest,
        ) -> rushwind_ai::BoxFuture<'a, Result<rushwind_ai::ChatResponse, AiError>> {
            let _ = _request;
            Box::pin(async { Err(AiError::Request("mock".to_string())) })
        }
    }

    struct MockObjectStorage;
    impl rushwind_oss::ObjectStorage for MockObjectStorage {
        fn put<'a>(
            &'a self,
            _key: &'a str,
            _body: &'a [u8],
            _content_type: Option<&'a str>,
        ) -> rushwind_oss::BoxFuture<'a, Result<(), rushwind_oss::StorageError>> {
            Box::pin(async { Ok(()) })
        }
        fn get<'a>(
            &'a self,
            _key: &'a str,
        ) -> rushwind_oss::BoxFuture<'a, Result<Vec<u8>, rushwind_oss::StorageError>> {
            Box::pin(async { Err(rushwind_oss::StorageError::NotFound) })
        }
        fn delete<'a>(
            &'a self,
            _key: &'a str,
        ) -> rushwind_oss::BoxFuture<'a, Result<(), rushwind_oss::StorageError>> {
            Box::pin(async { Ok(()) })
        }
    }

    /// A config-source mock answering from a fixed map; one instance
    /// answers nothing, the other one key.
    struct MockSource {
        values: HashMap<String, Vec<u8>>,
    }
    impl rushwind_config::Source for MockSource {
        fn load<'a>(
            &'a self,
            key: &'a str,
        ) -> rushwind_config::BoxFuture<'a, Result<Option<Vec<u8>>, rushwind_config::ConfigError>>
        {
            let value = self.values.get(key).cloned();
            Box::pin(async move { Ok(value) })
        }
    }

    const FAMILIES_YAML: &str = r#"
authn:
  a1: { engine: mockauthn, settings: {} }
authz:
  z1: { engine: mockauthz, settings: {} }
brokers:
  b1: { engine: mockbroker, settings: {} }
caches:
  c1: { engine: mockcache, settings: {} }
circuitbreakers:
  cb1: { engine: mockcb, settings: {} }
limiters:
  l1: { engine: mocklimiter, settings: {} }
metrics:
  engine: mockmetrics
  settings: {}
ai:
  engine: mockchat
  settings: {}
oss:
  engine: mockoss
  settings: {}
config_sources:
  - engine: mocksource-empty
    settings: {}
  - engine: mocksource-primed
    settings: {}
servers: []
"#;

    /// Every family assembles from its registered factory into its
    /// exposure: the named families into their instance maps, the
    /// single-instance families into their options, and the config
    /// source list into the priority fallback.
    #[tokio::test]
    async fn families_assemble_into_their_exposures() {
        let bootstrapped = Bootstrap::from_yaml_str(FAMILIES_YAML)
            .expect("yaml must parse")
            .authn_factory("mockauthn", |_settings| {
                Box::pin(async move { Ok(Arc::new(MockAuthn) as Arc<dyn Authenticator>) })
            })
            .authz_factory("mockauthz", |_settings| {
                Box::pin(async move { Ok(Arc::new(MockAuthz) as Arc<dyn AuthzEngine>) })
            })
            .broker_factory("mockbroker", |_settings| {
                Box::pin(async move { Ok(Arc::new(MockBroker) as Arc<dyn Broker>) })
            })
            .cache_factory("mockcache", |_settings| {
                Box::pin(async move { Ok(Arc::new(MockCache) as Arc<dyn Cache>) })
            })
            .circuitbreaker_factory("mockcb", |_settings| {
                Box::pin(async move { Ok(Arc::new(MockCircuitBreaker) as Arc<dyn CircuitBreaker>) })
            })
            .limiter_factory("mocklimiter", |_settings| {
                Box::pin(async move { Ok(Arc::new(MockLimiter) as Arc<dyn Limiter>) })
            })
            .metrics_factory("mockmetrics", |_settings| {
                Box::pin(async move { Ok(Arc::new(MockMetrics) as Arc<dyn Metrics>) })
            })
            .ai_factory("mockchat", |_settings| {
                Box::pin(async move { Ok(Arc::new(MockChatModel) as Arc<dyn ChatModel>) })
            })
            .oss_factory("mockoss", |_settings| {
                Box::pin(async move { Ok(Arc::new(MockObjectStorage) as Arc<dyn ObjectStorage>) })
            })
            .config_source_factory("mocksource-empty", |_settings| {
                Box::pin(async move {
                    Ok(Arc::new(MockSource {
                        values: HashMap::new(),
                    }) as Arc<dyn rushwind_config::Source>)
                })
            })
            .config_source_factory("mocksource-primed", |_settings| {
                Box::pin(async move {
                    let mut values = HashMap::new();
                    values.insert("k".to_string(), b"from-primed".to_vec());
                    Ok(Arc::new(MockSource { values }) as Arc<dyn rushwind_config::Source>)
                })
            })
            .build()
            .await
            .expect("assembly must succeed");

        assert_eq!(bootstrapped.authn.len(), 1, "authn instance map");
        assert!(bootstrapped.authn.contains_key("a1"));
        assert_eq!(bootstrapped.authz.len(), 1, "authz instance map");
        assert!(bootstrapped.authz.contains_key("z1"));
        assert_eq!(bootstrapped.brokers.len(), 1, "broker instance map");
        assert!(bootstrapped.brokers.contains_key("b1"));
        assert_eq!(bootstrapped.caches.len(), 1, "cache instance map");
        assert!(bootstrapped.caches.contains_key("c1"));
        assert_eq!(
            bootstrapped.circuitbreakers.len(),
            1,
            "circuitbreaker instance map"
        );
        assert!(bootstrapped.circuitbreakers.contains_key("cb1"));
        assert_eq!(bootstrapped.limiters.len(), 1, "limiter instance map");
        assert!(bootstrapped.limiters.contains_key("l1"));
        assert!(bootstrapped.ai.is_some(), "ai single instance");
        assert!(
            bootstrapped.object_storage.is_some(),
            "object storage single instance"
        );
        assert!(bootstrapped.metrics.is_some(), "metrics single instance");

        // The fallback walks in priority order: the empty source
        // answers nothing, the primed one answers; a key no source
        // answers is the unresolved error.
        let source = bootstrapped.config.expect("fallback must assemble");
        let answered = source
            .load("k")
            .await
            .expect("the primed source answers")
            .expect("the value is present");
        assert_eq!(answered, b"from-primed".to_vec());
        assert!(
            source.load("missing").await.is_err(),
            "all-absent resolves to the unresolved error"
        );
    }

    /// An engine name with no registered factory fails the assembly
    /// with the family-named error.
    #[tokio::test]
    async fn unknown_engine_is_an_error() {
        let yaml = r#"
brokers:
  b1: { engine: nope, settings: {} }
servers: []
"#;
        let error = Bootstrap::from_yaml_str(yaml)
            .expect("yaml must parse")
            .build()
            .await
            .expect_err("unknown engine must fail");
        assert!(matches!(
            error,
            BootstrapError::UnknownEngine { domain, name } if domain == "broker engine" && name == "nope"
        ));
    }

    /// A route-pack reference naming an authn instance that was never
    /// assembled fails the assembly.
    #[tokio::test]
    async fn unknown_authn_instance_is_an_error() {
        let yaml = r#"
servers:
  - kind: http
    bind: 127.0.0.1:0
    route_packs:
      - name: p
        authn: ghost
"#;
        let error = Bootstrap::from_yaml_str(yaml)
            .expect("yaml must parse")
            .route_pack("p", |_settings, _input| {
                Ok(RouteSurface::new(Router::new()))
            })
            .build()
            .await
            .expect_err("unknown instance must fail");
        assert!(matches!(
            error,
            BootstrapError::UnknownEngine { domain, name } if domain == "authn instance" && name == "ghost"
        ));
    }

    /// A permission point carrying both project axes is a config
    /// error.
    #[tokio::test]
    async fn project_axes_are_mutually_exclusive() {
        let yaml = r#"
authz:
  z1: { engine: mockauthz, settings: {} }
servers:
  - kind: http
    bind: 127.0.0.1:0
    route_packs:
      - name: p
        authz:
          engine: z1
          action: a
          resource: r
          project: fixed
          project_claim: claim
"#;
        let error = Bootstrap::from_yaml_str(yaml)
            .expect("yaml must parse")
            .authz_factory("mockauthz", |_settings| {
                Box::pin(async move { Ok(Arc::new(MockAuthz) as Arc<dyn AuthzEngine>) })
            })
            .route_pack("p", |_settings, _input| {
                Ok(RouteSurface::new(Router::new()))
            })
            .build()
            .await
            .expect_err("both project axes must fail");
        assert!(matches!(error, BootstrapError::Config(_)));
    }

    /// The bind wire parses the standard socket-address form and the
    /// host-any `":port"` form, and rejects hostnames and malformed
    /// text.
    #[test]
    fn bind_wires_parse_both_forms() {
        assert!(matches!(
            parse_bind(":7788"),
            Some(addr) if addr.port() == 7788 && addr.ip().is_unspecified()
        ));
        assert!(matches!(
            parse_bind("127.0.0.1:8080"),
            Some(addr) if addr.port() == 8080 && addr.ip().is_loopback()
        ));
        assert_eq!(parse_bind("localhost:8080"), None);
        assert_eq!(parse_bind("nope"), None);
    }

    /// Duration strings parse into seconds across the unit set.
    #[test]
    fn duration_strings_parse_to_seconds() {
        assert_eq!(parse_duration_string("300s"), Some(300.0));
        assert_eq!(parse_duration_string("90m"), Some(5400.0));
        assert_eq!(parse_duration_string("1.5h"), Some(5400.0));
        assert_eq!(parse_duration_string("0.4s"), Some(0.4));
    }

    /// Malformed duration strings reject.
    #[test]
    fn malformed_durations_reject() {
        assert_eq!(parse_duration_string(""), None);
        assert_eq!(parse_duration_string("abc"), None);
        assert_eq!(parse_duration_string("12q"), None);
    }

    /// The edge wire takes its request budget as a duration string or
    /// a plain second count; an absent field leaves no budget.
    #[test]
    fn edge_wires_accept_both_budget_forms() {
        let string_form: EdgeWire =
            serde_yaml::from_str("timeout: 10s").expect("duration-string form must parse");
        assert!(matches!(string_form.timeout, Some(d) if d.0.as_secs() == 10));
        let count_form: EdgeWire =
            serde_yaml::from_str("timeout: 10").expect("second-count form must parse");
        assert!(matches!(count_form.timeout, Some(d) if d.0.as_secs() == 10));
        let absent: EdgeWire = serde_yaml::from_str("{}").expect("empty edge must parse");
        assert!(absent.timeout.is_none());
    }

    /// Registered cron jobs mount by name on the `cron` server kind,
    /// and the server reports its configured endpoint.
    #[tokio::test]
    async fn cron_jobs_mount_on_cron_servers() {
        let yaml = r#"
servers:
  - kind: cron
    endpoint: cron://unit
    jobs: [tick]
"#;
        let bootstrapped = Bootstrap::from_yaml_str(yaml)
            .expect("yaml must parse")
            .cron_job(
                "tick",
                CronJob::new(
                    "tick",
                    CronSpec::parse("* * * * *").expect("spec must parse"),
                    || Box::pin(async {}),
                ),
            )
            .build()
            .await
            .expect("assembly must succeed");
        assert_eq!(bootstrapped.endpoints, vec!["cron://unit".to_string()]);
    }

    /// A cron job name that was never registered fails the assembly.
    #[tokio::test]
    async fn unknown_cron_job_is_an_error() {
        let yaml = r#"
servers:
  - kind: cron
    endpoint: cron://unit
    jobs: [ghost]
"#;
        let error = Bootstrap::from_yaml_str(yaml)
            .expect("yaml must parse")
            .build()
            .await
            .expect_err("unknown job must fail");
        assert!(matches!(
            error,
            BootstrapError::UnknownEngine { domain, name } if domain == "cron job" && name == "ghost"
        ));
    }

    /// Script instances assemble through the script domain's own
    /// factory registry into the manager, and the shutdown sweep
    /// closes and clears them.
    #[tokio::test]
    async fn scripts_assemble_and_close_on_shutdown() {
        rushwind_script_lua::register();
        let yaml = r#"
scripts:
  s1: { engine: lua }
"#;
        let bootstrapped = Bootstrap::from_yaml_str(yaml)
            .expect("yaml must parse")
            .build()
            .await
            .expect("assembly must succeed");
        let manager = bootstrapped.scripts.expect("manager must assemble");
        assert!(
            manager.get("s1").is_some(),
            "the instance must be registered"
        );

        bootstrapped.app.stop();
        let _ = bootstrapped
            .app
            .run(rushwind_transport::StopSignal::new())
            .await;
        assert!(
            manager.get("s1").is_none(),
            "the shutdown sweep closes and clears"
        );
    }

    /// The health section fails loudly when the feature is compiled
    /// out.
    #[cfg(not(feature = "health"))]
    #[tokio::test]
    async fn health_section_requires_the_feature() {
        let yaml = "health:\n  timeout_ms: 1\nservers: []\n";
        let error = Bootstrap::from_yaml_str(yaml)
            .expect("yaml must parse")
            .build()
            .await
            .expect_err("featureless health must fail");
        assert!(matches!(
            error,
            BootstrapError::Failed(msg) if msg.contains("feature 'health'")
        ));
    }

    /// The health mount fails loudly when the feature is compiled out.
    #[cfg(not(feature = "health"))]
    #[tokio::test]
    async fn health_mount_requires_the_feature() {
        let yaml =
            "servers:\n  - kind: http\n    bind: 127.0.0.1:0\n    mounts:\n      health: true\n";
        let error = Bootstrap::from_yaml_str(yaml)
            .expect("yaml must parse")
            .build()
            .await
            .expect_err("featureless health mount must fail");
        assert!(matches!(
            error,
            BootstrapError::Failed(msg) if msg.contains("feature 'health'")
        ));
    }

    /// The tracer section fails loudly when the feature is compiled
    /// out.
    #[cfg(not(feature = "trace"))]
    #[tokio::test]
    async fn tracer_section_requires_the_feature() {
        let yaml = "tracer:\n  endpoint: 127.0.0.1:4317\nservers: []\n";
        let error = Bootstrap::from_yaml_str(yaml)
            .expect("yaml must parse")
            .build()
            .await
            .expect_err("featureless tracer must fail");
        assert!(matches!(
            error,
            BootstrapError::Failed(msg) if msg.contains("feature 'trace'")
        ));
    }
}
