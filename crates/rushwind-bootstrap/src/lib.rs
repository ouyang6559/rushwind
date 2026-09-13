//! Config-driven assembly for RushWind applications.
//!
//! [`Bootstrap`] turns a YAML document into a running-shaped application:
//! the **storage engine** and the **HTTP servers** (with their route packs)
//! are selected by configuration and assembled under one [`App`]
//! lifecycle. Handlers and route logic stay in code — configuration picks
//! which registered pieces to mount, it never contains logic.
//!
//! # The assembly model
//!
//! Three registries, all explicit (no init-time magic, no linker
//! collection):
//!
//! - **route packs** — closures building an [`axum::Router`] from a
//!   [`RouteInput`] (which carries the configured storage, if any).
//!   Handlers are code; a pack is the unit configuration can mount.
//! - **storage factories** — closures turning engine-specific settings
//!   into an `Arc<dyn Repository>`. The schema is application knowledge
//!   and is captured by the closure, not described by configuration.
//! - **server factories** — closures turning server settings into an
//!   `Arc<dyn Server>`, for kinds beyond the built-in `http` (ws, quic,
//!   mqtt glue registers here).
//! - **storage endpoints** — `storage_endpoints` entries mount the
//!   storage line's HTTP edge (the built-in `"crud"` pack nests
//!   `rushwind-storage-axum`'s `CrudApi` under a prefix) or any
//!   application-registered api pack.
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
//!     route_packs: [health]
//! ```
//!
//! `route_packs: [health]` names a pack registered with
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
use rushwind_core::App;
use rushwind_storage::Repository;
use rushwind_storage_axum::CrudApi;
use rushwind_transport::Server;
use rushwind_transport_axum::AxumServer;
use serde::Deserialize;

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
}

/// What a route pack receives when building its router.
#[derive(Clone)]
pub struct RouteInput {
    /// The configured storage engine, if any. Shared with every pack and
    /// server on this bootstrap.
    pub repository: Option<Arc<dyn Repository>>,
}

/// Errors surfaced by assembly.
#[derive(Debug)]
#[non_exhaustive]
pub enum BootstrapError {
    /// The YAML document could not be parsed.
    Config(String),
    /// A `servers[].kind` with no registered factory.
    UnknownServerKind(String),
    /// A `route_packs[]` entry with no registered pack.
    UnknownRoutePack(String),
    /// A `storage.engine` with no registered factory.
    UnknownStorageEngine(String),
    /// A `storage_endpoints[].api` with no registered pack.
    UnknownApiPack(String),
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
            Self::UnknownApiPack(name) => write!(f, "unknown storage api pack: {name}"),
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
    /// HTTP edge mounted over the configured storage, in order. Requires
    /// `storage`.
    #[serde(default)]
    pub storage_endpoints: Vec<StorageEndpointConfig>,
    /// Servers to assemble, in order.
    #[serde(default)]
    pub servers: Vec<ServerConfig>,
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

/// One server to assemble.
#[derive(Debug, Deserialize)]
pub struct ServerConfig {
    /// The registered factory name (`http` is built in).
    pub kind: String,
    /// Factory-specific settings, passed verbatim.
    #[serde(flatten)]
    pub settings: serde_json::Value,
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
}

/// The route-pack closure type.
type RoutePackFn = Box<dyn Fn(RouteInput) -> Result<Router, BootstrapError> + Send + Sync>;

/// The storage-factory closure type.
type StorageFactoryFn = Box<
    dyn Fn(serde_json::Value) -> BoxFuture<'static, Result<Arc<dyn Repository>, BootstrapError>>
        + Send
        + Sync,
>;

/// The api-pack closure type: one storage HTTP edge, mounted under a
/// prefix. The built-in `"crud"` pack (backed by `rushwind-storage-axum`)
/// ships with the bootstrap; application packs register alongside it.
type ApiPackFn = Box<dyn Fn(RouteInput) -> Result<Router, BootstrapError> + Send + Sync>;

/// The server-factory closure type, for kinds beyond the built-in `http`.
type ServerFactoryFn = Box<
    dyn Fn(
            serde_json::Value,
            RouteInput,
        )
            -> BoxFuture<'static, Result<Arc<dyn rushwind_transport::Server>, BootstrapError>>
        + Send
        + Sync,
>;

/// Config-driven application assembler. See the crate docs.
pub struct Bootstrap {
    config: BootstrapConfig,
    route_packs: HashMap<String, RoutePackFn>,
    storage_factories: HashMap<String, StorageFactoryFn>,
    server_factories: HashMap<String, ServerFactoryFn>,
    api_packs: HashMap<String, ApiPackFn>,
}

impl std::fmt::Debug for Bootstrap {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Bootstrap").finish_non_exhaustive()
    }
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
            server_factories: HashMap::new(),
            api_packs,
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

    /// Registers a named route pack.
    pub fn route_pack<F>(mut self, name: impl Into<String>, pack: F) -> Self
    where
        F: Fn(RouteInput) -> Result<Router, BootstrapError> + Send + Sync + 'static,
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

    /// Registers a named server factory for a kind beyond the built-in
    /// `http` (ws, quic, mqtt glue registers here).
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

    /// Assembles the application: storage first (route packs may share
    /// it), then every configured server, then the [`App`] identity and
    /// lifecycle settings.
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
        let input = RouteInput {
            repository: repository.clone(),
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
            let router = pack(input.clone())?;
            storage_routers.push((endpoint_config.nest.clone(), router));
        }

        let mut endpoints = Vec::new();
        let mut servers = Vec::new();
        for server_config in &self.config.servers {
            // The built-in `http` kind is assembled here, where the pack
            // registry is in scope; every other kind goes through the
            // server-factory registry.
            let (server, endpoint) = match server_config.kind.as_str() {
                "http" => {
                    let http: HttpServerConfig =
                        serde_json::from_value(server_config.settings.clone()).map_err(|e| {
                            BootstrapError::Config(format!("http server settings: {e}"))
                        })?;
                    let mut router = Router::new();
                    for pack_name in &http.route_packs {
                        let pack = self
                            .route_packs
                            .get(pack_name)
                            .ok_or_else(|| BootstrapError::UnknownRoutePack(pack_name.clone()))?;
                        router = router.merge(pack(input.clone())?);
                    }
                    for (prefix, edge_router) in &storage_routers {
                        router = router.nest(prefix, edge_router.clone());
                    }
                    let server = AxumServer::new(http.bind, router)?;
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

        for server in servers {
            builder = builder.erased_server(server);
        }

        Ok(Bootstrapped {
            app: builder.build(),
            repository,
            endpoints,
        })
    }
}

/// Settings of the built-in `http` server kind.
#[derive(Debug, Deserialize)]
struct HttpServerConfig {
    bind: SocketAddr,
    #[serde(default)]
    route_packs: Vec<String>,
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
