//! Registration-only registry contract for RushWind services.
//!
//! The slice is deliberately one-directional: services **announce**
//! themselves so an admin console (or any discovery reader) can see them.
//! There is no discovery, no watcher, no routing — a Rust service that
//! needs to *find* other services does not use this crate.
//!
//! # The wire contract (extracted, not invented)
//!
//! The key layout and value format are **byte-compatible with the Go
//! predecessor** so a Go-side console sees Rust services without knowing
//! they are Rust:
//!
//! - Key: `{namespace}/{name}/{id}` — namespace defaults to
//!   [`DEFAULT_NAMESPACE`] (`/microservices`), matching
//!   `go-wind-plugins/registry/etcd` `options.namespace`.
//! - Value: `json.Marshal(wind.Instance)` — fields in declaration order
//!   (`id`, `name`, `version`, `endpoints`, `metadata`), with a nil Go map
//!   marshaling as `null`. [`registry_json`] reproduces this byte for byte;
//!   the golden tests pin it against literal Go output.
//!
//! If the format ever needs to evolve, the spec and its golden vectors move
//! to the `rushwind-protocols` repository first, and both ecosystems
//! regenerate against it — never hand-copy.
//!
//! # Lifecycle semantics
//!
//! [`Registrar::register`] returns a [`RegistrationHandle`]. Backends keep
//! the registration alive (lease/heartbeat) for as long as the handle
//! lives; dropping it is a **best-effort eventual removal** — the exact
//! timing is backend-dependent (lease TTL expiry). [`Registrar::deregister`]
//! removes immediately. Registrations must also survive backend restarts:
//! keepalive loops re-establish themselves, subscriptions... rather,
//! re-grant their leases and re-put their keys, forever.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;

use rushwind_transport::Instance;
use serde::Serialize;

/// Future type used across the registry contract.
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// The default key namespace, matching the Go registrar's default option.
pub const DEFAULT_NAMESPACE: &str = "/microservices";

/// Errors produced by registry backends.
#[derive(Debug)]
#[non_exhaustive]
pub enum RegistryError {
    /// The backend could not complete the operation.
    Failed(String),
}

impl std::fmt::Display for RegistryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Failed(msg) => write!(f, "registry operation failed: {msg}"),
        }
    }
}

impl std::error::Error for RegistryError {}

/// One service announcement: the transport [`Instance`] plus the optional
/// registry metadata.
///
/// `metadata` is `None` by default, which serializes as JSON `null` — the
/// byte-parity default, because the Go `App.Instance` helper never sets
/// metadata either.
#[derive(Debug, Clone, Default)]
pub struct Registration {
    /// The service instance being announced.
    pub instance: Instance,
    /// Free-form registry metadata; `None` serializes as `null`.
    pub metadata: Option<BTreeMap<String, String>>,
}

impl Registration {
    /// Wraps an instance with no metadata.
    pub fn new(instance: Instance) -> Self {
        Self {
            instance,
            metadata: None,
        }
    }

    /// Attaches registry metadata.
    pub fn with_metadata(mut self, metadata: BTreeMap<String, String>) -> Self {
        self.metadata = Some(metadata);
        self
    }
}

/// The registration key: `{namespace}/{name}/{id}`.
///
/// Byte-compatible with the Go registrar's
/// `fmt.Sprintf("%s/%s/%s", namespace, service.Name, service.ID)`.
pub fn registry_key(namespace: &str, instance: &Instance) -> String {
    format!("{namespace}/{}/{}", instance.name, instance.id)
}

/// Serializes a registration to the wire format, byte-compatible with
/// Go's `json.Marshal(wind.Instance)`: fields in declaration order, and a
/// `None` metadata as `null`.
pub fn registry_json(registration: &Registration) -> String {
    #[derive(Serialize)]
    struct Wire<'a> {
        id: &'a str,
        name: &'a str,
        version: &'a str,
        endpoints: &'a [String],
        metadata: Option<&'a BTreeMap<String, String>>,
    }
    let wire = Wire {
        id: &registration.instance.id,
        name: &registration.instance.name,
        version: &registration.instance.version,
        endpoints: &registration.instance.endpoints,
        metadata: registration.metadata.as_ref(),
    };
    // Infallible for this shape: strings, arrays and maps only.
    serde_json::to_string(&wire).expect("registry wire serialization is infallible")
}

/// A handle keeping one registration alive.
///
/// Dropping the handle is a best-effort cancellation: backends stop their
/// keepalive machinery, and removal completes through the backend's own
/// expiry (lease/TTL). [`RegistrationHandle::cancel`] is the immediate,
/// explicit form; both are idempotent.
pub struct RegistrationHandle {
    cancel: Option<Box<dyn FnOnce() + Send>>,
}

impl RegistrationHandle {
    /// Wraps a backend cancellation closure.
    pub fn from_cancel(cancel: impl FnOnce() + Send + 'static) -> Self {
        Self {
            cancel: Some(Box::new(cancel)),
        }
    }

    /// Cancels the keepalive immediately. Idempotent.
    pub fn cancel(&mut self) {
        if let Some(cancel) = self.cancel.take() {
            cancel();
        }
    }
}

impl Drop for RegistrationHandle {
    fn drop(&mut self) {
        self.cancel();
    }
}

/// A write-only registrar: announce instances to the registry.
///
/// Implementations must be robust against backend restarts — a lost
/// connection is retried internally, forever, with the registration
/// re-established (lease re-granted, key re-put). Errors surface only for
/// operations the backend cannot recover from on its own.
pub trait Registrar: Send + Sync {
    /// Announces the instance. The returned handle keeps the registration
    /// alive; see [`RegistrationHandle`] for the drop semantics.
    fn register<'a>(
        &'a self,
        registration: Registration,
    ) -> BoxFuture<'a, Result<RegistrationHandle, RegistryError>>;

    /// Removes the announcement immediately (key deletion plus keepalive
    /// teardown).
    fn deregister<'a>(
        &'a self,
        registration: Registration,
    ) -> BoxFuture<'a, Result<(), RegistryError>>;
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_instance() -> Instance {
        Instance {
            id: "order-01".to_string(),
            name: "order-service".to_string(),
            version: "v1.0.0".to_string(),
            endpoints: vec!["grpc://127.0.0.1:9000".to_string()],
        }
    }

    /// Golden vector: byte-identical to Go's
    /// `json.Marshal(wind.App.Instance("grpc://127.0.0.1:9000"))` — the
    /// common shape, where `App.Instance` leaves metadata nil.
    #[test]
    fn json_matches_go_marshal_with_nil_metadata() {
        let registration = Registration::new(sample_instance());
        assert_eq!(
            registry_json(&registration),
            r#"{"id":"order-01","name":"order-service","version":"v1.0.0","endpoints":["grpc://127.0.0.1:9000"],"metadata":null}"#
        );
    }

    /// Golden vector: metadata set on the Go side would marshal as a JSON
    /// object at the same position.
    #[test]
    fn json_matches_go_marshal_with_metadata() {
        let registration = Registration::new(sample_instance())
            .with_metadata(BTreeMap::from([("tier".to_string(), "gold".to_string())]));
        assert_eq!(
            registry_json(&registration),
            r#"{"id":"order-01","name":"order-service","version":"v1.0.0","endpoints":["grpc://127.0.0.1:9000"],"metadata":{"tier":"gold"}}"#
        );
    }

    #[test]
    fn key_layout_matches_go_registrar() {
        let key = registry_key(DEFAULT_NAMESPACE, &sample_instance());
        assert_eq!(key, "/microservices/order-service/order-01");
    }
}
