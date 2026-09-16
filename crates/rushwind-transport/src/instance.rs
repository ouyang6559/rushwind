//! Service instance description model.

/// Description of one runnable service instance, for registration with a
/// service registry.
///
/// The wire format of this struct — the exact layout written to and read
/// from registry backends — is specified in the `rushwind-protocols`
/// repository.
/// Serialization adapters must be built against that spec and validated with
/// its golden vectors.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Instance {
    /// Application identifier of the owning service.
    pub id: String,
    /// Human-readable service name.
    pub name: String,
    /// Service version string.
    pub version: String,
    /// Reachable endpoints (`scheme://host:port`) of this instance.
    pub endpoints: Vec<String>,
}
