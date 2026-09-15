//! The per-route wire facts the lifecycle glue needs.
//!
//! Pure data, copied field-by-field from a caller's route table at the
//! mount site. The struct exists so the glue consumes route facts without
//! referencing any generated type — a generated surface can thread these
//! to [`crate::glue`] and [`crate::bindgate`] without the generator's
//! output and the framework sharing a type dependency.

/// The per-route wire facts: the route template's variables, the body
/// mode, and the message names the pool resolves.
pub struct RouteWire {
    /// The operation id (`/<pkg>.<Svc>/<Method>`).
    pub operation_id: &'static str,
    /// Fully-qualified proto name of the request message.
    pub input_fq: &'static str,
    /// Fully-qualified proto name of the response message.
    pub output_fq: &'static str,
    /// Whether the route declares a body (`body: "*"`).
    pub body_star: bool,
    /// Path variable names of the route template.
    pub path_vars: &'static [&'static str],
}
