//! The value bridge between script runtimes and the host.
//!
//! Go's engine methods traffic in `any`; Rust needs one closed type, and
//! this is it. Every engine crate maps its native values (Lua tables,
//! Boa JS values, Starlark values, CEL results) into [`ScriptValue`] on
//! the way out and back on the way in — the same bridging job
//! `gluamapper`/`goja`-value-conversion do on the Go side, unified into
//! one contract type.
//!
//! The shape is deliberately lossy: no functions, no userdata, no
//! references into engine state. A script-side callable crossing the
//! boundary is [`ScriptValue::Null`]; hosts that want script callbacks
//! register them through the engine's own hook surface instead.

use std::collections::HashMap;

/// A script value marshalled across the engine boundary.
///
/// Data-only: null, booleans, integers, floats, strings, byte arrays,
/// homogeneous lists, and string-keyed maps. Engines that cannot
/// represent a variant (CEL has no tables) reject it with
/// [`crate::ScriptError::Failed`] rather than approximating.
#[derive(Debug, Clone, PartialEq, Default)]
#[non_exhaustive]
pub enum ScriptValue {
    /// The absent value. Also the marshal result for script values with
    /// no data-shape representation (functions, userdata, threads).
    #[default]
    Null,
    /// A boolean.
    Bool(bool),
    /// A signed integer.
    Int(i64),
    /// An unsigned integer.
    UInt(u64),
    /// A floating-point number.
    Float(f64),
    /// A UTF-8 string.
    String(String),
    /// An opaque byte array.
    Bytes(Vec<u8>),
    /// A positional list of values.
    Array(Vec<ScriptValue>),
    /// A string-keyed map of values.
    Map(HashMap<String, ScriptValue>),
}
