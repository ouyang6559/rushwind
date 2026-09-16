//! A feature-immune deserializer over serde_json's value model.
//!
//! The workspace's dependency graph (the script engines, chiefly)
//! enables serde_json's `arbitrary_precision` feature workspace-wide.
//! That feature makes serde_json hand every JSON number it
//! deserializes through `deserialize_any` to the visitor as a tagged
//! map instead of a number — and protojson's `google.protobuf.Value`
//! (the wire shape of this engine's leaf operands) deserializes
//! through `deserialize_any`, so float operands (`"value": 2.5`)
//! fail wholesale once that feature unifies on. This module
//! re-offers the value model to serde with numbers dispatched by
//! their actual shape (`as_i64`/`as_u64`/`as_f64` →
//! `visit_i64`/`visit_u64`/`visit_f64`), mirroring serde_json's own
//! value-model deserializer everywhere else. The text → value-model
//! parse itself stays serde_json's: that step is symmetric under the
//! feature and untouched here.

use serde::de::value::{MapDeserializer, SeqDeserializer};
use serde::de::{Error as SerdeError, IntoDeserializer, Visitor};
use serde::forward_to_deserialize_any;
use serde_json::Error;

/// The value-model deserializer: serde_json's value model with
/// shape-driven number dispatch.
pub(crate) struct CleanJson<'de> {
    value: &'de serde_json::Value,
}

impl<'de> CleanJson<'de> {
    /// Wraps a parsed value model.
    pub(crate) fn new(value: &'de serde_json::Value) -> Self {
        Self { value }
    }
}

impl<'de> IntoDeserializer<'de, Error> for CleanJson<'de> {
    type Deserializer = Self;

    fn into_deserializer(self) -> Self {
        self
    }
}

impl<'de> serde::Deserializer<'de> for CleanJson<'de> {
    type Error = Error;

    fn deserialize_any<V>(self, visitor: V) -> Result<V::Value, Error>
    where
        V: Visitor<'de>,
    {
        match self.value {
            serde_json::Value::Null => visitor.visit_unit(),
            serde_json::Value::Bool(b) => visitor.visit_bool(*b),
            // The one divergence from serde_json's value deserializer:
            // numbers dispatch by shape, never as the
            // arbitrary_precision tagged map.
            serde_json::Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    visitor.visit_i64(i)
                } else if let Some(u) = n.as_u64() {
                    visitor.visit_u64(u)
                } else if let Some(f) = n.as_f64() {
                    visitor.visit_f64(f)
                } else {
                    Err(Error::invalid_type(
                        serde::de::Unexpected::Other("number"),
                        &visitor,
                    ))
                }
            }
            serde_json::Value::String(s) => visitor.visit_str(s),
            serde_json::Value::Array(array) => {
                let mut deserializer = SeqDeserializer::new(array.iter().map(CleanJson::new));
                let seq = visitor.visit_seq(&mut deserializer)?;
                deserializer.end()?;
                Ok(seq)
            }
            serde_json::Value::Object(map) => {
                let mut deserializer =
                    MapDeserializer::new(map.iter().map(|(k, v)| (k.clone(), CleanJson::new(v))));
                let value = visitor.visit_map(&mut deserializer)?;
                deserializer.end()?;
                Ok(value)
            }
        }
    }

    fn deserialize_option<V>(self, visitor: V) -> Result<V::Value, Error>
    where
        V: Visitor<'de>,
    {
        match self.value {
            serde_json::Value::Null => visitor.visit_none(),
            _ => visitor.visit_some(self),
        }
    }

    fn deserialize_enum<V>(
        self,
        _name: &'static str,
        _variants: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Error>
    where
        V: Visitor<'de>,
    {
        // These wire enums are unit variants carried as plain strings;
        // map-shaped enum payloads are rejected rather than recursed.
        match self.value {
            serde_json::Value::String(variant) => {
                visitor.visit_enum(variant.clone().into_deserializer())
            }
            other => Err(Error::invalid_type(unexpected(other), &"string variant")),
        }
    }

    fn deserialize_ignored_any<V>(self, visitor: V) -> Result<V::Value, Error>
    where
        V: Visitor<'de>,
    {
        let _ = self;
        visitor.visit_unit()
    }

    forward_to_deserialize_any! {
        bool i8 i16 i32 i64 i128 u8 u16 u32 u64 u128 f32 f64 char str string
        bytes byte_buf unit unit_struct newtype_struct seq tuple
        tuple_struct map struct identifier
    }
}

/// The shape description serde_json's error reporting expects for a
/// rejected value — mirrors its own `Value::unexpected`.
fn unexpected(value: &serde_json::Value) -> serde::de::Unexpected<'_> {
    match value {
        serde_json::Value::Null => serde::de::Unexpected::Unit,
        serde_json::Value::Bool(b) => serde::de::Unexpected::Bool(*b),
        serde_json::Value::Number(_) => serde::de::Unexpected::Other("number"),
        serde_json::Value::String(_) => serde::de::Unexpected::Other("string"),
        serde_json::Value::Array(_) => serde::de::Unexpected::Seq,
        serde_json::Value::Object(_) => serde::de::Unexpected::Map,
    }
}
