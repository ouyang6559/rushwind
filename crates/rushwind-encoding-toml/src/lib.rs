//! TOML engine for the RushWind encoding contract, over the `toml`
//! crate.
//!
//! Unlike the plain-delegate engines in this workspace: the
//! `toml` crate exposes no writer-backed `serde::Serializer` for whole
//! documents, and the erased contract needs one. Marshal therefore
//! routes the value through `serde_json`'s in-memory value model
//! first, then translates that tree into a TOML value by hand
//! ([`json_to_toml_value`]) — by hand, not through serde, whose
//! number serialization changes shape under the
//! `arbitrary_precision` feature the workspace's dependency graph
//! enables on serde_json. For TOML-expressible data the result is
//! identical to a direct serialization — the caveats are TOML's own:
//! no binary payloads, integers are i64, no nulls, and values that
//! `serde_json`'s value model cannot hold (e.g. `u64` above `i64::MAX`)
//! fail with an encode error.
//!
//! # Usage
//!
//! ```no_run
//! rushwind_encoding_toml::register(); // once, at startup
//!
//! use rushwind_encoding::{get_codec, marshal};
//! let codec = get_codec("toml").unwrap();
//! # #[derive(serde::Serialize)]
//! # struct Ping { seq: u32 }
//! let bytes = marshal(codec.as_ref(), &Ping { seq: 1 }).unwrap();
//! # let _ = bytes;
//! ```

#![forbid(unsafe_code)]
#![deny(missing_docs)]

use erased_serde::{
    Deserializer as ErasedDeserializer, Serialize as ErasedSerialize,
    Serializer as ErasedSerializer,
};
use rushwind_encoding::{Codec, EncodingError};

/// The codec's registry name.
pub const NAME: &str = "toml";

/// The TOML codec. State-free; construct one or fetch it from the
/// registry under [`NAME`].
#[derive(Debug, Clone, Copy, Default)]
pub struct TomlCodec;

impl TomlCodec {
    /// Builds the codec.
    pub fn codec() -> Self {
        Self
    }

    /// Installs the codec in the registry under [`NAME`]. Call once at
    /// startup — registration is explicit, not import-time.
    pub fn register() {
        rushwind_encoding::register_codec(Self).expect("register the toml codec");
    }
}

/// Installs [`TomlCodec`] in the shared registry under [`NAME`] — the
/// free-function form of [`TomlCodec::register`].
pub fn register() {
    TomlCodec::register();
}

/// One JSON value to one TOML value, by shape: booleans, strings,
/// i64-range integers, and floats map to their TOML scalars;
/// `u64`s above `i64::MAX`, and nulls (TOML has none), have no TOML
/// shape and fail the whole translation; arrays and tables recurse.
/// The translation is shape-driven by hand on purpose — through
/// serde, the `arbitrary_precision` feature (enabled on serde_json by
/// the workspace's dependency graph) would land every integer as a
/// tagged table instead of a TOML integer.
fn json_to_toml_value(value: &serde_json::Value) -> Option<toml::Value> {
    Some(match value {
        serde_json::Value::Null => return None,
        serde_json::Value::Bool(b) => toml::Value::Boolean(*b),
        serde_json::Value::Number(number) => {
            if let Some(i) = number.as_i64() {
                toml::Value::Integer(i)
            } else if number.is_u64() {
                return None;
            } else if let Some(f) = number.as_f64() {
                toml::Value::Float(f)
            } else {
                return None;
            }
        }
        serde_json::Value::String(s) => toml::Value::String(s.clone()),
        serde_json::Value::Array(array) => {
            let mut toml_array = Vec::new();
            for element in array {
                toml_array.push(json_to_toml_value(element)?);
            }
            toml::Value::Array(toml_array)
        }
        serde_json::Value::Object(object) => {
            let mut table = toml::Table::new();
            for (key, element) in object {
                let toml_value = json_to_toml_value(element)?;
                table.insert(key.clone(), toml_value);
            }
            toml::Value::Table(table)
        }
    })
}

impl Codec for TomlCodec {
    fn name(&self) -> &'static str {
        NAME
    }

    fn marshal(&self, value: &dyn ErasedSerialize) -> Result<Vec<u8>, EncodingError> {
        // Through the JSON value model (see the crate docs): render the
        // value to JSON text, re-read it as an in-memory value, then
        // translate that tree into a TOML value element by element
        // ([`json_to_toml_value`]) — the hand-written, feature-immune
        // path (see the crate docs on `arbitrary_precision`).
        let mut json = Vec::new();
        value
            .erased_serialize(&mut <dyn ErasedSerializer>::erase(
                &mut serde_json::Serializer::new(&mut json),
            ))
            .map_err(|e| EncodingError::Failed(format!("toml encode: {e}")))?;
        let value: serde_json::Value = serde_json::from_slice(&json)
            .map_err(|e| EncodingError::Failed(format!("toml encode bridge: {e}")))?;
        let translated = json_to_toml_value(&value)
            .ok_or_else(|| EncodingError::Failed("toml encode: not a table".to_string()))?;
        toml::to_string(&translated)
            .map(String::into_bytes)
            .map_err(|e| EncodingError::Failed(format!("toml encode: {e}")))
    }

    fn unmarshal<'de>(
        &self,
        data: &'de [u8],
        decode: &mut dyn FnMut(&mut dyn ErasedDeserializer<'de>) -> Result<(), EncodingError>,
    ) -> Result<(), EncodingError> {
        let text = std::str::from_utf8(data)
            .map_err(|e| EncodingError::Failed(format!("toml payload is not utf-8: {e}")))?;
        let deserializer = toml::de::Deserializer::new(text);
        decode(&mut <dyn ErasedDeserializer>::erase(deserializer))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rushwind_encoding::{marshal, unmarshal};
    use serde::{Deserialize, Serialize};

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct Payload {
        seq: u32,
        note: String,
    }

    fn payload() -> Payload {
        Payload {
            seq: 7,
            note: "hello".into(),
        }
    }

    #[test]
    fn name_matches_go_constant() {
        assert_eq!(TomlCodec.name(), "toml");
    }

    #[test]
    fn round_trips_through_toml_text() {
        let codec = TomlCodec::codec();
        let bytes = marshal(&codec, &payload()).unwrap();
        let text = std::str::from_utf8(&bytes).unwrap();
        assert!(text.contains("seq = 7"), "toml table shape: {text:?}");
        let round: Payload = unmarshal(&codec, &bytes).unwrap();
        assert_eq!(round, payload());
    }

    #[test]
    fn garbage_input_fails() {
        let codec = TomlCodec::codec();
        let err = unmarshal::<Payload>(&codec, b"[[[not toml").unwrap_err();
        assert!(err.to_string().contains("toml decode"));
    }

    #[test]
    fn non_utf8_input_fails_cleanly() {
        let codec = TomlCodec::codec();
        let err = unmarshal::<Payload>(&codec, &[0xff, 0xfe]).unwrap_err();
        assert!(err.to_string().contains("utf-8"));
    }

    #[test]
    fn registers_into_the_shared_registry() {
        TomlCodec::register();
        let fetched = rushwind_encoding::get_codec(NAME).expect("registered");
        assert_eq!(fetched.name(), NAME);
    }
}
