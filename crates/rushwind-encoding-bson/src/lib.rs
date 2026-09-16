//! BSON engine for the RushWind encoding contract, over the `bson`
//! crate.
//!
//! Unlike the plain-delegate engines in this workspace: the
//! `bson` crate exposes no writer-backed `serde::Serializer` for whole
//! documents, and the erased contract needs one. Marshal therefore
//! routes the value through `serde_json`'s in-memory value model
//! first, then translates that tree into a BSON document element by
//! element ([`json_value_to_document`]) — by hand, not through serde,
//! whose number serialization changes shape under the
//! `arbitrary_precision` feature the workspace's dependency graph
//! enables on serde_json. The caveats are the bridge's:
//! BSON's binary and datetime scalars are not reachable through this
//! surface (they arrive as strings), and `u64` values above `i64::MAX`
//! fail with an encode error.
//!
//! # Usage
//!
//! ```no_run
//! rushwind_encoding_bson::register(); // once, at startup
//!
//! use rushwind_encoding::{get_codec, marshal};
//! let codec = get_codec("bson").unwrap();
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
pub const NAME: &str = "bson";

/// The BSON codec. State-free; construct one or fetch it from the
/// registry under [`NAME`].
#[derive(Debug, Clone, Copy, Default)]
pub struct BsonCodec;

impl BsonCodec {
    /// Builds the codec.
    pub fn codec() -> Self {
        Self
    }

    /// Installs the codec in the registry under [`NAME`]. Call once at
    /// startup — registration is explicit, not import-time.
    pub fn register() {
        rushwind_encoding::register_codec(Self).expect("register the bson codec");
    }
}

/// Installs [`BsonCodec`] in the shared registry under [`NAME`] — the
/// free-function form of [`BsonCodec::register`].
pub fn register() {
    BsonCodec::register();
}

/// Translates the bridge's JSON value tree into a BSON document — the
/// top level must be an object. Any untranslatable element anywhere in
/// the tree fails the whole translation (see [`json_value_to_bson`]).
fn json_value_to_document(value: &serde_json::Value) -> Option<bson::Document> {
    let object = match value {
        serde_json::Value::Object(object) => object,
        _ => return None,
    };
    let mut document = bson::Document::new();
    for (key, element) in object {
        let bson_value = json_value_to_bson(element)?;
        document.insert(key.clone(), bson_value);
    }
    Some(document)
}

/// One JSON value to one BSON value, by shape: `null`, booleans, and
/// strings map to their BSON scalars; integers within `i32` range
/// become `Int32` and other `i64`s `Int64`; `u64`s above `i64::MAX`
/// have no BSON shape and fail the translation; floats become
/// `Double`; arrays and objects recurse. The translation is
/// shape-driven by hand on purpose — through serde, the
/// `arbitrary_precision` feature (enabled on serde_json by the
/// workspace's dependency graph) would land every integer as a tagged
/// subdocument instead of a BSON integer.
fn json_value_to_bson(value: &serde_json::Value) -> Option<bson::Bson> {
    Some(match value {
        serde_json::Value::Null => bson::Bson::Null,
        serde_json::Value::Bool(b) => bson::Bson::Boolean(*b),
        serde_json::Value::Number(number) => {
            if let Some(i) = number.as_i64() {
                match i32::try_from(i) {
                    Ok(small) => bson::Bson::Int32(small),
                    Err(_) => bson::Bson::Int64(i),
                }
            } else if number.is_u64() {
                return None;
            } else if let Some(f) = number.as_f64() {
                bson::Bson::Double(f)
            } else {
                return None;
            }
        }
        serde_json::Value::String(s) => bson::Bson::String(s.clone()),
        serde_json::Value::Array(array) => {
            let mut bson_array = Vec::new();
            for element in array {
                bson_array.push(json_value_to_bson(element)?);
            }
            bson::Bson::Array(bson_array)
        }
        serde_json::Value::Object(object) => {
            let mut document = bson::Document::new();
            for (key, element) in object {
                let bson_value = json_value_to_bson(element)?;
                document.insert(key.clone(), bson_value);
            }
            bson::Bson::Document(document)
        }
    })
}

impl Codec for BsonCodec {
    fn name(&self) -> &'static str {
        NAME
    }

    fn marshal(&self, value: &dyn ErasedSerialize) -> Result<Vec<u8>, EncodingError> {
        // Through the JSON value model (see the crate docs): render the
        // value to JSON text, re-read it as an in-memory value, then
        // translate that value tree into a BSON document element by
        // element. The translation walks the tree by hand rather than
        // through serde: under the `arbitrary_precision` feature —
        // which the workspace's dependency graph enables on serde_json
        // regardless of this crate's own features — serde's Number
        // serialization changes shape and would land every integer as a
        // tagged subdocument instead of a BSON integer.
        let mut json = Vec::new();
        value
            .erased_serialize(&mut <dyn ErasedSerializer>::erase(
                &mut serde_json::Serializer::new(&mut json),
            ))
            .map_err(|e| EncodingError::Failed(format!("bson encode: {e}")))?;
        let value: serde_json::Value = serde_json::from_slice(&json)
            .map_err(|e| EncodingError::Failed(format!("bson encode bridge: {e}")))?;
        let document = json_value_to_document(&value)
            .ok_or_else(|| EncodingError::Failed("bson encode: not a document".to_string()))?;
        document
            .to_vec()
            .map_err(|e| EncodingError::Failed(format!("bson encode: {e}")))
    }

    fn unmarshal<'de>(
        &self,
        data: &'de [u8],
        decode: &mut dyn FnMut(&mut dyn ErasedDeserializer<'de>) -> Result<(), EncodingError>,
    ) -> Result<(), EncodingError> {
        // The raw slice-backed deserializer, erased directly.
        let deserializer = bson::RawDeserializer::new(data)
            .map_err(|e| EncodingError::Failed(format!("bson parse: {e}")))?;
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
        assert_eq!(BsonCodec.name(), "bson");
    }

    #[test]
    fn round_trips_through_bson_binary() {
        let codec = BsonCodec::codec();
        let bytes = marshal(&codec, &payload()).unwrap();
        // BSON documents start with their total length as a little-endian i32.
        let declared = i32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
        assert_eq!(declared as usize, bytes.len(), "bson length prefix");
        let round: Payload = unmarshal(&codec, &bytes).unwrap();
        assert_eq!(round, payload());
    }

    #[test]
    fn garbage_input_fails() {
        let codec = BsonCodec::codec();
        let err = unmarshal::<Payload>(&codec, b"definitely not bson").unwrap_err();
        assert!(err.to_string().contains("bson parse"));
    }

    #[test]
    fn registers_into_the_shared_registry() {
        BsonCodec::register();
        let fetched = rushwind_encoding::get_codec(NAME).expect("registered");
        assert_eq!(fetched.name(), NAME);
    }
}
