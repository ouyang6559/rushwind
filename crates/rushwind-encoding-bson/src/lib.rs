//! BSON engine for the RushWind encoding contract — the Go
//! `go-wind-plugins/encoding/bson` package ported onto the `bson`
//! crate.
//!
//! One structural divergence from the plain-delegate Go engines: the
//! `bson` crate exposes no writer-backed `serde::Serializer` for whole
//! documents, and the erased contract needs one. Marshal therefore
//! routes the value through `serde_json`'s in-memory value model
//! first, then encodes BSON from it. The caveats are the bridge's:
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

/// The codec's registry name — the Go `bson.Name` constant.
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
    /// startup — Go registers through package `init()` side effects;
    /// Rust registers explicitly.
    pub fn register() {
        rushwind_encoding::register_codec(Self).expect("register the bson codec");
    }
}

/// Installs [`BsonCodec`] in the shared registry under [`NAME`] — the
/// free-function form of [`BsonCodec::register`].
pub fn register() {
    BsonCodec::register();
}

impl Codec for BsonCodec {
    fn name(&self) -> &'static str {
        NAME
    }

    fn marshal(&self, value: &dyn ErasedSerialize) -> Result<Vec<u8>, EncodingError> {
        // Through the JSON value model (see the crate docs): render the
        // value to JSON text, re-read it as an in-memory value, then let
        // the bson crate encode real BSON.
        let mut json = Vec::new();
        value
            .erased_serialize(&mut <dyn ErasedSerializer>::erase(
                &mut serde_json::Serializer::new(&mut json),
            ))
            .map_err(|e| EncodingError::Failed(format!("bson encode: {e}")))?;
        let value: serde_json::Value = serde_json::from_slice(&json)
            .map_err(|e| EncodingError::Failed(format!("bson encode bridge: {e}")))?;
        bson::to_vec(&value).map_err(|e| EncodingError::Failed(format!("bson encode: {e}")))
    }

    fn unmarshal<'de>(
        &self,
        data: &'de [u8],
        decode: &mut dyn FnMut(&mut dyn ErasedDeserializer<'de>) -> Result<(), EncodingError>,
    ) -> Result<(), EncodingError> {
        // Parse eagerly to a `Bson` value, then deserialize the target
        // out of it: the bson crate has no public slice-backed
        // serde::Deserializer to erase directly.
        let document: bson::Bson = bson::from_slice(data)
            .map_err(|e| EncodingError::Failed(format!("bson parse: {e}")))?;
        let deserializer = bson::Deserializer::new(document);
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
