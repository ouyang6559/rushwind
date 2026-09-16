//! CBOR engine for the RushWind encoding contract (RFC 8949), over
//! `ciborium`.
//!
//! Unlike the plain-delegate engines in this workspace: the
//! `ciborium` crate keeps its serde serializer types private, and the
//! erased contract needs one. Both directions therefore route through
//! `serde_json`'s in-memory value model. The caveats are the bridge's:
//! CBOR byte strings and tagged values are not reachable through this
//! surface, and `u64` values above `i64::MAX` fail with an encode
//! error. For byte-exact CBOR work, use `ciborium` directly.
//!
//! # Usage
//!
//! ```no_run
//! rushwind_encoding_cbor::register(); // once, at startup
//!
//! use rushwind_encoding::{get_codec, marshal};
//! let codec = get_codec("cbor").unwrap();
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
pub const NAME: &str = "cbor";

/// The CBOR codec. State-free; construct one or fetch it from the
/// registry under [`NAME`].
#[derive(Debug, Clone, Copy, Default)]
pub struct CborCodec;

impl CborCodec {
    /// Builds the codec.
    pub fn codec() -> Self {
        Self
    }

    /// Installs the codec in the registry under [`NAME`]. Call once at
    /// startup — registration is explicit, not import-time.
    pub fn register() {
        rushwind_encoding::register_codec(Self).expect("register the cbor codec");
    }
}

/// Installs [`CborCodec`] in the shared registry under [`NAME`] — the
/// free-function form of [`CborCodec::register`].
pub fn register() {
    CborCodec::register();
}

impl Codec for CborCodec {
    fn name(&self) -> &'static str {
        NAME
    }

    fn marshal(&self, value: &dyn ErasedSerialize) -> Result<Vec<u8>, EncodingError> {
        // Through the JSON value model (see the crate docs): render the
        // value to JSON text, re-read it as an in-memory value, then let
        // ciborium encode real CBOR.
        let mut json = Vec::new();
        value
            .erased_serialize(&mut <dyn ErasedSerializer>::erase(
                &mut serde_json::Serializer::new(&mut json),
            ))
            .map_err(|e| EncodingError::Failed(format!("cbor encode: {e}")))?;
        let value: serde_json::Value = serde_json::from_slice(&json)
            .map_err(|e| EncodingError::Failed(format!("cbor encode bridge: {e}")))?;
        let mut buffer = Vec::new();
        ciborium::into_writer(&value, &mut buffer)
            .map_err(|e| EncodingError::Failed(format!("cbor encode: {e}")))?;
        Ok(buffer)
    }

    fn unmarshal<'de>(
        &self,
        data: &'de [u8],
        decode: &mut dyn FnMut(&mut dyn ErasedDeserializer<'de>) -> Result<(), EncodingError>,
    ) -> Result<(), EncodingError> {
        // Parse eagerly to an in-memory value (the ciborium serializer
        // types are private), then let `decode` read the target out of
        // it.
        let value: serde_json::Value = ciborium::from_reader(data)
            .map_err(|e| EncodingError::Failed(format!("cbor parse: {e}")))?;
        let mut deserializer = <dyn ErasedDeserializer>::erase(value);
        decode(&mut deserializer)
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
        assert_eq!(CborCodec.name(), "cbor");
    }

    #[test]
    fn round_trips_through_cbor_binary() {
        let codec = CborCodec::codec();
        let bytes = marshal(&codec, &payload()).unwrap();
        assert!(!bytes.starts_with(b"{"), "binary framing, not JSON text");
        let round: Payload = unmarshal(&codec, &bytes).unwrap();
        assert_eq!(round, payload());
    }

    #[test]
    fn garbage_input_fails() {
        let codec = CborCodec::codec();
        let err = unmarshal::<Payload>(&codec, b"definitely not cbor").unwrap_err();
        assert!(err.to_string().contains("cbor decode"));
    }

    #[test]
    fn registers_into_the_shared_registry() {
        CborCodec::register();
        let fetched = rushwind_encoding::get_codec(NAME).expect("registered");
        assert_eq!(fetched.name(), NAME);
    }
}
