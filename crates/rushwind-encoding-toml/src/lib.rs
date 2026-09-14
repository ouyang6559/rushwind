//! TOML engine for the RushWind encoding contract — the Go
//! `go-wind-plugins/encoding/toml` package ported onto the `toml`
//! crate.
//!
//! One structural divergence from the plain-delegate Go engines: the
//! `toml` crate exposes no writer-backed `serde::Serializer` for whole
//! documents, and the erased contract needs one. Marshal therefore
//! routes the value through `serde_json`'s in-memory value model
//! first, then renders TOML from it. For TOML-expressible data the
//! result is identical to a direct serialization — the caveats are
//! TOML's own: no binary payloads, integers are i64, and values that
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

/// The codec's registry name — the Go `toml.Name` constant.
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
    /// startup — Go registers through package `init()` side effects;
    /// Rust registers explicitly.
    pub fn register() {
        rushwind_encoding::register_codec(Self).expect("register the toml codec");
    }
}

/// Installs [`TomlCodec`] in the shared registry under [`NAME`] — the
/// free-function form of [`TomlCodec::register`].
pub fn register() {
    TomlCodec::register();
}

impl Codec for TomlCodec {
    fn name(&self) -> &'static str {
        NAME
    }

    fn marshal(&self, value: &dyn ErasedSerialize) -> Result<Vec<u8>, EncodingError> {
        // Through the JSON value model (see the crate docs): render the
        // value to JSON text, re-read it as an in-memory value, then let
        // the toml crate render real TOML.
        let mut json = Vec::new();
        value
            .erased_serialize(&mut <dyn ErasedSerializer>::erase(
                &mut serde_json::Serializer::new(&mut json),
            ))
            .map_err(|e| EncodingError::Failed(format!("toml encode: {e}")))?;
        let value: serde_json::Value = serde_json::from_slice(&json)
            .map_err(|e| EncodingError::Failed(format!("toml encode bridge: {e}")))?;
        toml::to_string(&value)
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
