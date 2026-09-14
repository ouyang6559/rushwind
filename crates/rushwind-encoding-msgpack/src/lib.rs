//! MessagePack engine for the RushWind encoding contract — the Go
//! `go-wind-plugins/encoding/msgpack` package ported onto `rmp-serde`.
//!
//! Like the Go engine, a direct delegate: compact binary, any serde
//! value round-trips.
//!
//! # Usage
//!
//! ```no_run
//! rushwind_encoding_msgpack::register(); // once, at startup
//!
//! use rushwind_encoding::{get_codec, marshal};
//! let codec = get_codec("msgpack").unwrap();
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

/// The codec's registry name — the Go `msgpack.Name` constant.
pub const NAME: &str = "msgpack";

/// The MessagePack codec. State-free; construct one or fetch it from
/// the registry under [`NAME`].
#[derive(Debug, Clone, Copy, Default)]
pub struct MsgpackCodec;

impl MsgpackCodec {
    /// Builds the codec.
    pub fn codec() -> Self {
        Self
    }

    /// Installs the codec in the registry under [`NAME`]. Call once at
    /// startup — Go registers through package `init()` side effects;
    /// Rust registers explicitly.
    pub fn register() {
        rushwind_encoding::register_codec(Self).expect("register the msgpack codec");
    }
}

/// Installs [`MsgpackCodec`] in the shared registry under [`NAME`] — the
/// free-function form of [`MsgpackCodec::register`].
pub fn register() {
    MsgpackCodec::register();
}

impl Codec for MsgpackCodec {
    fn name(&self) -> &'static str {
        NAME
    }

    fn marshal(&self, value: &dyn ErasedSerialize) -> Result<Vec<u8>, EncodingError> {
        let mut buffer = Vec::new();
        value
            .erased_serialize(&mut <dyn ErasedSerializer>::erase(
                &mut rmp_serde::Serializer::new(&mut buffer),
            ))
            .map_err(|e| EncodingError::Failed(format!("msgpack encode: {e}")))?;
        Ok(buffer)
    }

    fn unmarshal<'de>(
        &self,
        data: &'de [u8],
        decode: &mut dyn FnMut(&mut dyn ErasedDeserializer<'de>) -> Result<(), EncodingError>,
    ) -> Result<(), EncodingError> {
        let mut deserializer = rmp_serde::Deserializer::from_read_ref(data);
        decode(&mut <dyn ErasedDeserializer>::erase(&mut deserializer))
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
        assert_eq!(MsgpackCodec.name(), "msgpack");
    }

    #[test]
    fn round_trips_through_compact_binary() {
        let codec = MsgpackCodec::codec();
        let bytes = marshal(&codec, &payload()).unwrap();
        assert!(
            bytes.iter().any(|b| !b.is_ascii()),
            "binary framing, not text"
        );
        let round: Payload = unmarshal(&codec, &bytes).unwrap();
        assert_eq!(round, payload());
    }

    #[test]
    fn garbage_input_fails() {
        let codec = MsgpackCodec::codec();
        let err = unmarshal::<Payload>(&codec, b"definitely not msgpack").unwrap_err();
        assert!(err.to_string().contains("msgpack decode"));
    }

    #[test]
    fn registers_into_the_shared_registry() {
        MsgpackCodec::register();
        let fetched = rushwind_encoding::get_codec(NAME).expect("registered");
        assert_eq!(fetched.name(), NAME);
    }
}
