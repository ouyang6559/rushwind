//! Protobuf engine for the RushWind encoding contract — the Go
//! `go-wind-plugins/encoding/proto` package rethought for Rust.
//!
//! The Go codec is a registry member that type-asserts
//! `v.(proto.Message)` at runtime and delegates to binary proto. Rust
//! has no runtime equivalent of that assertion — whether a type is a
//! protobuf message must be known at compile time — so binary proto
//! cannot hide behind the object-safe [`Codec`
//! trait](rushwind_encoding::Codec). This engine is therefore a typed
//! sidecar: not a registry member, but the same wire behavior, generic
//! over `prost::Message`.
//!
//! For protojson (the JSON mapping), use the `json` engine together
//! with prost-generated types that carry serde derives (as
//! `rushwind-storage-proto` builds them with pbjson).
//!
//! # Usage
//!
//! ```
//! use prost::Message;
//! use rushwind_encoding_proto::ProtoCodec;
//!
//! #[derive(Clone, PartialEq, prost::Message)]
//! struct Ping {
//!     #[prost(uint32, tag = "1")]
//!     seq: u32,
//! }
//!
//! let bytes = ProtoCodec::codec().marshal(&Ping { seq: 7 }).unwrap();
//! let round: Ping = ProtoCodec::codec().unmarshal(&bytes).unwrap();
//! assert_eq!(round.seq, 7);
//! ```

#![forbid(unsafe_code)]
#![deny(missing_docs)]

use prost::Message;
use rushwind_encoding::EncodingError;

/// The binary protobuf codec — the Rust stand-in for the Go `proto`
/// registry codec, generic over `prost::Message` instead of runtime
/// type assertions.
#[derive(Debug, Clone, Copy, Default)]
pub struct ProtoCodec;

impl ProtoCodec {
    /// Builds the codec.
    pub fn codec() -> Self {
        Self
    }

    /// Encodes a protobuf message to binary wire format — the Go
    /// `codec.Marshal(v)` path, with the message bound at compile time.
    pub fn marshal<M: Message>(&self, message: &M) -> Result<Vec<u8>, EncodingError> {
        Ok(message.encode_to_vec())
    }

    /// Decodes binary wire format into a protobuf message — the Go
    /// `codec.Unmarshal(data, v)` path.
    pub fn unmarshal<M: Message + Default>(&self, data: &[u8]) -> Result<M, EncodingError> {
        M::decode(data).map_err(|e| EncodingError::Failed(format!("proto decode: {e}")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Clone, PartialEq, Message)]
    struct Ping {
        #[prost(uint32, tag = "1")]
        seq: u32,
    }

    #[test]
    fn round_trips_through_binary_wire_format() {
        let codec = ProtoCodec::codec();
        let bytes = codec.marshal(&Ping { seq: 7 }).unwrap();
        assert_eq!(bytes, vec![0x08, 0x07], "field 1 varint: {bytes:?}");
        let round: Ping = codec.unmarshal(&bytes).unwrap();
        assert_eq!(round, Ping { seq: 7 });
    }

    #[test]
    fn garbage_input_fails() {
        let codec = ProtoCodec::codec();
        let err = codec
            .unmarshal::<Ping>(&[0xff, 0xff, 0xff, 0xff, 0xff])
            .unwrap_err();
        assert!(err.to_string().contains("proto decode"));
    }
}
