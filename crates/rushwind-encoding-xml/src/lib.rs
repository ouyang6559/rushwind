//! XML engine for the RushWind encoding contract, over `quick-xml`.
//!
//! Unlike document-style XML mappers, `quick-xml`'s
//! serde support writes fields without a wrapper element. Marshal
//! therefore wraps the value in a `<value>` root element to keep the
//! output well-formed; unmarshal accepts any well-formed XML whose
//! root the target type can ignore or consume — serde mappings
//! typically see the root's children as the struct's fields.
//!
//! Caveats that belong to XML itself: maps and structs round-trip as
//! element trees, but scalars at the top level, `None`-valued options
//! (elements are either present or absent) and unit variants have no
//! natural element shape.
//!
//! # Usage
//!
//! ```no_run
//! rushwind_encoding_xml::register(); // once, at startup
//!
//! use rushwind_encoding::{get_codec, marshal};
//! let codec = get_codec("xml").unwrap();
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
pub const NAME: &str = "xml";

/// The element name marshal wraps values in so the output is a
/// well-formed XML document.
const ROOT_ELEMENT: &str = "value";

/// The XML codec. State-free; construct one or fetch it from the
/// registry under [`NAME`].
#[derive(Debug, Clone, Copy, Default)]
pub struct XmlCodec;

impl XmlCodec {
    /// Builds the codec.
    pub fn codec() -> Self {
        Self
    }

    /// Installs the codec in the registry under [`NAME`]. Call once at
    /// startup — registration is explicit, not import-time.
    pub fn register() {
        rushwind_encoding::register_codec(Self).expect("register the xml codec");
    }
}

/// Installs [`XmlCodec`] in the shared registry under [`NAME`] — the
/// free-function form of [`XmlCodec::register`].
pub fn register() {
    XmlCodec::register();
}

impl Codec for XmlCodec {
    fn name(&self) -> &'static str {
        NAME
    }

    fn marshal(&self, value: &dyn ErasedSerialize) -> Result<Vec<u8>, EncodingError> {
        let mut buffer = String::new();
        let serializer = quick_xml::se::Serializer::with_root(&mut buffer, Some(ROOT_ELEMENT))
            .map_err(|e| EncodingError::Failed(format!("xml root: {e}")))?;
        value
            .erased_serialize(&mut <dyn ErasedSerializer>::erase(serializer))
            .map_err(|e| EncodingError::Failed(format!("xml encode: {e}")))?;
        Ok(buffer.into_bytes())
    }

    fn unmarshal<'de>(
        &self,
        data: &'de [u8],
        decode: &mut dyn FnMut(&mut dyn ErasedDeserializer<'de>) -> Result<(), EncodingError>,
    ) -> Result<(), EncodingError> {
        let text = std::str::from_utf8(data)
            .map_err(|e| EncodingError::Failed(format!("xml payload is not utf-8: {e}")))?;
        let mut deserializer = quick_xml::de::Deserializer::from_str(text);
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
        assert_eq!(XmlCodec.name(), "xml");
    }

    #[test]
    fn round_trips_through_well_formed_xml() {
        let codec = XmlCodec::codec();
        let bytes = marshal(&codec, &payload()).unwrap();
        let text = std::str::from_utf8(&bytes).unwrap();
        assert!(
            text.starts_with("<value>") && text.ends_with("</value>"),
            "wrapped root element: {text:?}"
        );
        let round: Payload = unmarshal(&codec, &bytes).unwrap();
        assert_eq!(round, payload());
    }

    #[test]
    fn garbage_input_fails() {
        let codec = XmlCodec::codec();
        let err = unmarshal::<Payload>(&codec, b"<unclosed").unwrap_err();
        assert!(err.to_string().contains("xml"));
    }

    #[test]
    fn non_utf8_input_fails_cleanly() {
        let codec = XmlCodec::codec();
        let err = unmarshal::<Payload>(&codec, &[0xff, 0xfe]).unwrap_err();
        assert!(err.to_string().contains("utf-8"));
    }

    #[test]
    fn registers_into_the_shared_registry() {
        XmlCodec::register();
        let fetched = rushwind_encoding::get_codec(NAME).expect("registered");
        assert_eq!(fetched.name(), NAME);
    }
}
