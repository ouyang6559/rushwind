//! The protojson response codec — the reference's `EmitUnpopulated: true`
//! response encoding.
//!
//! Response serialization runs through the caller's descriptor pool's
//! dynamic message: the static message converts into a dynamic message,
//! which serializes with `skip_default_fields(false)` — the exact pairing
//! of the reference's `EmitUnpopulated` (unset scalars emit defaults,
//! unset singular message fields emit null, unset repeated/map emit [] /
//! {}, 64-bit integers emit as strings, enums emit value names — all per
//! protojson).
//!
//! Marshal failures surface as plain internal errors through the
//! envelope; messages the pool does not know are refused the same way.

use prost::Message;
use prost_reflect::{DescriptorPool, DynamicMessage, SerializeOptions};

use crate::envelope::{internal_error, StatusError};

/// Serializes a response message with EmitUnpopulated semantics. `pool`
/// must know `fq_name` — the caller's route table guarantees it for every
/// mounted route's output type.
pub fn serialize_response<T>(
    pool: &DescriptorPool,
    fq_name: &str,
    msg: &T,
) -> Result<Vec<u8>, StatusError>
where
    T: Message,
{
    let desc = pool.get_message_by_name(fq_name).ok_or_else(|| {
        internal_error(format!(
            "message {fq_name} not present in the descriptor pool"
        ))
    })?;
    let mut dyn_msg = DynamicMessage::new(desc);
    dyn_msg
        .transcode_from(msg)
        .map_err(|e| internal_error(format!("marshal {}", e)))?;
    let mut out = serde_json::Serializer::new(Vec::new());
    let opts = SerializeOptions::new().skip_default_fields(false);
    dyn_msg
        .serialize_with_options(&mut out, &opts)
        .map_err(|e| internal_error(format!("marshal {}", e)))?;
    Ok(out.into_inner())
}
