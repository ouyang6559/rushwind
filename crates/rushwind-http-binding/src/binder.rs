//! The form binder (binding-spec §2.2) — a faithful port of Kratos
//! `encoding/form/proto_decode.go`.
//!
//! Semantics preserved exactly:
//! * dotted nested paths; intermediate messages allocated on demand;
//! * field resolution by proto name OR json_name (both spellings);
//! * map keys via BOTH `map[k]` and `m.k` spellings (single-dot only),
//!   `field[]` suffix stripping for lists;
//! * silent skip: unknown fields, empty values;
//! * errors: multi-value on singular fields, oneof double-set, parse
//!   failures, non-whitelisted message leaves;
//! * leaf kinds: grouped scalars, enums by name-then-number, std base64
//!   bytes, and the well-known whitelist (Timestamp RFC3339Nano, Duration
//!   via a Go-ParseDuration port, wrappers reduced to scalars, FieldMask
//!   comma+snake normalization, Value string-wrapping, Struct via strict
//!   protojson — note Struct errors on unknown fields, unlike the body
//!   codec).
//!
//! `google.protobuf.Struct`'s `fields` special case (field resolved by number
//! when the name lookup fails) is carried over verbatim.

use std::collections::HashMap;

use prost_reflect::{
    DeserializeOptions, DynamicMessage, FieldDescriptor, Kind, MapKey, MessageDescriptor,
    ReflectMessage as _, Value,
};

use crate::envelope::StatusError;

const STRUCT_MESSAGE_FULLNAME: &str = "google.protobuf.Struct";
const STRUCT_FIELDS_FIELD_NUMBER: u32 = 1;

/// Binds form pairs (query or path variables) into a dynamic message.
///
/// Mirrors `form.DecodeValues(msg, values)`: every pair is fed through
/// `populate_field_values`; the first error aborts the whole binding.
pub fn bind_form(
    dyn_msg: &mut DynamicMessage,
    pairs: &[(String, Vec<String>)],
) -> Result<(), StatusError> {
    for (key, values) in pairs {
        let path: Vec<&str> = key.split('.').collect();
        populate_field_values(dyn_msg, &path, values).map_err(crate::envelope::codec_error)?;
    }
    Ok(())
}

fn populate_field_values(
    v: &mut DynamicMessage,
    field_path: &[&str],
    values: &[String],
) -> Result<(), String> {
    if field_path.is_empty() {
        return Err("no field path".into());
    }
    if values.is_empty() {
        return Err("no value provided".into());
    }

    // Only the root segment is inspected here; deeper segments are handled
    // by recursing into `populate_field_values` with the remainder of the
    // path (matching the Go codec's per-segment dispatch).
    let field_name = field_path[0];
    let Some(fd) = get_field_descriptor(&v.descriptor(), field_name) else {
        // Unknown field: silently ignored, whole key dropped.
        return Ok(());
    };
    if fd.is_map() && field_path.len() == 2 {
        return populate_map_field(v, &fd, &field_path.join("."), values);
    }
    if field_path.len() > 1 {
        let repeated = fd.is_list() || fd.is_map();
        let is_msg = matches!(fd.kind(), Kind::Message(_));
        if !is_msg || repeated {
            if fd.is_map() {
                // "post subfield": re-parse the MAP FIELD NAME segment.
                return populate_map_field(v, &fd, field_path[1], values);
            }
            return Err(format!("invalid path: {field_name:?} is not a message"));
        }
        // Descend: allocate the intermediate message on demand.
        let child = v.get_field_mut(&fd);
        let Value::Message(child_msg) = child else {
            return Err(format!("invalid path: {field_name:?} is not a message"));
        };
        return populate_field_values(child_msg, &field_path[1..], values);
    }

    // Oneof double-set check: any OTHER member already set → error.
    if let Some(of) = fd.containing_oneof() {
        for member in of.fields() {
            if member == fd {
                continue;
            }
            if v.has_field(&member) {
                return Err(format!("field already set for oneof {:?}", of.name()));
            }
        }
    }

    if fd.is_list() {
        return populate_repeated_field(v, &fd, values);
    }
    if fd.is_map() {
        return populate_map_field(v, &fd, &field_path.join("."), values);
    }
    if values.len() > 1 {
        return Err(format!(
            "too many values for field {:?}: {}",
            fd.name(),
            values.join(", ")
        ));
    }
    populate_field(v, &fd, &values[0])
}

fn populate_field(v: &mut DynamicMessage, fd: &FieldDescriptor, value: &str) -> Result<(), String> {
    if value.is_empty() {
        return Ok(());
    }
    let parsed = parse_field(fd, value)?;
    v.set_field(fd, parsed);
    Ok(())
}

fn populate_repeated_field(
    v: &mut DynamicMessage,
    fd: &FieldDescriptor,
    values: &[String],
) -> Result<(), String> {
    // Go appends to the (possibly already populated) list — repeated keys
    // across separate query parameters accumulate.
    let mut items = match v.get_field(fd).into_owned() {
        Value::List(l) => l,
        _ => Vec::with_capacity(values.len()),
    };
    for value in values {
        items.push(
            parse_field(fd, value).map_err(|e| format!("parsing list {:?}: {e}", fd.name()))?,
        );
    }
    v.set_field(fd, Value::List(items));
    Ok(())
}

fn populate_map_field(
    v: &mut DynamicMessage,
    fd: &FieldDescriptor,
    joined_path: &str,
    values: &[String],
) -> Result<(), String> {
    let Some((_, key_name)) = parse_url_query_map_key(joined_path) else {
        return Err("invalid formatting for map key".into());
    };
    // Map key/value descriptors live on the synthetic map-entry message.
    let entry = match fd.kind() {
        Kind::Message(md) => md,
        _ => return Err(format!("field {:?} is not a map", fd.name())),
    };
    let key_fd = entry
        .fields()
        .find(|f| f.name() == "key")
        .ok_or_else(|| format!("map {:?} has no key field", fd.name()))?;
    let value_fd = entry
        .fields()
        .find(|f| f.name() == "value")
        .ok_or_else(|| format!("map {:?} has no value field", fd.name()))?;
    let key = parse_field(&key_fd, &key_name)
        .map_err(|e| format!("parsing map key {:?}: {e}", fd.name()))?;
    let val = parse_field(&value_fd, &values[values.len() - 1])
        .map_err(|e| format!("parsing map value {:?}: {e}", fd.name()))?;
    let map_key = match key {
        Value::Bool(b) => MapKey::Bool(b),
        Value::I32(i) => MapKey::I32(i),
        Value::I64(i) => MapKey::I64(i),
        Value::U32(i) => MapKey::U32(i),
        Value::U64(i) => MapKey::U64(i),
        Value::String(s) => MapKey::String(s),
        _ => return Err(format!("parsing map key {:?}: invalid key type", fd.name())),
    };
    let map_value = val;
    // Merge into the existing map state for this field.
    let mut new_map = match v.get_field(fd).into_owned() {
        Value::Map(m) => m,
        _ => HashMap::new(),
    };
    new_map.insert(map_key, map_value);
    v.set_field(fd, Value::Map(new_map));
    Ok(())
}

fn get_field_descriptor(desc: &MessageDescriptor, field_name: &str) -> Option<FieldDescriptor> {
    let mut fd = desc
        .get_field_by_name(field_name)
        .or_else(|| desc.get_field_by_json_name(field_name));
    if fd.is_none() {
        if desc.full_name() == STRUCT_MESSAGE_FULLNAME {
            fd = desc.get_field(STRUCT_FIELDS_FIELD_NUMBER);
        } else if field_name.len() > 2 && field_name.ends_with("[]") {
            let stripped = &field_name[..field_name.len() - 2];
            fd = desc
                .get_field_by_name(stripped)
                .or_else(|| desc.get_field_by_json_name(stripped));
        } else if let Some((field, _)) = parse_url_query_map_key(field_name) {
            fd = desc
                .get_field_by_name(&field)
                .or_else(|| desc.get_field_by_json_name(&field));
        }
    }
    fd
}

/// Ports `parseURLQueryMapKey`: `map[key]` (bracket form) or `m.k` (single
/// dot form only). Returns (field, key) on success.
fn parse_url_query_map_key(key: &str) -> Option<(String, String)> {
    if let Some(start) = key.find('[') {
        let end = key.rfind(']')?;
        if start == 0 || end <= start || key.len() != end + 1 {
            return None;
        }
        return Some((key[..start].to_string(), key[start + 1..end].to_string()));
    }
    if key.matches('.').count() != 1 {
        return None;
    }
    let (m, k) = key.split_once('.')?;
    if m.is_empty() {
        return None;
    }
    Some((m.to_string(), k.to_string()))
}

/// Ports `parseField`: grouped scalar coercions, enums by name-then-number,
/// bytes via std base64, and the well-known message whitelist. Grouping
/// mirrors the codec's switch arms exactly.
fn parse_field(fd: &FieldDescriptor, value: &str) -> Result<Value, String> {
    match fd.kind() {
        Kind::Bool => go_parse_bool(value).map(Value::Bool),
        Kind::Enum(ed) => {
            let v = ed
                .get_value_by_name(value)
                .or_else(|| value.parse::<i32>().ok().and_then(|n| ed.get_value(n)))
                .ok_or_else(|| format!("{value:?} is not a valid value"))?;
            Ok(Value::EnumNumber(v.number()))
        }
        Kind::Int32 | Kind::Sint32 | Kind::Sfixed32 => value
            .parse::<i32>()
            .map(Value::I32)
            .map_err(|e| e.to_string()),
        Kind::Int64 | Kind::Sint64 | Kind::Sfixed64 => value
            .parse::<i64>()
            .map(Value::I64)
            .map_err(|e| e.to_string()),
        Kind::Uint32 | Kind::Fixed32 => value
            .parse::<u32>()
            .map(Value::U32)
            .map_err(|e| e.to_string()),
        Kind::Uint64 | Kind::Fixed64 => value
            .parse::<u64>()
            .map(Value::U64)
            .map_err(|e| e.to_string()),
        Kind::Float => value
            .parse::<f32>()
            .map(Value::F32)
            .map_err(|e| e.to_string()),
        Kind::Double => value
            .parse::<f64>()
            .map(Value::F64)
            .map_err(|e| e.to_string()),
        Kind::String => Ok(Value::String(value.to_string())),
        Kind::Bytes => {
            use prost::bytes::Bytes;
            let decoded = base64_std_decode(value)?;
            Ok(Value::Bytes(Bytes::from(decoded)))
        }
        Kind::Message(md) => parse_message(&md, value),
    }
}

/// std-alphabet base64 decode with canonical padding (Go StdEncoding: the
/// base64 crate's STANDARD engine rejects missing padding exactly like Go).
fn base64_std_decode(v: &str) -> Result<Vec<u8>, String> {
    use base64::Engine as _;
    base64::engine::general_purpose::STANDARD
        .decode(v)
        .map_err(|e| e.to_string())
}

/// Ports `jsonSnakeCase` (proto_decode.go): camelCase → snake_case per the
/// protobuf JSON field-name normalization.
fn json_snake_case(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        if c.is_ascii_uppercase() {
            out.push('_');
            out.push(c.to_ascii_lowercase());
        } else {
            out.push(c);
        }
    }
    out
}

/// Ports `parseMessage`: the well-known whitelist. Everything outside the
/// whitelist is `unsupported message type` — matching the codec exactly.
fn parse_message(md: &MessageDescriptor, value: &str) -> Result<Value, String> {
    match md.full_name() {
        "google.protobuf.Timestamp" => {
            // Go: time.ParseInLocation(time.RFC3339Nano, v, time.Local) —
            // the offset in the text decides the instant; the "local"
            // timezone only matters for inputs without an offset, which
            // RFC3339 requires anyway.
            let t = chrono::DateTime::parse_from_rfc3339(value).map_err(|e| e.to_string())?;
            let dur = t.signed_duration_since(chrono::DateTime::UNIX_EPOCH);
            let mut m = DynamicMessage::new(md.clone());
            if let Some(fd) = m.descriptor().get_field_by_name("seconds") {
                m.set_field(&fd, Value::I64(dur.num_seconds()));
            }
            if let Some(fd) = m.descriptor().get_field_by_name("nanos") {
                m.set_field(&fd, Value::I32(dur.subsec_nanos()));
            }
            Ok(Value::Message(m))
        }
        "google.protobuf.Duration" => {
            let (secs, nanos) = go_parse_duration(value)?;
            let mut m = DynamicMessage::new(md.clone());
            if let Some(fd) = m.descriptor().get_field_by_name("seconds") {
                m.set_field(&fd, Value::I64(secs));
            }
            if let Some(fd) = m.descriptor().get_field_by_name("nanos") {
                m.set_field(&fd, Value::I32(nanos))
            }
            Ok(Value::Message(m))
        }
        "google.protobuf.DoubleValue"
        | "google.protobuf.FloatValue"
        | "google.protobuf.Int64Value"
        | "google.protobuf.Int32Value"
        | "google.protobuf.UInt64Value"
        | "google.protobuf.UInt32Value"
        | "google.protobuf.BoolValue"
        | "google.protobuf.StringValue"
        | "google.protobuf.BytesValue" => {
            // Wrappers reduce to their wrapped scalar: the wrapper's value
            // field is parsed with the same scalar semantics.
            let value_fd = md
                .get_field_by_name("value")
                .ok_or_else(|| "wrapper without value field".to_string())?;
            let parsed = parse_field(&value_fd, value)?;
            let mut m = DynamicMessage::new(md.clone());
            m.set_field(&value_fd, parsed);
            Ok(Value::Message(m))
        }
        "google.protobuf.FieldMask" => {
            // Comma-separated paths, each snake_cased per protojson.
            let paths: Vec<Value> = value
                .split(',')
                .map(|p| Value::String(json_snake_case(p)))
                .collect();
            let mut m = DynamicMessage::new(md.clone());
            if let Some(fd) = m.descriptor().get_field_by_name("paths") {
                m.set_field(&fd, Value::List(paths));
            }
            Ok(Value::Message(m))
        }
        "google.protobuf.Value" => {
            // structpb.NewValue(string): the raw string wrapped as a
            // string-valued Value.
            let mut m = DynamicMessage::new(md.clone());
            if let Some(fd) = m.descriptor().get_field_by_name("string_value") {
                m.set_field(&fd, Value::String(value.to_string()));
            }
            Ok(Value::Message(m))
        }
        "google.protobuf.Struct" => {
            // protojson.Unmarshal with DEFAULT options — unlike the body
            // codec, unknown fields here are ERRORS.
            let mut de = serde_json::Deserializer::from_str(value);
            let opts = DeserializeOptions::new().deny_unknown_fields(true);
            let m = DynamicMessage::deserialize_with_options(md.clone(), &mut de, &opts)
                .map_err(|e| e.to_string())?;
            de.end().map_err(|e| e.to_string())?;
            Ok(Value::Message(m))
        }
        other => Err(format!("unsupported message type: {other:?}")),
    }
}

/// Ports Go `time.ParseDuration` (time/format.go): optional sign, then one
/// or more [number][unit] pairs; fractional values allowed, exponents not;
/// units ns/us/µs/μs/ms/s/m/h; overflow beyond ~292 years is an error.
fn go_parse_duration(v: &str) -> Result<(i64, i32), String> {
    let err = || "time: invalid duration".to_string();
    let mut s = v;
    let mut neg = false;
    if let Some(rest) = s.strip_prefix('-') {
        neg = true;
        s = rest;
    } else if let Some(rest) = s.strip_prefix('+') {
        s = rest;
    }
    if s == "0" {
        return Ok((0, 0));
    }
    if s.is_empty() {
        return Err(err());
    }
    let mut total_nanos: u64 = 0;
    let mut saw_any = false;
    let bytes = s.as_bytes();
    let mut i = 0usize;
    while i < bytes.len() {
        let start = i;
        while i < bytes.len() && bytes[i].is_ascii_digit() {
            i += 1;
        }
        let num_text = &s[start..i];
        let frac_text = if i < bytes.len() && bytes[i] == b'.' {
            let frac_start = i;
            i += 1;
            while i < bytes.len() && bytes[i].is_ascii_digit() {
                i += 1;
            }
            if i == frac_start + 1 {
                return Err(err());
            }
            Some(&s[frac_start..i])
        } else {
            None
        };
        if num_text.is_empty() && frac_text.is_none() {
            return Err(err());
        }
        let unit_start = i;
        while i < bytes.len() && !bytes[i].is_ascii_digit() && bytes[i] != b'.' {
            i += 1;
        }
        let unit_text = &s[unit_start..i];
        let unit_nanos: u64 = match unit_text {
            "ns" => 1,
            "us" | "\u{00b5}s" | "\u{03bc}s" => 1_000,
            "ms" => 1_000_000,
            "s" => 1_000_000_000,
            "m" => 60_000_000_000,
            "h" => 3_600_000_000_000,
            _ => return Err(err()),
        };
        let int_val: u64 = if num_text.is_empty() {
            0
        } else {
            num_text.parse::<u64>().map_err(|_| err())?
        };
        let frac_val: u64 = if let Some(f) = frac_text {
            let digits = f.trim_start_matches('.');
            let mut scaled: u64 = 0;
            let mut scale = unit_nanos / 10;
            for c in digits.chars() {
                let d = c.to_digit(10).ok_or_else(err)?;
                scaled += (d as u64).checked_mul(scale).ok_or_else(err)?;
                scale /= 10;
                if scale == 0 {
                    break;
                }
            }
            scaled
        } else {
            0
        };
        let add_int = int_val.checked_mul(unit_nanos).ok_or_else(err)?;
        let add_frac = frac_val.min(unit_nanos);
        total_nanos = total_nanos
            .checked_add(add_int)
            .and_then(|x| x.checked_add(add_frac))
            .ok_or_else(err)?;
        saw_any = true;
    }
    if !saw_any {
        return Err(err());
    }
    // Go's overflow bound: math.MaxInt64 nanoseconds (~292 years).
    const MAX: u64 = 0x7fff_ffff_ffff_ffff;
    if total_nanos > MAX {
        return Err(err());
    }
    let mut secs = (total_nanos / 1_000_000_000) as i64;
    let mut nanos = (total_nanos % 1_000_000_000) as i32;
    if neg {
        secs = -secs;
        nanos = -nanos;
    }
    Ok((secs, nanos))
}

/// Ports `strconv.ParseBool`: the exact Go spelling set.
fn go_parse_bool(v: &str) -> Result<bool, String> {
    match v {
        "1" | "t" | "T" | "true" | "TRUE" | "True" => Ok(true),
        "0" | "f" | "F" | "false" | "FALSE" | "False" => Ok(false),
        _ => Err(format!("strconv.ParseBool: parsing {v:?}: invalid syntax")),
    }
}
