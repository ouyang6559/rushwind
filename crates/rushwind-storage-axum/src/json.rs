//! JSON ↔ [`Record`] conversions, schema-aware on the write face.

use rushwind_storage::{Record, Schema, StorageError, Value};

/// Converts a write payload into a [`Record`].
///
/// The body must be a JSON object; every key must be a declared column and
/// every value must fit the column's declared kind — unknown columns and
/// kind mismatches are [`StorageError::InvalidQuery`], so malformed writes
/// are rejected before they reach the engine.
pub fn record_from_json(
    value: &serde_json::Value,
    schema: &Schema,
) -> Result<Record, StorageError> {
    let Some(map) = value.as_object() else {
        return Err(StorageError::InvalidQuery(
            "write body must be a JSON object".into(),
        ));
    };
    let mut record = Record::new();
    for (key, raw) in map {
        let column = schema.column(key).ok_or_else(|| {
            StorageError::InvalidQuery(format!("unknown column {key:?} in write payload"))
        })?;
        let converted = match column.kind {
            rushwind_storage::ColumnKind::Bool => match raw {
                serde_json::Value::Bool(b) => Value::Bool(*b),
                serde_json::Value::Null => Value::Null,
                other => return Err(kind_error(key, "a boolean", other)),
            },
            rushwind_storage::ColumnKind::Int => match raw {
                serde_json::Value::Number(n) => Value::Int(n.as_i64().ok_or_else(|| {
                    StorageError::InvalidQuery(format!("column {key:?} needs an in-range integer"))
                })?),
                serde_json::Value::Null => Value::Null,
                other => return Err(kind_error(key, "an integer", other)),
            },
            rushwind_storage::ColumnKind::Real => match raw {
                serde_json::Value::Number(n) => Value::Real(n.as_f64().ok_or_else(|| {
                    StorageError::InvalidQuery(format!("column {key:?} needs a finite number"))
                })?),
                serde_json::Value::Null => Value::Null,
                other => return Err(kind_error(key, "a number", other)),
            },
            rushwind_storage::ColumnKind::Text => match raw {
                serde_json::Value::String(s) => Value::Text(s.clone()),
                serde_json::Value::Null => Value::Null,
                other => return Err(kind_error(key, "a string", other)),
            },
        };
        record.insert(key.as_str(), converted);
    }
    Ok(record)
}

/// Renders a stored row as a JSON object, kinds driving the JSON types.
pub fn record_to_json(record: &Record) -> serde_json::Value {
    let mut map = serde_json::Map::new();
    for (field, value) in record.iter() {
        let json = match value {
            Value::Null => serde_json::Value::Null,
            Value::Bool(b) => serde_json::Value::Bool(*b),
            Value::Int(i) => serde_json::Value::Number((*i).into()),
            Value::Real(f) => serde_json::Number::from_f64(*f)
                .map(serde_json::Value::Number)
                .unwrap_or(serde_json::Value::Null),
            Value::Text(s) => serde_json::Value::String(s.clone()),
        };
        map.insert(field.to_owned(), json);
    }
    serde_json::Value::Object(map)
}

fn kind_error(column: &str, expected: &str, got: &serde_json::Value) -> StorageError {
    StorageError::InvalidQuery(format!(
        "column {column:?} expects {expected}, got {}",
        json_type_name(got)
    ))
}

fn json_type_name(value: &serde_json::Value) -> &'static str {
    match value {
        serde_json::Value::Null => "null",
        serde_json::Value::Bool(_) => "a boolean",
        serde_json::Value::Number(_) => "a number",
        serde_json::Value::String(_) => "a string",
        serde_json::Value::Array(_) => "an array",
        serde_json::Value::Object(_) => "an object",
    }
}
