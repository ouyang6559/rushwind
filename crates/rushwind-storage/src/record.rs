//! Dynamic row representation.

use std::collections::BTreeMap;

use crate::error::StorageError;
use crate::value::Value;

/// One row, keyed by column name.
///
/// Repositories speak in [`Record`]s rather than concrete structs: the same
/// row shape flows through every engine, and typed callers translate at
/// their own boundary (a derive-macro layer can do this mechanically later;
/// go-crud gets the equivalent for free from proto reflection).
///
/// Column iteration is deterministic (`BTreeMap` ordering) so tests and
/// audits are reproducible.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Record(BTreeMap<String, Value>);

impl Record {
    /// Creates an empty record.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets one field, builder style.
    pub fn set(mut self, name: impl Into<String>, value: impl Into<Value>) -> Self {
        self.0.insert(name.into(), value.into());
        self
    }

    /// Sets one field in place.
    pub fn insert(&mut self, name: impl Into<String>, value: impl Into<Value>) {
        self.0.insert(name.into(), value.into());
    }

    /// Reads one field.
    pub fn get(&self, name: &str) -> Option<&Value> {
        self.0.get(name)
    }

    /// Removes one field, returning its previous value.
    pub fn remove(&mut self, name: &str) -> Option<Value> {
        self.0.remove(name)
    }

    /// Retains only the named fields (the projection semantics of a field
    /// mask).
    pub fn retain(&mut self, keep: impl Fn(&str) -> bool) {
        self.0.retain(|name, _| keep(name));
    }

    /// The column names currently present, in deterministic order.
    pub fn columns(&self) -> impl Iterator<Item = &str> {
        self.0.keys().map(String::as_str)
    }

    /// Number of fields present.
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Returns `true` when no field is present.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Iterates the fields in deterministic order.
    pub fn iter(&self) -> impl Iterator<Item = (&str, &Value)> {
        self.0.iter().map(|(k, v)| (k.as_str(), v))
    }
}

/// Structs that convert themselves into a [`Record`] — the write face of
/// the mapper pair (the derive macro in `rushwind-storage-macros`
/// implements this).
pub trait ToRecord {
    /// The record view of `self`.
    fn to_record(&self) -> Record;
}

/// Structs constructible from a [`Record`] — the read face of the mapper
/// pair. Unknown or wrongly-typed fields are [`StorageError::InvalidQuery`].
pub trait FromRecord: Sized {
    /// Builds `Self` from a record view.
    fn from_record(record: &Record) -> Result<Self, StorageError>;
}

impl FromIterator<(String, Value)> for Record {
    fn from_iter<T: IntoIterator<Item = (String, Value)>>(iter: T) -> Self {
        Record(BTreeMap::from_iter(iter))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn set_get_roundtrip() {
        let row = Record::new().set("id", 1i64).set("name", "bolt");
        assert_eq!(row.get("id"), Some(&Value::Int(1)));
        assert_eq!(row.get("name").and_then(Value::as_str), Some("bolt"));
        assert!(row.get("absent").is_none());
    }

    #[test]
    fn retain_projects_fields() {
        let mut row = Record::new()
            .set("id", 1i64)
            .set("name", "bolt")
            .set("age", 3i64);
        row.retain(|name| name == "id" || name == "name");
        assert_eq!(row.len(), 2);
        assert!(row.get("age").is_none());
    }
}
