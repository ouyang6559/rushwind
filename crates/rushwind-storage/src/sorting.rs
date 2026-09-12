//! List ordering.

use crate::error::StorageError;
use crate::schema::Schema;

/// Sort direction.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SortDir {
    /// Ascending.
    Asc,
    /// Descending.
    Desc,
}

/// One ordering term.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SortField {
    /// The column to order by.
    pub field: String,
    /// The direction.
    pub dir: SortDir,
}

/// The ordering of a list query: terms applied left to right as primary,
/// secondary, … keys. Empty means "primary key ascending".
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Sort {
    /// The ordering terms.
    pub fields: Vec<SortField>,
}

impl Sort {
    /// Orders by one field.
    pub fn by(field: impl Into<String>, dir: SortDir) -> Self {
        Self {
            fields: vec![SortField {
                field: field.into(),
                dir,
            }],
        }
    }

    /// Appends a secondary ordering term, builder style.
    pub fn then(mut self, field: impl Into<String>, dir: SortDir) -> Self {
        self.fields.push(SortField {
            field: field.into(),
            dir,
        });
        self
    }

    /// Returns `true` when the query leaves ordering to the engine's default.
    pub fn is_default(&self) -> bool {
        self.fields.is_empty()
    }

    /// Checks every term names a declared column.
    pub fn validate(&self, schema: &Schema) -> Result<(), StorageError> {
        for term in &self.fields {
            if schema.column(&term.field).is_none() {
                return Err(StorageError::InvalidQuery(format!(
                    "unknown sort column {:?} in table {:?}",
                    term.field, schema.table
                )));
            }
        }
        Ok(())
    }
}
