//! Table schema descriptions.
//!
//! The contract makes a table's shape an explicit first-class value:
//! engines derive their native machinery from a [`Schema`], and query
//! validation happens against it before anything reaches the driver.

use crate::error::StorageError;

/// The declared kind of a column.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ColumnKind {
    /// Boolean.
    Bool,
    /// Signed 64-bit integer.
    Int,
    /// 64-bit float.
    Real,
    /// UTF-8 text.
    Text,
}

impl ColumnKind {
    /// The kind expected of [`crate::Value`] payloads for this column.
    pub fn accepts(&self, kind: &str) -> bool {
        match self {
            ColumnKind::Bool => kind == "bool" || kind == "null",
            ColumnKind::Int => kind == "int" || kind == "null",
            ColumnKind::Real => kind == "real" || kind == "int" || kind == "null",
            ColumnKind::Text => kind == "text" || kind == "null",
        }
    }
}

/// One named column of a [`Schema`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Column {
    /// The column name as the engine sees it.
    pub name: String,
    /// The declared kind.
    pub kind: ColumnKind,
}

/// A table description: name, primary key, and columns.
///
/// Construct through [`Schema::builder`] so the invariants (unique column
/// names, the primary key is a declared column) are checked once in
/// [`SchemaBuilder::build`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Schema {
    /// The table (or collection) name.
    pub table: String,
    /// The primary key column name.
    pub primary_key: String,
    /// The declared columns.
    pub columns: Vec<Column>,
}

impl Schema {
    /// Starts a schema for `table` whose primary key `primary_key` is
    /// declared as an integer column.
    pub fn builder(table: impl Into<String>, primary_key: impl Into<String>) -> SchemaBuilder {
        let pk: String = primary_key.into();
        SchemaBuilder {
            schema: Schema {
                table: table.into(),
                primary_key: pk.clone(),
                columns: vec![Column {
                    name: pk,
                    kind: ColumnKind::Int,
                }],
            },
        }
    }

    /// Looks up a column by name.
    pub fn column(&self, name: &str) -> Option<&Column> {
        self.columns.iter().find(|c| c.name == name)
    }

    /// Returns `true` when `name` is the primary key column.
    pub fn is_primary(&self, name: &str) -> bool {
        self.primary_key == name
    }
}

/// Builder for [`Schema`]; see [`Schema::builder`].
pub struct SchemaBuilder {
    schema: Schema,
}

impl SchemaBuilder {
    /// Declares one column.
    pub fn column(mut self, name: impl Into<String>, kind: ColumnKind) -> Self {
        self.schema.columns.push(Column {
            name: name.into(),
            kind,
        });
        self
    }

    /// Declares several columns at once.
    pub fn columns(
        mut self,
        cols: impl IntoIterator<Item = (impl Into<String>, ColumnKind)>,
    ) -> Self {
        for (name, kind) in cols {
            self = self.column(name, kind);
        }
        self
    }

    /// Validates and finishes the schema.
    ///
    /// Fails when the primary key is missing from the column list or when a
    /// column name is duplicated.
    pub fn build(self) -> Result<Schema, StorageError> {
        let schema = self.schema;
        if !schema.columns.iter().any(|c| c.name == schema.primary_key) {
            return Err(StorageError::InvalidQuery(format!(
                "primary key {:?} is not a declared column of table {:?}",
                schema.primary_key, schema.table
            )));
        }
        let mut seen = std::collections::BTreeSet::new();
        for col in &schema.columns {
            if !seen.insert(col.name.as_str()) {
                return Err(StorageError::InvalidQuery(format!(
                    "duplicate column {:?} in table {:?}",
                    col.name, schema.table
                )));
            }
        }
        Ok(schema)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builder_auto_declares_int_primary_key() {
        let schema = Schema::builder("t", "id")
            .column("name", ColumnKind::Text)
            .build()
            .expect("valid schema");
        assert_eq!(schema.column("id").map(|c| c.kind), Some(ColumnKind::Int));
        assert!(schema.is_primary("id"));
    }

    #[test]
    fn duplicate_columns_are_rejected() {
        let err = Schema::builder("t", "id")
            .column("name", ColumnKind::Text)
            .column("name", ColumnKind::Int)
            .build()
            .expect_err("duplicate must fail");
        assert!(matches!(err, StorageError::InvalidQuery(_)));
    }
}
