//! Line-protocol and InfluxQL generation — pure functions, unit-tested
//! offline.

use rushwind_storage::{Record, Value};

/// Escapes a measurement or tag value for line protocol (commas, spaces,
/// equals signs).
pub fn escape_line_token(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    for ch in text.chars() {
        if matches!(ch, ',' | ' ' | '=') {
            out.push('\\');
        }
        out.push(ch);
    }
    out
}

/// Escapes a string inside an InfluxQL single-quoted literal.
pub fn escape_query_string(text: &str) -> String {
    text.replace('\\', "\\\\").replace('\'', "''")
}

/// The line-protocol point for a materialized row: the primary key is a
/// **tag** (series identity), other non-null columns are fields, and the
/// timestamp is pinned to epoch 0 so same-id writes overwrite in place.
/// The tag-form of the primary key (a series-identifying string).
fn display_id(value: &Value) -> String {
    // The pk is an integer by contract; other widths degrade to their
    // scalar text.
    match value {
        Value::Int(i) => i.to_string(),
        Value::Real(f) => format!("{f}"),
        Value::Text(s) => s.clone(),
        Value::Bool(b) => b.to_string(),
        Value::Null => String::new(),
    }
}

/// The line-protocol point for a materialized row: the primary key is a
/// **tag** (series identity), other non-null columns are fields, and the
/// timestamp is pinned to epoch 0 so same-id writes overwrite in place.
pub fn line(measurement: &str, row: &Record) -> String {
    let mut tags = String::new();
    let mut fields = Vec::new();
    for (column, value) in row.iter() {
        if column == "id" {
            tags.push_str(",id=");
            tags.push_str(&escape_line_token(&display_id(value)));
            continue;
        }
        let field = match value {
            Value::Null => continue, // absent fields read back as NULL
            Value::Bool(b) => format!("{}={}", escape_line_token(column), b),
            Value::Int(i) => format!("{}={}i", escape_line_token(column), i),
            Value::Real(f) => format!("{}={}", escape_line_token(column), f),
            Value::Text(s) => format!(
                "{}=\"{}\"",
                escape_line_token(column),
                s.replace('\\', "\\\\").replace('"', "\\\"")
            ),
        };
        fields.push(field);
    }
    if fields.is_empty() {
        // InfluxDB requires at least one field; keep a sentinel so the
        // series (and its NULL fields) still exists.
        fields.push("_present=true".to_owned());
    }
    format!(
        "{measurement}{tags} {fields} 0",
        measurement = escape_line_token(measurement),
        tags = tags,
        fields = fields.join(",")
    )
}

/// `SELECT JSON * FROM measurement` — the full-scan the adapter filters
/// and orders in memory.
pub fn select_all(measurement: &str) -> String {
    format!("SELECT * FROM \"{}\"", escape_query_string(measurement))
}

/// The InfluxQL delete for one series: `DELETE FROM m WHERE "id" = '…'`.
pub fn delete_by_id(measurement: &str, primary_key: &str, id: i64) -> String {
    format!(
        "DELETE FROM \"{}\" WHERE \"{}\" = '{}'",
        escape_query_string(measurement),
        escape_query_string(primary_key),
        id
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> Record {
        Record::new()
            .set("id", 3i64)
            .set("name", "it's bolt")
            .set("age", 7i64)
            .set("score", Value::Null)
    }

    #[test]
    fn line_protocol_pins_series_and_fields() {
        let line = line("widgets", &sample());
        assert_eq!(line, "widgets,id=3 age=7i,name=\"it's bolt\" 0");
        // Null columns are omitted; they read back as NULL.
        assert!(!line.contains("score"));
    }

    #[test]
    fn line_protocol_escapes_tokens_and_strings() {
        let row = Record::new()
            .set("id", 1i64)
            .set("name", "a b,c=d")
            .set("age", 1i64);
        let line = line("my table", &row);
        assert_eq!(line, "my\\ table,id=1 age=1i,name=\"a b,c=d\" 0");
    }

    #[test]
    fn queries_quote_identifiers_and_strings() {
        assert_eq!(select_all("my table"), "SELECT * FROM \"my table\"");
        assert_eq!(
            delete_by_id("widgets", "id", 9),
            "DELETE FROM \"widgets\" WHERE \"id\" = '9'"
        );
    }
}
