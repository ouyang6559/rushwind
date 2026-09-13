//! The mapper pair: ToRecord / FromRecord roundtrips, kind mismatches, and
//! the Option<_> ↔ NULL mapping.

use rushwind_storage::{FromRecord, Record, StorageError, ToRecord, Value};
use rushwind_storage_macros::{FromRecord, ToRecord};

#[derive(Debug, PartialEq, ToRecord, FromRecord)]
struct User {
    id: i64,
    name: String,
    score: Option<f64>,
    active: bool,
}

#[test]
fn roundtrips_through_the_record_view() {
    let user = User {
        id: 7,
        name: "bolt".into(),
        score: Some(2.5),
        active: true,
    };
    let record = user.to_record();
    assert_eq!(record.get("id"), Some(&Value::Int(7)));
    assert_eq!(record.get("name").and_then(Value::as_str), Some("bolt"));
    assert_eq!(record.get("score"), Some(&Value::Real(2.5)));
    assert_eq!(record.get("active"), Some(&Value::Bool(true)));

    let back = User::from_record(&record).expect("reads back");
    assert_eq!(back, user);
}

#[test]
fn none_maps_to_null_and_back() {
    let user = User {
        id: 1,
        name: "anon".into(),
        score: None,
        active: false,
    };
    let record = user.to_record();
    assert_eq!(record.get("score"), Some(&Value::Null));

    let back = User::from_record(&record).expect("reads back");
    assert_eq!(back.score, None);
    // A missing field behaves like NULL for optional fields.
    let mut sparse = Record::new();
    sparse.insert("id", 1i64);
    sparse.insert("name", "anon");
    sparse.insert("active", false);
    let back = User::from_record(&sparse).expect("reads back");
    assert_eq!(back.score, None);
}

#[test]
fn missing_required_fields_are_invalid_query() {
    let record = Record::new().set("name", "half");
    let err = User::from_record(&record).expect_err("id is missing");
    assert!(matches!(err, StorageError::InvalidQuery(m) if m.contains("id")));
}

#[test]
fn kind_mismatches_are_invalid_query() {
    let record = Record::new()
        .set("id", 1i64)
        .set("name", 42i64)
        .set("active", true);
    let err = User::from_record(&record).expect_err("name carries an int");
    assert!(matches!(err, StorageError::InvalidQuery(m) if m.contains("name")));
}
