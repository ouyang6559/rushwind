//! The four-field status error envelope — the port of Kratos's `Status`
//! message encoding as `DefaultErrorEncoder` emits it under
//! `EmitUnpopulated: true`: the four fields in field-number order with the
//! fixed `metadata: {}` trailer, strings escaped by the standard JSON
//! rules (serde_json's escape set matches protojson's: control characters,
//! quote, backslash; non-ASCII left as UTF-8), and `code` carrying the
//! **numeric** annotation value — the same number the reference passes to
//! both `errors.New` and `WriteHeader`.
//!
//! The message has no descriptor in a caller's compile closure (the
//! vendored errors.proto carries only the extension declarations), hence
//! the hand-rolled encoder.
//!
//! [`StatusError`] is the carrying type: a resolved (status, reason) pair
//! plus free-form text. Callers obtain pairs from their annotation tables
//! — never hand-picking numbers — and collapse unknowns into the
//! `FromError` Unknown shape (500, empty reason).

use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};

/// A status error: HTTP status and envelope fields carried verbatim from a
/// resolved (status, reason) pair.
#[derive(Debug, Clone)]
pub struct StatusError {
    /// HTTP status == envelope `code`. Values outside 100..=599 are
    /// clamped to 500 by the constructor; the encoder still guards.
    pub status: i32,
    /// Envelope `reason` — an enum value name from the caller's error
    /// annotation tables, or one of the runtime reasons.
    pub reason: &'static str,
    /// Envelope `message` — free-form description text.
    pub message: String,
}

impl StatusError {
    /// Builds a status error from a resolved (status, reason) pair.
    pub fn new(status: i32, reason: &'static str, message: impl Into<String>) -> Self {
        let status = if (100..=599).contains(&status) {
            status
        } else {
            500
        };
        Self {
            status,
            reason,
            message: message.into(),
        }
    }
}

impl std::fmt::Display for StatusError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "status error: code = {} reason = {} message = {}",
            self.status, self.reason, self.message
        )
    }
}

impl std::error::Error for StatusError {}

/// The `FromError` Unknown shape: status 500, empty reason.
pub fn internal_error(message: impl Into<String>) -> StatusError {
    StatusError::new(500, "", message)
}

/// The codec-failure pair: 400, `CODEC` — what the reference's
/// `DefaultRequestDecoder` and `binding.BindQuery` produce for every
/// decode/bind failure.
pub fn codec_error(message: impl Into<String>) -> StatusError {
    StatusError::new(400, "CODEC", message)
}

/// Encodes a status error into the envelope response. The `code` field
/// carries the bare numeric value — never the status-line text
/// `StatusCode`'s Display would produce (a regression pinned after the
/// assembly smoke test caught it emitting `500 Internal Server Error`).
pub fn error_response(err: StatusError) -> Response {
    let code = err.status;
    let status = StatusCode::from_u16(u16::try_from(err.status).unwrap_or(500))
        .unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
    let reason = serde_json::to_string(err.reason).unwrap_or_else(|_| "\"\"".to_string());
    let message = serde_json::to_string(&err.message).unwrap_or_else(|_| "\"\"".to_string());
    let body =
        format!("{{\"code\":{code},\"reason\":{reason},\"message\":{message},\"metadata\":{{}}}}");
    (status, [(header::CONTENT_TYPE, "application/json")], body).into_response()
}
