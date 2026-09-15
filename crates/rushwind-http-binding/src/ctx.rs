//! The per-request context bag services receive — the port of the
//! reference's `context.Context` propagation (its generated handlers hand
//! every service method a ctx carrying the Transport operation id and the
//! auth middleware's injected identity).
//!
//! `claims` stays the raw verified bag: the reference tokens carry custom
//! claims (user/tenant/role fields) the services read by name.

/// The context handed to every service trait method as its first
/// parameter.
#[derive(Clone, Debug, Default)]
pub struct RequestContext {
    /// The verified JWT claim bag; `None` on the auth-free subtree where
    /// no authentication ran.
    pub claims: Option<serde_json::Map<String, serde_json::Value>>,
    /// The operation id — `/<pkg>.<Svc>/<Method>`, the reference's
    /// `Transport.Operation`.
    pub operation: &'static str,
    /// The HTTP method of the matched binding.
    pub method: String,
    /// The request path.
    pub path: String,
    /// The best-effort client IP: `X-Forwarded-For` (first hop), else
    /// `X-Real-IP`, else empty.
    pub ip: String,
    /// The `User-Agent` header, empty when absent.
    pub user_agent: String,
    /// Lowercased header names → first value. Services read the select
    /// headers the reference reads off its transport ctx (captcha pairs,
    /// X-Forwarded-Proto, …).
    pub headers: std::collections::HashMap<String, String>,
    /// Request cookies (parsed `Cookie` header).
    pub cookies: std::collections::HashMap<String, String>,
    /// The response-header vehicle — the reference's `ReplyHeader`: the
    /// service layer appends (name, value) pairs (Set-Cookie pairs) that
    /// the lifecycle tail merges into the outgoing response.
    pub reply_headers: std::sync::Arc<std::sync::Mutex<Vec<(String, String)>>>,
}

impl RequestContext {
    /// Appends a response header (the ReplyHeader.Add path).
    pub fn add_reply_header(&self, name: &str, value: impl Into<String>) {
        self.reply_headers
            .lock()
            .expect("reply headers poisoned")
            .push((name.to_string(), value.into()));
    }
}

impl RequestContext {
    /// A string claim by name.
    pub fn claim_str(&self, name: &str) -> Option<&str> {
        self.claims.as_ref()?.get(name).and_then(|v| v.as_str())
    }

    /// An unsigned claim by name; accepts integral JSON numbers and
    /// numeric strings (JWT libraries encode uint32s either way).
    pub fn claim_u32(&self, name: &str) -> Option<u32> {
        let value = self.claims.as_ref()?.get(name)?;
        match value {
            serde_json::Value::Number(n) => n.as_u64().map(|v| v as u32),
            serde_json::Value::String(s) => s.parse().ok(),
            _ => None,
        }
    }

    /// A string-list claim by name (JSON array of strings).
    pub fn claim_str_list(&self, name: &str) -> Vec<String> {
        match self.claims.as_ref().and_then(|c| c.get(name)) {
            Some(serde_json::Value::Array(items)) => items
                .iter()
                .filter_map(|v| v.as_str().map(|s| s.to_owned()))
                .collect(),
            _ => Vec::new(),
        }
    }
}
