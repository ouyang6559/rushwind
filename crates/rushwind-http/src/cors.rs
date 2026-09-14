//! The CORS middleware — the Go `cors`, configured like the Go admin's
//! `server.yaml` block: explicit origin list, credential flag, method
//! and header lists, max age.
//!
//! A `*` origin delegates to `AllowOrigin::any()`; combined with
//! credentials it switches to per-request mirroring instead (browsers
//! reject `Access-Control-Allow-Origin: *` together with
//! `Access-Control-Allow-Credentials: true`, so mirroring is the only
//! shape that works). An empty origin list allows nothing — CORS stays
//! off until configured.

use std::time::Duration;

use axum::http::{HeaderName, HeaderValue, Method};
use axum::Router;
use tower_http::cors::{AllowHeaders, AllowMethods, AllowOrigin, CorsLayer, ExposeHeaders};

use crate::request_id::HEADER_X_REQUEST_ID;

/// The default cross-origin request methods when none are configured.
pub const DEFAULT_ALLOW_METHODS: &[&str] =
    &["GET", "HEAD", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"];
/// The default preflight-request headers when none are configured.
pub const DEFAULT_ALLOW_HEADERS: &[&str] = &["content-type", "authorization", HEADER_X_REQUEST_ID];
/// The default response-exposed headers when none are configured.
pub const DEFAULT_EXPOSE_HEADERS: &[&str] = &[HEADER_X_REQUEST_ID];

/// The CORS policy, the Go admin's `server.yaml` CORS block.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CorsOptions {
    allow_origins: Vec<String>,
    allow_credentials: bool,
    allow_methods: Vec<String>,
    allow_headers: Vec<String>,
    expose_headers: Vec<String>,
    max_age: Option<Duration>,
}

impl CorsOptions {
    /// Allows one origin — an exact match, or the literal `*` for any.
    pub fn with_allow_origin(mut self, origin: impl Into<String>) -> Self {
        self.allow_origins.push(origin.into());
        self
    }

    /// Allows credentialed requests (`Access-Control-Allow-Credentials`).
    pub fn with_allow_credentials(mut self, allow: bool) -> Self {
        self.allow_credentials = allow;
        self
    }

    /// Allows one request method, e.g. `"PATCH"`.
    pub fn with_allow_method(mut self, method: impl Into<String>) -> Self {
        self.allow_methods.push(method.into());
        self
    }

    /// Allows one request header, e.g. `"x-captcha-value"`.
    pub fn with_allow_header(mut self, header: impl Into<String>) -> Self {
        self.allow_headers.push(header.into());
        self
    }

    /// Exposes one response header to the browser.
    pub fn with_expose_header(mut self, header: impl Into<String>) -> Self {
        self.expose_headers.push(header.into());
        self
    }

    /// Sets the preflight cache duration.
    pub fn with_max_age(mut self, max_age: Duration) -> Self {
        self.max_age = Some(max_age);
        self
    }

    /// Builds the tower-http layer — the only place that knows CORS
    /// lives in tower-http.
    pub(crate) fn to_layer(&self) -> CorsLayer {
        let any_origin = self.allow_origins.iter().any(|origin| origin == "*");
        let origin = if any_origin && self.allow_credentials {
            // Credentialed + any-origin is a browser-rejected shape;
            // mirror the request origin instead.
            AllowOrigin::predicate(|_, _| true)
        } else if any_origin {
            AllowOrigin::any()
        } else {
            AllowOrigin::list(
                self.allow_origins
                    .iter()
                    .filter_map(|origin| HeaderValue::from_str(origin).ok()),
            )
        };

        let methods: Vec<Method> =
            parse_or_default(&self.allow_methods, DEFAULT_ALLOW_METHODS, |m| {
                Method::from_bytes(m.as_bytes()).ok()
            });
        let headers: Vec<HeaderName> =
            parse_or_default(&self.allow_headers, DEFAULT_ALLOW_HEADERS, |header| {
                HeaderName::from_lowercase(header.as_bytes()).ok()
            });
        let expose: Vec<HeaderName> =
            parse_or_default(&self.expose_headers, DEFAULT_EXPOSE_HEADERS, |header| {
                HeaderName::from_lowercase(header.as_bytes()).ok()
            });

        let mut layer = CorsLayer::new()
            .allow_origin(origin)
            .allow_methods(AllowMethods::list(methods))
            .allow_headers(AllowHeaders::list(headers))
            .expose_headers(ExposeHeaders::list(expose))
            .allow_credentials(self.allow_credentials);
        if let Some(max_age) = self.max_age {
            layer = layer.max_age(max_age);
        }
        layer
    }
}

/// Parses the configured entries; an empty configuration falls back to
/// the defaults, invalid entries are skipped.
fn parse_or_default<T>(
    configured: &[String],
    defaults: &[&str],
    parse: impl Fn(&str) -> Option<T>,
) -> Vec<T> {
    if configured.is_empty() {
        defaults.iter().filter_map(|entry| parse(entry)).collect()
    } else {
        configured
            .iter()
            .filter_map(|entry| parse(entry.as_str()))
            .collect()
    }
}

/// Wraps the router with the CORS middleware built from `options`.
pub fn with_cors(router: Router, options: CorsOptions) -> Router {
    router.layer(options.to_layer())
}
