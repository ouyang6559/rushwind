//! Authentication contract for RushWind, extracted from the Go predecessor
//! `go-wind-plugins/security/authn`.
//!
//! The contract splits authentication in two halves mirroring the Go
//! `Authenticator` interface:
//!
//! - **Extraction** — pulling the raw credential out of a request. Go pulls
//!   it from gRPC metadata via `AuthFromMD`; Rust has no request context, so
//!   [`Authenticator::extract_token`] reads the header pairs a transport
//!   exposes (the HTTP-family [`Handshake`](rushwind_transport::Handshake)
//!   snapshot is the carrier today). The default implements the
//!   `Authorization: <scheme> <token>` shape; engines whose credentials ride
//!   a different header override it.
//! - **Validation** — [`Authenticator::authenticate_token`] turns a raw
//!   credential into [`AuthClaims`], and [`Authenticator::create_identity`]
//!   mints one. Each engine crate (`rushwind-authn-*`) implements exactly
//!   this half against one credential scheme.
//!
//! [`Authenticator::authenticate`] composes the two — the exact shape of
//! every Go engine's `Authenticate` — and, like them, collapses every
//! extraction failure to
//! [`AuthnError::MissingBearerToken`](crate::AuthnError::MissingBearerToken).
//!
//! # Landing point
//!
//! [`AuthenticationGate`] adapts any [`Authenticator`] onto the session
//! middleware's [`GateChain`](rushwind_transport::GateChain) — the one
//! authn/authz enforcement point session transports have, per
//! `docs/session-middleware.md`. HTTP-family per-route enforcement is the
//! application's middleware; the contract crate only supplies the
//! primitives.
//!
//! # Divergences from the Go predecessor
//!
//! | Go | Rust |
//! |:---|:---|
//! | `Authenticate(ctx)` reads gRPC metadata | [`Authenticator::authenticate`] reads a `&[(String, String)]` header slice; header names and schemes compare case-insensitively (gRPC normalizes to lowercase; HTTP presents either case) |
//! | `MDWithAuth` injects credentials into outgoing context metadata | [`format_authorization`] formats the header value; setting it on a client request is the caller's job |
//! | `CreateIdentityWithContext` round-trips through context mutation | dropped — `create_identity` returns the credential string, the caller places it |
//! | engines register via `init()` + per-package constructors | direct constructors per engine crate, the registry/storage pattern |
//! | `Close()` | `Drop` |
//!
//! # Engine matrix (ported)
//!
//! | Crate | Credential scheme |
//! |:---|:---|
//! | `rushwind-authn-apikey` | opaque bearer keys, static or validator-backed |
//! | `rushwind-authn-basicauth` | RFC 7617 `Basic` credentials |
//! | `rushwind-authn-hmac` | `keyID.timestamp.signature` HMAC-SHA256 request signatures |
//! | `rushwind-authn-jwt` | signed JWTs (HS/RS/PS/ES/EdDSA families) |
//! | `rushwind-authn-noop` | accepts everything, mints nothing |
//! | `rushwind-authn-presharedkey` | one-of-N static bearer keys |
//! | `rushwind-authn-session` | opaque session IDs against a pluggable store |
//!
//! The Go engines `mtls`, `oauth2`, `oidc`, and the `authz` family's
//! external-policy engines remain unported — their carriers (peer TLS
//! certificates, OAuth flows, remote policy services) have no contract
//! surface in RushWind yet.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

mod authenticator;
mod claims;
mod error;
mod gate;
mod headers;

pub use authenticator::Authenticator;
pub use claims::{
    AuthClaims, CLAIM_FIELD_AUDIENCE, CLAIM_FIELD_EXPIRATION_TIME, CLAIM_FIELD_ISSUED_AT,
    CLAIM_FIELD_ISSUER, CLAIM_FIELD_JWT_ID, CLAIM_FIELD_NOT_BEFORE, CLAIM_FIELD_SCOPE,
    CLAIM_FIELD_SUBJECT,
};
pub use error::AuthnError;
pub use gate::AuthenticationGate;
pub use headers::{
    auth_from_headers, format_authorization, HEADER_AUTHORIZE, SCHEME_BASIC, SCHEME_BEARER,
    SCHEME_DIGEST,
};
