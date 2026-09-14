<div align="center">

<img src="assets/logo/rushwind-icon.svg" alt="RushWind" width="128">

# RushWind

**English** | [中文](./README.md) | [日本語](./README_ja.md)

</div>

---

## Design philosophy

> **Not a batteries-included framework. A box of bricks.**

RushWind does exactly one thing: **reliable multi-server lifecycle orchestration**. The core defines contracts — a transport trait, a shutdown signal, an instance model — and every concrete protocol stack is a separate adapter crate that the user snaps in as needed. The core contains no logging, no registry, no config center: those are bricks, not the baseplate.

Compared to the Go predecessor [go-wind](https://github.com/tx7do/go-wind) (the same philosophy expressed in Go): RushWind is not a port but a re-expression under Rust's ownership, cancellation and error models. The semantic divergence list lives in [docs/architecture.md](./docs/architecture.md) (Chinese). The data-access layer follows the same recipe: [go-crud](https://github.com/tx7do/go-crud)'s "one repository contract driving many engines" is re-expressed as the `rushwind-storage` contract plus one adapter crate per engine.

## Status

**P0 (scaffold)**: lifecycle core, transport contracts, and the cross-adapter conformance suite are in place and fully green. Adapters follow the roadmap:

| Phase | Scope |
|:---|:---|
| P0 | core lifecycle, transport contracts, conformance suite |
| P1 | `rushwind-transport-axum` (admin/API), `rushwind-transport-ws` (session middleware chain: gates + admission policy + session-shutdown bus, see [session-middleware.md](./docs/session-middleware.md), Chinese) |
| P2 | `rushwind-transport-quic` (raw QUIC sessions + full session chain: gates/refuse, atomic admission, handshake deadline — delivered; h3/webtransport layering later); `rushwind-transport-mqtt` (external-broker consumer bridge — delivered); registration-only registry slice ← current |
| storage | `rushwind-storage` (contract) + four engines: in-memory reference, SeaORM (SQLite/PostgreSQL/MySQL), MongoDB; cross-cutting `rushwind-storage-cache` / `rushwind-storage-soft-delete` / `rushwind-storage-observe` (transparent decorators); the `rushwind-storage-tree` algorithm brick; `rushwind-storage-proto` (proto-defined contract + protojson + AIP text syntax); `rushwind-storage-macros` (DTO mapping derives); counterpart of [go-crud](https://github.com/tx7do/go-crud) |
| auth | `rushwind-authn` / `rushwind-authz` (contracts) + the engine matrix: seven authentication engines (apikey / basicauth / hmac / jwt / noop / presharedkey / session), three authorization engines (acl / rbac / noop); `AuthenticationGate` adapts an authenticator onto the session gate chain — contracts in [docs/security-authn-authz.md](./docs/security-authn-authz.md) (Chinese) |
| config | `rushwind-config` (contract: the `Source` trait with defaulted watch capabilities, plus `FallbackSource` priority composition with merged change streams) + two engines: env (environment variables with a prefix), file (one file, parent-directory watched with burst coalescing and stale-value suppression); counterpart of `go-wind-plugins/config` |
| P3 | `rushwind-bootstrap` (serde YAML config-driven assembly: storage factories + route packs + server factories) — delivered |
| next | h3/webtransport layering, mqtt handler registry, kcp (if legacy interop), rushwind-protocols standalone repo |

## Layout

| Path | Role |
|:---|:---|
| `crates/rushwind-core` | lifecycle orchestration: concurrent start, cascading shutdown, bounded phases, outcome observation |
| `crates/rushwind-transport` | contracts: `Server` trait, `StopSignal`, `Instance`, `ServerError` |
| `crates/rushwind-transport-axum` | axum adapter: serves a `Router` under the lifecycle; shutdown mapping in the architecture doc |
| `crates/rushwind-transport-ws` | WS session route builder: gates + admission policy + session-shutdown bus; contracts in the session-middleware doc |
| `crates/rushwind-transport-quic` | QUIC adapter: quinn accept loop under the lifecycle, full session chain; `stop()` is a real release (Endpoint::close) |
| `crates/rushwind-transport-mqtt` | MQTT consumer bridge: subscribes to an external broker, reconnect backoff + subscription re-establishment, serial pump into the handler |
| `crates/rushwind-bootstrap` | config-driven assembly: YAML → storage engine + HTTP servers + route packs, under one lifecycle |
| `crates/rushwind-authn` | the authentication contract: `Authenticator` trait (extraction/validation halves), the `AuthClaims` bag, the error taxonomy, and `AuthenticationGate` — see [docs/security-authn-authz.md](./docs/security-authn-authz.md) (Chinese) |
| `crates/rushwind-authn-apikey` | API-key engine: static key set / per-key claims / validator callback |
| `crates/rushwind-authn-basicauth` | basic-auth engine: RFC 7617 credentials against a static user table or a validator callback |
| `crates/rushwind-authn-hmac` | HMAC engine: keyID.timestamp.signature verification with a clock-skew window |
| `crates/rushwind-authn-jwt` | JWT engine: mint and verify over the HS/RS/PS/ES/EdDSA families, validation profile aligned to golang-jwt v5 defaults |
| `crates/rushwind-authn-noop` | noop engine: accepts everything, mints nothing |
| `crates/rushwind-authn-presharedkey` | preshared-key engine: set-membership check, mint by random draw |
| `crates/rushwind-authn-session` | session engine: opaque session IDs against a pluggable session store |
| `crates/rushwind-authz` | the authorization contract: the `Engine` trait (single verdict + three bulk filters), the Subject/Action/Resource/Project model, the JSON policy interchange — see [docs/security-authn-authz.md](./docs/security-authn-authz.md) (Chinese) |
| `crates/rushwind-authz-acl` | ACL engine: ordered allow/deny rules with wildcard matching, default-deny, deny-overrides |
| `crates/rushwind-authz-rbac` | RBAC engine: role→permission and user→role maps, transitive inheritance with cycle guards |
| `crates/rushwind-authz-noop` | noop engine: every verdict passes, every filter returns empty |
| `crates/rushwind-config` | the configuration-source contract: the `Source` trait (load plus defaulted watch/watch_value capabilities), the `SignalStream`/`ValueStream` stream contracts, and `FallbackSource` — priority composition where the first answer wins and change streams merge into the effective value, no task boundaries |
| `crates/rushwind-config-env` | environment-variable engine: default key + prefix resolution; an unset variable is absence, not an error |
| `crates/rushwind-config-file` | file engine: whole-file reads + parent-directory watching (editor atomic-rename safe), event bursts coalesced, stale values suppressed by content, stream drop stops the watch |
| `crates/rushwind-storage` | storage contracts: `Repository` trait, three paging strategies (Page/Offset/Token), filter tree, five-level viewer tenancy, field mask, audit hook |
| `crates/rushwind-storage-memory` | in-memory reference engine: the semantic baseline for filters/sorting/cursors, zero dependencies |
| `crates/rushwind-storage-seaorm` | SeaORM engine: SQLite/PostgreSQL/MySQL all enabled, per-dialect SQL pinned by snapshot tests, SQLite passes the suite, live suites run in CI containers |
| `crates/rushwind-storage-cache` | cache-aside decorator: singleflight miss-coalescing, scope-aware cache keys, generation-guarded invalidation |
| `crates/rushwind-storage-proto` | the proto-defined wire contract: generated from `proto/rushwind/storage/v1/query.proto` (prost + pbjson), 29-operator mapping + AIP text parsing |
| `crates/rushwind-storage-mongodb` | MongoDB engine: FilterExpr→BSON translation unit-tested offline, LIKE family compiled to escaped regex, live suite in CI containers |
| `crates/rushwind-storage-elasticsearch` | Elasticsearch engine: REST with refresh-on-write, `.keyword` exact matching, atomic bulk with rollback |
| `crates/rushwind-storage-opensearch` | OpenSearch engine: thin reuse of the ES wire shape (wire-compatible) |
| `crates/rushwind-storage-cassandra` | Cassandra engine: bucket-fixed partition + contract-evaluator filtering, LWT atomic batches |
| `crates/rushwind-storage-influxdb` | InfluxDB engine: measurement as table, id as series tag, InfluxQL deletes |
| `crates/rushwind-storage-clickhouse` | ClickHouse engine: SQL over HTTP, mutations_sync for read-your-writes, probe-based conflict detection |
| `crates/rushwind-storage-soft-delete` | soft-delete decorator: tombstone writes, filtered on every read path, restore/purge — engine-agnostic |
| `crates/rushwind-storage-macros` | `ToRecord`/`FromRecord` derive macros: DTO↔Record mapping generated at compile time (the go-utils/mapper counterpart) |
| `crates/rushwind-storage-tree` | tree queries: children/roots/ancestors/subtree as contract-level traversal with cycle guards, on any engine |
| `crates/rushwind-storage-observe` | observability decorator: one `tracing` span per call (table/op/outcome); OTel export is a subscriber choice |
| `crates/rushwind-storage-axum` | the HTTP edge: any Repository as CRUD routes, list queries via protojson `q` or AIP `filter`, viewer hook for tenancy |
| `crates/rushwind-testkit` | cross-adapter conformance suites — every transport and engine must pass them in full |
| `examples/multi-server` | two-server lifecycle demo (cascade, phase ordering) |
| `examples/axum-admin` | axum adapter demo: health route + signal-driven graceful shutdown |
| `examples/ws-gateway` | WS gateway demo: gate rejection + session cap + lifecycle-aligned session teardown |
| `examples/quic-gateway` | QUIC gateway demo: loopback gate + session cap + handshake deadline + endpoint-level teardown |
| `examples/mqtt-ingest` | MQTT consumer demo: point at an external broker, clean signal-driven exit |
| `examples/bootstrap-demo` | assembly demo: one YAML + memory factory + route packs, a full service |
| `examples/storage-basics` | the same repository code over in-memory and SQLite engines, line-for-line identical output |

## For adapter authors

A new transport = implement `Server` + pass the conformance suite. Two things:

```rust
// crates/rushwind-transport-<your-stack>/tests/conformance.rs
rushwind_testkit::rushwind_conformance_suite!(crate::your_server_factory);
```

A storage engine works the same way = implement `Repository` + pass the storage suite:

```rust
// crates/rushwind-storage-<your-engine>/tests/conformance.rs
rushwind_testkit::rushwind_storage_conformance_suite!(crate::fresh_repo);
```

A green `cargo test` is the definition of conformant; CI enforces it for every adapter crate.

## Development gates

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

CI runs the same gates on a Linux/Windows/macOS matrix. The entire workspace is `#![forbid(unsafe_code)]`.

## License

[MIT License](./LICENSE)
