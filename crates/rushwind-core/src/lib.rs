//! RushWind core: application lifecycle orchestration.
//!
//! [`App`] owns a set of [`rushwind_transport::Server`] instances and drives
//! them through a fixed lifecycle:
//!
//! 1. **Start** — all servers run concurrently until a shutdown trigger: a
//!    cooperative [`StopSignal`](rushwind_transport::StopSignal) (from
//!    [`App::stop`], the caller-supplied signal, or an OS termination
//!    signal) or any server exiting (error, self-exit or panic).
//! 2. **Before hooks** — sequential, each with a fresh deadline.
//! 3. **Stop** — every server's teardown runs concurrently, each bounded by
//!    a fresh deadline. Panics inside a server are isolated and recorded.
//! 4. **After hooks** — sequential, each with a fresh deadline.
//!
//! Every deadline is created *at the moment its phase begins*, never
//! inherited from an earlier context, so each budget is always fully
//! available. An ill-behaved server that ignores its stop signal is
//! abandoned when the drain deadline lapses; a hanging teardown is cut off
//! by its own deadline. The terminal outcome is observable via
//! [`App::subscribe_done`] and [`App::outcome`].
//!
//! This crate is deliberately dependency-light: `tokio` (OS signal
//! handling, deadline timers) and `futures` (`FuturesUnordered` for
//! borrowed concurrency, `catch_unwind` for panic isolation). No logging,
//! no metrics, no registries — those belong to adapter crates.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

mod app;

pub use app::{App, Builder};
