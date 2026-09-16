//! Metrics-reporting contract for RushWind.
//!
//! A minimal, engine-agnostic surface over the three core metric types:
//!
//! - **Counter** — monotonically increasing ([`Metrics::counter`] adds);
//!   request counts, error counts.
//! - **Histogram** — a distribution of observations
//!   ([`Metrics::histogram`] records one); latencies, payload sizes.
//! - **Gauge** — a point-in-time value ([`Metrics::gauge`] sets); queue
//!   depths, active connections.
//!
//! Business code depends only on this trait; engines live in their own
//! crates (`rushwind-metrics-*`), one backend per crate, the
//! registry/storage pattern.
//!
//! # Design notes
//!
//! - labels are `&[(&str, &str)]` pairs; engines canonicalize by sorting,
//!   so call-site order never changes the series identity
//! - no request context — recording is synchronous and fire-and-forget
//!   on every engine
//! - shutdown rides `Drop` where an engine holds flush machinery
//! - recording never fails the caller: engines drop and continue by
//!   contract
//!
//! # Engine matrix
//!
//! | Crate | Backend |
//! |:---|:---|
//! | `rushwind-metrics-prometheus` | pull-based: a Prometheus registry, text-format exposition for a `/metrics` route |
//! | `rushwind-metrics-otel` | push-based: OTLP export (gRPC or HTTP) to any compatible collector |
//! | `rushwind-metrics-datadog` | push-based: DogStatsD lines over UDP to a local Datadog Agent |

#![forbid(unsafe_code)]
#![deny(missing_docs)]

/// The metrics-reporting surface.
///
/// Implementations are [`Send`] + [`Sync`] and shareable behind an
/// `Arc`; every method takes `&self` and never fails the caller. A
/// metric is identified by its name plus its label **set** — engines
/// create the underlying instrument lazily on first use and reuse it
/// for subsequent calls with the same identity.
pub trait Metrics: Send + Sync {
    /// Adds `value` to the monotonic counter `name`.
    fn counter(&self, name: &str, value: f64, labels: &[(&str, &str)]);

    /// Records one observation of `value` into the histogram `name`.
    fn histogram(&self, name: &str, value: f64, labels: &[(&str, &str)]);

    /// Sets the gauge `name` to `value`.
    fn gauge(&self, name: &str, value: f64, labels: &[(&str, &str)]);
}

/// The canonical label order every engine uses: sorted by key, so two
/// call sites passing the same labels in different orders address the
/// same series. Engines call this on entry.
pub fn canonical_labels<'a>(labels: &[(&'a str, &'a str)]) -> Vec<(&'a str, &'a str)> {
    let mut sorted = labels.to_vec();
    sorted.sort_unstable();
    sorted
}
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn canonical_labels_sort_by_key() {
        let sorted = canonical_labels(&[("z", "1"), ("a", "2"), ("m", "3")]);
        assert_eq!(sorted, vec![("a", "2"), ("m", "3"), ("z", "1")]);
    }

    #[test]
    fn canonical_labels_of_different_orders_agree() {
        assert_eq!(
            canonical_labels(&[("b", "1"), ("a", "2")]),
            canonical_labels(&[("a", "2"), ("b", "1")])
        );
    }
}
