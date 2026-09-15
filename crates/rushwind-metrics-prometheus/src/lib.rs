//! Prometheus engine for the Rust metrics contract, ported from
//! `go-wind-plugins/metrics/prometheus` over the `prometheus` crate.
//!
//! Instruments are **lazily registered** on first use and cached, so
//! subsequent calls with the same name and label set reuse the existing
//! instrument — the Go provider's cache tables, one per kind. Labelled
//! metrics become `*Vec` collectors whose label keys are fixed by the
//! first call and reused for every later call, values looked up per
//! call; the label order is the contract's canonical (sorted) one, so
//! call-site order never matters.
//!
//! This is a **pull** backend: samples live in a [`Registry`]. Expose
//! them by rendering [`encode`](PrometheusMetrics::encode) on a
//! `/metrics` route of any HTTP server — the Go engine's
//! `promhttp.Handler()` mount — or hand [`registry`](PrometheusMetrics::registry)
//! to a custom gatherer.
//!
//! # Divergences from the Go predecessor
//!
//! | Go | Rust |
//! |:---|:---|
//! | help strings optional (empty) | the Rust client requires one; the engine generates `<name> <kind>` help text |
//! | `Registry()` returns a `Gatherer` for `promhttp` | [`PrometheusMetrics::registry`] returns the registry, [`PrometheusMetrics::encode`] renders the text format directly |
//! | registration errors surface from `New` | the constructor is infallible; per-sample creation failures drop the sample, the contract's never-fail rule |
//!
//! [`Registry`]: prometheus::Registry

#![forbid(unsafe_code)]
#![deny(missing_docs)]

use std::collections::HashMap;
use std::sync::Mutex;

use prometheus::TextEncoder;
use rushwind_metrics::{canonical_labels, Metrics};

/// Builder for [`PrometheusMetrics`].
pub struct PrometheusOptions {
    namespace: Option<String>,
    subsystem: Option<String>,
    registry: prometheus::Registry,
}

impl Default for PrometheusOptions {
    /// The Go `New` default: no namespace, no subsystem, a **fresh**
    /// registry.
    fn default() -> Self {
        Self {
            namespace: None,
            subsystem: None,
            registry: prometheus::Registry::new(),
        }
    }
}

impl PrometheusOptions {
    /// Options with a fresh registry and no prefixes.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the metric namespace prefix (the Go `Namespace` field).
    pub fn with_namespace(mut self, namespace: &str) -> Self {
        self.namespace = Some(namespace.to_string());
        self
    }

    /// Sets the metric subsystem prefix (the Go `Subsystem` field).
    pub fn with_subsystem(mut self, subsystem: &str) -> Self {
        self.subsystem = Some(subsystem.to_string());
        self
    }

    /// Replaces the fresh registry — the global default
    /// (`prometheus::default_registry().clone()`) or any shared one.
    /// The Go `NewWithDefaultRegistry` shape.
    pub fn with_registry(mut self, registry: prometheus::Registry) -> Self {
        self.registry = registry;
        self
    }
}

/// The per-kind instrument caches, the Go provider's tables.
struct Tables {
    counters: HashMap<String, prometheus::Counter>,
    counter_vecs: HashMap<String, prometheus::CounterVec>,
    histograms: HashMap<String, prometheus::Histogram>,
    histogram_vecs: HashMap<String, prometheus::HistogramVec>,
    gauges: HashMap<String, prometheus::Gauge>,
    gauge_vecs: HashMap<String, prometheus::GaugeVec>,
    label_keys: HashMap<String, Vec<String>>,
}

/// The Prometheus-backed metrics provider.
pub struct PrometheusMetrics {
    options: PrometheusOptions,
    tables: Mutex<Tables>,
}

impl PrometheusMetrics {
    /// Builds the provider from its options.
    pub fn new(options: PrometheusOptions) -> Self {
        Self {
            options,
            tables: Mutex::new(Tables {
                counters: HashMap::new(),
                counter_vecs: HashMap::new(),
                histograms: HashMap::new(),
                histogram_vecs: HashMap::new(),
                gauges: HashMap::new(),
                gauge_vecs: HashMap::new(),
                label_keys: HashMap::new(),
            }),
        }
    }

    /// Constructs from the bootstrap factory's settings wire shape: the
    /// optional namespace and subsystem label prefixes, everything else
    /// the builder default. The only failure is the settings parse.
    pub fn from_settings(settings: serde_json::Value) -> Result<Self, serde_json::Error> {
        #[derive(Debug, Default, serde::Deserialize)]
        #[serde(default)]
        struct PrometheusSettings {
            namespace: Option<String>,
            subsystem: Option<String>,
        }
        let wire: PrometheusSettings = serde_json::from_value(settings)?;
        let mut options = PrometheusOptions::new();
        if let Some(namespace) = wire.namespace {
            options = options.with_namespace(&namespace);
        }
        if let Some(subsystem) = wire.subsystem {
            options = options.with_subsystem(&subsystem);
        }
        Ok(Self::new(options))
    }

    /// The registry this provider gathers into — hand it to a custom
    /// exposition path, or use [`PrometheusMetrics::encode`].
    pub fn registry(&self) -> &prometheus::Registry {
        &self.options.registry
    }

    /// Renders the gathered samples in the Prometheus text exposition
    /// format — the body a `/metrics` route serves.
    pub fn encode(&self) -> Result<String, prometheus::Error> {
        let encoder = TextEncoder::new();
        let families = self.options.registry.gather();
        encoder.encode_to_string(&families)
    }

    /// The Go `cachedLabelKeys`: the label keys of a metric name, fixed
    /// by its first labelled call and reused afterwards. The keys arrive
    /// in canonical (sorted) order.
    fn cached_label_keys(
        &self,
        tables: &mut Tables,
        name: &str,
        labels: &[(&str, &str)],
    ) -> Vec<String> {
        if let Some(keys) = tables.label_keys.get(name) {
            return keys.clone();
        }
        let keys: Vec<String> = labels.iter().map(|(k, _)| (*k).to_string()).collect();
        tables.label_keys.insert(name.to_string(), keys.clone());
        keys
    }

    /// Label values ordered along the cached keys; an absent key yields
    /// the empty string, the Go map-lookup zero value.
    fn label_values<'a>(labels: &[(&'a str, &'a str)], keys: &[String]) -> Vec<&'a str> {
        keys.iter()
            .map(|key| {
                labels
                    .iter()
                    .find(|(k, _)| *k == key)
                    .map(|(_, v)| *v)
                    .unwrap_or("")
            })
            .collect()
    }

    fn scalar_opts(&self, name: &str, kind: &str) -> prometheus::Opts {
        let mut opts = prometheus::Opts::new(name, format!("{name} {kind}"));
        if let Some(namespace) = &self.options.namespace {
            opts = opts.namespace(namespace.clone());
        }
        if let Some(subsystem) = &self.options.subsystem {
            opts = opts.subsystem(subsystem.clone());
        }
        opts
    }

    fn histogram_opts(&self, name: &str, kind: &str) -> prometheus::HistogramOpts {
        let mut opts = prometheus::HistogramOpts::new(name, format!("{name} {kind}"));
        if let Some(namespace) = &self.options.namespace {
            opts = opts.namespace(namespace.clone());
        }
        if let Some(subsystem) = &self.options.subsystem {
            opts = opts.subsystem(subsystem.clone());
        }
        opts
    }

    /// The plain (label-less) path: one instrument per name, cached.
    /// The closure creates *and registers* the instrument on first use.
    fn scalar_entry<T, F>(&self, table: &mut HashMap<String, T>, name: &str, create: F) -> Option<T>
    where
        T: Clone + 'static,
        F: FnOnce() -> Option<T>,
    {
        if let Some(existing) = table.get(name) {
            return Some(existing.clone());
        }
        let instrument = create()?;
        table.insert(name.to_string(), instrument.clone());
        Some(instrument)
    }

    /// The labelled path: one `*Vec` collector per name, label keys
    /// fixed by the first call. The closure creates *and registers* the
    /// collector on first use.
    fn vec_entry<T, F>(
        &self,
        table: &mut HashMap<String, T>,
        name: &str,
        keys: &[String],
        create: F,
    ) -> Option<T>
    where
        T: Clone + 'static,
        F: FnOnce(&[&str]) -> Option<T>,
    {
        if let Some(existing) = table.get(name) {
            return Some(existing.clone());
        }
        let label_names: Vec<&str> = keys.iter().map(String::as_str).collect();
        let vec = create(&label_names)?;
        table.insert(name.to_string(), vec.clone());
        Some(vec)
    }

    /// Creates one instrument and registers it; `None` on either
    /// failure — the sample is dropped, the contract's never-fail rule.
    fn registered<C: Clone + prometheus::core::Collector + 'static>(
        &self,
        create: impl FnOnce() -> Result<C, prometheus::Error>,
    ) -> Option<C> {
        let instrument = create().ok()?;
        self.options
            .registry
            .register(Box::new(instrument.clone()))
            .ok()?;
        Some(instrument)
    }
}

impl Metrics for PrometheusMetrics {
    fn counter(&self, name: &str, value: f64, labels: &[(&str, &str)]) {
        let labels = canonical_labels(labels);
        let Ok(mut tables) = self.tables.lock() else {
            return;
        };
        if labels.is_empty() {
            if let Some(counter) = self.scalar_entry(&mut tables.counters, name, || {
                self.registered(|| {
                    prometheus::Counter::with_opts(self.scalar_opts(name, "counter"))
                })
            }) {
                counter.inc_by(value);
            }
            return;
        }
        let keys = self.cached_label_keys(&mut tables, name, &labels);
        let values = Self::label_values(&labels, &keys);
        if let Some(vec) = self.vec_entry(&mut tables.counter_vecs, name, &keys, |label_names| {
            self.registered(|| {
                prometheus::CounterVec::new(self.scalar_opts(name, "counter"), label_names)
            })
        }) {
            vec.with_label_values(&values).inc_by(value);
        }
    }

    fn histogram(&self, name: &str, value: f64, labels: &[(&str, &str)]) {
        let labels = canonical_labels(labels);
        let Ok(mut tables) = self.tables.lock() else {
            return;
        };
        if labels.is_empty() {
            if let Some(histogram) = self.scalar_entry(&mut tables.histograms, name, || {
                self.registered(|| {
                    prometheus::Histogram::with_opts(self.histogram_opts(name, "histogram"))
                })
            }) {
                histogram.observe(value);
            }
            return;
        }
        let keys = self.cached_label_keys(&mut tables, name, &labels);
        let values = Self::label_values(&labels, &keys);
        if let Some(vec) = self.vec_entry(&mut tables.histogram_vecs, name, &keys, |label_names| {
            self.registered(|| {
                prometheus::HistogramVec::new(self.histogram_opts(name, "histogram"), label_names)
            })
        }) {
            vec.with_label_values(&values).observe(value);
        }
    }

    fn gauge(&self, name: &str, value: f64, labels: &[(&str, &str)]) {
        let labels = canonical_labels(labels);
        let Ok(mut tables) = self.tables.lock() else {
            return;
        };
        if labels.is_empty() {
            if let Some(gauge) = self.scalar_entry(&mut tables.gauges, name, || {
                self.registered(|| prometheus::Gauge::with_opts(self.scalar_opts(name, "gauge")))
            }) {
                gauge.set(value);
            }
            return;
        }
        let keys = self.cached_label_keys(&mut tables, name, &labels);
        let values = Self::label_values(&labels, &keys);
        if let Some(vec) = self.vec_entry(&mut tables.gauge_vecs, name, &keys, |label_names| {
            self.registered(|| {
                prometheus::GaugeVec::new(self.scalar_opts(name, "gauge"), label_names)
            })
        }) {
            vec.with_label_values(&values).set(value);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn provider() -> PrometheusMetrics {
        PrometheusMetrics::new(PrometheusOptions::new())
    }

    fn text(provider: &PrometheusMetrics) -> String {
        provider.encode().expect("text exposition renders")
    }

    #[test]
    fn settings_wire_parses_prefixes() {
        let provider = PrometheusMetrics::from_settings(serde_json::json!({
            "namespace": "wirens"
        }))
        .expect("wire must parse");
        provider.counter("requests_total", 1.0, &[]);
        let body = text(&provider);
        assert!(body.contains("wirens_"), "{body}");
    }

    #[test]
    fn settings_wire_rejects_malformed() {
        assert!(
            PrometheusMetrics::from_settings(serde_json::json!({ "namespace": 7 })).is_err(),
            "a non-string namespace must fail the parse"
        );
    }

    #[test]
    fn counters_accumulate_across_calls() {
        let provider = provider();
        provider.counter("requests_total", 1.0, &[]);
        provider.counter("requests_total", 2.0, &[]);
        let body = text(&provider);
        assert!(body.contains("requests_total 3"), "{body}");
    }

    #[test]
    fn gauges_hold_the_latest_value() {
        let provider = provider();
        provider.gauge("queue_depth", 41.0, &[]);
        provider.gauge("queue_depth", 42.0, &[]);
        let body = text(&provider);
        assert!(body.contains("queue_depth 42"), "{body}");
    }

    #[test]
    fn histograms_record_observations() {
        let provider = provider();
        provider.histogram("request_duration_seconds", 0.042, &[]);
        provider.histogram("request_duration_seconds", 0.084, &[]);
        let body = text(&provider);
        assert!(body.contains("request_duration_seconds_count 2"), "{body}");
        assert!(body.contains("request_duration_seconds_bucket"), "{body}");
    }

    #[test]
    fn labelled_series_carry_their_labels() {
        let provider = provider();
        provider.counter("requests_total", 1.0, &[("method", "GET")]);
        let body = text(&provider);
        assert!(body.contains(r#"requests_total{method="GET"} 1"#), "{body}");
    }

    #[test]
    fn call_site_label_order_never_splits_the_series() {
        let provider = provider();
        provider.counter("requests_total", 1.0, &[("method", "GET"), ("route", "/a")]);
        // Same label set, opposite order: one series, accumulated.
        provider.counter("requests_total", 2.0, &[("route", "/a"), ("method", "GET")]);
        let body = text(&provider);
        assert!(
            body.contains(r#"requests_total{method="GET",route="/a"} 3"#),
            "{body}"
        );
    }

    #[test]
    fn repeated_calls_reuse_the_instrument() {
        // A double registration would make gather fail; the cached
        // tables keep it to one series.
        let provider = provider();
        for _ in 0..4 {
            provider.counter("hits", 1.0, &[("code", "200")]);
        }
        let body = text(&provider);
        assert!(body.contains(r#"hits{code="200"} 4"#), "{body}");
        assert_eq!(body.matches("hits{").count(), 1, "{body}");
    }

    #[test]
    fn namespace_and_subsystem_prefix_the_name() {
        let provider = PrometheusMetrics::new(
            PrometheusOptions::new()
                .with_namespace("myapp")
                .with_subsystem("api"),
        );
        provider.counter("requests_total", 1.0, &[]);
        let body = text(&provider);
        assert!(body.contains("myapp_api_requests_total 1"), "{body}");
    }

    #[test]
    fn missing_label_values_default_to_empty() {
        // The first call fixes the keys; a later call missing one
        // supplies the empty string, the Go zero-value lookup.
        let provider = provider();
        provider.counter("requests_total", 1.0, &[("method", "GET"), ("route", "/a")]);
        provider.counter("requests_total", 1.0, &[("method", "POST")]);
        let body = text(&provider);
        assert!(
            body.contains(r#"requests_total{method="POST",route=""} 1"#),
            "{body}"
        );
    }

    #[test]
    fn the_default_registry_variant_registers_into_it() {
        // The Go NewWithDefaultRegistry shape: instruments land in the
        // global default registry. Namespaced names keep the test from
        // colliding with other tests sharing that process-wide registry.
        let provider = PrometheusMetrics::new(
            PrometheusOptions::new()
                .with_namespace("rushwind_metrics_test")
                .with_registry(prometheus::default_registry().clone()),
        );
        provider.gauge("test_gauge", 7.0, &[]);
        let body = text(&provider);
        assert!(
            body.contains("rushwind_metrics_test_test_gauge 7"),
            "{body}"
        );
    }

    #[test]
    fn the_trait_object_is_shareable() {
        // Object safety + Send + Sync behind an Arc: the shape every
        // consumer holds the engine by.
        let provider: std::sync::Arc<dyn Metrics> =
            std::sync::Arc::new(PrometheusMetrics::new(PrometheusOptions::new()));
        fn assert_send_sync<T: Send + Sync>(_: &T) {}
        assert_send_sync(&provider);
        provider.counter("shared_total", 1.0, &[]);
        provider.histogram("shared_seconds", 0.5, &[]);
        provider.gauge("shared_depth", 1.0, &[]);
    }
}
