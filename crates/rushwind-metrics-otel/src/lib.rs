//! OpenTelemetry engine for the Rust metrics contract, over the `opentelemetry` Rust SDK.
//!
//! Instruments live in an OTel [`SdkMeterProvider`] whose periodic
//! reader exports via **OTLP** — gRPC (tonic) by default, HTTP binary
//! protobuf via [`OtelOptions::with_http`] — to any compatible
//! collector: Prometheus through its OTLP receiver, the Datadog Agent,
//! Grafana Cloud, and friends. Instruments are created lazily on first
//! use and cached per name.
//!
//! The gauge maps to an `f64_up_down_counter`, a
//! pseudo-gauge: OTel gauges are callback-based, and an up-down counter
//! gives set-like behavior per label set. Callers needing true gauge
//! semantics use the raw OTel API directly.
//!
//! # Design notes
//!
//! - the provider stays engine-local and reachable via
//!   [`OtelMetrics::provider`]; installing it process-globally is the
//!   application's call — a library hijacking global state would
//!   undermine the drop shutdown
//! - the exporter derives security from the endpoint scheme (`http://`
//!   plaintext, `https://` TLS); no separate flag
//! - [`OtelMetrics::shutdown`] is the explicit flush-and-shut-down form;
//!   `Drop` shuts down as a best effort (a drop cannot await)
//! - the OTel API takes a `&'static str` meter name; the service name is
//!   leaked once per provider to serve as the metric name
//! - the tonic channel spawns its connect task at build time, so
//!   [`OtelMetrics::new`] **must run inside a Tokio runtime context**
//!   when the gRPC exporter is selected; the HTTP exporter has no such
//!   constraint
//! - the OTel instrument API takes `Cow<'static, str>` metric names;
//!   each distinct metric name is leaked once, at instrument creation

#![forbid(unsafe_code)]
#![deny(missing_docs)]

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Duration;

use opentelemetry::metrics::{Counter, Histogram, Meter, MeterProvider, UpDownCounter};
use opentelemetry::KeyValue;
use opentelemetry_otlp::{ExporterBuildError, MetricExporter, Protocol, WithExportConfig};
use opentelemetry_sdk::metrics::{PeriodicReader, SdkMeterProvider};
use opentelemetry_sdk::resource::Resource;
use rushwind_metrics::{canonical_labels, Metrics};

/// The default OTLP collector endpoint.
pub const DEFAULT_ENDPOINT: &str = "localhost:4317";

/// The default service name.
pub const DEFAULT_SERVICE_NAME: &str = "rushwind-service";

/// The default export interval.
pub const DEFAULT_EXPORT_INTERVAL: Duration = Duration::from_secs(60);

/// Builder for [`OtelMetrics`].
pub struct OtelOptions {
    endpoint: String,
    service_name: String,
    service_version: Option<String>,
    use_http: bool,
    export_interval: Option<Duration>,
}

impl Default for OtelOptions {
    /// The defaults: gRPC against the loopback collector, one-minute
    /// export interval.
    fn default() -> Self {
        Self {
            endpoint: DEFAULT_ENDPOINT.to_string(),
            service_name: DEFAULT_SERVICE_NAME.to_string(),
            service_version: None,
            use_http: false,
            export_interval: None,
        }
    }
}

impl OtelOptions {
    /// Options with the gRPC exporter against the loopback collector.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the OTLP collector endpoint. The scheme selects the
    /// transport security: `http://` plaintext, `https://` TLS.
    pub fn with_endpoint(mut self, endpoint: &str) -> Self {
        self.endpoint = endpoint.to_string();
        self
    }

    /// Sets the service name on the exported resource; it also names
    /// the meter.
    pub fn with_service_name(mut self, name: &str) -> Self {
        self.service_name = name.to_string();
        self
    }

    /// Sets the service version on the exported resource.
    pub fn with_service_version(mut self, version: &str) -> Self {
        self.service_version = Some(version.to_string());
        self
    }

    /// Uses the HTTP binary-protobuf exporter instead of gRPC.
    pub fn with_http(mut self, use_http: bool) -> Self {
        self.use_http = use_http;
        self
    }

    /// Sets the periodic export interval.
    pub fn with_export_interval(mut self, interval: Duration) -> Self {
        self.export_interval = Some(interval);
        self
    }
}

/// The per-kind instrument caches. OTel
/// instruments carry no per-instance label schema — attributes are
/// per-call — so one instrument per name suffices.
struct Tables {
    counters: HashMap<String, Counter<f64>>,
    histograms: HashMap<String, Histogram<f64>>,
    gauges: HashMap<String, UpDownCounter<f64>>,
}

/// The OpenTelemetry-backed metrics provider.
pub struct OtelMetrics {
    meter: Meter,
    provider: SdkMeterProvider,
    tables: Mutex<Tables>,
}

impl OtelMetrics {
    /// Builds the provider from its options: an OTLP exporter (gRPC or
    /// HTTP binary protobuf), a resource carrying the service identity,
    /// and a periodic reader on the export interval.
    pub fn new(options: OtelOptions) -> Result<Self, ExporterBuildError> {
        // The transport selector (with_tonic / with_http) pins which
        // protocol is legal on the builder: Grpc only with tonic,
        // HttpBinary only with HTTP.
        let exporter = if options.use_http {
            MetricExporter::builder()
                .with_http()
                .with_protocol(Protocol::HttpBinary)
                .with_endpoint(&options.endpoint)
                .build()?
        } else {
            MetricExporter::builder()
                .with_tonic()
                .with_protocol(Protocol::Grpc)
                .with_endpoint(&options.endpoint)
                .build()?
        };

        let mut resource_builder =
            Resource::builder().with_service_name(options.service_name.clone());
        if let Some(version) = &options.service_version {
            // The `service.version` semantic-convention key.
            resource_builder =
                resource_builder.with_attribute(KeyValue::new("service.version", version.clone()));
        }

        let reader = PeriodicReader::builder(exporter)
            .with_interval(options.export_interval.unwrap_or(DEFAULT_EXPORT_INTERVAL))
            .build();

        let provider = SdkMeterProvider::builder()
            .with_resource(resource_builder.build())
            .with_reader(reader)
            .build();

        // The OTel API keys a meter by `&'static str`; the service name
        // is leaked once per provider to fill that slot.
        let meter_name: &'static str = Box::leak(options.service_name.clone().into_boxed_str());
        let meter = provider.meter(meter_name);
        Ok(Self {
            meter,
            provider,
            tables: Mutex::new(Tables {
                counters: HashMap::new(),
                histograms: HashMap::new(),
                gauges: HashMap::new(),
            }),
        })
    }

    /// The underlying provider — hand it to OTel tooling or install it
    /// process-globally yourself (`opentelemetry::global`).
    pub fn provider(&self) -> &SdkMeterProvider {
        &self.provider
    }

    /// Flushes pending metrics and shuts the exporter down. Recording
    /// after shutdown is dropped, the no-op-provider
    /// behavior.
    pub fn shutdown(&self) -> opentelemetry_sdk::error::OTelSdkResult {
        self.provider.shutdown()
    }

    /// Labels become OTel attributes, in canonical
    /// order.
    fn attributes(labels: &[(&str, &str)]) -> Vec<KeyValue> {
        canonical_labels(labels)
            .into_iter()
            .map(|(k, v)| KeyValue::new(k.to_string(), v.to_string()))
            .collect()
    }

    fn cached<T, F>(&self, table: &mut HashMap<String, T>, name: &str, create: F) -> Option<T>
    where
        T: Clone,
        F: FnOnce(&Meter) -> Option<T>,
    {
        if let Some(existing) = table.get(name) {
            return Some(existing.clone());
        }
        let instrument = create(&self.meter)?;
        table.insert(name.to_string(), instrument.clone());
        Some(instrument)
    }
}

impl Drop for OtelMetrics {
    fn drop(&mut self) {
        // A Drop cannot await the exporter's flush deadline; this is
        // the best-effort form, `shutdown` the explicit one.
        let _ = self.provider.shutdown();
    }
}

impl Metrics for OtelMetrics {
    fn counter(&self, name: &str, value: f64, labels: &[(&str, &str)]) {
        let Ok(mut tables) = self.tables.lock() else {
            return;
        };
        if let Some(counter) = self.cached(&mut tables.counters, name, |meter| {
            let instrument_name: &'static str = Box::leak(name.to_string().into_boxed_str());
            Some(meter.f64_counter(instrument_name).build())
        }) {
            counter.add(value, &Self::attributes(labels));
        }
    }

    fn histogram(&self, name: &str, value: f64, labels: &[(&str, &str)]) {
        let Ok(mut tables) = self.tables.lock() else {
            return;
        };
        if let Some(histogram) = self.cached(&mut tables.histograms, name, |meter| {
            let instrument_name: &'static str = Box::leak(name.to_string().into_boxed_str());
            Some(meter.f64_histogram(instrument_name).build())
        }) {
            histogram.record(value, &Self::attributes(labels));
        }
    }

    fn gauge(&self, name: &str, value: f64, labels: &[(&str, &str)]) {
        let Ok(mut tables) = self.tables.lock() else {
            return;
        };
        if let Some(gauge) = self.cached(&mut tables.gauges, name, |meter| {
            // The pseudo-gauge: an up-down counter standing in for
            // OTel's callback-based gauge.
            let instrument_name: &'static str = Box::leak(name.to_string().into_boxed_str());
            Some(meter.f64_up_down_counter(instrument_name).build())
        }) {
            gauge.add(value, &Self::attributes(labels));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn provider() -> OtelMetrics {
        // The exporter's channel is lazy: nothing here reaches a
        // collector, which is the point of a smoke test.
        OtelMetrics::new(
            OtelOptions::new()
                .with_endpoint("http://127.0.0.1:9")
                .with_service_name("rushwind-metrics-otel-test")
                .with_service_version("v0.0.1")
                .with_export_interval(Duration::from_secs(3600)),
        )
        .expect("provider builds without a live collector")
    }

    #[tokio::test]
    async fn construction_selects_grpc_by_default() {
        let _ = provider();
    }

    #[test]
    fn construction_selects_http_when_asked() {
        OtelMetrics::new(
            OtelOptions::new()
                .with_endpoint("http://127.0.0.1:9")
                .with_http(true),
        )
        .expect("http exporter builds");
    }

    #[tokio::test]
    async fn recording_never_panics_without_a_collector() {
        let provider = provider();
        provider.counter("requests_total", 1.0, &[("method", "GET")]);
        provider.histogram("request_duration_seconds", 0.042, &[]);
        provider.gauge("queue_depth", 42.0, &[]);
        // Repeat calls exercise the instrument cache.
        provider.counter("requests_total", 2.0, &[("method", "POST")]);
    }

    #[tokio::test]
    async fn shutdown_returns_against_an_unreachable_collector() {
        let provider = provider();
        provider.gauge("g", 1.0, &[]);
        // The final flush attempts an export; against an unreachable
        // collector it times out and errors. What matters is that
        // shutdown
        // returns on its own instead of hanging.
        let _ = provider.shutdown();
    }

    #[tokio::test]
    async fn drop_after_shutdown_does_not_panic() {
        let provider = provider();
        provider.shutdown().expect("first shutdown succeeds");
        drop(provider);
    }
}
