//! Tracing contract for RushWind, extracted from the Go predecessor
//! `go-wind-plugins/tracer/otlp`: an OTLP-backed
//! [`TracerProviderBuilder`] reproducing the Go `New` one-call
//! provider setup, plus W3C trace-context carrier helpers for the
//! server/client span lifecycle.
//!
//! # The Go shapes, translated
//!
//! The Go domain is a thin configuration wrapper over
//! `go.opentelemetry.io/otel`: it configures an OTLP exporter (gRPC
//! or HTTP), a ratio sampler, resource attributes, a batch processor,
//! and installs the global provider plus the W3C `TraceContext` /
//! Baggage propagator.
//!
//! Rust has no process-global `otel.SetTracerProvider` — the provider
//! is an explicit value the caller hands to the layers that need it
//! (it is `Clone`). [`TracerProviderBuilder`] reproduces the Go `New`
//! options surface: transport choice, sample ratio, batch and export
//! timeouts, headers, service name/version.
//!
//! Propagation is the W3C `TraceContext` format over a string-map
//! carrier — the wire shape the Go `propagation.TextMapCarrier`
//! implementations exchange. [`MapCarrier`] adapts a string map to
//! both the extractor and injector halves; [`inject`] and [`extract`]
//! are the two lifecycle halves of the Go `Tracer.Start` for
//! client/outbound and server/inbound spans respectively.
//!
//! # Divergences from the Go adapter
//!
//! - No global registration: the provider is returned, not installed.
//! - Baggage propagation is not wired — the Go composite includes
//!   `propagation.Baggage{}`, but RushWind callers exchange only
//!   trace context today.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

use std::collections::HashMap;
use std::time::Duration;

use opentelemetry::global;

/// The std result alias for the builder return.
type StdResult<T, E> = std::result::Result<T, E>;
use opentelemetry::Context;
use opentelemetry_otlp::SpanExporter;
use opentelemetry_otlp::{WithExportConfig, WithHttpConfig, WithTonicConfig};
use opentelemetry_sdk::trace::{Sampler, SdkTracerProvider};
use opentelemetry_sdk::Resource;

/// The default OTLP export batch timeout — the Go default.
const DEFAULT_BATCH_TIMEOUT: Duration = Duration::from_secs(5);
/// The default export request timeout — the Go default.
const DEFAULT_EXPORT_TIMEOUT: Duration = Duration::from_secs(10);
/// The default sample ratio — the Go default (sample everything).
const DEFAULT_SAMPLE_RATIO: f64 = 1.0;
/// The default tracer name — the Go `defaultTracerName`.
pub const DEFAULT_TRACER_NAME: &str = "go-wind";

/// The OTLP transport the exporter uses — the Go `useHTTP` switch.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Transport {
    /// OTLP over gRPC (the Go default).
    #[default]
    Grpc,
    /// OTLP over HTTP protobuf.
    Http,
}

/// The OTLP tracer-provider settings — the Go `options`.
#[derive(Debug, Clone)]
pub struct OtlpOptions {
    /// The OTLP endpoint as `host:port`. No default — the caller
    /// always names the collector.
    pub endpoint: String,
    /// The transport. Default gRPC.
    pub transport: Transport,
    /// Whether to skip TLS. Default false (TLS on).
    pub insecure: bool,
    /// The trace sample ratio (0.0–1.0). Default 1.0.
    pub sample_ratio: f64,
    /// The batch-export timeout. Default 5 s.
    pub batch_timeout: Duration,
    /// The export request timeout. Default 10 s.
    pub export_timeout: Duration,
    /// The service name resource attribute.
    pub service_name: String,
    /// The service version resource attribute.
    pub service_version: String,
    /// Additional headers for OTLP requests.
    pub headers: Vec<(String, String)>,
}

impl Default for OtlpOptions {
    fn default() -> Self {
        Self {
            endpoint: String::new(),
            transport: Transport::Grpc,
            insecure: false,
            sample_ratio: DEFAULT_SAMPLE_RATIO,
            batch_timeout: DEFAULT_BATCH_TIMEOUT,
            export_timeout: DEFAULT_EXPORT_TIMEOUT,
            service_name: String::new(),
            service_version: String::new(),
            headers: Vec::new(),
        }
    }
}

/// The Go `New`: one call that builds the configured tracing
/// provider. Unlike the Go adapter there is no global registration —
/// the caller owns the provider (it is `Clone`) and passes it to the
/// layers that need tracing.
#[derive(Debug, Clone)]
pub struct TracerProviderBuilder {
    options: OtlpOptions,
    /// Overrides the tracer name scopes are created under — the Go
    /// `WithTracerName`.
    tracer_name: Option<String>,
}

impl TracerProviderBuilder {
    /// Starts a builder from the OTLP options.
    pub fn new(options: OtlpOptions) -> Self {
        Self {
            options,
            tracer_name: None,
        }
    }

    /// Overrides the tracer name scopes are created under.
    pub fn tracer_name(mut self, name: impl Into<String>) -> Self {
        self.tracer_name = Some(name.into());
        self
    }

    /// The tracer name scopes are created under — the Go
    /// `defaultTracerName` unless overridden.
    pub fn resolved_tracer_name(&self) -> String {
        self.tracer_name
            .clone()
            .unwrap_or_else(|| DEFAULT_TRACER_NAME.to_string())
    }

    /// Builds the [`SdkTracerProvider`] — the Go `New` minus the
    /// global registration. Sampler, resource, and batch processor
    /// follow the Go configuration.
    pub fn build(self) -> StdResult<SdkTracerProvider, opentelemetry_otlp::ExporterBuildError> {
        let options = self.options;
        let sample_ratio = if (0.0..=1.0).contains(&options.sample_ratio) {
            options.sample_ratio
        } else {
            DEFAULT_SAMPLE_RATIO
        };
        let scheme = if options.insecure { "http" } else { "https" };
        let endpoint = format!("{scheme}://{}", options.endpoint);

        let header_map: HashMap<String, String> = options.headers.iter().cloned().collect();
        let exporter = match options.transport {
            Transport::Http => SpanExporter::builder()
                .with_http()
                .with_endpoint(endpoint.clone())
                .with_protocol(opentelemetry_otlp::Protocol::HttpBinary)
                .with_timeout(options.export_timeout)
                .with_headers(header_map)
                .build()?,
            Transport::Grpc => SpanExporter::builder()
                .with_tonic()
                .with_endpoint(endpoint)
                .with_timeout(options.export_timeout)
                .with_metadata(headers_to_metadata(&options.headers))
                .build()?,
        };

        let resource = Resource::builder()
            .with_service_name(options.service_name.clone())
            .with_attribute(opentelemetry::KeyValue::new(
                "service.version",
                options.service_version.clone(),
            ))
            .build();

        Ok(SdkTracerProvider::builder()
            .with_sampler(Sampler::TraceIdRatioBased(sample_ratio))
            .with_resource(resource)
            .with_batch_exporter(exporter)
            .build())
    }
}

/// The string-map carrier trace context is exchanged through — the
/// adapter over the Go `propagation.TextMapCarrier` implementations,
/// which are all string maps at the wire level.
#[derive(Debug, Default, Clone)]
pub struct MapCarrier(pub HashMap<String, String>);

impl MapCarrier {
    /// Builds a carrier from a starter map.
    pub fn from_map(map: HashMap<String, String>) -> Self {
        Self(map)
    }

    /// Reads one carried value — for tests and callers that need a
    /// specific header after injection.
    pub fn get(&self, key: &str) -> Option<&str> {
        self.0.get(key).map(|value| value.as_str())
    }
}

impl opentelemetry::propagation::Extractor for MapCarrier {
    fn get(&self, key: &str) -> Option<&str> {
        self.0.get(key).map(|value| value.as_str())
    }

    fn keys(&self) -> Vec<&str> {
        self.0.keys().map(|key| key.as_str()).collect()
    }
}

impl opentelemetry::propagation::Injector for MapCarrier {
    fn set(&mut self, key: &str, value: String) {
        self.0.insert(key.to_string(), value);
    }
}

/// Converts string header pairs into a tonic metadata map — the
/// gRPC exporter's header carrier.
fn headers_to_metadata(headers: &[(String, String)]) -> tonic::metadata::MetadataMap {
    let mut metadata = tonic::metadata::MetadataMap::new();
    for (key, value) in headers {
        let parsed: tonic::metadata::MetadataValue<tonic::metadata::Ascii> = value
            .parse()
            .expect("header value must be valid metadata ASCII");
        let key: tonic::metadata::MetadataKey<tonic::metadata::Ascii> = key
            .parse()
            .expect("header key must be valid metadata ASCII");
        metadata.insert(key, parsed);
    }
    metadata
}

/// The extract half of the Go `Tracer.Start` for server/consumer
/// spans: pulls the remote trace context out of `carrier`.
pub fn extract(carrier: &MapCarrier) -> Context {
    global::get_text_map_propagator(|propagator| propagator.extract(carrier))
}

/// The inject half of the Go `Tracer.Start` for client/producer
/// spans: writes `context`'s trace context into `carrier`.
pub fn inject(context: &opentelemetry::Context, carrier: &mut MapCarrier) {
    global::get_text_map_propagator(|propagator| {
        propagator.inject_context(context, carrier);
    });
}
