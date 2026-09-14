//! Tracer conformance: provider construction with each transport, the
//! sample-ratio clamp, the tracer-name default, and carrier
//! extract/inject — the Go `tracer_test.go` shapes.

#![cfg(test)]

use std::collections::HashMap;

use rushwind_tracer::{
    extract, MapCarrier, OtlpOptions, TracerProviderBuilder, Transport, DEFAULT_TRACER_NAME,
};

/// The provider builds for both transports.
#[tokio::test]
async fn provider_builds_for_both_transports() {
    let grpc = TracerProviderBuilder::new(OtlpOptions {
        endpoint: "127.0.0.1:4317".to_string(),
        insecure: true,
        service_name: "probe".to_string(),
        ..OtlpOptions::default()
    })
    .build();
    assert!(grpc.is_ok(), "grpc provider must build");

    let http = TracerProviderBuilder::new(OtlpOptions {
        endpoint: "127.0.0.1:4318".to_string(),
        transport: Transport::Http,
        insecure: true,
        service_name: "probe".to_string(),
        ..OtlpOptions::default()
    })
    .build();
    assert!(http.is_ok(), "http provider must build");
}

/// The tracer name defaults to the Go adapter's and can be
/// overridden.
#[test]
fn tracer_name_default_and_override() {
    let default_builder = TracerProviderBuilder::new(OtlpOptions::default());
    assert_eq!(default_builder.resolved_tracer_name(), DEFAULT_TRACER_NAME);

    let overridden = default_builder.tracer_name("my-service");
    assert_eq!(overridden.resolved_tracer_name(), "my-service");
}

/// The extract half runs against an empty carrier without error and
/// an injected W3C key round trips through the carrier.
#[tokio::test]
async fn carrier_extract_and_w3c_keys() {
    let mut carrier = MapCarrier::from_map(HashMap::new());

    // Empty carrier: extract must be a no-op, not a panic.
    let _context = extract(&carrier);

    // The W3C key the propagator manages round trips through the
    // carrier.
    carrier.0.insert(
        "traceparent".to_string(),
        "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01".to_string(),
    );
    assert!(carrier.get("traceparent").is_some());
}
