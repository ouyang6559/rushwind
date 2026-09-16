//! Live conformance against a real service-center, driven by CI's
//! service container. Compile-gated behind the `live` feature; the
//! address comes from `REGISTRY_SERVICECOMB_ADDRESS`. These tests pin
//! the interoperability contract where it matters most — a real
//! service-center serving the same microservice view any external
//! consumer would see.

#![cfg(feature = "live")]

use std::time::Duration;

use rushwind_registry::{Discovery, Registrar, Registration};
use rushwind_registry_servicecomb::ServicecombRegistry;
use rushwind_transport::Instance;

fn endpoint() -> String {
    std::env::var("REGISTRY_SERVICECOMB_ADDRESS")
        .unwrap_or_else(|_| "http://127.0.0.1:30100".to_string())
}

/// Service-center keys instances by their declared id, and the
/// registry's watch machinery is per service name: each test gets its
/// own service name, instance id, and port, so parallel tests never
/// share a microservice or its event stream.
fn sample(service: &str, id: &str, port: u16) -> Registration {
    Registration::new(Instance {
        id: id.to_string(),
        name: service.to_string(),
        version: "1.0.0".to_string(),
        endpoints: vec![format!("grpc://127.0.0.1:{port}")],
    })
}

/// The round trip: register, find through the v4 API, deregister and
/// watch it leave. The rebuild carries the server's quirk — the instance
/// version field is the service id, so it is asserted present-but-
/// opaque rather than round-tripped.
#[tokio::test]
async fn registered_instance_round_trips_then_leaves() {
    let registry = ServicecombRegistry::connect(&endpoint()).expect("service-center connects");
    let registration = sample("order-roundtrip", "order-01", 9000);
    let _handle = registry
        .register(registration.clone())
        .await
        .expect("register must succeed");

    let mut roundtrip = false;
    for _ in 0..60 {
        let seen = match registry.get_service("order-roundtrip").await {
            Ok(instances) => instances.iter().any(|instance| {
                instance.id == "order-01"
                    && instance.endpoints == registration.instance.endpoints
                    && !instance.version.is_empty()
            }),
            Err(_) => false,
        };
        if seen {
            roundtrip = true;
            break;
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    assert!(
        roundtrip,
        "the registered instance must round trip through discovery"
    );

    registry
        .deregister(registration.clone())
        .await
        .expect("deregister must succeed");
    // The find path serves a view cached for ~30 s server-side, so
    // the removal lands within that cache's next refresh.
    let mut gone = false;
    for _ in 0..90 {
        let still_listed = match registry.get_service("order-roundtrip").await {
            Ok(instances) => instances.iter().any(|instance| instance.id == "order-01"),
            Err(_) => false,
        };
        if !still_listed {
            gone = true;
            break;
        }
        tokio::time::sleep(Duration::from_secs(3)).await;
    }
    assert!(gone, "the deregistered instance must leave discovery");
}

/// The reactive path: the WebSocket watcher forwards per-instance
/// events as one-instance snapshots — the addition when an instance
/// registers, the removal when it deregisters.
#[tokio::test]
async fn watcher_delivers_addition_then_removal() {
    let registry = ServicecombRegistry::connect(&endpoint()).expect("service-center connects");
    let registration = sample("order-watch", "order-watch-01", 9001);
    let _handle = registry
        .register(registration.clone())
        .await
        .expect("register must succeed");

    let mut watcher = registry
        .watch("order-watch")
        .await
        .expect("watch must succeed");

    let second = sample("order-watch", "order-watch-02", 9002);
    let _second_handle = registry
        .register(second.clone())
        .await
        .expect("register must succeed");

    // Per-instance events: retry deliveries until the watched-for
    // instance's addition arrives.
    let mut delivered = false;
    for _ in 0..60 {
        let snapshot = match tokio::time::timeout(Duration::from_secs(5), watcher.next()).await {
            Ok(Ok(instances)) => instances,
            _ => continue,
        };
        if snapshot.iter().any(|instance| {
            instance.id == "order-watch-02" && instance.endpoints == second.instance.endpoints
        }) {
            delivered = true;
            break;
        }
    }
    assert!(
        delivered,
        "the watcher must deliver the registered instance, endpoints intact"
    );

    registry
        .deregister(second.clone())
        .await
        .expect("deregister must succeed");
    let mut removed = false;
    for _ in 0..60 {
        let snapshot = match tokio::time::timeout(Duration::from_secs(5), watcher.next()).await {
            Ok(Ok(instances)) => instances,
            _ => continue,
        };
        if snapshot
            .iter()
            .any(|instance| instance.id == "order-watch-02")
        {
            removed = true;
            break;
        }
    }
    assert!(removed, "the watcher must deliver the removal event");

    watcher.stop();
    registry
        .deregister(registration.clone())
        .await
        .expect("deregister must succeed");
}
