//! Live conformance against a real consul, driven by CI's service
//! container. Compile-gated behind the `live` feature; the agent
//! address comes from `CONSUL_HTTP_ADDR` (a full base URL). These
//! tests pin the interoperability contract where it matters most — a
//! real consul serving the same view a Go-side consumer would see.

#![cfg(feature = "live")]

use std::time::Duration;

use rushwind_registry::{Discovery, Registrar, Registration};
use rushwind_registry_consul::{ConsulOptions, ConsulRegistry};
use rushwind_transport::Instance;

fn endpoint() -> String {
    std::env::var("CONSUL_HTTP_ADDR").unwrap_or_else(|_| "http://127.0.0.1:8500".to_string())
}

/// Each test gets its own instance id: parallel tests must not see
/// each other's registrations through the shared service name.
fn sample(id: &str) -> Registration {
    Registration::new(Instance {
        id: id.to_string(),
        name: "order-service".to_string(),
        version: "v1.0.0".to_string(),
        endpoints: vec!["grpc://127.0.0.1:9000".to_string()],
    })
}

/// The suite registrar: the Go registrar's shape with health-check
/// registration off. The TCP checks would target endpoints nothing
/// serves — from the container those ports are dead — which marks the
/// service critical and invisible to the passing-only health view. The
/// TTL check and its heartbeat stay on.
fn registry() -> ConsulRegistry {
    let options = ConsulOptions {
        enable_health_check: false,
        ..ConsulOptions::default()
    };
    ConsulRegistry::connect_with(&endpoint(), options).expect("consul connects")
}

/// The Go resolver's view of one registered instance, when the health
/// view contains it.
#[tokio::test]
async fn registered_instance_is_discoverable_then_removed() {
    let registry = registry();
    let registration = sample("order-01");
    let _handle = registry
        .register(registration.clone())
        .await
        .expect("register must succeed");

    // The TTL heartbeat marks its check passing within ~1 s; the
    // health view follows. Poll for the round trip: the endpoint URL
    // smuggled through the tagged address, and the version tag.
    let mut roundtrip = false;
    for _ in 0..30 {
        let seen = match registry.get_service("order-service").await {
            Ok(instances) => instances.iter().any(|instance| {
                instance.id == "order-01"
                    && instance.endpoints == registration.instance.endpoints
                    && instance.version == "v1.0.0"
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
    let mut gone = false;
    for _ in 0..30 {
        let still_listed = match registry.get_service("order-service").await {
            Ok(instances) => instances.iter().any(|instance| instance.id == "order-01"),
            Err(_) => false,
        };
        if !still_listed {
            gone = true;
            break;
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    assert!(gone, "the deregistered instance must leave discovery");
}

/// The reactive path: the poll loop's blocking query observes the
/// health-view change and broadcasts the instance list to the watcher.
#[tokio::test]
async fn watcher_delivers_the_registered_instance() {
    let registry = registry();
    let mut watcher = registry
        .watch("order-service")
        .await
        .expect("watch must succeed");

    let registration = sample("order-watch-01");
    let _handle = registry
        .register(registration.clone())
        .await
        .expect("register must succeed");

    let snapshot = tokio::time::timeout(Duration::from_secs(45), watcher.next())
        .await
        .expect("watch delivery arrives")
        .expect("watch delivery must succeed");
    let delivered = snapshot
        .iter()
        .find(|instance| instance.id == "order-watch-01")
        .filter(|instance| {
            instance.endpoints == registration.instance.endpoints && instance.version == "v1.0.0"
        });
    assert!(
        delivered.is_some(),
        "the watcher must deliver the registered instance, endpoint and version intact"
    );

    watcher.stop();
    registry
        .deregister(registration.clone())
        .await
        .expect("deregister must succeed");
}
