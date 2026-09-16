//! Live conformance against a real polaris server, driven by CI's
//! service container. Compile-gated behind the `live` feature; the
//! HTTP endpoint comes from `REGISTRY_POLARIS_ADDRESS`. These tests
//! pin the interoperability contract where it matters most — a real
//! polaris serving the same instance view any external consumer would
//! see. The name asymmetry is preserved: registrations land under
//! `{name}{scheme}`, so the assertions query that name.

#![cfg(feature = "live")]

use std::time::Duration;

use rushwind_registry::{Discovery, Registrar, Registration};
use rushwind_registry_polaris::PolarisRegistry;
use rushwind_transport::Instance;

fn endpoint() -> String {
    std::env::var("REGISTRY_POLARIS_ADDRESS")
        .unwrap_or_else(|_| "http://127.0.0.1:8090".to_string())
}

/// Polaris assigns each instance a server-side id and keys instances
/// by `{namespace, service, host, port}`: instances are identified by
/// the endpoint and version that do round trip, and each test gets
/// its own port so parallel tests hold distinct server-side records.
fn sample(port: u16) -> Registration {
    Registration::new(Instance {
        id: "order-01".to_string(),
        name: "order-service".to_string(),
        version: "1.0.0".to_string(),
        endpoints: vec![format!("grpc://127.0.0.1:{port}")],
    })
}

#[tokio::test]
async fn registered_instance_round_trips_then_leaves() {
    let registry = PolarisRegistry::connect(&endpoint()).expect("polaris connects");
    let registration = sample(9000);
    let _handle = registry
        .register(registration.clone())
        .await
        .expect("register must succeed");

    let mut roundtrip = false;
    for _ in 0..30 {
        let seen = match registry.get_service("order-servicegrpc").await {
            Ok(instances) => instances.iter().any(|instance| {
                instance.endpoints == registration.instance.endpoints && instance.version == "1.0.0"
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
        let still_listed = match registry.get_service("order-servicegrpc").await {
            Ok(instances) => instances.iter().any(|instance| {
                instance.endpoints == registration.instance.endpoints && instance.version == "1.0.0"
            }),
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

/// The reactive path: the poll loop notices the healthy list change
/// and wakes the watcher — the addition when an instance registers,
/// the removal when it deregisters.
#[tokio::test]
async fn watcher_delivers_addition_then_removal() {
    let registry = PolarisRegistry::connect(&endpoint()).expect("polaris connects");
    let registration = sample(9001);
    let _handle = registry
        .register(registration.clone())
        .await
        .expect("register must succeed");

    let mut watcher = registry
        .watch("order-servicegrpc")
        .await
        .expect("watch must succeed");

    let mut delivered = false;
    for _ in 0..30 {
        let snapshot = match tokio::time::timeout(Duration::from_secs(10), watcher.next()).await {
            Ok(Ok(instances)) => instances,
            _ => continue,
        };
        if snapshot
            .iter()
            .any(|instance| instance.endpoints == registration.instance.endpoints)
        {
            delivered = true;
            break;
        }
    }
    assert!(
        delivered,
        "the watcher must deliver the registered instance, endpoints intact"
    );

    registry
        .deregister(registration.clone())
        .await
        .expect("deregister must succeed");
    let mut removed = false;
    for _ in 0..30 {
        let snapshot = match tokio::time::timeout(Duration::from_secs(10), watcher.next()).await {
            Ok(Ok(instances)) => instances,
            _ => continue,
        };
        if snapshot
            .iter()
            .all(|instance| instance.endpoints != registration.instance.endpoints)
        {
            removed = true;
            break;
        }
    }
    assert!(removed, "the watcher must deliver the removal");
    watcher.stop();
}
