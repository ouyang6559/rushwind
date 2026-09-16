//! Live conformance against a real nacos server, driven by CI's
//! service container. Compile-gated behind the `live` feature; the
//! server address comes from `REGISTRY_NACOS_ADDRESS`. These tests pin
//! the interoperability contract where it matters most — a real nacos
//! serving the same instance view any external consumer would see.
//!
//! Registration targets `{name}.{scheme}` but a bare-name query under
//! `{name}` finds nothing — an asymmetry of the underlying service
//! model, so discovery here
//! targets the suffixed service name, the one that actually holds
//! the registered instances.

#![cfg(feature = "live")]

use std::time::Duration;

use rushwind_registry::{Discovery, Registrar, Registration};
use rushwind_registry_nacos::NacosRegistry;
use rushwind_transport::Instance;

fn endpoint() -> String {
    std::env::var("REGISTRY_NACOS_ADDRESS").unwrap_or_else(|_| "127.0.0.1:8848".to_string())
}

/// Nacos assigns each instance a server-generated id — the registered
/// id never round trips — so instances are identified by the endpoint
/// and version that do. Nacos also keys instances by endpoint address:
/// each test gets its own version AND port, so parallel tests hold
/// distinct server-side instance records.
fn sample(version: &str, port: u16) -> Registration {
    Registration::new(Instance {
        id: "order-01".to_string(),
        name: "order-service".to_string(),
        version: version.to_string(),
        endpoints: vec![format!("grpc://127.0.0.1:{port}")],
    })
}

#[tokio::test]
async fn registered_instance_round_trips_then_leaves() {
    let registry = NacosRegistry::connect(&endpoint())
        .await
        .expect("nacos connects");
    let registration = sample("v1.0.0-roundtrip", 9000);
    let _handle = registry
        .register(registration.clone())
        .await
        .expect("register must succeed");

    let mut roundtrip = false;
    for _ in 0..30 {
        let seen = match registry.get_service("order-service.grpc").await {
            Ok(instances) => instances.iter().any(|instance| {
                instance.endpoints == registration.instance.endpoints
                    && instance.version == registration.instance.version
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
        let still_listed = match registry.get_service("order-service.grpc").await {
            Ok(instances) => instances.iter().any(|instance| {
                instance.endpoints == registration.instance.endpoints
                    && instance.version == registration.instance.version
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

/// The reactive path: the SDK subscription's pushes deliver fresh
/// snapshots — the addition when the instance registers, the removal
/// when it deregisters. Push-channel establishment is bimodal **per
/// connection** on this server, and the
/// server's instance visibility lags registration by seconds, so each
/// phase retries with a fresh subscription on a fresh connection until
/// a snapshot carrying the change arrives. The registration itself
/// stays bound to the owning registry's connection — ephemeral
/// instances die with it — and so does the deregistration.
#[tokio::test]
async fn watcher_delivers_addition_then_removal() {
    let registry = NacosRegistry::connect(&endpoint())
        .await
        .expect("nacos connects");
    let registration = sample("v1.0.0-watch", 9001);
    let _handle = registry
        .register(registration.clone())
        .await
        .expect("register must succeed");

    assert!(
        await_snapshot(&endpoint(), &registration, true).await,
        "the watcher must deliver the registered instance, endpoint and version intact"
    );

    registry
        .deregister(registration.clone())
        .await
        .expect("deregister must succeed");
    assert!(
        await_snapshot(&endpoint(), &registration, false).await,
        "the watcher must deliver the removal"
    );
}

/// Pops fresh watch connections until one delivers a snapshot whose
/// view of `registration`'s instance matches `present`.
async fn await_snapshot(endpoint: &str, registration: &Registration, present: bool) -> bool {
    for _round in 0..6 {
        let Ok(watch_registry) = NacosRegistry::connect(endpoint).await else {
            continue;
        };
        let Ok(mut watcher) = watch_registry.watch("order-service.grpc").await else {
            continue;
        };
        for _ in 0..6 {
            let snapshot = match tokio::time::timeout(Duration::from_secs(5), watcher.next()).await
            {
                Ok(Ok(instances)) => instances,
                _ => continue,
            };
            let matches = snapshot.iter().any(|instance| {
                instance.endpoints == registration.instance.endpoints
                    && instance.version == registration.instance.version
            });
            if matches == present {
                watcher.stop();
                return true;
            }
        }
        watcher.stop();
    }
    false
}
