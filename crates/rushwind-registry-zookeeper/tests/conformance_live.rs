//! Live conformance against a real ZooKeeper, driven by CI's service
//! container. Compile-gated behind the `live` feature; the ensemble
//! address comes from `REGISTRY_ZOOKEEPER_ADDRESS`. These tests pin the
//! interoperability contract where it matters most — a real ZooKeeper
//! serving byte-exact go-wind wire values at the go-wind node layouts,
//! with child watches firing in both directions.

#![cfg(feature = "live")]

use std::time::Duration;

use rushwind_registry::{
    registry_json, registry_key, Discovery, Registrar, Registration, DEFAULT_NAMESPACE,
};
use rushwind_registry_zookeeper::ZookeeperRegistry;
use rushwind_transport::Instance;

fn endpoint() -> String {
    std::env::var("REGISTRY_ZOOKEEPER_ADDRESS").unwrap_or_else(|_| "127.0.0.1:2181".to_string())
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

#[tokio::test]
async fn registered_node_serves_the_go_wire_value() {
    let registry = ZookeeperRegistry::connect(&endpoint())
        .await
        .expect("zookeeper connects");
    let registration = sample("order-01");
    let _handle = registry
        .register(registration.clone())
        .await
        .expect("register must succeed");

    // A raw ZooKeeper read sees the byte-exact go-wind wire value at
    // the go-wind node — the interoperability contract, live.
    let key = registry_key(DEFAULT_NAMESPACE, &registration.instance);
    let raw = zookeeper_async::ZooKeeper::connect(&endpoint(), Duration::from_secs(5), |_| {})
        .await
        .expect("raw client connects");
    let (data, _) = raw.get_data(&key, false).await.expect("node must exist");
    assert_eq!(data, registry_json(&registration).into_bytes());

    // And the discovery read parses it back into the instance.
    let instances = registry
        .get_service("order-service")
        .await
        .expect("get_service must succeed");
    assert!(
        instances.iter().any(|instance| instance.id == "order-01"
            && instance.endpoints == registration.instance.endpoints
            && instance.version == "v1.0.0"),
        "the registered instance must round trip through discovery"
    );

    registry
        .deregister(registration.clone())
        .await
        .expect("deregister must succeed");
    assert!(
        raw.exists(&key, false)
            .await
            .expect("exists query works")
            .is_none(),
        "deregister removes the node immediately"
    );
    let instances = registry
        .get_service("order-service")
        .await
        .expect("get_service must succeed");
    assert!(
        instances.iter().all(|instance| instance.id != "order-01"),
        "the deregistered instance must leave discovery"
    );
}

/// The reactive path: the re-arm loop's child watch fires on both
/// instance creation and instance removal, each delivering a fresh
/// full snapshot.
#[tokio::test]
async fn watcher_delivers_addition_then_removal() {
    let registry = ZookeeperRegistry::connect(&endpoint())
        .await
        .expect("zookeeper connects");
    let registration_a = sample("order-watch-a");
    let _handle_a = registry
        .register(registration_a.clone())
        .await
        .expect("register must succeed");

    let mut watcher = registry
        .watch("order-service")
        .await
        .expect("watch must succeed");

    // Establishment snapshot: instance a is present.
    let snapshot = tokio::time::timeout(Duration::from_secs(30), watcher.next())
        .await
        .expect("establishment snapshot arrives")
        .expect("establishment snapshot must succeed");
    assert!(
        snapshot
            .iter()
            .any(|instance| instance.id == "order-watch-a"),
        "the establishment snapshot must include the registered instance"
    );

    // Creating instance b fires the child watch; the fresh snapshot
    // includes both instances.
    let registration_b = sample("order-watch-b");
    let _handle_b = registry
        .register(registration_b.clone())
        .await
        .expect("register must succeed");
    let snapshot = tokio::time::timeout(Duration::from_secs(30), watcher.next())
        .await
        .expect("addition snapshot arrives")
        .expect("addition snapshot must succeed");
    assert!(
        snapshot
            .iter()
            .any(|instance| instance.id == "order-watch-a")
            && snapshot
                .iter()
                .any(|instance| instance.id == "order-watch-b"),
        "the addition snapshot must include both instances"
    );

    // Removing instance b fires the child watch again; the fresh
    // snapshot excludes it.
    registry
        .deregister(registration_b.clone())
        .await
        .expect("deregister must succeed");
    let snapshot = tokio::time::timeout(Duration::from_secs(30), watcher.next())
        .await
        .expect("removal snapshot arrives")
        .expect("removal snapshot must succeed");
    assert!(
        snapshot
            .iter()
            .all(|instance| instance.id != "order-watch-b"),
        "the removal snapshot must exclude the deregistered instance"
    );

    watcher.stop();
    registry
        .deregister(registration_a.clone())
        .await
        .expect("deregister must succeed");
}
