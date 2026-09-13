//! Live conformance against a real etcd, driven by CI's service
//! container. Compile-gated behind the `live` feature; the endpoint comes
//! from `REGISTRY_ETCD_ENDPOINT`. These tests pin the interoperability
//! contract where it matters most — a real etcd serving byte-exact
//! go-wind wire values.

#![cfg(feature = "live")]

use std::time::Duration;

use rushwind_registry::{
    registry_json, registry_key, Discovery, Registrar, Registration, DEFAULT_NAMESPACE,
};
use rushwind_registry_etcd::EtcdRegistry;
use rushwind_transport::Instance;

fn endpoint() -> String {
    std::env::var("REGISTRY_ETCD_ENDPOINT").unwrap_or_else(|_| "http://localhost:2379".to_string())
}

/// Each test gets its own instance id: parallel tests must not keep each
/// other's registrations alive through racing keepalive loops.
fn sample(id: &str) -> Registration {
    Registration::new(Instance {
        id: id.to_string(),
        name: "order-service".to_string(),
        version: "v1.0.0".to_string(),
        endpoints: vec!["grpc://127.0.0.1:9000".to_string()],
    })
}

async fn raw_get(key: &str) -> Option<String> {
    let client = etcd_client::Client::connect([endpoint()], None)
        .await
        .expect("raw client connects");
    let mut kv = client.kv_client();
    let response = kv.get(key.as_bytes(), None).await.expect("raw get works");
    response
        .kvs()
        .first()
        .map(|kv| String::from_utf8_lossy(kv.value()).into_owned())
}

#[tokio::test]
async fn registered_key_serves_the_go_wire_value() {
    let registry = EtcdRegistry::connect(&[endpoint()])
        .await
        .expect("etcd connects");
    let registration = sample("order-01");
    let _handle = registry
        .register(registration.clone())
        .await
        .expect("register must succeed");

    // A raw etcd read sees the byte-exact go-wind wire value at the
    // go-wind key — the interoperability contract, live.
    let key = registry_key(DEFAULT_NAMESPACE, &registration.instance);
    let value = raw_get(&key).await.expect("key must exist");
    assert_eq!(value, registry_json(&registration));

    registry
        .deregister(registration.clone())
        .await
        .expect("deregister must succeed");
    assert!(
        raw_get(&key).await.is_none(),
        "deregister removes the key immediately"
    );
}

#[tokio::test]
async fn dropped_handle_expires_through_the_lease() {
    // TTL 5 s: the keepalive cadence is TTL/3, so a 1 s lease under CI
    // load can expire before the "must be live" assertion runs. Expiry
    // itself is still exercised — just with load-tolerant margins.
    let registry = EtcdRegistry::connect_with(&[endpoint()], DEFAULT_NAMESPACE, 5)
        .await
        .expect("etcd connects");
    // A unique key per test: parallel tests must not keep each other's
    // registrations alive through racing keepalive loops.
    let registration = sample("order-drop");
    let key = registry_key(DEFAULT_NAMESPACE, &registration.instance);

    let handle = registry
        .register(registration.clone())
        .await
        .expect("register must succeed");
    // Keep the registration alive briefly, then abandon it: aborting the
    // keepalive must let the lease expire and remove the key.
    assert!(raw_get(&key).await.is_some(), "registration must be live");
    drop(handle);
    tokio::time::sleep(Duration::from_secs(7)).await;
    assert!(
        raw_get(&key).await.is_none(),
        "dropping the handle must let the lease expire the key"
    );
}

/// Discovery must see exactly what registration wrote and what
/// deregistration removed: the live wire value parses back into the
/// discoverable instance, and leaves it on deregister.
#[tokio::test]
async fn get_service_lists_then_drops_the_registered_instance() {
    let registry = EtcdRegistry::connect(&[endpoint()])
        .await
        .expect("etcd connects");
    let registration = sample("order-disc-01");
    let _handle = registry
        .register(registration.clone())
        .await
        .expect("register must succeed");

    let instances = registry
        .get_service("order-service")
        .await
        .expect("get_service must succeed");
    assert!(
        instances
            .iter()
            .any(|instance| instance.id == "order-disc-01"),
        "the live wire value must parse back into a discoverable instance"
    );

    registry
        .deregister(registration.clone())
        .await
        .expect("deregister must succeed");
    let instances = registry
        .get_service("order-service")
        .await
        .expect("get_service must succeed");
    assert!(
        instances
            .iter()
            .all(|instance| instance.id != "order-disc-01"),
        "the deregistered instance must leave discovery"
    );
}

/// The watcher's establishment snapshot contains the registered instance;
/// deleting its key surfaces as a fresh snapshot without it — the
/// reactive change stream, live.
#[tokio::test]
async fn watcher_snapshots_registration_then_removal() {
    let registry = EtcdRegistry::connect(&[endpoint()])
        .await
        .expect("etcd connects");
    let registration = sample("order-watch-01");
    let _handle = registry
        .register(registration.clone())
        .await
        .expect("register must succeed");

    let mut watcher = registry
        .watch("order-service")
        .await
        .expect("watch must succeed");

    // Establishment snapshot: the registered instance must be present.
    let snapshot = tokio::time::timeout(Duration::from_secs(10), watcher.next())
        .await
        .expect("establishment snapshot arrives")
        .expect("establishment snapshot must succeed");
    assert!(
        snapshot
            .iter()
            .any(|instance| instance.id == "order-watch-01"),
        "the establishment snapshot must include the registered instance"
    );

    // Deleting the key must surface as a fresh snapshot without it.
    registry
        .deregister(registration.clone())
        .await
        .expect("deregister must succeed");
    let snapshot = tokio::time::timeout(Duration::from_secs(10), watcher.next())
        .await
        .expect("removal snapshot arrives")
        .expect("removal snapshot must succeed");
    assert!(
        snapshot
            .iter()
            .all(|instance| instance.id != "order-watch-01"),
        "the removal snapshot must exclude the deregistered instance"
    );

    watcher.stop();
}
