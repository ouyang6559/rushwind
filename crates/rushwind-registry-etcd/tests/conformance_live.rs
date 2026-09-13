//! Live conformance against a real etcd, driven by CI's service
//! container. Compile-gated behind the `live` feature; the endpoint comes
//! from `REGISTRY_ETCD_ENDPOINT`. These tests pin the interoperability
//! contract where it matters most — a real etcd serving byte-exact
//! go-wind wire values.

#![cfg(feature = "live")]

use std::time::Duration;

use rushwind_registry::{registry_json, registry_key, Registrar, Registration, DEFAULT_NAMESPACE};
use rushwind_registry_etcd::EtcdRegistrar;
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
    let registrar = EtcdRegistrar::connect(&[endpoint()])
        .await
        .expect("etcd connects");
    let registration = sample("order-01");
    let _handle = registrar
        .register(registration.clone())
        .await
        .expect("register must succeed");

    // A raw etcd read sees the byte-exact go-wind wire value at the
    // go-wind key — the interoperability contract, live.
    let key = registry_key(DEFAULT_NAMESPACE, &registration.instance);
    let value = raw_get(&key).await.expect("key must exist");
    assert_eq!(value, registry_json(&registration));

    registrar
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
    let registrar = EtcdRegistrar::connect_with(&[endpoint()], DEFAULT_NAMESPACE, 1)
        .await
        .expect("etcd connects");
    // A unique key per test: parallel tests must not keep each other's
    // registrations alive through racing keepalive loops.
    let registration = sample("order-drop");
    let key = registry_key(DEFAULT_NAMESPACE, &registration.instance);

    let handle = registrar
        .register(registration.clone())
        .await
        .expect("register must succeed");
    // Keep the registration alive briefly, then abandon it: aborting the
    // keepalive must let the 1 s lease expire and remove the key.
    assert!(raw_get(&key).await.is_some(), "registration must be live");
    drop(handle);
    tokio::time::sleep(Duration::from_millis(2500)).await;
    assert!(
        raw_get(&key).await.is_none(),
        "dropping the handle must let the lease expire the key"
    );
}
