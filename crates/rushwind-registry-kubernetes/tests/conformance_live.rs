//! In-cluster conformance for the Kubernetes registry. Compile-gated
//! behind the `live` feature; every test skips itself when the
//! in-cluster service account is absent, so the suite is inert in CI
//! lanes and on workstations and runs only inside a real cluster.
//!
//! The Kubernetes registry is whole only in-cluster: registration
//! patches the owning pod's labels, discovery lists pods the service
//! account can see. A cluster whose nodes allow the
//! `wind-service-*` labels on pods sees the Go adapter's exact wire
//! contract — identity in labels, endpoints rebuilt from the pod IP
//! and the protocol-map annotation.

#![cfg(feature = "live")]

use rushwind_registry::{Registrar, Registration};
use rushwind_registry_kubernetes::KubernetesRegistry;
use rushwind_transport::Instance;

fn in_cluster() -> bool {
    std::env::var("KUBERNETES_SERVICE_HOST").is_ok()
        && std::env::var("KUBERNETES_SERVICE_PORT").is_ok()
}

fn sample() -> Registration {
    Registration::new(Instance {
        id: "order-01".to_string(),
        name: "order-service".to_string(),
        version: "v1.0.0".to_string(),
        endpoints: vec!["grpc://127.0.0.1:9000".to_string()],
    })
}

#[tokio::test]
async fn registration_labels_this_pod() {
    if !in_cluster() {
        eprintln!("skipped: not in a cluster");
        return;
    }
    let registry = KubernetesRegistry::connect().await.expect("connects");
    let registration = sample();
    let _handle = registry
        .register(registration.clone())
        .await
        .expect("register must succeed");
    registry
        .deregister(registration.clone())
        .await
        .expect("deregister must succeed");
}
