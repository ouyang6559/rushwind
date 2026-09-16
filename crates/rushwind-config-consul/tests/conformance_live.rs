//! Live conformance against a real Consul, driven by CI's service
//! container. Compile-gated behind the `live` feature; the address
//! comes from `CONFIG_CONSUL_ADDR`.

#![cfg(feature = "live")]

use rushwind_config::Source;
use rushwind_config_consul::ConsulSource;

fn addr() -> String {
    std::env::var("CONFIG_CONSUL_ADDR").unwrap_or_else(|_| "http://127.0.0.1:8500".to_string())
}

/// Load reads the live KV value; an absent key reads as None.
#[tokio::test]
async fn load_reads_and_reports_absent() {
    let source = ConsulSource::new(&addr(), "rushwind-test/cfg").expect("path valid");

    let absent = source
        .load("rushwind-test/never-here")
        .await
        .expect("load must succeed");
    assert_eq!(absent, None, "an absent key reads as None");

    // Seed through the raw HTTP API.
    let client = reqwest::Client::new();
    client
        .put(format!("{}/v1/kv/rushwind-test/cfg", addr()))
        .body(b"live-value".to_vec())
        .send()
        .await
        .expect("seed put must succeed");

    let value = source
        .load("rushwind-test/cfg")
        .await
        .expect("load must succeed");
    assert_eq!(value, Some(b"live-value".to_vec()));
}
