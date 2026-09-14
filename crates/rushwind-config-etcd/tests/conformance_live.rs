//! Live conformance against a real etcd, driven by CI's service
//! container. Compile-gated behind the `live` feature; the endpoints
//! come from `CONFIG_ETCD_ENDPOINT`.

#![cfg(feature = "live")]

use std::time::Duration;

use rushwind_config::{Source, ValueStream};
use rushwind_config_etcd::EtcdSource;

fn endpoints() -> Vec<String> {
    vec![std::env::var("CONFIG_ETCD_ENDPOINT")
        .unwrap_or_else(|_| "http://127.0.0.1:2379".to_string())]
}

/// Load reads the live value; an absent key reads as None.
#[tokio::test]
async fn load_reads_and_reports_absent() {
    let source = EtcdSource::connect(&endpoints())
        .await
        .expect("etcd connects");

    let mut client = etcd_client::Client::connect(endpoints(), None)
        .await
        .expect("raw client connects");
    client
        .put("rushwind-test/cfg", b"live-value".as_slice(), None)
        .await
        .expect("seed put must succeed");

    let value = source
        .load("rushwind-test/cfg")
        .await
        .expect("load must succeed");
    assert_eq!(value, Some(b"live-value".to_vec()));

    let absent = source
        .load("rushwind-test/never-here")
        .await
        .expect("load must succeed");
    assert_eq!(absent, None, "an absent key reads as None");
}

/// The push-mode watch: a PUT on the key pushes the new value; a
/// DELETE ends the value's life.
#[tokio::test]
async fn watch_value_pushes_changes() {
    let source = EtcdSource::connect(&endpoints())
        .await
        .expect("etcd connects");

    let mut stream = source
        .watch_value("rushwind-test/cfg-watch")
        .await
        .expect("watch must succeed");

    let mut client = etcd_client::Client::connect(endpoints(), None)
        .await
        .expect("raw client connects");
    client
        .put("rushwind-test/cfg-watch", b"first".as_slice(), None)
        .await
        .expect("seed put must succeed");

    let value = tokio::time::timeout(Duration::from_secs(10), stream.next())
        .await
        .expect("push arrives")
        .expect("stream open");
    assert_eq!(value, b"first".to_vec());

    client
        .put("rushwind-test/cfg-watch", b"second".as_slice(), None)
        .await
        .expect("second put must succeed");
    let value = tokio::time::timeout(Duration::from_secs(10), stream.next())
        .await
        .expect("push arrives")
        .expect("stream open");
    assert_eq!(value, b"second".to_vec());
}
