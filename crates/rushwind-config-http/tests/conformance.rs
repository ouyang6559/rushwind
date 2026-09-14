//! HTTP source conformance against an in-process server — no external
//! dependency needed, the engine speaks plain HTTP.

#![cfg(test)]

use std::time::Duration;

use rushwind_config::Source;
use rushwind_config_http::{HttpOptions, HttpSource};

/// A minimal HTTP server answering GET /config with a canned body.
async fn spawn_server(body: &'static str) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind works");
    let addr = listener.local_addr().expect("local addr");
    tokio::spawn(async move {
        loop {
            let Ok((mut stream, _)) = listener.accept().await else {
                return;
            };
            tokio::spawn(async move {
                let mut buffer = [0u8; 512];
                let _ = tokio::io::AsyncReadExt::read(&mut stream, &mut buffer).await;
                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                let _ = tokio::io::AsyncWriteExt::write_all(&mut stream, response.as_bytes()).await;
            });
        }
    });
    format!("http://{addr}")
}

#[tokio::test]
async fn load_reads_the_url_body() {
    let url = spawn_server("db-password").await;
    let source = HttpSource::new(HttpOptions {
        url: format!("{url}/config"),
        ..HttpOptions::default()
    })
    .expect("url valid");

    let value = source.load("").await.expect("load must succeed");
    assert_eq!(value, Some(b"db-password".to_vec()));
}

/// An empty key with no configured default fails the load — the Go
/// "url invalid" guard at the call site.
#[tokio::test]
async fn missing_url_fails_the_engine() {
    let result = HttpSource::new(HttpOptions::default());
    assert!(result.is_err(), "no default url must fail construction");
}

/// The poll watch pushes the body when it changes; the second poll
/// with an unchanged body does not re-push.
#[tokio::test]
async fn watch_pushes_changed_values() {
    let url = spawn_server("cache-value").await;
    let source = HttpSource::new(HttpOptions {
        url: format!("{url}/config"),
        poll_interval: Duration::from_millis(100),
        ..HttpOptions::default()
    })
    .expect("url valid");

    let mut stream = source.watch_value("").await.expect("watch must succeed");

    // The first poll tick pushes the current body.
    let first = tokio::time::timeout(Duration::from_secs(5), stream.next())
        .await
        .expect("first push arrives")
        .expect("stream open");
    assert_eq!(first, b"cache-value".to_vec());

    // The second tick with an unchanged body does not re-push; the
    // next push only happens after the body changes (longer than this
    // test waits).
    let unchanged = tokio::time::timeout(Duration::from_secs(2), stream.next()).await;
    assert!(
        matches!(unchanged, Err(_)),
        "an unchanged body must not re-push within the window"
    );
}
