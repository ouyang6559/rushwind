//! Shared test fixtures: an embedded rumqttd broker on an OS-assigned
//! port, and a publish helper whose completion is observed via the QoS 1
//! acknowledgement.
#![allow(dead_code)]

use std::net::SocketAddr;
use std::time::{Duration, Instant};

/// Starts an embedded rumqttd broker on a free loopback port and returns
/// its address. The broker runs on its own threads for the remainder of
/// the test process.
pub fn broker() -> SocketAddr {
    // Pre-bind to pick a free port, release it, and hand the port to the
    // embedded broker (rumqttd does not report the bound port of a :0
    // listener).
    let probe = std::net::TcpListener::bind(("127.0.0.1", 0)).expect("probe bind must succeed");
    let port = probe.local_addr().expect("probe local_addr").port();
    drop(probe);

    let config_json = serde_json::json!({
        "id": 0,
        "router": {
            "max_connections": 100,
            "max_outgoing_packet_count": 100,
            "max_segment_size": 1_048_576,
            "max_segment_count": 10
        },
        "v4": {
            "1": {
                "name": "test-broker",
                "listen": format!("127.0.0.1:{port}"),
                "tls": null,
                "next_connection_delay_ms": 1,
                "connections": {
                    "connection_timeout_ms": 60000,
                    "max_payload_size": 1_048_576,
                    "max_inflight_count": 100
                }
            }
        }
    });
    let config: rumqttd::Config =
        serde_json::from_value(config_json).expect("broker config must deserialize");

    // Broker::start joins its server threads and never returns while the
    // broker is healthy — it is designed to be called from main. Run it on
    // a dedicated thread and continue with the test.
    std::thread::Builder::new()
        .name("embedded-mqtt-broker".to_string())
        .spawn(move || {
            let mut broker = rumqttd::Broker::new(config);
            broker.start().expect("embedded broker must start");
        })
        .expect("broker thread must spawn");

    let addr = SocketAddr::from(([127, 0, 0, 1], port));
    wait_for_broker(addr);
    addr
}

/// Blocks until the broker accepts TCP connections.
fn wait_for_broker(addr: SocketAddr) {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if std::net::TcpStream::connect_timeout(&addr, Duration::from_millis(200)).is_ok() {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "embedded broker never became reachable"
        );
        std::thread::sleep(Duration::from_millis(50));
    }
}

/// Publishes one QoS 1 message and waits for its acknowledgement.
pub async fn publish(addr: SocketAddr, client_id: &str, topic: &str, payload: &[u8]) {
    let options = rumqttc::MqttOptions::new(client_id, addr.ip().to_string(), addr.port());
    let (client, mut eventloop) = rumqttc::AsyncClient::new(options, 16);
    let (done_tx, done_rx) = tokio::sync::oneshot::channel::<()>();
    tokio::spawn(async move {
        loop {
            match eventloop.poll().await {
                // The QoS 1 ack proves the broker accepted the message;
                // the pump's job is done.
                Ok(rumqttc::Event::Incoming(rumqttc::Incoming::PubAck(_))) => {
                    let _ = done_tx.send(());
                    return;
                }
                Ok(_) => {}
                Err(_) => return,
            }
        }
    });
    client
        .publish(topic, rumqttc::QoS::AtLeastOnce, false, payload)
        .await
        .expect("publish must enqueue");
    tokio::time::timeout(Duration::from_secs(5), done_rx)
        .await
        .expect("publish must be acked in time")
        .ok();
}
