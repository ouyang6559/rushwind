//! MQTT consumer demo: subscribes to `demo/#` on an external broker and
//! prints every message, under the RushWind lifecycle.
//!
//! The broker is external infrastructure — point this at whatever you run
//! (EMQX, mosquitto, rumqttd in another terminal). The bridge reconnects
//! with capped backoff if the broker restarts, and lifecycle shutdown
//! (Ctrl+C / SIGTERM) ends the pump cleanly.
//!
//! Try it: `cargo run -p mqtt-ingest -- 127.0.0.1:1883`, then publish to
//! `demo/hello` from any MQTT client.

use std::net::SocketAddr;
use std::sync::Arc;

use rushwind_core::App;
use rushwind_transport::StopSignal;
use rushwind_transport_mqtt::MqttBridge;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let broker: SocketAddr = match std::env::args().nth(1) {
        Some(arg) => arg.parse()?,
        None => SocketAddr::from(([127, 0, 0, 1], 1883)),
    };

    let bridge = MqttBridge::builder(broker, "rushwind-mqtt-ingest-demo")
        .subscribe("demo/#")
        .session_handler(|message| async move {
            println!(
                "[ingest] {} = {}",
                message.topic,
                String::from_utf8_lossy(&message.payload)
            );
        })
        .build()?;
    println!("[ingest] consuming from mqtt://{broker} on demo/#");

    let app = App::builder()
        .name("mqtt-ingest-demo")
        .version("0.0.1")
        .erased_server(Arc::new(bridge))
        .build();
    let result = app.run(StopSignal::new()).await;
    println!("[ingest] lifecycle finished: {result:?}");
    Ok(())
}
