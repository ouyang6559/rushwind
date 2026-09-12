//! Lifecycle conformance for the MQTT consumer bridge.

use std::sync::Arc;

use rushwind_transport::Server;
use rushwind_transport_mqtt::MqttBridge;

mod common;

fn mqtt_bridge() -> Arc<dyn Server> {
    let server = MqttBridge::builder(common::broker(), "rushwind-conformance")
        .session_handler(|_message| async {})
        .build()
        .expect("bridge must build");
    Arc::new(server)
}

rushwind_testkit::rushwind_conformance_suite!(crate::mqtt_bridge);
