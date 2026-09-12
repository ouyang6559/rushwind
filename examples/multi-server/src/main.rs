//! Multi-server lifecycle demo.
//!
//! Two toy listeners — one standing in for an admin/API surface, one for a
//! realtime session surface — run concurrently under a single [`App`].
//! Trigger a shutdown with Ctrl+C (or SIGTERM on Unix): every server's
//! teardown runs in the bounded stop phase, then the after-stop hook fires,
//! and `run` returns the terminal outcome.

use std::sync::Arc;
use std::time::Duration;

use rushwind_core::App;
use rushwind_transport::{Server, ServerError, ServerFuture, StopSignal};

/// A toy listener that idles until signalled.
struct ToyServer {
    label: &'static str,
}

impl Server for ToyServer {
    fn endpoint(&self) -> Result<String, ServerError> {
        Ok(format!("toy://{}", self.label))
    }

    fn start(&self, stop: StopSignal) -> ServerFuture<'_> {
        Box::pin(async move {
            println!("[{}] started", self.label);
            stop.wait().await;
            Err(ServerError::Cancelled)
        })
    }

    fn stop(&self) -> ServerFuture<'_> {
        Box::pin(async move {
            println!("[{}] stopped", self.label);
            Ok(())
        })
    }
}

#[tokio::main]
async fn main() {
    let app = App::builder()
        .name("multi-server-demo")
        .version("0.0.1")
        .server(Arc::new(ToyServer { label: "admin-api" }))
        .server(Arc::new(ToyServer {
            label: "session-gateway",
        }))
        .stop_timeout(Duration::from_secs(10))
        .after_stop(|_| async {
            println!("[demo] after-stop hook: buffers flushed");
            Ok(())
        })
        .build();

    println!(
        "[demo] {} v{} running with two listeners; press Ctrl+C to shut down",
        app.name().unwrap_or("unnamed"),
        app.version().unwrap_or("?")
    );

    let result = app.run(StopSignal::new()).await;
    println!("[demo] lifecycle finished: {result:?}");
}
