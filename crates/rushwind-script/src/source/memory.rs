//! The in-memory script source.
//!
//! Serves scripts from a map with zero IO — dynamic short-lived
//! scripts, unit tests, RPC-pushed snippets. `set`/`delete` both
//! notify watchers; the notification carrier is a
//! tokio `watch` channel (coalesced latest-state signaling,
//! buffered-1 semantics) rather than one OS channel per
//! watcher.
//!
//! Watchers are stored as one
//! `watch::Sender` per watched key rather than a list of per-watcher
//! channels — every receiver sees the same coalesced signal, and a
//! dropped stream simply stops its receiver without touching the
//! sender, so dropping the stream is the unregistration. The source
//! dropping ends every one of its streams.

use std::collections::HashMap;
use std::sync::Mutex;

use tokio::sync::watch;

use crate::source::{ScriptSource, SignalStream};
use crate::{BoxFuture, ScriptError};

#[derive(Default)]
struct MemCell {
    code: Option<String>,
    signal: Option<watch::Sender<()>>,
}

#[derive(Default)]
struct MemState {
    cells: HashMap<String, MemCell>,
}

/// A source keeping its scripts in memory.
pub struct MemSource {
    state: Mutex<MemState>,
}

impl MemSource {
    /// Creates an empty memory source.
    pub fn new() -> Self {
        Self {
            state: Mutex::new(MemState::default()),
        }
    }

    /// Inserts or overwrites a script and notifies the key's watchers.
    pub fn set(&self, key: &str, code: &str) {
        let mut state = self.state.lock().expect("mem source lock");
        let cell = state.cells.entry(key.to_string()).or_default();
        cell.code = Some(code.to_string());
        if let Some(signal) = &cell.signal {
            signal.send_replace(());
        }
    }

    /// Removes a script and notifies the key's watchers.
    pub fn delete(&self, key: &str) {
        let mut state = self.state.lock().expect("mem source lock");
        if let Some(cell) = state.cells.get_mut(key) {
            cell.code = None;
            if let Some(signal) = &cell.signal {
                signal.send_replace(());
            }
        }
    }
}

impl Default for MemSource {
    fn default() -> Self {
        Self::new()
    }
}

impl ScriptSource for MemSource {
    fn load<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<String, ScriptError>> {
        let code = {
            let state = self.state.lock().expect("mem source lock");
            state
                .cells
                .get(key)
                .and_then(|cell| cell.code.clone())
                .ok_or_else(|| ScriptError::Failed(format!("mem source: key {key:?} not found")))
        };
        Box::pin(async move { code })
    }

    fn watch<'a>(
        &'a self,
        key: &'a str,
    ) -> BoxFuture<'a, Result<Box<dyn SignalStream>, ScriptError>> {
        let receiver = {
            let mut state = self.state.lock().expect("mem source lock");
            let cell = state.cells.entry(key.to_string()).or_default();
            let signal = cell.signal.get_or_insert_with(|| watch::channel(()).0);
            signal.subscribe()
        };
        Box::pin(async move { Ok(Box::new(MemSignalStream { receiver }) as Box<dyn SignalStream>) })
    }
}

/// The watch stream over one `watch::Receiver`: a tick per `set` or
/// `delete` on the key, ended when the source drops its sender.
struct MemSignalStream {
    receiver: watch::Receiver<()>,
}

impl SignalStream for MemSignalStream {
    fn next<'a>(&'a mut self) -> BoxFuture<'a, Option<()>> {
        Box::pin(async move {
            match self.receiver.changed().await {
                Ok(()) => Some(()),
                Err(_) => None,
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    const TIMEOUT: std::time::Duration = std::time::Duration::from_secs(2);

    #[tokio::test]
    async fn missing_keys_fail() {
        let source = MemSource::new();
        assert!(matches!(
            source.load("missing").await,
            Err(ScriptError::Failed(msg)) if msg.contains("not found")
        ));
    }

    #[tokio::test]
    async fn set_then_load_round_trips() {
        let source = MemSource::new();
        source.set("k", "print(1)");
        assert_eq!(source.load("k").await.expect("load k"), "print(1)");
    }

    #[tokio::test]
    async fn set_overwrites() {
        let source = MemSource::new();
        source.set("k", "v1");
        source.set("k", "v2");
        assert_eq!(source.load("k").await.expect("load k"), "v2");
    }

    #[tokio::test]
    async fn delete_removes_the_script() {
        let source = MemSource::new();
        source.set("k", "v1");
        source.delete("k");
        assert!(source.load("k").await.is_err());
    }

    #[tokio::test]
    async fn a_set_signals_the_watch_stream() {
        let source = Arc::new(MemSource::new());
        source.set("k", "v1");
        let mut stream = source.watch("k").await.expect("watch k");
        source.set("k", "v2");
        tokio::time::timeout(TIMEOUT, stream.next())
            .await
            .expect("signal arrives")
            .expect("stream alive");
        assert_eq!(source.load("k").await.expect("re-read"), "v2");
    }

    #[tokio::test]
    async fn a_delete_signals_the_watch_stream() {
        let source = Arc::new(MemSource::new());
        source.set("k", "v1");
        let mut stream = source.watch("k").await.expect("watch k");
        source.delete("k");
        tokio::time::timeout(TIMEOUT, stream.next())
            .await
            .expect("signal arrives")
            .expect("stream alive");
        assert!(source.load("k").await.is_err());
    }

    #[tokio::test]
    async fn dropping_the_stream_is_the_cancellation() {
        let source = Arc::new(MemSource::new());
        source.set("k", "v1");
        let stream = source.watch("k").await.expect("watch k");
        drop(stream);
        // The source stays fully functional after the stream's death.
        source.set("k", "v2");
        assert_eq!(source.load("k").await.expect("load"), "v2");
    }
}
