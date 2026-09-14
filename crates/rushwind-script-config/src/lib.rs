//! The config-source adapter for the Rust script contract: any
//! configuration-domain [`Source`](rushwind_config::Source) bound as
//! a script-domain source — the env/file/fallback engines and the
//! remote http/etcd/consul engines alike. A script engine taking its
//! source through this adapter reads its scripts out of the same
//! backends the config domain reads its configuration from.
//!
//! The two contracts differ in one load shape: a config source
//! answers `Ok(None)` for an absent key — the fallback composition's
//! absent signal — while a script source treats a missing key as an
//! error, not an absent signal. The adapter maps each side onto the
//! other:
//!
//! | Config side | Script side |
//! |:---|:---|
//! | `Ok(Some(bytes))` | UTF-8 validation, then the script text |
//! | `Ok(None)` (absent) | [`ScriptError::Failed`] not-found failure |
//! | `Err(NotWatchable)` | [`ScriptError::CapabilityNotSupported`] |
//! | any other `Err` | [`ScriptError::Failed`] joining the cause |
//!
//! The watch adapter is a pass-through: a config
//! [`SignalStream`](rushwind_config::SignalStream) ticks once per
//! underlying change and the script-side stream forwards each tick
//! unchanged — the two contracts share the signal-only shape, so
//! nothing is dropped or synthesized, and either stream's end ends
//! both.
//!
//! Push-mode watching — the config domain's
//! [`ValueStream`](rushwind_config::ValueStream) — has no script-side
//! counterpart (the script contract watches signals only), so the
//! adapter does not expose it; a push consumer composes on the config
//! side of the bridge.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

use rushwind_config::{
    ConfigError, SharedSource as ConfigSourceHandle, SignalStream as ConfigSignalStream,
};
use rushwind_script::{BoxFuture, ScriptError, ScriptSource, SignalStream};

/// Maps a configuration error onto the script taxonomy: the
/// watch-capability refusal keeps its category, everything else
/// joins its cause.
fn bridge_error(err: ConfigError) -> ScriptError {
    match err {
        ConfigError::NotWatchable => ScriptError::CapabilityNotSupported,
        err => ScriptError::Failed(err.to_string()),
    }
}

/// A script-domain source backed by a configuration-domain source.
pub struct ConfigSource {
    source: ConfigSourceHandle,
}

impl ConfigSource {
    /// Binds a configuration source as a script source.
    pub fn new(source: ConfigSourceHandle) -> Self {
        Self { source }
    }
}

impl ScriptSource for ConfigSource {
    fn load<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<String, ScriptError>> {
        Box::pin(async move {
            match self.source.load(key).await {
                Ok(Some(bytes)) => match String::from_utf8(bytes) {
                    Ok(text) => Ok(text),
                    Err(_) => Err(ScriptError::Failed(format!(
                        "config source: key {key:?} is not valid UTF-8"
                    ))),
                },
                Ok(None) => Err(ScriptError::Failed(format!(
                    "config source: key {key:?} not found"
                ))),
                Err(err) => Err(bridge_error(err)),
            }
        })
    }

    fn watch<'a>(
        &'a self,
        key: &'a str,
    ) -> BoxFuture<'a, Result<Box<dyn SignalStream>, ScriptError>> {
        Box::pin(async move {
            let stream = match self.source.watch(key).await {
                Ok(stream) => stream,
                Err(err) => return Err(bridge_error(err)),
            };
            let adapter: Box<dyn SignalStream> = Box::new(SignalAdapter { stream });
            Ok(adapter)
        })
    }
}

/// A script-domain signal stream forwarding a configuration-domain
/// signal stream: every underlying tick arrives unchanged, and the
/// underlying end-of-stream ends both.
pub struct SignalAdapter {
    stream: Box<dyn ConfigSignalStream>,
}

impl SignalStream for SignalAdapter {
    fn next<'a>(&'a mut self) -> BoxFuture<'a, Option<()>> {
        self.stream.next()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};

    /// A configurable test source: keyed bytes, a failure key, and an
    /// optional two-tick watch.
    struct TestSource {
        bytes: Mutex<HashMap<String, Vec<u8>>>,
        watchable: bool,
    }

    impl TestSource {
        fn with_entries(entries: &[(&str, &[u8])], watchable: bool) -> Self {
            let mut bytes = HashMap::new();
            for (key, value) in entries {
                bytes.insert((*key).to_string(), value.to_vec());
            }
            Self {
                bytes: Mutex::new(bytes),
                watchable,
            }
        }
    }

    impl rushwind_config::Source for TestSource {
        fn load<'a>(
            &'a self,
            key: &'a str,
        ) -> rushwind_config::BoxFuture<'a, Result<Option<Vec<u8>>, ConfigError>> {
            let outcome = if key == "fail" {
                Err(ConfigError::Failed("boom".to_string()))
            } else {
                Ok(self
                    .bytes
                    .lock()
                    .expect("test source lock")
                    .get(key)
                    .cloned())
            };
            Box::pin(async move { outcome })
        }

        fn watch<'a>(
            &'a self,
            _key: &'a str,
        ) -> rushwind_config::BoxFuture<'a, Result<Box<dyn ConfigSignalStream>, ConfigError>>
        {
            if !self.watchable {
                return Box::pin(async { Err(ConfigError::NotWatchable) });
            }
            Box::pin(async { Ok(Box::new(TickStream { ticks: 2 }) as Box<dyn ConfigSignalStream>) })
        }
    }

    /// A watch stream ticking a fixed count, then ending.
    struct TickStream {
        ticks: usize,
    }

    impl ConfigSignalStream for TickStream {
        fn next<'a>(&'a mut self) -> rushwind_config::BoxFuture<'a, Option<()>> {
            let tick = if self.ticks > 0 {
                self.ticks -= 1;
                Some(())
            } else {
                None
            };
            Box::pin(async move { tick })
        }
    }

    #[tokio::test]
    async fn loads_bridge_through_utf8() {
        let source = ConfigSource::new(Arc::new(TestSource::with_entries(
            &[("k", b"print(1)")],
            false,
        )));
        assert_eq!(source.load("k").await.expect("load k"), "print(1)");
    }

    #[tokio::test]
    async fn absent_keys_map_to_not_found() {
        let source = ConfigSource::new(Arc::new(TestSource::with_entries(&[], false)));
        assert!(matches!(
            source.load("missing").await,
            Err(ScriptError::Failed(msg)) if msg.contains("not found")
        ));
    }

    #[tokio::test]
    async fn non_utf8_payloads_fail() {
        let source = ConfigSource::new(Arc::new(TestSource::with_entries(
            &[("k", &[0xffu8, 0xfe])],
            false,
        )));
        assert!(matches!(
            source.load("k").await,
            Err(ScriptError::Failed(msg)) if msg.contains("not valid UTF-8")
        ));
    }

    #[tokio::test]
    async fn config_failures_join_their_causes() {
        let source = ConfigSource::new(Arc::new(TestSource::with_entries(&[], false)));
        assert!(matches!(
            source.load("fail").await,
            Err(ScriptError::Failed(msg)) if msg.contains("boom")
        ));
    }

    #[tokio::test]
    async fn watch_ticks_forward_unchanged() {
        let source = ConfigSource::new(Arc::new(TestSource::with_entries(&[], true)));
        let mut stream = source.watch("k").await.expect("watch");
        assert_eq!(stream.next().await, Some(()));
        assert_eq!(stream.next().await, Some(()));
        assert_eq!(stream.next().await, None);
    }

    #[tokio::test]
    async fn non_watchable_sources_reject_with_the_capability_error() {
        let source = ConfigSource::new(Arc::new(TestSource::with_entries(&[], false)));
        assert!(matches!(
            source.watch("k").await,
            Err(ScriptError::CapabilityNotSupported)
        ));
    }

    #[tokio::test]
    async fn binds_as_a_shared_script_source() {
        let bound: rushwind_script::SharedScriptSource = Arc::new(ConfigSource::new(Arc::new(
            TestSource::with_entries(&[("k", b"x = 1")], false),
        )));
        assert_eq!(bound.load("k").await.expect("load k"), "x = 1");
    }
}
