//! The transform source wrapper: hooks applied to raw script content
//! after it leaves the wrapped source and before it reaches the
//! caller.
//!
//! Typical hooks: decryption of scripts stored encrypted at rest,
//! decompression, validation or sanitization, legacy-encoding
//! conversion. Transforms run in registration order — the output of
//! transform N is the input of transform N+1 — and both the key and
//! the raw content are handed to each hook so a hook can vary by key
//! (some keys encrypted, others not). A failing transform fails the
//! load with its index in the message, no fallback.
//!
//! The watch capability delegates transparently to the wrapped
//! source: signals are untransformed — re-load after a signal to get
//! the freshly transformed content.
//!
//! Divergence from the Go predecessor: `Close` is `Drop`, and the
//! nil-inner / nil-transform constructor checks are dropped
//! (`SharedScriptSource` and [`TransformFn`] cannot be null).

use crate::source::{ScriptSource, SharedScriptSource, SignalStream};
use crate::{BoxFuture, ScriptError};

/// One transform hook: raw content in, transformed content out. The
/// `key` is the load key the raw content came from.
pub type TransformFn = Box<dyn Fn(&str, String) -> Result<String, ScriptError> + Send + Sync>;

/// A wrapper applying transform hooks to a wrapped source's loads.
pub struct TransformSource {
    inner: SharedScriptSource,
    transforms: Vec<TransformFn>,
}

impl TransformSource {
    /// Creates the wrapper with at least one transform.
    pub fn new(
        inner: SharedScriptSource,
        transforms: Vec<TransformFn>,
    ) -> Result<Self, ScriptError> {
        if transforms.is_empty() {
            return Err(ScriptError::Failed(
                "transform source: at least one transform function is required".to_string(),
            ));
        }
        Ok(Self { inner, transforms })
    }

    /// Chains one more transform onto this wrapper, returning the
    /// widened wrapper. The added transform runs last.
    pub fn then(mut self, transform: TransformFn) -> Result<Self, ScriptError> {
        self.transforms.push(transform);
        Ok(self)
    }
}

impl ScriptSource for TransformSource {
    fn load<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<String, ScriptError>> {
        Box::pin(async move {
            let raw = match self.inner.load(key).await {
                Ok(raw) => raw,
                Err(err) => {
                    return Err(ScriptError::Failed(format!(
                        "transform source: load {key:?}: {err}"
                    )))
                }
            };
            let mut result = raw;
            for (index, transform) in self.transforms.iter().enumerate() {
                result = match transform(key, result) {
                    Ok(transformed) => transformed,
                    Err(err) => {
                        return Err(ScriptError::Failed(format!(
                            "transform source: transform[{index}] for {key:?}: {err}"
                        )))
                    }
                };
            }
            Ok(result)
        })
    }

    fn watch<'a>(
        &'a self,
        key: &'a str,
    ) -> BoxFuture<'a, Result<Box<dyn SignalStream>, ScriptError>> {
        Box::pin(async move {
            match self.inner.watch(key).await {
                Ok(stream) => Ok(stream),
                Err(ScriptError::CapabilityNotSupported) => Err(ScriptError::Failed(
                    "transform source: inner source does not implement watching".to_string(),
                )),
                Err(err) => Err(err),
            }
        })
    }
}

/// The no-op transform: returns its input unchanged. A placeholder and
/// test fixture, the Go `IdentityTransform`.
pub fn identity_transform(_key: &str, raw: String) -> Result<String, ScriptError> {
    Ok(raw)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    fn append_transform(suffix: &'static str) -> TransformFn {
        Box::new(move |_key, raw| Ok(format!("{raw}{suffix}")))
    }

    fn key_gated_transform(gate: &'static str, suffix: &'static str) -> TransformFn {
        Box::new(move |key, raw| {
            if key == gate {
                Ok(format!("{raw}{suffix}"))
            } else {
                Ok(raw)
            }
        })
    }

    fn failing_transform() -> TransformFn {
        Box::new(|_key, _raw| Err(ScriptError::Failed("transform refused".to_string())))
    }

    #[tokio::test]
    async fn an_empty_transform_list_is_rejected() {
        let inner = Arc::new(crate::source::memory::MemSource::new());
        assert!(matches!(
            TransformSource::new(inner, vec![]),
            Err(ScriptError::Failed(msg)) if msg.contains("at least one transform")
        ));
    }

    #[tokio::test]
    async fn one_transform_applies_to_the_load() {
        let inner = Arc::new(crate::source::memory::MemSource::new());
        inner.set("k", "raw");
        let source = TransformSource::new(inner, vec![append_transform("-t1")]).expect("compose");
        assert_eq!(source.load("k").await.expect("transformed load"), "raw-t1");
    }

    #[tokio::test]
    async fn chained_transforms_apply_in_registration_order() {
        let inner = Arc::new(crate::source::memory::MemSource::new());
        inner.set("k", "raw");
        let source = TransformSource::new(inner, vec![append_transform("-A")])
            .expect("compose")
            .then(append_transform("-B"))
            .expect("chain");
        assert_eq!(source.load("k").await.expect("chained load"), "raw-A-B");
    }

    #[tokio::test]
    async fn a_failing_transform_fails_the_load_with_its_index() {
        let inner = Arc::new(crate::source::memory::MemSource::new());
        inner.set("k", "raw");
        let source =
            TransformSource::new(inner, vec![append_transform("-ok"), failing_transform()])
                .expect("compose");
        assert!(matches!(
            source.load("k").await,
            Err(ScriptError::Failed(msg)) if msg.contains("transform[1]")
        ));
    }

    #[tokio::test]
    async fn an_inner_failure_wraps_into_the_load_error() {
        let source = TransformSource::new(
            Arc::new(crate::source::memory::MemSource::new()),
            vec![Box::new(identity_transform)],
        )
        .expect("compose");
        assert!(matches!(
            source.load("missing").await,
            Err(ScriptError::Failed(msg)) if msg.contains("transform source: load")
        ));
    }

    #[tokio::test]
    async fn a_transform_may_key_its_behavior() {
        let inner = Arc::new(crate::source::memory::MemSource::new());
        inner.set("enc", "cipher");
        inner.set("plain", "clear");
        let source = TransformSource::new(inner, vec![key_gated_transform("enc", "-decrypted")])
            .expect("compose");
        assert_eq!(
            source.load("enc").await.expect("gated key transformed"),
            "cipher-decrypted"
        );
        assert_eq!(
            source.load("plain").await.expect("ungated key untouched"),
            "clear"
        );
    }

    #[tokio::test]
    async fn watch_delegates_to_the_inner_source() {
        let inner = Arc::new(crate::source::memory::MemSource::new());
        inner.set("k", "v1");
        let source = TransformSource::new(inner.clone(), vec![Box::new(identity_transform)])
            .expect("compose");
        let mut stream = source.watch("k").await.expect("watch delegates");
        inner.set("k", "v2");
        tokio::time::timeout(std::time::Duration::from_secs(2), stream.next())
            .await
            .expect("delegated stream signals")
            .expect("stream alive");
    }

    #[tokio::test]
    async fn watch_fails_when_the_inner_source_cannot_watch() {
        let inner: SharedScriptSource = Arc::new(NonWatchableInner);
        let source =
            TransformSource::new(inner, vec![Box::new(identity_transform)]).expect("compose");
        assert!(matches!(
            source.watch("k").await,
            Err(ScriptError::Failed(msg)) if msg.contains("does not implement")
        ));
    }

    /// A reader-only inner: no watch capability.
    struct NonWatchableInner;

    impl ScriptSource for NonWatchableInner {
        fn load<'a>(&'a self, _key: &'a str) -> BoxFuture<'a, Result<String, ScriptError>> {
            Box::pin(async { Ok("x".to_string()) })
        }
    }

    #[tokio::test]
    async fn the_identity_transform_is_a_no_op() {
        let inner = Arc::new(crate::source::memory::MemSource::new());
        inner.set("k", "raw");
        let source =
            TransformSource::new(inner, vec![Box::new(identity_transform)]).expect("compose");
        assert_eq!(source.load("k").await.expect("identity load"), "raw");
    }
}
