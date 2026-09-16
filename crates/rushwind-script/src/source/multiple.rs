//! The multi-strategy source aggregator.
//!
//! Wraps several sub-sources behind one [`ScriptSource`] with a
//! strategy selecting among them:
//!
//! - [`MultiStrategy::Fallback`] walks the sub-sources in order and
//!   the first successful load wins — the primary-plus-local-backup
//!   shape. All failures join into one error, a joined
//!   rendering.
//! - [`MultiStrategy::FirstOk`] races every sub-source's load
//!   concurrently and the first success wins — the low-latency
//!   mirrored-read shape. The sub-load futures race inside this
//!   call's own future with no task boundaries, the orchestrator
//!   doctrine, and a winner's remaining racers are dropped.
//!
//! The watch capability delegates to the first sub-source whose watch
//! comes up; [`ScriptError::CapabilityNotSupported`] answers —
//! sub-sources that cannot watch — are skipped, matching the
//! capability-probe walk, and when every candidate fails the
//! failures join.
//!
//! `Close` is `Drop` — the
//! sub-source arcs release with the aggregator.

use futures::stream::{FuturesUnordered, StreamExt};

use crate::source::{ScriptSource, SharedScriptSource, SignalStream};
use crate::{BoxFuture, ScriptError};

/// Selects how [`MultiSource`] aggregates its sub-sources.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum MultiStrategy {
    /// Sub-sources are tried in order; the first success wins.
    #[default]
    Fallback,
    /// Sub-sources race concurrently; the first success wins.
    FirstOk,
}

/// A source aggregating several sub-sources under one strategy.
pub struct MultiSource {
    sources: Vec<SharedScriptSource>,
    strategy: MultiStrategy,
}

impl MultiSource {
    /// Creates the aggregator. At least one sub-source is required.
    pub fn new(
        strategy: MultiStrategy,
        sources: Vec<SharedScriptSource>,
    ) -> Result<Self, ScriptError> {
        if sources.is_empty() {
            return Err(ScriptError::Failed(
                "multi source: at least one source is required".to_string(),
            ));
        }
        Ok(Self { sources, strategy })
    }

    /// The [`MultiStrategy::Fallback`] shortcut.
    pub fn new_fallback(sources: Vec<SharedScriptSource>) -> Result<Self, ScriptError> {
        Self::new(MultiStrategy::Fallback, sources)
    }

    /// The [`MultiStrategy::FirstOk`] shortcut.
    pub fn new_first_ok(sources: Vec<SharedScriptSource>) -> Result<Self, ScriptError> {
        Self::new(MultiStrategy::FirstOk, sources)
    }
}

impl ScriptSource for MultiSource {
    fn load<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<String, ScriptError>> {
        Box::pin(async move {
            match self.strategy {
                MultiStrategy::Fallback => {
                    let mut failures: Vec<String> = Vec::new();
                    for source in &self.sources {
                        match source.load(key).await {
                            Ok(code) => return Ok(code),
                            Err(err) => failures.push(err.to_string()),
                        }
                    }
                    Err(ScriptError::Failed(format!(
                        "multi source(fallback): all sources failed for {key:?}: {}",
                        failures.join("; ")
                    )))
                }
                MultiStrategy::FirstOk => {
                    let mut racers: FuturesUnordered<_> =
                        self.sources.iter().map(|source| source.load(key)).collect();
                    let mut failures: Vec<String> = Vec::new();
                    while let Some(outcome) = racers.next().await {
                        match outcome {
                            Ok(code) => return Ok(code),
                            Err(err) => failures.push(err.to_string()),
                        }
                    }
                    Err(ScriptError::Failed(format!(
                        "multi source(first-ok): all sources failed for {key:?}: {}",
                        failures.join("; ")
                    )))
                }
            }
        })
    }

    fn watch<'a>(
        &'a self,
        key: &'a str,
    ) -> BoxFuture<'a, Result<Box<dyn SignalStream>, ScriptError>> {
        Box::pin(async move {
            let mut failures: Vec<String> = Vec::new();
            for (index, source) in self.sources.iter().enumerate() {
                match source.watch(key).await {
                    Ok(stream) => return Ok(stream),
                    // The "this sub-source cannot watch" marker: skip.
                    Err(ScriptError::CapabilityNotSupported) => {}
                    Err(err) => failures.push(format!("source[{index}]: {err}")),
                }
            }
            if failures.is_empty() {
                Err(ScriptError::Failed(
                    "multi source: none of the sub-sources implements watching".to_string(),
                ))
            } else {
                Err(ScriptError::Failed(format!(
                    "multi source: no watcher available: {}",
                    failures.join("; ")
                )))
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::Arc;

    /// A fake source: a scripted answer, an optional latency, a
    /// load counter, and a drop flag standing in for closed
    /// bookkeeping.
    struct FakeSource {
        code: Option<&'static str>,
        err: Option<&'static str>,
        delay: Option<std::time::Duration>,
        load_calls: Arc<AtomicUsize>,
        dropped: Arc<AtomicBool>,
    }

    impl FakeSource {
        fn compose(
            code: Option<&'static str>,
            err: Option<&'static str>,
            delay: Option<std::time::Duration>,
        ) -> Arc<Self> {
            Arc::new(Self {
                code,
                err,
                delay,
                load_calls: Arc::new(AtomicUsize::new(0)),
                dropped: Arc::new(AtomicBool::new(false)),
            })
        }

        fn with_code(code: &'static str) -> Arc<Self> {
            Self::compose(Some(code), None, None)
        }

        fn with_err(err: &'static str) -> Arc<Self> {
            Self::compose(None, Some(err), None)
        }

        fn with_delay(code: &'static str, delay: std::time::Duration) -> Arc<Self> {
            Self::compose(Some(code), None, Some(delay))
        }
    }

    impl Drop for FakeSource {
        fn drop(&mut self) {
            self.dropped.store(true, Ordering::SeqCst);
        }
    }

    impl ScriptSource for FakeSource {
        fn load<'a>(&'a self, _key: &'a str) -> BoxFuture<'a, Result<String, ScriptError>> {
            self.load_calls.fetch_add(1, Ordering::SeqCst);
            let delay = self.delay;
            let answer = match (self.err, self.code) {
                (Some(err), _) => Err(ScriptError::Failed(err.to_string())),
                (None, Some(code)) => Ok(code.to_string()),
                (None, None) => Ok(String::new()),
            };
            Box::pin(async move {
                if let Some(delay) = delay {
                    tokio::time::sleep(delay).await;
                }
                answer
            })
        }
    }

    #[test]
    fn an_empty_source_list_is_rejected() {
        assert!(matches!(
            MultiSource::new(MultiStrategy::Fallback, vec![]),
            Err(ScriptError::Failed(msg)) if msg.contains("at least one source")
        ));
    }

    #[tokio::test]
    async fn fallback_answers_through_the_first_source_that_succeeds() {
        let multi = MultiSource::new_fallback(vec![
            FakeSource::with_code("high"),
            FakeSource::with_code("low"),
        ])
        .expect("compose");
        assert_eq!(multi.load("k").await.expect("first source wins"), "high");
    }

    #[tokio::test]
    async fn fallback_falls_through_a_failing_first_source() {
        let multi = MultiSource::new_fallback(vec![
            FakeSource::with_err("one"),
            FakeSource::with_code("answer"),
        ])
        .expect("compose");
        assert_eq!(
            multi.load("k").await.expect("second source answers"),
            "answer"
        );
    }

    #[tokio::test]
    async fn fallback_joins_when_every_source_fails() {
        let multi = MultiSource::new_fallback(vec![
            FakeSource::with_err("one"),
            FakeSource::with_err("two"),
        ])
        .expect("compose");
        let msg = multi.load("k").await.unwrap_err().to_string();
        assert!(msg.contains("one"), "joined failure mentions one: {msg}");
        assert!(msg.contains("two"), "joined failure mentions two: {msg}");
    }

    #[tokio::test]
    async fn first_ok_lets_the_faster_source_win_from_the_front() {
        let multi = MultiSource::new_first_ok(vec![
            FakeSource::with_delay("fast", std::time::Duration::ZERO),
            FakeSource::with_delay("slow", std::time::Duration::from_millis(300)),
        ])
        .expect("compose");
        assert_eq!(multi.load("k").await.expect("fast source wins"), "fast");
    }

    #[tokio::test]
    async fn first_ok_lets_the_faster_source_win_from_the_back() {
        let multi = MultiSource::new_first_ok(vec![
            FakeSource::with_delay("slow", std::time::Duration::from_millis(300)),
            FakeSource::with_delay("fast", std::time::Duration::ZERO),
        ])
        .expect("compose");
        assert_eq!(multi.load("k").await.expect("fast source wins"), "fast");
    }

    #[tokio::test]
    async fn first_ok_joins_when_every_source_fails() {
        let multi = MultiSource::new_first_ok(vec![
            FakeSource::with_err("one"),
            FakeSource::with_err("two"),
        ])
        .expect("compose");
        let msg = multi.load("k").await.unwrap_err().to_string();
        assert!(
            msg.contains("one") && msg.contains("two"),
            "joined failures: {msg}"
        );
    }

    #[tokio::test]
    async fn dropping_the_aggregator_drops_its_sub_sources() {
        let a = FakeSource::with_code("a");
        let b = FakeSource::with_code("b");
        let dropped_a = a.dropped.clone();
        let dropped_b = b.dropped.clone();
        let multi = MultiSource::new_fallback(vec![a, b]).expect("compose");
        assert!(!dropped_a.load(Ordering::SeqCst) && !dropped_b.load(Ordering::SeqCst));
        drop(multi);
        assert!(dropped_a.load(Ordering::SeqCst) && dropped_b.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn watch_delegates_to_the_first_watchable_sub_source() {
        let watchable = Arc::new(crate::source::memory::MemSource::new());
        watchable.set("k", "v1");
        let multi = MultiSource::new_fallback(vec![watchable.clone(), FakeSource::with_code("x")])
            .expect("compose");
        let mut stream = multi
            .watch("k")
            .await
            .expect("watchable sub-source answers");
        watchable.set("k", "v2");
        tokio::time::timeout(std::time::Duration::from_secs(2), stream.next())
            .await
            .expect("delegated stream signals")
            .expect("stream alive");
    }

    #[tokio::test]
    async fn watch_fails_when_no_sub_source_can_watch() {
        let multi = MultiSource::new_fallback(vec![FakeSource::with_code("x")]).expect("compose");
        assert!(matches!(
            multi.watch("k").await,
            Err(ScriptError::Failed(msg)) if msg.contains("none of the sub-sources")
        ));
    }
}
