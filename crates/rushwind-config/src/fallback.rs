//! The priority composition: first source that answers wins, watches
//! merge into one effective-value stream.

use std::sync::Arc;

use futures::stream::FuturesUnordered;
use futures::StreamExt;

use crate::error::ConfigError;
use crate::{BoxFuture, Source, ValueStream};

/// A source that walks sub-sources in priority order.
///
/// [`load`](Source::load) tries each sub-source in turn; the first that
/// returns a value wins — higher-priority sources shadow lower-priority
/// ones. Errors are collected and, if *every* source either failed or
/// answered "absent", surfaced together: all-fail joins the errors, all
/// absent resolves to [`ConfigError::Unresolved`], a mix prefers the
/// joined failures (the Go `errors.Join` shape).
///
/// [`watch_value`](Source::watch_value) merges every watchable
/// sub-source into one stream: on any sub-source change the **effective**
/// value is re-read through the priority walk and delivered, so a
/// lower-priority source's notification still surfaces the
/// higher-priority answer. Sub-sources that only read are skipped (their
/// [`ConfigError::NotWatchable`]); if none can watch, the request fails.
///
/// The signal-mode [`watch`](Source::watch) is *not* merged — the Go
/// composition merges push-mode only.
pub struct FallbackSource {
    sources: Vec<Arc<dyn Source>>,
}

impl FallbackSource {
    /// Composes sources in priority order: the first source has the
    /// highest priority. At least one source is required.
    pub fn new(sources: Vec<Arc<dyn Source>>) -> Result<Self, ConfigError> {
        if sources.is_empty() {
            return Err(ConfigError::Failed(
                "fallback: at least one source is required".to_string(),
            ));
        }
        Ok(Self { sources })
    }
}

impl Source for FallbackSource {
    fn load<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<Option<Vec<u8>>, ConfigError>> {
        Box::pin(async move {
            let mut failures: Vec<String> = Vec::new();
            for source in &self.sources {
                match source.load(key).await {
                    Ok(Some(data)) => return Ok(Some(data)),
                    Ok(None) => {}
                    Err(err) => failures.push(err.to_string()),
                }
            }
            if !failures.is_empty() {
                // The Go errors.Join shape: the walked failures win over
                // the "no source resolved this" message.
                return Err(ConfigError::Failed(failures.join("; ")));
            }
            Err(ConfigError::Unresolved(key.to_string()))
        })
    }

    fn watch_value<'a>(
        &'a self,
        key: &'a str,
    ) -> BoxFuture<'a, Result<Box<dyn ValueStream>, ConfigError>> {
        Box::pin(async move {
            // Discover the watchable sub-sources. `NotWatchable` is the
            // "this source cannot watch" marker and is skipped; any other
            // failure propagates — the Go sub-source-wrap behavior.
            let mut streams: Vec<Box<dyn ValueStream>> = Vec::new();
            for source in &self.sources {
                match source.watch_value(key).await {
                    Ok(stream) => streams.push(stream),
                    Err(ConfigError::NotWatchable) => {}
                    Err(err) => return Err(err),
                }
            }
            if streams.is_empty() {
                return Err(ConfigError::Failed(
                    "fallback: none of the sub-sources offers value watching".to_string(),
                ));
            }
            Ok(Box::new(FallbackValueStream {
                key: key.to_string(),
                sources: self.sources.clone(),
                subs: streams.into_iter().map(SubSlot::new).collect(),
            }) as Box<dyn ValueStream>)
        })
    }
}

/// The priority-ordered load walk used by the merged stream: first
/// `Some` wins, everything else — errors included — is skipped. The Go
/// merge ignores a failed effective re-read and keeps waiting; this walk
/// answers `Ok(None)` for exactly that case.
async fn load_through(
    sources: &[Arc<dyn Source>],
    key: &str,
) -> Result<Option<Vec<u8>>, ConfigError> {
    for source in sources {
        if let Ok(Some(data)) = source.load(key).await {
            return Ok(Some(data));
        }
    }
    Ok(None)
}

/// One merged sub-stream, plus the value it delivered before its turn to
/// be read.
struct SubSlot {
    stream: Box<dyn ValueStream>,
    /// A value this sub delivered while another sub won the race round;
    /// parked so a same-wave delivery is coalesced, never lost.
    parked: Option<Vec<u8>>,
    /// The sub-stream ended; it never races again.
    ended: bool,
}

impl SubSlot {
    fn new(stream: Box<dyn ValueStream>) -> Self {
        Self {
            stream,
            parked: None,
            ended: false,
        }
    }
}

/// The merged watch stream behind [`FallbackSource::watch_value`].
///
/// No task boundaries: each [`next`](ValueStream::next) call races the
/// live sub-streams concurrently inside the call's own future — the
/// first delivery parks its value, the round ends, and the effective
/// value is re-read through the priority walk. Deliveries that arrive in
/// the same poll wave are parked on their sub-slots and coalesced into
/// the next read, never dropped.
pub struct FallbackValueStream {
    key: String,
    sources: Vec<Arc<dyn Source>>,
    subs: Vec<SubSlot>,
}

impl ValueStream for FallbackValueStream {
    fn next<'a>(&'a mut self) -> BoxFuture<'a, Option<Vec<u8>>> {
        Box::pin(async move {
            loop {
                // Coalesce any parked deliveries: clear them all, then
                // re-read the effective value once. Unreadable effective
                // values loop back to re-arm the race, the Go merge's
                // skip-and-keep-waiting shape.
                if self.subs.iter().any(|slot| slot.parked.is_some()) {
                    for slot in &mut self.subs {
                        slot.parked = None;
                    }
                    if let Ok(Some(effective)) = load_through(&self.sources, &self.key).await {
                        return Some(effective);
                    }
                    continue;
                }

                // Race every still-live sub-stream; the first delivery
                // parks and ends the round. Sub-streams that end mark
                // themselves and drop out of future rounds.
                let mut racing: FuturesUnordered<_> = self
                    .subs
                    .iter_mut()
                    .filter(|slot| !slot.ended)
                    .map(|slot| async move {
                        match slot.stream.next().await {
                            Some(value) => {
                                slot.parked = Some(value);
                                true
                            }
                            None => {
                                slot.ended = true;
                                false
                            }
                        }
                    })
                    .collect();
                if racing.is_empty() {
                    // Every sub-stream ended: the merged stream ends with
                    // them, the Go close-when-all-finish behavior.
                    return None;
                }
                while let Some(parked) = racing.next().await {
                    if parked {
                        break;
                    }
                }
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex as StdMutex;
    use tokio::sync::broadcast;

    /// A read-only source: one fixed answer for every key.
    struct Static(Option<&'static str>);

    impl Source for Static {
        fn load<'a>(
            &'a self,
            _key: &'a str,
        ) -> BoxFuture<'a, Result<Option<Vec<u8>>, ConfigError>> {
            Box::pin(async move { Ok(self.0.map(|s| s.as_bytes().to_vec())) })
        }
    }

    /// A source whose every load fails.
    struct Failing(&'static str);

    impl Source for Failing {
        fn load<'a>(
            &'a self,
            _key: &'a str,
        ) -> BoxFuture<'a, Result<Option<Vec<u8>>, ConfigError>> {
            Box::pin(async move { Err(ConfigError::Failed(self.0.to_string())) })
        }
    }

    /// A source whose watch capability itself fails.
    struct WatchFails;

    impl Source for WatchFails {
        fn load<'a>(
            &'a self,
            _key: &'a str,
        ) -> BoxFuture<'a, Result<Option<Vec<u8>>, ConfigError>> {
            Box::pin(async move { Ok(Some(b"irrelevant".to_vec())) })
        }

        fn watch_value<'a>(
            &'a self,
            _key: &'a str,
        ) -> BoxFuture<'a, Result<Box<dyn ValueStream>, ConfigError>> {
            Box::pin(async move { Err(ConfigError::Failed("watch broke".to_string())) })
        }
    }

    /// A watchable source: mutable data plus a broadcast fan-out of
    /// pushed values, the shape real engines build over a broker.
    struct Watchable {
        data: StdMutex<Option<Vec<u8>>>,
        tx: StdMutex<Option<broadcast::Sender<Vec<u8>>>>,
    }

    impl Watchable {
        fn with_data(value: Option<&'static str>) -> Arc<Self> {
            let (tx, _rx) = broadcast::channel(16);
            Arc::new(Self {
                data: StdMutex::new(value.map(|s| s.as_bytes().to_vec())),
                tx: StdMutex::new(Some(tx)),
            })
        }

        /// Updates the served data and pushes the new value.
        fn push(&self, value: &str) {
            *self.data.lock().expect("watchable data lock") = Some(value.as_bytes().to_vec());
            if let Some(tx) = &*self.tx.lock().expect("watchable tx lock") {
                let _ = tx.send(value.as_bytes().to_vec());
            }
        }

        /// Takes the sender: every open watch stream sees the channel
        /// close and ends.
        fn close(&self) {
            *self.tx.lock().expect("watchable tx lock") = None;
        }
    }

    impl Source for Watchable {
        fn load<'a>(
            &'a self,
            _key: &'a str,
        ) -> BoxFuture<'a, Result<Option<Vec<u8>>, ConfigError>> {
            Box::pin(async move { Ok(self.data.lock().expect("watchable data lock").clone()) })
        }

        fn watch_value<'a>(
            &'a self,
            _key: &'a str,
        ) -> BoxFuture<'a, Result<Box<dyn ValueStream>, ConfigError>> {
            let tx = self.tx.lock().expect("watchable tx lock").clone();
            Box::pin(async move {
                match tx {
                    Some(tx) => {
                        Ok(Box::new(ChannelStream { rx: tx.subscribe() }) as Box<dyn ValueStream>)
                    }
                    None => Err(ConfigError::Failed("watchable closed".to_string())),
                }
            })
        }
    }

    /// Bridges a broadcast receiver into the contract's stream shape.
    struct ChannelStream {
        rx: broadcast::Receiver<Vec<u8>>,
    }

    impl ValueStream for ChannelStream {
        fn next<'a>(&'a mut self) -> BoxFuture<'a, Option<Vec<u8>>> {
            Box::pin(async move {
                loop {
                    match self.rx.recv().await {
                        Ok(value) => return Some(value),
                        Err(broadcast::error::RecvError::Lagged(_)) => continue,
                        Err(broadcast::error::RecvError::Closed) => return None,
                    }
                }
            })
        }
    }

    fn boxed(source: impl Source + 'static) -> Arc<dyn Source> {
        Arc::new(source)
    }

    #[tokio::test]
    async fn composition_requires_at_least_one_source() {
        assert!(FallbackSource::new(vec![]).is_err());
    }

    #[tokio::test]
    async fn the_first_source_shadows_the_rest() {
        let fallback = FallbackSource::new(vec![
            boxed(Static(Some("high"))),
            boxed(Static(Some("low"))),
        ])
        .unwrap();
        assert_eq!(fallback.load("k").await.unwrap(), Some(b"high".to_vec()));
    }

    #[tokio::test]
    async fn an_absent_first_source_falls_through() {
        let fallback =
            FallbackSource::new(vec![boxed(Static(None)), boxed(Static(Some("low")))]).unwrap();
        assert_eq!(fallback.load("k").await.unwrap(), Some(b"low".to_vec()));
    }

    #[tokio::test]
    async fn failing_sources_are_skipped_while_one_answers() {
        let fallback =
            FallbackSource::new(vec![boxed(Failing("down")), boxed(Static(Some("answer")))])
                .unwrap();
        assert_eq!(fallback.load("k").await.unwrap(), Some(b"answer".to_vec()));
    }

    #[tokio::test]
    async fn all_failures_join_into_one_error() {
        let fallback =
            FallbackSource::new(vec![boxed(Failing("one")), boxed(Failing("two"))]).unwrap();
        let err = fallback.load("k").await.unwrap_err();
        assert_eq!(
            err,
            ConfigError::Failed(
                "config operation failed: one; config operation failed: two".to_string()
            )
        );
    }

    #[tokio::test]
    async fn all_absent_resolves_to_unresolved() {
        let fallback = FallbackSource::new(vec![boxed(Static(None)), boxed(Static(None))]).unwrap();
        assert_eq!(
            fallback.load("missing").await.unwrap_err(),
            ConfigError::Unresolved("missing".to_string())
        );
    }

    #[tokio::test]
    async fn mixed_failures_win_over_the_unresolved_message() {
        // One failure plus one clean absence: the Go errors.Join shape
        // surfaces the failure, not the no-source message.
        let fallback =
            FallbackSource::new(vec![boxed(Failing("boom")), boxed(Static(None))]).unwrap();
        assert!(matches!(
            fallback.load("k").await.unwrap_err(),
            ConfigError::Failed(_)
        ));
    }

    #[tokio::test]
    async fn watch_value_without_watchable_sources_fails() {
        let fallback = FallbackSource::new(vec![boxed(Static(Some("x")))]).unwrap();
        assert!(matches!(
            fallback
                .watch_value("k")
                .await
                .err()
                .expect("watch_value outcome"),
            ConfigError::Failed(_)
        ));
    }

    #[tokio::test]
    async fn watch_value_sub_failures_propagate() {
        let fallback = FallbackSource::new(vec![boxed(WatchFails)]).unwrap();
        assert_eq!(
            fallback
                .watch_value("k")
                .await
                .err()
                .expect("watch_value outcome"),
            ConfigError::Failed("watch broke".to_string())
        );
    }

    #[tokio::test]
    async fn not_watchable_subs_are_skipped_not_fatal() {
        // A reader-only source next to a watchable one: the merge comes
        // up over the watchable half alone.
        let watchable = Watchable::with_data(Some("watched"));
        let fallback =
            FallbackSource::new(vec![watchable.clone(), boxed(Static(Some("static")))]).unwrap();
        let mut stream = fallback
            .watch_value("k")
            .await
            .expect("watch_value comes up");
        watchable.push("changed");
        assert_eq!(
            tokio::time::timeout(std::time::Duration::from_secs(2), stream.next())
                .await
                .expect("merge delivers")
                .expect("value"),
            b"changed".to_vec()
        );
    }

    #[tokio::test]
    async fn a_pushed_change_forwards_the_new_value() {
        let watchable = Watchable::with_data(Some("v0"));
        let fallback = FallbackSource::new(vec![watchable.clone()]).unwrap();
        let mut stream = fallback
            .watch_value("k")
            .await
            .expect("watch_value comes up");
        watchable.push("v1");
        let value = tokio::time::timeout(std::time::Duration::from_secs(2), stream.next())
            .await
            .expect("merge delivers")
            .expect("value");
        assert_eq!(value, b"v1".to_vec());
    }

    #[tokio::test]
    async fn a_lower_priority_change_surfaces_the_effective_answer() {
        let high = Watchable::with_data(Some("high"));
        let low = Watchable::with_data(Some("low"));
        let fallback = FallbackSource::new(vec![high.clone(), low.clone()]).unwrap();
        let mut stream = fallback
            .watch_value("k")
            .await
            .expect("watch_value comes up");
        // The low-priority source changes; the effective value is still
        // whatever the high-priority source serves.
        low.push("low-changed");
        let value = tokio::time::timeout(std::time::Duration::from_secs(2), stream.next())
            .await
            .expect("merge delivers")
            .expect("value");
        assert_eq!(value, b"high".to_vec());
    }

    #[tokio::test]
    async fn the_merged_stream_ends_when_every_sub_stream_ends() {
        let watchable = Watchable::with_data(Some("v0"));
        let fallback = FallbackSource::new(vec![watchable.clone()]).unwrap();
        let mut stream = fallback
            .watch_value("k")
            .await
            .expect("watch_value comes up");
        // The merged stream holds its sources strongly, so closing the
        // watch — not dropping the source — is what ends a sub-stream;
        // the close fans out as channel-close to the sub receiver.
        watchable.close();
        let ended = tokio::time::timeout(std::time::Duration::from_secs(2), stream.next())
            .await
            .expect("merge terminates");
        assert_eq!(ended, None);
    }

    /// The Go swap-reader semantics: a watched source whose data is
    /// replaced between pushes serves the replacement on the effective
    /// re-read.
    #[tokio::test]
    async fn effective_reload_reads_the_current_data_not_the_pushed_payload() {
        let watchable = Watchable::with_data(Some("before"));
        let fallback = FallbackSource::new(vec![watchable.clone()]).unwrap();
        let mut stream = fallback
            .watch_value("k")
            .await
            .expect("watch_value comes up");
        // The push payload differs from what load now serves; the merge
        // delivers the loaded answer.
        watchable.push("wake");
        *watchable.data.lock().expect("watchable data lock") = Some(b"replaced".to_vec());
        let value = tokio::time::timeout(std::time::Duration::from_secs(2), stream.next())
            .await
            .expect("merge delivers")
            .expect("value");
        assert_eq!(value, b"replaced".to_vec());
    }
}
