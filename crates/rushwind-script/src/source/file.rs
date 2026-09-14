//! The local-filesystem script source.
//!
//! Reads scripts straight from the filesystem — no dependencies, the
//! dev/debug default. The watch capability keeps the Go predecessor's
//! shape: a one-second mtime poll per watched path signaling on
//! modification, rather than the config domain's directory-notification
//! engine (the Go script source itself polls; `rushwind-config-file`
//! diverged from its own predecessor by using notify, and this source
//! stays faithful to its own).
//!
//! Divergence from the Go predecessor: the watch stream has no
//! end-of-context termination — dropping the stream is the
//! cancellation, and the stream otherwise ticks forever; the source's
//! `mtimes` bookkeeping map (write-only in the predecessor) is
//! dropped, the stream tracking its own baseline.

use std::path::PathBuf;
use std::time::{Duration, SystemTime};

use tokio::time::Interval;

use crate::source::{ScriptSource, SignalStream};
use crate::{BoxFuture, ScriptError};

/// A source reading scripts from local files by path.
pub struct FileSource;

impl FileSource {
    /// Creates the source.
    pub fn new() -> Self {
        Self
    }
}

impl Default for FileSource {
    fn default() -> Self {
        Self::new()
    }
}

impl ScriptSource for FileSource {
    fn load<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<String, ScriptError>> {
        Box::pin(async move {
            std::fs::read_to_string(key)
                .map_err(|err| ScriptError::Failed(format!("file source: read {key:?}: {err}")))
        })
    }

    fn watch<'a>(
        &'a self,
        key: &'a str,
    ) -> BoxFuture<'a, Result<Box<dyn SignalStream>, ScriptError>> {
        let path = PathBuf::from(key);
        let baseline = std::fs::metadata(&path).and_then(|meta| meta.modified());
        Box::pin(async move {
            let baseline = baseline
                .map_err(|err| ScriptError::Failed(format!("file source: stat {key:?}: {err}")))?;
            let mut interval = tokio::time::interval(Duration::from_secs(1));
            // The predecessor's ticker first fires after the interval;
            // a tokio interval fires immediately, so consume that
            // first tick here to keep the cadence at one second.
            interval.tick().await;
            Ok(Box::new(FileSignalStream {
                path,
                baseline,
                interval,
            }) as Box<dyn SignalStream>)
        })
    }
}

/// The mtime poll stream: one tick per observed modification of the
/// watched path. Runs until dropped; a vanishing or unreadable path is
/// skipped for that tick, the Go shape.
struct FileSignalStream {
    path: PathBuf,
    baseline: SystemTime,
    interval: Interval,
}

impl SignalStream for FileSignalStream {
    fn next<'a>(&'a mut self) -> BoxFuture<'a, Option<()>> {
        Box::pin(async move {
            loop {
                self.interval.tick().await;
                let Ok(meta) = std::fs::metadata(&self.path) else {
                    continue;
                };
                let Ok(modified) = meta.modified() else {
                    continue;
                };
                if modified > self.baseline {
                    self.baseline = modified;
                    return Some(());
                }
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testutil::TempDir;

    const TIMEOUT: std::time::Duration = std::time::Duration::from_secs(3);

    #[tokio::test]
    async fn a_present_file_loads() {
        let dir = TempDir::new("file-load");
        let path = dir.write("a.lua", "return 1");
        let source = FileSource::new();
        assert_eq!(
            source.load(path.to_str().unwrap()).await.expect("load"),
            "return 1"
        );
    }

    #[tokio::test]
    async fn a_missing_file_fails() {
        let dir = TempDir::new("file-missing");
        let missing = dir.path("missing.lua");
        let source = FileSource::new();
        assert!(matches!(
            source.load(missing.to_str().unwrap()).await,
            Err(ScriptError::Failed(msg)) if msg.contains("file source")
        ));
    }

    #[tokio::test]
    async fn a_watched_file_modification_signals() {
        let dir = TempDir::new("file-watch");
        let path = dir.write("a.lua", "v1");
        let source = FileSource::new();
        let mut stream = source
            .watch(path.to_str().unwrap())
            .await
            .expect("watch comes up");
        tokio::time::sleep(Duration::from_millis(100)).await;
        std::fs::write(&path, "v2").expect("modify");
        tokio::time::timeout(TIMEOUT, stream.next())
            .await
            .expect("modification signals")
            .expect("stream alive");
        assert_eq!(
            source.load(path.to_str().unwrap()).await.expect("re-read"),
            "v2"
        );
    }

    #[tokio::test]
    async fn a_missing_file_cannot_be_watched() {
        let dir = TempDir::new("file-watch-missing");
        let missing = dir.path("missing.lua");
        let source = FileSource::new();
        assert!(matches!(
            source.watch(missing.to_str().unwrap()).await,
            Err(ScriptError::Failed(msg)) if msg.contains("file source")
        ));
    }
}
