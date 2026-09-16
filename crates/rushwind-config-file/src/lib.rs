//! File engine for the Rust configuration contract.
//!
//! The carrier is one file: [`load`](FileSource::load) reads its whole
//! content as raw bytes, and [`watch_value`](FileSource::watch_value)
//! pushes the new content on every write. The default path is fixed at
//! construction; an explicit key on a call replaces it for that call.
//!
//! Watching follows a hard-won lesson: the **parent
//! directory** is watched, not the file — editors save by writing a
//! temp file and renaming over the target, and a file-level watch is
//! lost at the rename. Events for the target path with a create or
//! modify kind trigger a re-read; a read that fails (the write has not
//! landed yet) is skipped and the stream keeps waiting. The first
//! content is *not* pushed at watch start — only changes are.
//!
//! # Design notes
//!
//! - Each stream owns its watcher: two concurrent streams each see
//!   every event, where one shared watcher would split the events
//!   between them.
//! - Watching starts lazily with the stream.
//! - Event **bursts** coalesce into one delivery, and a re-read
//!   yielding already-delivered content is suppressed — desktop backends
//!   emit several (sometimes delayed) events per save, and a stale
//!   replay would serve an old value over a newer one.
//! - The stream ends when its watcher fails fatally; dropping the
//!   stream stops the watch (drop is the cancellation).
//!
//! Portability note: the event kinds that end a stream are
//! backend-specific; on the common desktop backends (inotify,
//! ReadDirectoryChangesW, FSEvents) a watch error is fatal and rare.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use notify::{EventKind, RecursiveMode, Watcher};
use rushwind_config::{BoxFuture, ConfigError, Source, ValueStream};
use tokio::sync::mpsc;

/// How often the pump thread re-checks the stop flag. A dropped stream
/// stops its thread within one interval.
const STOP_POLL: Duration = Duration::from_millis(200);

/// The file configuration source.
pub struct FileSource {
    default_path: PathBuf,
}

impl FileSource {
    /// Creates the source with the default config path. The path is
    /// resolved to absolute so watched events match it; an empty path
    /// is a construction error.
    pub fn new(path: &str) -> Result<Self, ConfigError> {
        if path.is_empty() {
            return Err(ConfigError::Failed("path invalid".to_string()));
        }
        Ok(Self {
            default_path: lexical_absolute(Path::new(path)),
        })
    }

    /// An explicit key replaces the default path for
    /// one call.
    fn resolve(&self, key: &str) -> PathBuf {
        if key.is_empty() {
            self.default_path.clone()
        } else {
            lexical_absolute(Path::new(key))
        }
    }
}

/// An absolute path stands; a relative one is
/// joined against the current directory. Lexical only — no existence
/// check.
fn lexical_absolute(path: &Path) -> PathBuf {
    if path.is_absolute() {
        path.to_path_buf()
    } else {
        match std::env::current_dir() {
            Ok(cwd) => cwd.join(path),
            // No current directory to anchor against: the path as given.
            Err(_) => path.to_path_buf(),
        }
    }
}

impl Source for FileSource {
    fn load<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<Option<Vec<u8>>, ConfigError>> {
        Box::pin(async move {
            let path = self.resolve(key);
            // A read failure — including a missing file — is an error.
            // Absence-as-None is the env
            // engine's answer, not this one's.
            std::fs::read(&path)
                .map(Some)
                .map_err(|e| ConfigError::Failed(format!("read file {}: {e}", path.display())))
        })
    }

    fn watch_value<'a>(
        &'a self,
        key: &'a str,
    ) -> BoxFuture<'a, Result<Box<dyn ValueStream>, ConfigError>> {
        Box::pin(async move {
            let path = self.resolve(key);

            // Watch the parent directory — file-level watches die with
            // the atomic rename editors save through.
            let dir = path
                .parent()
                .map(|p| p.to_path_buf())
                .unwrap_or_else(|| PathBuf::from("."));
            let (notify_tx, notify_rx) = std::sync::mpsc::channel();
            // On macOS the poll watcher, not FSEvents: FSEvents'
            // event delivery in headless CI environments is
            // unreliable — minutes late or never — and a sub-second
            // poll of the one watched directory is cheap.
            #[cfg(target_os = "macos")]
            let mut watcher = notify::PollWatcher::new(
                notify_tx,
                notify::Config::default().with_poll_interval(Duration::from_millis(500)),
            )
            .map_err(|e| ConfigError::Failed(format!("create file watcher: {e}")))?;
            #[cfg(not(target_os = "macos"))]
            let mut watcher = notify::recommended_watcher(notify_tx)
                .map_err(|e| ConfigError::Failed(format!("create file watcher: {e}")))?;
            watcher
                .watch(&dir, RecursiveMode::NonRecursive)
                .map_err(|e| {
                    ConfigError::Failed(format!("watch directory {}: {e}", dir.display()))
                })?;

            // The pump: watcher events on a std channel, values on a
            // tokio unbounded channel the async stream reads.
            let (value_tx, value_rx) = mpsc::unbounded_channel();
            let stop = Arc::new(AtomicBool::new(false));
            let target = path.clone();
            let thread_stop = Arc::clone(&stop);
            std::thread::spawn(move || {
                // Holding the watcher keeps the subscription alive for
                // the thread's lifetime.
                let _watcher = watcher;
                // The last content pushed. Some backends deliver
                // several events per save, some delayed; a re-read that
                // yields already-delivered content is suppressed so a
                // stale event never replays an old value over a newer
                // one.
                let mut last_sent: Option<Vec<u8>> = None;
                loop {
                    match notify_rx.recv_timeout(STOP_POLL) {
                        Ok(Ok(event)) => {
                            let relevant = event.paths.iter().any(|p| p == &target)
                                && matches!(
                                    event.kind,
                                    EventKind::Create(_) | EventKind::Modify(_)
                                );
                            if !relevant {
                                continue;
                            }
                            // One write surfaces as a burst of events on
                            // some backends; drain the burst and deliver
                            // one value for it — the change-notification
                            // semantics, not event replay.
                            let mut fatal = false;
                            loop {
                                match notify_rx.try_recv() {
                                    Ok(Ok(_)) => {}
                                    Ok(Err(_)) => fatal = true,
                                    Err(std::sync::mpsc::TryRecvError::Empty) => break,
                                    Err(std::sync::mpsc::TryRecvError::Disconnected) => {
                                        fatal = true;
                                        break;
                                    }
                                }
                            }
                            // The write may not have landed yet; a read
                            // that fails is skipped and the pump continues.
                            if let Ok(data) = std::fs::read(&target) {
                                if last_sent.as_deref() != Some(data.as_slice()) {
                                    last_sent = Some(data.clone());
                                    if value_tx.send(data).is_err() {
                                        // The stream is gone; stop pumping.
                                        break;
                                    }
                                }
                            }
                            if fatal {
                                // A fatal watch error ends the stream.
                                break;
                            }
                        }
                        Ok(Err(_)) => {
                            // A fatal watch error ends the stream.
                            break;
                        }
                        Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                            if thread_stop.load(Ordering::Relaxed) {
                                break;
                            }
                        }
                        Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => break,
                    }
                }
            });

            Ok(Box::new(FileValueStream {
                value_rx,
                _stop: StopFlag(stop),
            }) as Box<dyn ValueStream>)
        })
    }
}

/// A file-content change stream. Dropping it stops the directory watch
/// within [`STOP_POLL`].
pub struct FileValueStream {
    value_rx: mpsc::UnboundedReceiver<Vec<u8>>,
    _stop: StopFlag,
}

impl ValueStream for FileValueStream {
    fn next<'a>(&'a mut self) -> BoxFuture<'a, Option<Vec<u8>>> {
        Box::pin(async move { self.value_rx.recv().await })
    }
}

/// Sets the stop flag on drop, releasing the pump thread.
struct StopFlag(Arc<AtomicBool>);

impl Drop for StopFlag {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    /// One process-unique scratch directory per test; cleaned up on
    /// drop, best effort.
    struct Scratch(PathBuf);

    impl Scratch {
        fn new(tag: &str) -> Self {
            let dir = std::env::temp_dir()
                .join(format!("rushwind-config-file-{tag}-{}", std::process::id()));
            let _ = std::fs::remove_dir_all(&dir);
            std::fs::create_dir_all(&dir).expect("scratch dir created");
            Self(dir)
        }

        fn path(&self, name: &str) -> PathBuf {
            self.0.join(name)
        }
    }

    impl Drop for Scratch {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    async fn next_with_timeout(stream: &mut dyn ValueStream) -> Option<Vec<u8>> {
        // The budget is sized for FSEvents: macOS file-watch deliveries
        // routinely lag several seconds behind the write, so the tight
        // budgets the instant backends allow would flake the ARM lanes.
        tokio::time::timeout(Duration::from_secs(30), stream.next())
            .await
            .expect("stream delivers within the test budget")
    }

    #[tokio::test]
    async fn an_empty_path_is_a_construction_error() {
        assert!(matches!(FileSource::new(""), Err(ConfigError::Failed(_))));
    }

    #[tokio::test]
    async fn load_reads_the_whole_file() {
        let scratch = Scratch::new("load");
        let path = scratch.path("config.yaml");
        std::fs::write(&path, b"key: value").expect("seed file");
        let source = FileSource::new(path.to_str().expect("utf-8 path")).unwrap();
        assert_eq!(source.load("").await.unwrap(), Some(b"key: value".to_vec()));
    }

    #[tokio::test]
    async fn a_missing_file_is_an_error_not_absence() {
        let scratch = Scratch::new("missing");
        let path = scratch.path("nope.yaml");
        let source = FileSource::new(path.to_str().expect("utf-8 path")).unwrap();
        assert!(matches!(
            source.load("").await.unwrap_err(),
            ConfigError::Failed(_)
        ));
    }

    #[tokio::test]
    async fn an_explicit_key_replaces_the_default_path() {
        let scratch = Scratch::new("explicit");
        let other = scratch.path("other.yaml");
        std::fs::write(&other, b"other").expect("seed file");
        let source = FileSource::new(scratch.path("default.yaml").to_str().unwrap()).unwrap();
        assert_eq!(
            source.load(other.to_str().unwrap()).await.unwrap(),
            Some(b"other".to_vec())
        );
    }

    #[tokio::test]
    async fn watch_pushes_new_content_on_write() {
        let scratch = Scratch::new("watch");
        let path = scratch.path("live.yaml");
        std::fs::write(&path, b"v1").expect("seed file");
        let source = FileSource::new(path.to_str().expect("utf-8 path")).unwrap();

        let mut stream = source.watch_value("").await.unwrap();
        // Only changes are pushed, never the initial content — the
        // first delivery must come from the write below, so the write
        // lands before the wait starts.
        std::fs::write(&path, b"v2").expect("rewrite file");
        let value = next_with_timeout(stream.as_mut()).await;
        assert_eq!(value, Some(b"v2".to_vec()));

        // And again: the stream keeps watching after a delivery.
        std::fs::write(&path, b"v3").expect("rewrite file");
        let value = next_with_timeout(stream.as_mut()).await;
        assert_eq!(value, Some(b"v3".to_vec()));
    }

    #[tokio::test]
    async fn unrelated_directory_events_do_not_push() {
        let scratch = Scratch::new("unrelated");
        let watched = scratch.path("watched.yaml");
        let sibling = scratch.path("sibling.yaml");
        std::fs::write(&watched, b"v1").expect("seed file");
        std::fs::write(&sibling, b"s1").expect("seed sibling");
        let source = FileSource::new(watched.to_str().expect("utf-8 path")).unwrap();

        let mut stream = source.watch_value("").await.unwrap();
        // Churn the sibling: events fire on the directory, none of them
        // concern the watched file. The stream must stay silent.
        for round in 0..3 {
            std::fs::write(&sibling, format!("s{round}")).expect("rewrite sibling");
        }
        // Prove silence with a bounded wait, then prove liveness.
        let quiet = tokio::time::timeout(Duration::from_millis(500), stream.next()).await;
        assert!(quiet.is_err(), "sibling events must not push");
        std::fs::write(&watched, b"v2").expect("rewrite watched");
        assert_eq!(
            next_with_timeout(stream.as_mut()).await,
            Some(b"v2".to_vec())
        );
    }

    #[tokio::test]
    async fn dropping_the_stream_stops_the_watch() {
        let scratch = Scratch::new("drop");
        let path = scratch.path("dropped.yaml");
        std::fs::write(&path, b"v1").expect("seed file");
        let source = FileSource::new(path.to_str().expect("utf-8 path")).unwrap();
        let stream = source.watch_value("").await.unwrap();
        drop(stream);
        // No assertion beyond "this terminates and nothing panics": the
        // pump thread exits within STOP_POLL of the drop.
        tokio::time::sleep(Duration::from_millis(400)).await;
    }
}
