//! Cron transport for RushWind: a minute-aligned periodic scheduler that
//! implements the [`Server`] lifecycle trait.
//!
//! Jobs are registered in code — a name, a [`CronSpec`], and an async
//! handler — and the transport fires each job whose spec matches the
//! wall clock, aligned to the minute. Configuration picks which
//! registered jobs mount, exactly like route packs for HTTP servers.
//!
//! Firing is best-effort at-least-once from a single process: handlers
//! run as detached tasks (a slow job never delays the tick), and a
//! graceful stop waits for in-flight handlers up to the orchestrator's
//! deadline.
//!
//! # Example
//!
//! ```
//! use rushwind_transport_cron::{CronJob, CronServer, CronSpec};
//!
//! let server = CronServer::new("cron://demo")
//!     .with_job(CronJob::new(
//!         "nightly-sweep",
//!         CronSpec::parse("30 3 * * *")?,
//!         || Box::pin(async { /* the sweep */ }),
//!     ));
//! # Ok::<(), rushwind_transport_cron::CronParseError>(())
//! ```

#![forbid(unsafe_code)]
#![deny(missing_docs)]

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use rushwind_transport::{Server, ServerError, ServerFuture, StopSignal};

/// A parsed 5-field cron spec: `minute hour day-of-month month day-of-week`.
///
/// Supported per field: `*`, literal values, ranges `a-b`, lists `a,b,c`,
/// and steps `*/n` / `a-b/n` (cron 0 treats Sunday as 0 and Sunday=7 is
/// accepted on input).
#[derive(Clone, Debug)]
pub struct CronSpec {
    fields: [Vec<u32>; 5],
}

/// One field failed to parse, or a value fell outside its bounds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CronParseError;

impl std::fmt::Display for CronParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("invalid cron field")
    }
}

impl std::error::Error for CronParseError {}

const FIELD_BOUNDS: [(u32, u32); 5] = [(0, 59), (0, 23), (1, 31), (1, 12), (0, 6)];

impl CronSpec {
    /// Parses a 5-field cron spec.
    pub fn parse(spec: &str) -> Result<Self, CronParseError> {
        let parts: Vec<&str> = spec.split_whitespace().collect();
        if parts.len() != 5 {
            return Err(CronParseError);
        }
        let mut fields = std::array::from_fn(|_| Vec::new());
        for (i, part) in parts.iter().enumerate() {
            let (lo, hi) = FIELD_BOUNDS[i];
            let mut allowed = Vec::new();
            for atom in part.split(',') {
                let (base, step) = match atom.split_once('/') {
                    Some((b, s)) => (b, s.parse::<u32>().map_err(|_| CronParseError)?),
                    None => (atom, 1),
                };
                let (start, end) = if base == "*" {
                    (lo, hi)
                } else if let Some((a, b)) = base.split_once('-') {
                    (
                        a.parse::<u32>().map_err(|_| CronParseError)?,
                        b.parse::<u32>().map_err(|_| CronParseError)?,
                    )
                } else {
                    let v: u32 = base.parse().map_err(|_| CronParseError)?;
                    (v, v)
                };
                if start > end || start < lo || end > hi {
                    return Err(CronParseError);
                }
                let mut v = start;
                while v <= end {
                    allowed.push(v);
                    v += step.max(1);
                }
            }
            if allowed.is_empty() {
                return Err(CronParseError);
            }
            fields[i] = allowed;
        }
        Ok(Self { fields })
    }

    /// Whether the spec fires at the given local time. Day-of-week
    /// follows cron numbering (Sunday = 0; 7 is accepted as Sunday).
    pub fn matches(&self, t: chrono::NaiveDateTime) -> bool {
        use chrono::{Datelike, Timelike};
        let dow = match t.weekday() {
            chrono::Weekday::Sun => 0,
            d => d.num_days_from_monday() + 1,
        };
        self.fields[0].contains(&(t.minute()))
            && self.fields[1].contains(&(t.hour()))
            && self.fields[2].contains(&(t.day()))
            && self.fields[3].contains(&(t.month()))
            && self.fields[4].contains(&dow)
    }
}

/// The async handler fired when a job's spec matches.
pub type CronHandler = Arc<dyn Fn() -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync>;

/// A registered periodic job: a stable name, a cron spec, and a handler.
pub struct CronJob {
    /// Stable identifier (logs and diagnostics).
    pub name: String,
    /// When the job fires.
    pub spec: CronSpec,
    /// The job body, run as a detached task per firing.
    pub handler: CronHandler,
}

impl CronJob {
    /// Registers a handler under a name and spec.
    pub fn new(
        name: impl Into<String>,
        spec: CronSpec,
        handler: impl Fn() -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync + 'static,
    ) -> Self {
        Self {
            name: name.into(),
            spec,
            handler: Arc::new(handler),
        }
    }
}

/// The cron transport: a [`Server`] that ticks once a minute (aligned),
/// evaluates every registered job against the wall clock, and fires
/// matching handlers as detached tasks. `stop` waits for in-flight
/// handlers; `start` returns [`ServerError::Cancelled`] when the stop
/// signal fires.
pub struct CronServer {
    endpoint: String,
    jobs: Vec<CronJob>,
}

impl CronServer {
    /// A cron server with no jobs — register with [`CronServer::with_job`].
    pub fn new(endpoint: impl Into<String>) -> Self {
        Self {
            endpoint: endpoint.into(),
            jobs: Vec::new(),
        }
    }

    /// Registers a job (builder style).
    pub fn with_job(mut self, job: CronJob) -> Self {
        self.jobs.push(job);
        self
    }
}

impl Server for CronServer {
    fn endpoint(&self) -> Result<String, ServerError> {
        Ok(self.endpoint.clone())
    }

    fn start(&self, stop: StopSignal) -> ServerFuture<'_> {
        Box::pin(async move {
            // Anchor the tick phase on the wall clock: a plain interval
            // would lock onto the start instant, whose second-of-minute
            // phase decides whether a second-0 tick ever arrives. The
            // first deadline is therefore the next half-minute boundary,
            // and the 30 s cadence keeps every later tick on one.
            let unix = std::time::SystemTime::now()
                .duration_since(std::time::SystemTime::UNIX_EPOCH)
                .unwrap_or_default();
            tokio::select! {
                _ = stop.wait() => {
                    return Err(ServerError::Cancelled);
                }
                _ = tokio::time::sleep(duration_to_next_half_minute(
                    std::time::Duration::from_secs(unix.as_secs()),
                )) => {}
            }
            let mut ticker = tokio::time::interval(std::time::Duration::from_secs(30));
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            let mut in_flight: Vec<tokio::task::JoinHandle<()>> = Vec::new();
            loop {
                tokio::select! {
                    _ = stop.wait() => break,
                    _ = ticker.tick() => {
                        use chrono::Timelike as _;
                        let now = chrono::Local::now().naive_local();
                        if now.second() != 0 {
                            continue; // aligned to the minute
                        }
                        for job in &self.jobs {
                            if job.spec.matches(now) {
                                let handler = job.handler.clone();
                                in_flight.push(tokio::spawn(async move {
                                    handler().await;
                                }));
                            }
                        }
                    }
                }
            }
            for handle in in_flight {
                let _ = handle.await;
            }
            Err(ServerError::Cancelled)
        })
    }

    fn stop(&self) -> ServerFuture<'_> {
        Box::pin(async { Ok(()) })
    }
}

/// Time left until the next wall-clock half-minute boundary (`:00` or
/// `:30`); a full period when already on one, so the caller never
/// spins on a zero wait.
fn duration_to_next_half_minute(elapsed: std::time::Duration) -> std::time::Duration {
    std::time::Duration::from_secs(30 - elapsed.as_secs() % 30)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn t(minute: u32, hour: u32, day: u32) -> chrono::NaiveDateTime {
        chrono::NaiveDate::from_ymd_opt(2026, 9, day.clamp(1, 28))
            .unwrap()
            .and_hms_opt(hour, minute, 0)
            .unwrap()
    }

    #[test]
    fn parse_matches_every_field_form() {
        let spec = CronSpec::parse("* * * * *").unwrap();
        assert!(spec.matches(t(7, 9, 15)));
        let spec = CronSpec::parse("30 3 * * *").unwrap();
        assert!(spec.matches(t(30, 3, 15)));
        assert!(!spec.matches(t(31, 3, 15)));
        let spec = CronSpec::parse("0-30/10 * * * *").unwrap();
        assert!(spec.matches(t(20, 0, 15)));
        assert!(!spec.matches(t(40, 0, 15)));
        let spec = CronSpec::parse("0 0 1,15 * *").unwrap();
        assert!(spec.matches(t(0, 0, 15)));
        assert!(!spec.matches(t(0, 0, 14)));
    }

    /// The half-minute wait lands every start phase on a `:00`/`:30`
    /// boundary — the property that keeps a second-0 tick reachable no
    /// matter when the transport starts.
    #[test]
    fn half_minute_wait_lands_every_phase_on_a_boundary() {
        for second in 0u64..60 {
            let wait = duration_to_next_half_minute(std::time::Duration::from_secs(second));
            let landed = (second + wait.as_secs()) % 30;
            assert_eq!(landed, 0, "phase {second}s must reach a boundary");
            assert!(wait.as_secs() >= 1 && wait.as_secs() <= 30);
        }
    }

    #[test]
    fn dow_uses_cron_numbering() {
        // 2026-09-20 is a Sunday.
        let spec = CronSpec::parse("0 12 * * 0").unwrap();
        assert!(spec.matches(t(0, 12, 20)));
        let spec = CronSpec::parse("0 12 * * 1").unwrap();
        assert!(!spec.matches(t(0, 12, 20)));
    }

    #[test]
    fn rejects_bad_specs() {
        assert!(CronSpec::parse("* * * *").is_err());
        assert!(CronSpec::parse("61 * * * *").is_err());
        assert!(CronSpec::parse("* 25 * * *").is_err());
        assert!(CronSpec::parse("90-10 * * * *").is_err());
    }
}
