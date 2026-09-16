//! Google-SRE probabilistic circuit breaker for the RushWind
//! circuitbreaker contract.
//!
//! From "Site Reliability Engineering" (chapter 22): unlike
//! threshold-based breakers, acceptance is probabilistic —
//! `accept = max(0, (requests - K·errors) / (requests + 1))` — so the
//! rejection rate rises smoothly with the error ratio instead of
//! cutting the service off abruptly. There is no hard open/close
//! transition.
//!
//! `K` controls sensitivity: `K = 1` never trips (rejects only above
//! a 100% error rate), `K = 2` starts rejecting above 50%, `K = 0.5`
//! above 200% (lenient).
//!
//! The sliding window of request/error buckets rotates like the BBR
//! engine's. `State` is derived, not stored: the breaker is
//! conceptually `closed`, reporting `open` when acceptance decays to
//! zero and `half-open` while partially accepting.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

use std::sync::Mutex;
use std::time::{Duration, Instant};

use rushwind_circuitbreaker::{CircuitBreaker, CircuitError, State};

/// The sensitivity multiplier. Default 2.0.
const DEFAULT_K: f64 = 2.0;
/// The sliding-window length. Default 10 s.
const DEFAULT_WINDOW: Duration = Duration::from_secs(10);
/// The number of buckets in the window. Default 40.
const DEFAULT_BUCKET_COUNT: usize = 40;

#[derive(Clone)]
struct SreBucket {
    requests: u64,
    errors: u64,
}

struct SreState {
    buckets: Vec<SreBucket>,
    last_rotate: Instant,
    closed: bool,
}

/// A Google-SRE probabilistic circuit breaker.
pub struct SreBreaker {
    state: Mutex<SreState>,
    k: f64,
    bucket_duration: Duration,
    start: Instant,
}

impl SreBreaker {
    /// Builds an SRE breaker with the default options.
    pub fn new() -> Self {
        Self::with_options(SreOptions::default())
    }

    /// Builds an SRE breaker with explicit options.
    pub fn with_options(options: SreOptions) -> Self {
        let bucket_duration = options.window / options.bucket_count as u32;
        Self {
            state: Mutex::new(SreState {
                buckets: vec![
                    SreBucket {
                        requests: 0,
                        errors: 0,
                    };
                    options.bucket_count
                ],
                last_rotate: Instant::now(),
                closed: false,
            }),
            k: options.k,
            bucket_duration,
            start: Instant::now(),
        }
    }

    /// Constructs from the bootstrap factory's settings wire shape.
    pub fn from_settings(settings: serde_json::Value) -> Result<Self, CircuitError> {
        #[derive(serde::Deserialize)]
        struct SreSettings {
            k: Option<f64>,
            window_ms: Option<u64>,
            bucket_count: Option<usize>,
        }
        let settings: SreSettings = serde_json::from_value(settings)
            .map_err(|e| CircuitError::Failed(format!("settings parse: {e}")))?;
        let defaults = SreOptions::default();
        Ok(Self::with_options(SreOptions {
            k: settings.k.unwrap_or(defaults.k),
            window: Duration::from_millis(
                settings
                    .window_ms
                    .unwrap_or(defaults.window.as_millis() as u64),
            ),
            bucket_count: settings.bucket_count.unwrap_or(defaults.bucket_count),
        }))
    }

    /// Attempts to admit a request — the SRE acceptance formula
    /// `max(0, (requests - K·errors) / (requests + 1))` as a
    /// probability; no data yet always admits.
    pub fn allow(&self) -> bool {
        let mut state = self.state.lock().unwrap();
        if state.closed {
            return false;
        }
        self.rotate_locked(&mut state, Instant::now());

        let (requests, errors) = self.window_totals(&state);
        let accept = if requests > 0 {
            ((requests as f64 - self.k * errors as f64) / (requests as f64 + 1.0)).max(0.0)
        } else {
            1.0 // no data yet — allow
        };

        if accept >= 1.0 {
            return true;
        }
        random_unit() < accept
    }

    /// Records a successful request outcome.
    pub fn mark_success(&self) {
        let mut state = self.state.lock().unwrap();
        self.rotate_locked(&mut state, Instant::now());
        let index = self.bucket_index(&state, Instant::now());
        state.buckets[index].requests += 1;
    }

    /// Records a failed request outcome.
    pub fn mark_failure(&self) {
        let mut state = self.state.lock().unwrap();
        self.rotate_locked(&mut state, Instant::now());
        let index = self.bucket_index(&state, Instant::now());
        state.buckets[index].requests += 1;
        state.buckets[index].errors += 1;
    }

    /// The derived state: `closed` with no data,
    /// `half-open` while partially accepting, `open` when acceptance
    /// has decayed to zero.
    pub fn state(&self) -> State {
        let state = self.state.lock().unwrap();
        let (requests, errors) = self.window_totals(&state);
        if requests == 0 {
            return State::Closed;
        }
        let accept = (requests as f64 - self.k * errors as f64) / (requests as f64 + 1.0);
        if accept <= 0.0 {
            State::Open
        } else if accept < 1.0 {
            State::HalfOpen
        } else {
            State::Closed
        }
    }

    /// The window totals.
    fn window_totals(&self, state: &SreState) -> (u64, u64) {
        let mut requests = 0u64;
        let mut errors = 0u64;
        for bucket in &state.buckets {
            requests += bucket.requests;
            errors += bucket.errors;
        }
        (requests, errors)
    }

    /// Clears buckets that fell out of the
    /// window.
    fn rotate_locked(&self, state: &mut SreState, now: Instant) {
        let elapsed = now.duration_since(state.last_rotate);
        let steps = (elapsed.as_nanos() / self.bucket_duration.as_nanos()) as usize;
        if steps == 0 {
            return;
        }
        let n = state.buckets.len();
        if steps >= n {
            state.buckets.fill(SreBucket {
                requests: 0,
                errors: 0,
            });
        } else {
            for offset in 0..steps {
                let current = self.bucket_index(state, now);
                let idx = (current + n - steps + offset + 1) % n;
                state.buckets[idx] = SreBucket {
                    requests: 0,
                    errors: 0,
                };
            }
        }
        state.last_rotate = now;
    }

    /// The ring index for `now`, anchored at the limiter's
    /// construction.
    fn bucket_index(&self, state: &SreState, now: Instant) -> usize {
        let elapsed = now.duration_since(self.start);
        let bucket_nanos = self.bucket_duration.as_nanos();
        (elapsed.as_nanos() / bucket_nanos) as usize % state.buckets.len()
    }
}

impl Default for SreBreaker {
    fn default() -> Self {
        Self::new()
    }
}

impl CircuitBreaker for SreBreaker {
    fn allow(&self) -> std::result::Result<(), CircuitError> {
        if SreBreaker::allow(self) {
            Ok(())
        } else {
            Err(CircuitError::Open)
        }
    }

    fn mark_success(&self) {
        SreBreaker::mark_success(self);
    }

    fn mark_failure(&self) {
        SreBreaker::mark_failure(self);
    }

    fn state(&self) -> State {
        SreBreaker::state(self)
    }

    fn close(&self) {
        self.state.lock().unwrap().closed = true;
    }
}

/// A uniform random sample in `[0, 1)` from the OS CSPRNG.
fn random_unit() -> f64 {
    let mut bytes = [0u8; 8];
    let _ = getrandom::fill(&mut bytes);
    u64::from_le_bytes(bytes) as f64 / u64::MAX as f64
}

/// The SRE engine's settings.
#[derive(Debug, Clone)]
pub struct SreOptions {
    /// The sensitivity multiplier. Default 2.0.
    pub k: f64,
    /// The sliding-window length. Default 10 s.
    pub window: Duration,
    /// The number of buckets in the window. Default 40.
    pub bucket_count: usize,
}

impl Default for SreOptions {
    fn default() -> Self {
        Self {
            k: DEFAULT_K,
            window: DEFAULT_WINDOW,
            bucket_count: DEFAULT_BUCKET_COUNT,
        }
    }
}
