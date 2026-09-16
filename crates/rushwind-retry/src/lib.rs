//! Composable retry for RushWind: configurable retry with exponential
//! backoff and optional jitter (full or equal), a maximum attempt
//! count and total timeout, retry predicates (deciding which errors
//! are retryable), and cancellation — here riding the awaited
//! future rather than a context parameter.
//!
//! The design is transport-agnostic: [`Retrier::execute`] wraps any
//! fallible async closure, composing with circuit breakers, rate
//! limiters, and any I/O operation.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

use std::future::Future;
use std::time::Duration;

/// The retry outcome: the last attempt's error after exhausting the
/// attempts, or a timeout.
#[derive(Debug)]
#[non_exhaustive]
pub enum RetryError<E> {
    /// All attempts were consumed; carries the final attempt's error.
    MaxAttempts(E),
    /// The total timeout was exceeded.
    Timeout,
}

impl<E: std::fmt::Display> std::fmt::Display for RetryError<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MaxAttempts(e) => write!(f, "retry: max attempts exceeded: {e}"),
            Self::Timeout => write!(f, "retry: total timeout exceeded"),
        }
    }
}

impl<E: std::error::Error + 'static> std::error::Error for RetryError<E> {}

/// The backoff strategy between attempts.
#[derive(Debug, Clone, Copy)]
pub enum Backoff {
    /// A fixed delay between attempts.
    Fixed(Duration),
    /// Exponential: `initial * factor^attempt`, capped at `max`.
    Exponential {
        /// The delay after the first failed attempt.
        initial: Duration,
        /// The multiplier per subsequent attempt.
        factor: f64,
        /// The ceiling.
        max: Duration,
    },
}

impl Default for Backoff {
    fn default() -> Self {
        // The defaults: exponential, 200 ms initial, factor 2,
        // max 10 s.
        Self::Exponential {
            initial: Duration::from_millis(200),
            factor: 2.0,
            max: Duration::from_secs(10),
        }
    }
}

impl Backoff {
    /// The delay before `attempt` (0-based: after the first failure).
    fn delay(&self, attempt: u32) -> Duration {
        match *self {
            Backoff::Fixed(d) => d,
            Backoff::Exponential {
                initial,
                factor,
                max,
            } => {
                let factor = if factor > 0.0 { factor } else { 2.0 };
                let millis = initial.as_millis() as f64 * factor.powi(attempt as i32);
                let capped = millis.min(max.as_millis() as f64);
                Duration::from_millis(capped as u64)
            }
        }
    }
}

/// The jitter strategy randomising backoff intervals.
#[derive(Debug, Clone, Copy, Default)]
pub enum Jitter {
    /// No randomisation.
    #[default]
    None,
    /// The delay is uniformly randomised in `[0, delay)` — spreads
    /// retried load maximally.
    Full,
    /// The delay is randomised in `[delay/2, delay)` — keeps a
    /// predictable floor.
    Equal,
}

impl Jitter {
    /// Applies the strategy to a computed delay.
    /// Applies the strategy to a computed delay (public for the
    /// conformance suite).
    pub fn apply(&self, delay: Duration) -> Duration {
        if delay.is_zero() || matches!(self, Jitter::None) {
            return delay;
        }
        let mut bytes = [0u8; 8];
        let _ = getrandom::fill(&mut bytes);
        let uniform = u64::from_le_bytes(bytes) as f64 / u64::MAX as f64;
        let millis = delay.as_millis() as f64;
        let jittered = match self {
            Jitter::None => millis,
            Jitter::Full => millis * uniform,
            Jitter::Equal => millis * (0.5 + 0.5 * uniform),
        };
        Duration::from_millis(jittered as u64)
    }
}

/// The retry configuration.
#[derive(Debug, Clone)]
pub struct Retrier {
    max_attempts: u32,
    backoff: Backoff,
    jitter: Jitter,
    max_total_wait: Option<Duration>,
}

impl Default for Retrier {
    fn default() -> Self {
        Self {
            max_attempts: 3,
            backoff: Backoff::default(),
            jitter: Jitter::None,
            max_total_wait: None,
        }
    }
}

impl Retrier {
    /// Builds a retrier with the defaults: 3 attempts, exponential
    /// backoff (200 ms initial, ×2, 10 s max), no jitter, no total
    /// timeout.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the maximum number of attempts including the first.
    /// Must be >= 1.
    pub fn max_attempts(mut self, n: u32) -> Self {
        if n >= 1 {
            self.max_attempts = n;
        }
        self
    }

    /// Sets the backoff strategy.
    pub fn backoff(mut self, backoff: Backoff) -> Self {
        self.backoff = backoff;
        self
    }

    /// Sets the jitter strategy.
    pub fn jitter(mut self, jitter: Jitter) -> Self {
        self.jitter = jitter;
        self
    }

    /// Sets the total-timeout bound across all attempts and waits.
    pub fn max_total_wait(mut self, timeout: Duration) -> Self {
        self.max_total_wait = Some(timeout);
        self
    }

    /// Executes the fallible closure, retrying per the configuration
    /// while `is_retryable` accepts the error. A closure error the
    /// predicate rejects surfaces immediately as
    /// [`RetryError::MaxAttempts`] carrying it — the caller's
    /// predicate decides retryability, not this type.
    pub async fn execute<F, Fut, T, E>(
        &self,
        is_retryable: impl Fn(&E) -> bool,
        f: F,
    ) -> Result<T, RetryError<E>>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = std::result::Result<T, E>>,
    {
        let started = std::time::Instant::now();
        let mut f = f;
        let mut last_error: Option<E> = None;

        for attempt in 0..self.max_attempts {
            if attempt > 0 {
                let delay = self.jitter.apply(self.backoff.delay(attempt - 1));
                if let Some(budget) = self.max_total_wait {
                    if started.elapsed() + delay >= budget {
                        return Err(RetryError::Timeout);
                    }
                }
                tokio::time::sleep(delay).await;
            }

            match f().await {
                Ok(value) => return Ok(value),
                Err(e) => {
                    if !is_retryable(&e) {
                        return Err(RetryError::MaxAttempts(e));
                    }
                    if let Some(budget) = self.max_total_wait {
                        if started.elapsed() >= budget {
                            return Err(RetryError::Timeout);
                        }
                    }
                    last_error = Some(e);
                }
            }
        }

        Err(RetryError::MaxAttempts(
            last_error.expect("at least one attempt must have run"),
        ))
    }
}
