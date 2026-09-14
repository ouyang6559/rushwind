//! Retry conformance: max attempts, backoff pacing, jitter bounds,
//! non-retryable short-circuit, and the total timeout — the Go
//! `retry_test.go` shapes.

#![cfg(test)]

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

use rushwind_retry::{Backoff, Jitter, Retrier};

/// Attempts count includes the first: max_attempts 3 means one
/// initial call plus two retries, then MaxAttempts carries the last
/// error.
#[tokio::test]
async fn max_attempts_carries_last_error() {
    let retrier = Retrier::default().max_attempts(3);
    let calls = Arc::new(AtomicU32::new(0));
    let result: Result<(), _> = retrier
        .execute(
            |_| true,
            || {
                calls.fetch_add(1, Ordering::SeqCst);
                async { Err("transient".to_string()) }
            },
        )
        .await;
    match result {
        Err(rushwind_retry::RetryError::MaxAttempts(e)) => {
            assert_eq!(e, "transient");
            assert_eq!(calls.load(Ordering::SeqCst), 3);
        }
        other => panic!("expected MaxAttempts, got {other:?}"),
    }
}

/// A non-retryable error surfaces immediately without burning the
/// remaining attempts — the Go Classifier.
#[tokio::test]
async fn non_retryable_short_circuits() {
    let retrier = Retrier::default().max_attempts(5);
    let calls = Arc::new(AtomicU32::new(0));
    let result: Result<(), _> = retrier
        .execute(
            |e: &String| e == "transient",
            || {
                calls.fetch_add(1, Ordering::SeqCst);
                async { Err("permanent".to_string()) }
            },
        )
        .await;
    match result {
        Err(rushwind_retry::RetryError::MaxAttempts(e)) => {
            assert_eq!(e, "permanent");
            assert_eq!(calls.load(Ordering::SeqCst), 1, "no retries burned");
        }
        other => panic!("expected MaxAttempts, got {other:?}"),
    }
}

/// The success path: a failing closure that recovers on the second
/// attempt returns the value.
#[tokio::test]
async fn recovers_after_failures() {
    let retrier = Retrier::default().max_attempts(5);
    let attempts = Arc::new(AtomicU32::new(0));
    let attempts_for_closure = Arc::clone(&attempts);
    let result: Result<u32, _> = retrier
        .execute(
            |_| true,
            move || {
                let attempts = Arc::clone(&attempts_for_closure);
                async move {
                    let n = attempts.fetch_add(1, Ordering::SeqCst) + 1;
                    if n < 3 {
                        Err("flaky".to_string())
                    } else {
                        Ok(n)
                    }
                }
            },
        )
        .await;
    assert_eq!(result.expect("must succeed"), 3);
    assert_eq!(attempts.load(Ordering::SeqCst), 3);
}

/// The total timeout bounds the retries even when attempts remain.
#[tokio::test]
async fn total_timeout_bounds_the_retries() {
    let retrier = Retrier::default()
        .max_attempts(10)
        .backoff(Backoff::Fixed(Duration::from_millis(100)))
        .max_total_wait(Duration::from_millis(250));
    let result: Result<(), _> = retrier
        .execute(|_| true, || async { Err("slow".to_string()) })
        .await;
    assert!(
        matches!(result, Err(rushwind_retry::RetryError::Timeout)),
        "the budget must expire before the attempts run out, got {result:?}"
    );
}

/// Full jitter stays within `[0, delay)`; equal jitter within
/// `[delay/2, delay)` — sampled over many draws.
#[test]
fn jitter_stays_in_bounds() {
    let delay = Duration::from_millis(1000);
    for _ in 0..100 {
        let full = Jitter::Full.apply(delay);
        assert!(full < delay, "full jitter must stay below the delay");
        let equal = Jitter::Equal.apply(delay);
        assert!(
            equal >= Duration::from_millis(500) && equal < delay,
            "equal jitter must land in [delay/2, delay), got {equal:?}"
        );
    }
}
