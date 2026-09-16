//! Token-bucket conformance: burst consumption, refill pacing, Wait
//! blocking, close semantics, and the invalid-config guard.

#![cfg(test)]

use std::time::Duration;

use rushwind_ratelimit::Limiter;
use rushwind_ratelimit_tokenbucket::{TokenBucket, TokenBucketOptions};

fn limiter(rate: f64, burst: f64) -> TokenBucket {
    TokenBucket::new(TokenBucketOptions { rate, burst }).expect("valid config")
}

/// The initial burst is immediately available, then the bucket is
/// empty and Allow rejects.
#[test]
fn burst_consumes_then_rejects() {
    let limiter = limiter(10.0, 3.0);
    assert!(limiter.allow(), "burst token 1");
    assert!(limiter.allow(), "burst token 2");
    assert!(limiter.allow(), "burst token 3");
    assert!(!limiter.allow(), "the empty bucket rejects");
}

/// Wait blocks until a token refills, then succeeds — the deficit is
/// exactly 1/rate seconds.
#[tokio::test]
async fn wait_acquires_after_refill() {
    let limiter = limiter(50.0, 1.0);
    assert!(limiter.allow(), "the initial token");

    let started = std::time::Instant::now();
    limiter.wait().await.expect("wait must acquire");
    let elapsed = started.elapsed();
    assert!(
        elapsed >= Duration::from_millis(15),
        "wait must block at least one refill tick, took {elapsed:?}"
    );
}

/// An invalid configuration (zero rate) fails construction.
#[test]
fn invalid_config_fails() {
    assert!(TokenBucket::new(TokenBucketOptions {
        rate: 0.0,
        burst: 10.0
    })
    .is_err());
    assert!(TokenBucket::new(TokenBucketOptions {
        rate: -1.0,
        burst: 10.0
    })
    .is_err());
    assert!(TokenBucket::new(TokenBucketOptions {
        rate: 10.0,
        burst: 0.0
    })
    .is_err());
}

/// Close rejects Allow immediately and fails Wait with Limited —
/// the close semantics.
#[tokio::test]
async fn close_rejects_and_fails_wait() {
    let limiter = limiter(10.0, 1.0);
    limiter.close();
    assert!(!limiter.allow(), "a closed limiter rejects");
    let result = limiter.wait().await;
    assert!(
        matches!(result, Err(rushwind_ratelimit::RateLimitError::Limited)),
        "wait on a closed limiter must fail with Limited, got {result:?}"
    );
}

/// Tokens refill over time at the configured rate: after draining,
/// waiting one rate period restores one token.
#[tokio::test]
async fn refill_paces_by_rate() {
    let limiter = limiter(20.0, 1.0);
    assert!(limiter.allow(), "the initial token");
    assert!(!limiter.allow(), "drained");

    tokio::time::sleep(Duration::from_millis(80)).await;
    assert!(
        limiter.allow(),
        "one token must have refilled after 80ms at 20/s"
    );
    assert!(!limiter.allow(), "drained again");
}
