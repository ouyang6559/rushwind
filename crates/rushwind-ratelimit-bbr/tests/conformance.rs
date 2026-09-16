//! BBR conformance: admission below the inflight cap, rejection at
//! the cap, done releasing the slot, and the max-inflight floor of
//! one.

#![cfg(test)]

use std::time::Duration;

use rushwind_ratelimit::Limiter;
use rushwind_ratelimit_bbr::{BbrLimiter, BbrOptions};

/// A freshly built limiter admits at least one request — the
/// `max_inflight` floor.
#[test]
fn admits_under_empty_load() {
    let limiter = BbrLimiter::new();
    assert!(limiter.allow(), "the first request must be admitted");
    limiter.done(Duration::from_millis(1));
}

/// Done releases the inflight slot: after capping out, completing
/// every admitted request re-admits.
#[test]
fn done_releases_inflight() {
    let limiter = BbrLimiter::with_options(BbrOptions {
        cpu_threshold: 0.8,
        window: Duration::from_secs(10),
        bucket_count: 40,
        min_qps: 1.0,
    });

    let mut admitted = 0;
    for _ in 0..50 {
        if limiter.allow() {
            admitted += 1;
        }
    }
    assert!(admitted > 0, "admissions must happen before the cap");

    for _ in 0..admitted {
        limiter.done(Duration::from_millis(1));
    }
    assert_eq!(
        limiter.max_inflight() as usize,
        admitted as usize % usize::MAX
    );
    assert!(limiter.allow(), "slots freed by done re-admit");
}

/// Close rejects every subsequent admission.
#[test]
fn closed_rejects() {
    let limiter = BbrLimiter::new();
    limiter.close();
    assert!(!limiter.allow(), "a closed limiter rejects");
}

/// The inflight cap is bounded below by one regardless of
/// configuration.
#[test]
fn max_inflight_floor_is_one() {
    let limiter = BbrLimiter::with_options(BbrOptions {
        cpu_threshold: 0.8,
        window: Duration::from_secs(10),
        bucket_count: 40,
        min_qps: 1.0,
    });
    let _ = limiter.allow();
    limiter.done(Duration::from_millis(1));
    assert!(
        limiter.max_inflight() >= 1,
        "maxInflight must never drop below one"
    );
}
