//! SRE conformance: no errors = always accept; sustained errors drive
//! acceptance to zero; the state reflects the decay; and new windows
//! recover.

#![cfg(test)]

use std::time::Duration;

use rushwind_circuitbreaker::CircuitBreaker;
use rushwind_circuitbreaker_sres::SreBreaker;

fn breaker() -> SreBreaker {
    SreBreaker::with_options(rushwind_circuitbreaker_sres::SreOptions {
        k: 2.0,
        window: Duration::from_secs(10),
        bucket_count: 40,
    })
}

/// With zero errors, every request is accepted.
#[test]
fn no_errors_always_accepts() {
    let b = breaker();
    for _ in 0..10 {
        b.mark_success();
    }
    for _ in 0..20 {
        assert!(b.allow(), "healthy traffic must always be accepted");
    }
}

/// Sustained failures drive the breaker open — the acceptance
/// probability decays smoothly but reaches zero.
#[test]
fn sustained_errors_reject_everything() {
    let b = breaker();
    for _ in 0..200 {
        b.mark_failure();
    }
    let mut any_accepted = false;
    for _ in 0..100 {
        if b.allow() {
            any_accepted = true;
        }
    }
    assert!(!any_accepted, "all-failure traffic must be rejected");
    assert_eq!(b.state(), rushwind_circuitbreaker::State::Open);
}

/// After failures, successful traffic recovers acceptance.
#[test]
fn success_after_failures_recovers() {
    let b = breaker();
    for _ in 0..100 {
        b.mark_failure();
    }
    for _ in 0..50 {
        b.mark_success();
    }
    let state = b.state();
    assert!(
        state != rushwind_circuitbreaker::State::Open,
        "mixed success must not keep the breaker fully open"
    );
}
