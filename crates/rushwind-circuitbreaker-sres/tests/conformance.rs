//! SRE conformance: no errors = always accept; sustained errors drive
//! acceptance to zero; the state reflects the decay; and new windows
//! recover.

#![cfg(test)]

use std::time::Duration;

use rushwind_circuitbreaker_sres::SreBreaker;

fn breaker() -> SreBreaker {
    SreBreaker::with_options(rushwind_circuitbreaker_sres::SreOptions {
        k: 2.0,
        window: Duration::from_secs(10),
        bucket_count: 40,
    })
}

/// With zero traffic, acceptance is 1.0 — always allowed (the
/// "no data yet — allow" branch). After successes, acceptance
/// decays toward requests/(requests+1), so the overwhelming
/// majority still pass.
#[test]
fn no_errors_always_accepts() {
    let b = breaker();

    // Zero traffic: the "no data yet — allow" branch.
    for _ in 0..20 {
        assert!(b.allow(), "with no data yet, allow must always pass");
    }

    // Healthy traffic: acceptance = requests/(requests+1) — high but
    // probabilistic, so assert a large majority. Two hundred
    // successes first: acceptance concentrates near enough to one
    // that the sampling cannot dip below the floor.
    for _ in 0..200 {
        b.mark_success();
    }
    let mut accepted = 0;
    for _ in 0..100 {
        if b.allow() {
            accepted += 1;
        }
    }
    assert!(
        accepted >= 90,
        "healthy traffic must overwhelmingly pass: {accepted}/100"
    );
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

/// After failures, a recovery window with successes outweighing the
/// K-scaled error debt lifts acceptance back above zero — requests
/// pass again in the majority. The K=2 debt of 50 failures is 100
/// requests of credit, so the recovery phase supplies 200 successes.
#[test]
fn success_after_failures_recovers() {
    let b = breaker();

    // All-failure phase: the breaker decays toward full rejection.
    for _ in 0..50 {
        b.mark_failure();
    }
    let mut rejected = 0;
    for _ in 0..50 {
        if !b.allow() {
            rejected += 1;
        } else {
            // Only executed requests count — a rejected request must
            // not be marked (the contract: don't mark after a
            // failed Allow).
            b.mark_failure();
        }
    }
    assert!(rejected > 0, "the failure phase must reject sometimes");

    // Recovery phase: 200 successes clear the 50-failure debt (K=2)
    // and lift acceptance to (250-100)/251 ~= 0.6.
    for _ in 0..200 {
        b.mark_success();
    }
    let mut accepted = 0;
    for _ in 0..50 {
        if b.allow() {
            accepted += 1;
        }
        b.mark_success();
    }
    assert!(
        accepted >= 20,
        "acceptance must recover: accepted={accepted} of 50"
    );
}
