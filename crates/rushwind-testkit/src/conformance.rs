//! The conformance suite macro.
//!
//! [`rushwind_conformance_suite!`] expands to a module of `#[tokio::test]`
//! functions pinning the [`App`](crate::rushwind_core::App) lifecycle and
//! the adapter-side obligations of the [`Server`](crate::rushwind_transport::Server)
//! contract. See the crate-level documentation for the wiring contract.

/// Generates the full lifecycle conformance suite for a
/// [`Server`](crate::rushwind_transport::Server) implementation.
///
/// `$factory` must be a **crate-rooted path** to a function of type
/// `fn() -> Arc<dyn Server>` producing a **fresh** instance of the
/// adapter's server (e.g. `crate::my_server`); the suite expands into a
/// nested module, where a bare identifier from the invoking module does
/// not resolve. It feeds the adapter-behavior half of the suite; the
/// orchestrator-semantics half runs against the built-in probes and makes
/// no demands of the adapter.
///
/// Invoke this from a test module inside the adapter crate's
/// integration tests (see the crate-level documentation for the full
/// wiring, including the required `tokio` dev-dependency).
#[macro_export]
macro_rules! rushwind_conformance_suite {
    ($factory:expr) => {
        mod rushwind_conformance {
            use std::sync::{Arc, Mutex};
            use std::time::Duration;

            use $crate::probe::{ProbeKind, ProbeServer};
            use $crate::rushwind_core::App;
            use $crate::rushwind_transport::{Server, ServerError, StopSignal};

            // ---- shared helpers -------------------------------------------------

            /// Runs the app under a 10 s meta-deadline so that a broken
            /// lifecycle (e.g. a cascade that never tears a sibling down)
            /// fails the test instead of hanging it.
            async fn bounded_run(app: &App, ext: StopSignal) -> Result<(), ServerError> {
                match tokio::time::timeout(Duration::from_secs(10), app.run(ext)).await {
                    Ok(r) => r,
                    Err(_) => panic!("App::run did not return within the meta-deadline"),
                }
            }

            /// Fires `signal` after `delay_ms`, off-loop, so the lifecycle
            /// under test reaches its steady state first.
            fn spawn_delayed_signal(signal: StopSignal, delay_ms: u64) {
                tokio::spawn(async move {
                    tokio::time::sleep(Duration::from_millis(delay_ms)).await;
                    signal.signal();
                });
            }

            // ---- orchestrator semantics (probe-driven) --------------------------

            #[tokio::test]
            async fn shutdown_cascades_on_server_failure() {
                let failing = Arc::new(ProbeServer::new(ProbeKind::FailAfter(50)));
                let sibling = Arc::new(ProbeServer::new(ProbeKind::Normal));
                let sibling_log = sibling.log();
                let app = App::builder()
                    .server(failing)
                    .server(sibling)
                    .stop_timeout(Duration::from_secs(10))
                    .build();
                let outcome = bounded_run(&app, StopSignal::new()).await;
                assert!(
                    outcome.is_err(),
                    "a failing server must surface an error outcome"
                );
                assert!(
                    sibling_log
                        .lock()
                        .expect("probe log poisoned")
                        .contains(&"stop:invoked"),
                    "the failing server's sibling must still be torn down"
                );
            }

            #[tokio::test]
            async fn shutdown_phases_run_in_order() {
                let events: Arc<Mutex<Vec<&'static str>>> = Arc::new(Mutex::new(Vec::new()));
                let probe = Arc::new(ProbeServer::with_log(
                    ProbeKind::Normal,
                    Arc::clone(&events),
                ));
                let before_events = Arc::clone(&events);
                let after_events = Arc::clone(&events);
                let app = App::builder()
                    .server(probe)
                    .stop_timeout(Duration::from_secs(10))
                    .before_stop(move |_| {
                        let ev = Arc::clone(&before_events);
                        async move {
                            ev.lock().expect("log poisoned").push("hook:before");
                            Ok(())
                        }
                    })
                    .after_stop(move |_| {
                        let ev = Arc::clone(&after_events);
                        async move {
                            ev.lock().expect("log poisoned").push("hook:after");
                            Ok(())
                        }
                    })
                    .build();
                let ext = StopSignal::new();
                spawn_delayed_signal(ext.clone(), 50);
                let outcome = bounded_run(&app, ext).await;
                assert!(outcome.is_ok());
                assert_eq!(
                    *events.lock().expect("log poisoned"),
                    vec!["hook:before", "stop:invoked", "hook:after"],
                    "lifecycle phases must run: before-hooks, teardown, after-hooks"
                );
            }

            #[tokio::test]
            async fn stop_phase_deadline_is_enforced() {
                let hanging = Arc::new(ProbeServer::new(ProbeKind::HangStop));
                let app = App::builder()
                    .server(hanging)
                    .stop_timeout(Duration::from_millis(300))
                    .build();
                let ext = StopSignal::new();
                spawn_delayed_signal(ext.clone(), 50);
                let outcome = bounded_run(&app, ext).await;
                let timed_out = match outcome {
                    Err(ServerError::Timeout) => true,
                    _ => false,
                };
                assert!(
                    timed_out,
                    "a hanging teardown must be cut off and surface ServerError::Timeout"
                );
            }

            #[tokio::test]
            async fn ill_behaved_server_is_abandoned_at_deadline() {
                let stubborn = Arc::new(ProbeServer::new(ProbeKind::IgnoreSignal));
                let app = App::builder()
                    .server(stubborn)
                    .stop_timeout(Duration::from_millis(300))
                    .build();
                let ext = StopSignal::new();
                spawn_delayed_signal(ext.clone(), 50);
                // If the drain deadline is not enforced, this never returns
                // and the meta-deadline fails the test.
                let outcome = bounded_run(&app, ext).await;
                assert!(outcome.is_ok());
            }

            #[tokio::test]
            async fn panicking_server_is_isolated_and_siblings_still_tear_down() {
                let panicking = Arc::new(ProbeServer::new(ProbeKind::PanicAfter(50)));
                let sibling = Arc::new(ProbeServer::new(ProbeKind::Normal));
                let sibling_log = sibling.log();
                let app = App::builder()
                    .server(panicking)
                    .server(sibling)
                    .stop_timeout(Duration::from_secs(10))
                    .build();
                let outcome = bounded_run(&app, StopSignal::new()).await;
                let isolated = match outcome {
                    Err(ServerError::Panicked(_)) => true,
                    _ => false,
                };
                assert!(
                    isolated,
                    "a panicking server must surface ServerError::Panicked"
                );
                assert!(
                    sibling_log
                        .lock()
                        .expect("probe log poisoned")
                        .contains(&"stop:invoked"),
                    "the panicking server's sibling must still be torn down"
                );
            }

            // ---- adapter behavior (factory-driven) ------------------------------

            #[tokio::test]
            async fn endpoint_is_uri_shaped() {
                let server = $factory();
                match server.endpoint() {
                    Ok(ep) => assert!(
                        ep.contains("://"),
                        "endpoint() must return scheme://authority form, got: {ep}"
                    ),
                    Err(e) => panic!("endpoint() must resolve before start: {e}"),
                }
            }

            #[tokio::test]
            async fn cooperative_shutdown_completes() {
                let server = $factory();
                let app = App::builder()
                    .erased_server(server)
                    .stop_timeout(Duration::from_secs(10))
                    .build();
                let ext = StopSignal::new();
                spawn_delayed_signal(ext.clone(), 50);
                let outcome = bounded_run(&app, ext).await;
                assert!(
                    outcome.is_ok(),
                    "a well-behaved server must shut down cleanly on signal"
                );
            }
        }
    };
}
