//! Span emission: every operation produces a `rushwind.storage` span with
//! table / op / outcome, captured through a real fmt subscriber.

use std::io::Write;
use std::sync::{Arc, Mutex};

use rushwind_storage::{QueryCtx, Record, Repository, Value};
use rushwind_storage_memory::MemoryRepo;
use rushwind_storage_observe::ObservedRepo;

/// A fmt-subscriber writer that keeps everything in memory for assertions.
#[derive(Clone, Default)]
struct Capture(Arc<Mutex<Vec<u8>>>);

impl Capture {
    fn contents(&self) -> String {
        String::from_utf8_lossy(&self.0.lock().expect("capture poisoned")).into_owned()
    }
}

impl Write for Capture {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0
            .lock()
            .expect("capture poisoned")
            .extend_from_slice(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// Runs `body` with the observed repository under a capturing subscriber.
/// `with_default` is thread-local, so parallel tests stay isolated.
async fn under_capture<T>(body: impl AsyncFnOnce(Arc<dyn Repository>, Capture) -> T) -> T {
    let capture = Capture::default();
    let inner = MemoryRepo::new(rushwind_testkit::storage_conformance::suite_schema())
        .expect("schema is valid");
    let subscriber = tracing_subscriber::fmt()
        .with_ansi(false)
        .with_max_level(tracing::Level::INFO)
        .with_span_events(tracing_subscriber::fmt::format::FmtSpan::CLOSE)
        .with_writer({
            let capture = capture.clone();
            move || capture.clone()
        })
        .finish();
    let repo = ObservedRepo::new(Arc::new(inner)) as Arc<dyn Repository>;
    tracing::subscriber::with_default(subscriber, || {
        futures::executor::block_on(body(repo, capture.clone()))
    })
}

#[tokio::test]
async fn operations_emit_outcome_spans() {
    let output = under_capture(async |repo: Arc<dyn Repository>, capture| {
        let stored = repo
            .create(QueryCtx::all_access(), Record::new().set("name", "bolt"))
            .await
            .expect("create lands");
        let id = stored.get("id").and_then(Value::as_i64).expect("int id");
        repo.get(QueryCtx::all_access(), Value::Int(id))
            .await
            .expect("get lands");
        repo.get(QueryCtx::all_access(), Value::Int(404))
            .await
            .expect("missing get lands");
        repo.delete(QueryCtx::all_access(), Value::Int(id))
            .await
            .expect("delete lands");
        repo.delete(QueryCtx::all_access(), Value::Int(id))
            .await
            .expect_err("second delete is NotFound");
        capture.contents()
    })
    .await;

    assert!(
        output.contains("rushwind.storage"),
        "spans carry the name: {output}"
    );
    assert!(output.contains(r#"op="create""#), "{output}");
    assert!(output.contains(r#"op="get""#), "{output}");
    assert!(output.contains(r#"op="delete""#), "{output}");
    assert!(output.contains("table=widgets"), "{output}");
    assert!(
        output.matches(r#"outcome="ok""#).count() >= 3,
        "successful calls record ok: {output}"
    );
    assert!(
        output.matches(r#"outcome="missing""#).count() >= 2,
        "misses record missing (a 404 get and a second delete): {output}"
    );
}

#[tokio::test]
async fn schema_passes_through() {
    let repo =
        under_capture(async |repo: Arc<dyn Repository>, _capture| repo.schema().table.clone())
            .await;
    assert_eq!(repo, "widgets");
}
