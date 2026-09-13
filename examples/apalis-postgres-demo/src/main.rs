//! Pushes, schedules and consumes apalis tasks through the Postgres
//! storage — the whole delivery story (claim, ack, retry backoff,
//! dead-letter, orphan recovery) running against a real queue.
//!
//! Point `APALIS_PG_DATABASE_URL` at a Postgres instance
//! (default `postgres://rushwind:rushwind@127.0.0.1:5432/rushwind`).
//! The schema is created on startup; run the consumer and the producer
//! in separate shells to watch the queue drain:
//!
//! ```bash
//! cargo run -p apalis-postgres-demo -- consume
//! cargo run -p apalis-postgres-demo -- produce
//! ```

use apalis_core::{
    builder::{WorkerBuilder, WorkerFactoryFn},
    monitor::Monitor,
    storage::Storage,
};
use rushwind_apalis_postgres::PostgresStorage;
use serde::{Deserialize, Serialize};

/// The task payload: an email to send.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct Email {
    to: String,
    subject: String,
}

/// The worker-side handler. Returning `Err` triggers the retry
/// backoff; exhausting the attempt budget dead-letters the task.
async fn send_email(email: Email) -> Result<(), std::io::Error> {
    tracing::info!(to = %email.to, subject = %email.subject, "sending email");
    if email.subject == "fail me" {
        return Err(std::io::Error::other("smtp refused"));
    }
    Ok(())
}

async fn storage() -> PostgresStorage<Email> {
    let url = std::env::var("APALIS_PG_DATABASE_URL")
        .unwrap_or_else(|_| "postgres://rushwind:rushwind@127.0.0.1:5432/rushwind".to_string());
    let storage = PostgresStorage::<Email>::connect(&url, "emails")
        .await
        .expect("connect to postgres");
    PostgresStorage::<()>::setup(storage.pool())
        .await
        .expect("create schema");
    storage
}

/// Enqueues an immediate task and a scheduled one.
async fn produce() -> Result<(), Box<dyn std::error::Error>> {
    let mut storage = storage().await;
    let pushed = storage
        .push(Email {
            to: "user@example.com".into(),
            subject: "hello from rushwind".into(),
        })
        .await?;
    tracing::info!(task_id = %pushed.task_id, "pushed immediate task");

    // Due in two minutes — the claim query simply won't see it before.
    let scheduled = storage
        .schedule(
            Email {
                to: "user@example.com".into(),
                subject: "hello, later".into(),
            },
            chrono::Utc::now().timestamp() + 120,
        )
        .await?;
    tracing::info!(task_id = %scheduled.task_id, "scheduled task for +2m");

    // A task whose handler fails: watch it retry with backoff, then
    // dead-letter into `killed`.
    let doomed = storage
        .push(Email {
            to: "user@example.com".into(),
            subject: "fail me".into(),
        })
        .await?;
    tracing::info!(task_id = %doomed.task_id, "pushed a task destined to dead-letter");
    Ok(())
}

/// Runs the worker until interrupted.
async fn consume() -> Result<(), Box<dyn std::error::Error>> {
    let storage = storage().await;
    tracing::info!("worker up; waiting for tasks on queue 'emails'");
    Monitor::new()
        .register({
            WorkerBuilder::new("rw-apalis-demo")
                .backend(storage)
                .build_fn(send_email)
        })
        .run()
        .await?;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();
    match std::env::args().nth(1).unwrap_or_default().as_str() {
        "produce" => produce().await,
        "consume" => consume().await,
        other => {
            eprintln!("usage: apalis-postgres-demo <produce|consume> (got `{other}`)");
            std::process::exit(2);
        }
    }
}
