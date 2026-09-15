use anyhow::Result;

use apalis::prelude::*;
use email_service::{Email, send_email};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

async fn produce_jobs(storage: &mut MemoryStorage<Email>) -> Result<()> {
    let to = "test@example.com";
    let email1 = Email {
        to: to.to_owned(),
        text: "Test background job from apalis".to_owned(),
        subject: "Background email job".to_owned(),
    };
    let task = TaskBuilder::new(email1).idempotency_key(to).build();
    storage.push_task(task).await?;

    let email2 = Email {
        to: to.to_owned(),
        text: "Test background job from apalis".to_owned(),
        subject: "[Copy] Background email job".to_owned(),
    };
    let task = TaskBuilder::new(email2).idempotency_key(to).build();

    storage.push_task(task).await?;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    use tracing_subscriber::EnvFilter;

    let fmt_layer = tracing_subscriber::fmt::layer().with_target(false);
    let filter_layer =
        EnvFilter::try_from_default_env().or_else(|_| EnvFilter::try_new("debug"))?;
    tracing_subscriber::registry()
        .with(filter_layer)
        .with(fmt_layer)
        .init();

    let mut backend = MemoryStorage::new();
    produce_jobs(&mut backend).await?;

    WorkerBuilder::new("tasty-orange")
        .backend(backend)
        .enable_tracing()
        .build(send_email)
        .run()
        .await?;
    Ok(())
}
