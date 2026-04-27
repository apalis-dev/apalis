use anyhow::Result;

use apalis::prelude::*;
use email_service::{Email, send_email};

async fn produce_jobs(storage: &mut MemoryStorage<Email>) -> Result<()> {
    let to = "test@example.com";
    let email1 = Email {
        to: to.to_string(),
        text: "Test background job from apalis".to_string(),
        subject: "Background email job".to_string(),
    };
    let task = TaskBuilder::new(email1).with_idempotency_key(to).build();
    storage.push_task(task).await?;

    let email2 = Email {
        to: to.to_string(),
        text: "Test background job from apalis".to_string(),
        subject: "[Copy] Background email job".to_string(),
    };
    let task = TaskBuilder::new(email2).with_idempotency_key(to).build();

    storage.push_task(task).await?;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    unsafe {
        std::env::set_var("RUST_LOG", "debug");
    };
    let mut backend = MemoryStorage::new();
    produce_jobs(&mut backend).await?;
    tracing_subscriber::fmt::init();
    WorkerBuilder::new("tasty-orange")
        .backend(backend)
        .enable_tracing()
        .build(send_email)
        .run()
        .await?;
    Ok(())
}
