#![allow(missing_docs)]
use anyhow::Result;
use apalis::prelude::*;

use email_service::Email;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

async fn produce_emails(storage: &mut MemoryStorage<Email>) -> Result<()> {
    for i in 0..2 {
        storage
            .push(Email {
                to: format!("test{i}@example.com"),
                text: "Test background job from apalis".to_owned(),
                subject: "Background email job".to_owned(),
            })
            .await?;
    }
    Ok(())
}

async fn send_email(_: Email) {
    unimplemented!("panic from unimplemented")
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

    let mut email_storage: MemoryStorage<Email> = MemoryStorage::new();

    produce_emails(&mut email_storage).await?;
    WorkerBuilder::new("tasty-banana")
        .backend(email_storage)
        .retry(RetryPolicy::retries(1))
        .catch_panic()
        .enable_tracing()
        .concurrency(2)
        .on_event(|_c, e| tracing::debug!("{e:?}"))
        .build(send_email)
        .run()
        .await?;
    Ok(())
}
