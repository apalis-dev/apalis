use std::time::Duration;

use anyhow::Result;
use apalis::layers::retry::HasherRng;
use apalis::layers::retry::backoff::MakeBackoff;
use apalis::layers::retry::{RetryPolicy, backoff::ExponentialBackoffMaker};

use apalis::prelude::*;
use email_service::{Email, send_email};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

/// Produces jobs to check retries
/// See [send_email] for the logic explained here
async fn produce_jobs(storage: &mut MemoryStorage<Email>) -> Result<()> {
    storage
        .push(Email {
            // Valid email should just run once (attempts = 1)
            to: format!("test{}@example.com", 0),
            text: "Test background job from apalis".to_owned(),
            subject: "Background email job".to_owned(),
        })
        .await?;
    storage
        .push(Email {
            // Invalid email, should fail and retry 3 times (attempts = 4)
            to: "test.at.example.com".to_owned(),
            text: "Test background job from apalis".to_owned(),
            subject: "Background email job".to_owned(),
        })
        .await?;
    storage
        .push(Email {
            // Invalid character, job will abort. Should only run once (attempts = 1)
            to: "A@b@c@example.com".to_owned(),
            text: "Test background job from apalis".to_owned(),
            subject: "Background email job".to_owned(),
        })
        .await?;
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
    tracing_subscriber::fmt::init();
    let backoff = ExponentialBackoffMaker::new(
        Duration::from_millis(1000),
        Duration::from_millis(5000),
        1.25,
        HasherRng::default(),
    )?
    .make_backoff();
    WorkerBuilder::new("tasty-orange")
        .backend(backend)
        .retry(
            RetryPolicy::retries(3)
                .with_backoff(backoff)
                .retry_if(|e: &BoxDynError| e.downcast_ref::<AbortError>().is_none()),
        )
        .enable_tracing()
        .on_event(|ctx, _ev| println!("{:?}", ctx.get_service()))
        .build(send_email)
        .run()
        .await?;
    Ok(())
}
