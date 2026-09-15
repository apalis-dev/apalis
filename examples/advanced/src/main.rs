#![allow(missing_docs)]
pub mod cache;
mod client;
mod error;
mod layer;

use apalis::{layers::catch_panic::CatchPanicLayer, prelude::*};

use email_service::Email;
use layer::ClientLayer;

use tracing::{Instrument, Span, debug, info};
use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};

use crate::{cache::ValidEmailCache, client::EmailClient, error::Error};

async fn produce_jobs(storage: &mut MemoryStorage<Email>) {
    for i in 0..10 {
        storage
            .push(Email {
                to: format!("test{i}@example.com"),
                text: "Test background job from apalis".to_owned(),
                subject: "Background email job".to_owned(),
            })
            .await
            .unwrap();
    }
}
/// Send an email.
///
/// The email address is validated before sending. Once the email has been
/// successfully sent, cache maintenance is allowed to continue independently
/// of the job.
async fn send_email(
    email: Email,
    svc: Data<EmailClient>,
    cache: Data<ValidEmailCache>,
    ctx: TaskContext,
) -> Result<(), Error> {
    let address = email.to.clone();

    // Fast path: avoid the expensive validation request when the address
    // is already known to be valid.
    if let Some(v) = cache.get(&address) {
        if v.valid {
            // We dont need to check this email
            svc.send_unchecked(email).await?;
            return Ok(());
        }
        return Err(Error::InvalidEmail);
    }

    // The actual job is complete once the email has been sent.
    svc.send(email).await?;

    // Cache maintenance isn't required for the job to succeed, so it can
    // continue independently after the job has completed.
    let cache = cache.clone();

    tokio::spawn(
        async move {
            // Do cache refreshing after the task is complete
            ctx.executed().await;
            cache::refresh(address, &cache).await;
            debug!("Email validation cache refreshed");
        }
        .instrument(Span::current()),
    );

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), BoxDynError> {
    let cpu_n = std::thread::available_parallelism().map_or(4, std::num::NonZeroUsize::get);

    let fmt_layer = tracing_subscriber::fmt::layer().with_target(false);
    let filter_layer =
        EnvFilter::try_from_default_env().or_else(|_| EnvFilter::try_new("debug"))?;
    tracing_subscriber::registry()
        .with(filter_layer)
        .with(fmt_layer)
        .init();

    let mut backend = MemoryStorage::new();

    // In a real application these jobs would normally come from an HTTP
    // endpoint, another service, or a persistent producer.
    produce_jobs(&mut backend).await;

    WorkerBuilder::new("email-sender")
        .backend(backend)
        // Spawn each tasks future via tokio::spawn
        .parallelize(tokio::spawn)
        // Process several emails concurrently.
        .concurrency(cpu_n)
        // Retry 3 times on error
        .retry(RetryPolicy::retries(3))
        // Convert panics in the job/layer stack into failed/aborted jobs
        // rather than taking down the worker.
        .layer(CatchPanicLayer::with_panic_handler(|e| {
            let panic_info = if let Some(s) = e.downcast_ref::<&str>() {
                s.to_string()
            } else if let Some(s) = e.downcast_ref::<String>() {
                s.clone()
            } else {
                "Unknown panic".to_owned()
            };
            // We return [AbortError] to signify that the task was killed
            AbortError::new(Error::Panic(panic_info))
        }))
        // Add the worker span to jobs and make their execution visible
        // through the tracing subscriber.
        .enable_tracing()
        // Application-specific middleware can add logging, metrics,
        // authorization, etc. around every job.
        .layer(ClientLayer::new(EmailClient::new()))
        // Share clients/resources between all jobs executed by this worker.
        .data(ValidEmailCache::new())
        .on_event(|ctx, e| info!("worker: [{}] emit event [{}]", ctx.name(), e))
        .build(send_email)
        // Stop accepting work and gracefully shut down when Ctrl+C is received.
        .run_until(tokio::signal::ctrl_c())
        .await
        .unwrap();

    Ok(())
}
