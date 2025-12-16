mod cache;
mod expensive_client;
mod layer;

use std::time::Duration;

use apalis::{layers::catch_panic::CatchPanicLayer, prelude::*};

use email_service::Email;
use layer::LogLayer;

use tracing::{Instrument, Span, log::info};

use crate::{cache::ValidEmailCache, expensive_client::EmailService};

async fn produce_jobs(storage: &mut MemoryStorage<Email>) {
    for i in 0..5 {
        storage
            .push(Email {
                to: format!("test{i}@example.com"),
                text: "Test background job from apalis".to_string(),
                subject: "Background email job".to_string(),
            })
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_secs(i)).await;
    }
}

#[derive(thiserror::Error, Debug)]
pub enum PanicError {
    #[error("{0}")]
    Panic(String),
}

/// Quick solution to prevent spam.
/// If email in cache, then send email else complete the job but let a validation process run in the background,
async fn send_email(
    email: Email,
    svc: Data<EmailService>,
    worker: WorkerContext,
    cache: Data<ValidEmailCache>,
) -> Result<(), BoxDynError> {
    info!("Job started in worker {:?}", worker.name());
    let cache_clone = cache.clone();
    let email_to = email.to.clone();
    let res = cache.get(&email_to);
    match res {
        None => {
            // We may not prioritize or care when the email is not in cache
            // This will run outside the layers scope and after the job has completed.
            // This can be important for starting long running jobs that don't block the queue
            // Its also possible to acquire context types and clone them into the futures context.
            // They will also be gracefully shutdown if [`Monitor`] has a shutdown signal
            tokio::spawn(
                worker.track(
                    async move {
                        if cache::fetch_validity(email_to, &cache_clone).await {
                            svc.send(email).await;
                            info!("Email added to cache")
                        }
                    }
                    .instrument(Span::current()),
                ), // Its still gonna use the jobs current tracing span. Important eg using sentry.
            );
        }

        Some(_) => {
            svc.send(email).await;
        }
    }

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), std::io::Error> {
    unsafe {
        std::env::set_var("RUST_LOG", "debug");
    }

    tracing_subscriber::fmt::init();

    let mut backend = MemoryStorage::new();
    produce_jobs(&mut backend).await;

    WorkerBuilder::new("tasty-banana")
        .backend(backend)
        .enable_tracing()
        // This handles any panics that may occur in any of the layers below
        // .catch_panic()
        // Or just to customize (do not include both)
        .layer(CatchPanicLayer::with_panic_handler(|e| {
            let panic_info = if let Some(s) = e.downcast_ref::<&str>() {
                s.to_string()
            } else if let Some(s) = e.downcast_ref::<String>() {
                s.clone()
            } else {
                "Unknown panic".to_string()
            };
            // Abort tells the backend to kill job
            AbortError::new(PanicError::Panic(panic_info))
        }))
        .layer(LogLayer::new("some-log-example"))
        // Add shared context to all jobs executed by this worker
        .data(EmailService::new())
        .data(ValidEmailCache::new())
        .build(send_email)
        .run_until(tokio::signal::ctrl_c()) // This will wait for ctrl+c then gracefully shutdown
        .await
        .unwrap();
    Ok(())
}
