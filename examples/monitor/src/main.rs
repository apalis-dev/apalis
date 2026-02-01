use anyhow::Result;
use apalis::layers::WorkerBuilderExt;
use apalis::layers::tracing::{ContextualTaskSpan, TraceLayer, TracingContext};
use apalis::prelude::*;
use apalis_file_storage::JsonStorage;
use std::error::Error;
use std::fmt;
use std::time::Duration;
use tracing::info;
use tracing_subscriber::prelude::*;

use tokio::time::sleep;

use email_service::Email;

#[derive(Debug)]
struct InvalidEmailError {
    email: String,
}

impl fmt::Display for InvalidEmailError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "UnknownEmail: {} is not a valid email", self.email)
    }
}

impl Error for InvalidEmailError {}

async fn email_service(email: Email) -> Result<(), InvalidEmailError> {
    tracing::info!("Checking if dns configured");
    sleep(Duration::from_millis(1008)).await;
    tracing::info!("Failed in 1 sec");
    Err(InvalidEmailError { email: email.to })
}

async fn produce_task(storage: &mut JsonStorage<Email>) -> Result<()> {
    storage
        .push(Email {
            to: "test@example".to_string(),
            text: "Test background job from apalis".to_string(),
            subject: "Welcome Sentry Email".to_string(),
        })
        .await?;
    Ok(())
}

async fn produce_task_with_ctx(storage: &mut JsonStorage<Email>) -> Result<()> {
    let email = Email {
        to: "test@example".to_string(),
        text: "Test background job from apalis".to_string(),
        subject: "Welcome Sentry Email".to_string(),
    };
    // This might come from a http request etc
    let context = TracingContext::new()
        .with_trace_id("1234567890abcdef")
        .with_span_id("abcdef1234567890")
        .with_trace_flags(1)
        .with_trace_state("key=value");
    let task = Task::builder(email).meta(context).build();
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

    let mut avocado_backend = JsonStorage::new_temp()?;
    produce_task(&mut avocado_backend).await?;

    let mut pear_backend = JsonStorage::new_temp()?;
    produce_task_with_ctx(&mut pear_backend).await?;

    Monitor::new()
        .register(move |_run_id| {
            let avocado_worker = WorkerBuilder::new("tasty-avocado")
                .backend(avocado_backend.clone())
                .enable_tracing()
                .build(email_service);
            avocado_worker
        })
        .register(move |_run_id| {
            let pear_worker = WorkerBuilder::new("tasty-pear")
                .backend(pear_backend.clone())
                .layer(TraceLayer::new().make_span_with(ContextualTaskSpan::new()))
                .build(email_service);
            pear_worker
        })
        // Collect all the events from all workers
        .on_event(|ctx, ev| {
            info!("Received {} event from {} Worker", ev, ctx.name());
        })
        // Define when a worker should restart
        .should_restart(|ctx, err, runs| {
            if ctx.name() == "tasty-pear"
                && err.to_string().contains("Recoverable Error")
                && runs < 5
            {
                return true;
            }
            false
        })
        // Graceful shutdown will wait 5s before forcing an exit if workers have not stopped
        .shutdown_timeout(Duration::from_secs(5))
        // Shutdown will be triggered by CTRL + C
        .run_with_signal(tokio::signal::ctrl_c())
        .await?;

    Ok(())
}
