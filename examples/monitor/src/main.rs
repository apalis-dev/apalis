use std::error::Error;
use std::fmt;
use std::time::Duration;

use anyhow::Result;
use apalis::layers::tracing::*;
use apalis::prelude::*;
use apalis_file_storage::JsonStorage;
use email_service::Email;
use tokio::time::sleep;
use tracing::info;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

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

async fn email_service(email: Email, worker: WorkerContext) -> Result<(), InvalidEmailError> {
    tracing::info!("Checking if dns configured");
    sleep(Duration::from_millis(1000)).await;
    worker.stop().unwrap();
    tracing::info!("Failed in 1 sec");
    Err(InvalidEmailError { email: email.to })
}

async fn produce_task(storage: &mut JsonStorage<Email>) -> Result<()> {
    storage
        .push(Email {
            to: "test@example".to_owned(),
            text: "Test background job from apalis".to_owned(),
            subject: "Welcome Sentry Email".to_owned(),
        })
        .await
        .unwrap();
    Ok(())
}

async fn produce_task_with_ctx(storage: &mut JsonStorage<Email>) -> Result<()> {
    let email = Email {
        to: "test@example".to_owned(),
        text: "Test background job from apalis".to_owned(),
        subject: "Welcome Sentry Email".to_owned(),
    };
    let context = TracingContext::from(OtelTraceContext::current());
    let task = TaskBuilder::new(email).metadata(&context).build();
    storage.push_task(task).await.unwrap();
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

    let avocado_backend = JsonStorage::new_temp()?;
    let mut av = avocado_backend.clone();
    tokio::spawn(async move {
        produce_task(&mut av).await.unwrap();
    });

    let pear_backend = JsonStorage::new_temp()?;
    let mut pb = pear_backend.clone();

    tokio::spawn(async move {
        produce_task_with_ctx(&mut pb).await.unwrap();
    });

    let monitor = Monitor::new()
        .register(move |_restarts| {
            WorkerBuilder::new("tasty-avocado")
                .backend(avocado_backend.clone())
                .enable_tracing()
                .build(email_service)
        })
        .register(move |_restarts| {
            WorkerBuilder::new("tasty-pear")
                .backend(pear_backend.clone())
                .layer(TraceLayer::new().make_span_with(ContextualTaskSpan::new()))
                .build(email_service)
        })
        // Collect all the events from all workers
        .on_event(|wrk, ev| {
            info!("Received {} event from {} Worker", ev, wrk.name());
        })
        // Define when a worker should restart
        .should_restart(|wrk, err| {
            let runs = wrk.restarts();
            if wrk.name() == "tasty-pear"
                && err.to_string().contains("Recoverable Error")
                && runs < 5
            {
                return false;
            }
            true
        })
        // Graceful shutdown will wait 5s before forcing an exit if workers have not stopped
        .shutdown_timeout(Duration::from_secs(5));

    monitor
        // Shutdown will be triggered by CTRL + C
        .run_with_signal(tokio::signal::ctrl_c())
        .await?;

    Ok(())
}
