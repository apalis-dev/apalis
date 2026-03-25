use anyhow::Result;
use apalis::layers::tracing::{ContextualTaskSpan, TraceLayer, TracingContext};
use apalis::layers::WorkerBuilderExt;
use apalis::prelude::*;
use futures::SinkExt;
use opentelemetry::global;
use opentelemetry::trace::{TraceContextExt as _, TracerProvider as _};
use opentelemetry_sdk::{propagation::TraceContextPropagator, trace::SdkTracerProvider};
use std::error::Error;
use std::fmt;
use std::time::Duration;
use tracing::{instrument, Span};
use tracing_opentelemetry::OpenTelemetrySpanExt as _;
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

fn print_otel_context(message: &str) {
    let span = Span::current();
    let ctx = span.context();
    let otel_span = ctx.span();
    let sc = otel_span.span_context();
    let trace_id = sc.trace_id();
    let span_id = sc.span_id();
    tracing::info!(
        trace_id = %trace_id,
        span_id = %span_id,
        "{}", message
    );
}

async fn email_service(email: Email) -> Result<(), InvalidEmailError> {
    print_otel_context("email_service");
    tracing::info!("Checking if dns configured");
    sleep(Duration::from_millis(1008)).await;
    tracing::info!("Failed in 1 sec");
    Err(InvalidEmailError { email: email.to })
}

#[instrument(skip(storage))]
async fn produce_task(storage: &mut MemoryStorage<Email>) -> Result<()> {
    print_otel_context("produce_task");
    storage
        .push(Email {
            to: "test@example".to_string(),
            text: "Test background job from apalis".to_string(),
            subject: "Welcome Sentry Email".to_string(),
        })
        .await?;
    Ok(())
}

#[instrument(skip(storage))]
async fn produce_task_with_ctx(storage: &mut MemoryStorage<Email>) -> Result<()> {
    print_otel_context("produce_task_with_ctx");
    let email = Email {
        to: "test@example".to_string(),
        text: "Test background job from apalis".to_string(),
        subject: "Welcome Sentry Email".to_string(),
    };
    let task = Task::builder(email).meta(TracingContext::current()).build();
    storage.send(task).await?;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    use tracing_subscriber::EnvFilter;
    std::env::set_var("RUST_LOG", "debug");

    let tracer_provider = SdkTracerProvider::builder().build();
    let tracer = tracer_provider.tracer("tracing-example");
    global::set_tracer_provider(tracer_provider);
    global::set_text_map_propagator(TraceContextPropagator::new());

    let fmt_layer = tracing_subscriber::fmt::layer().with_target(false);
    let otel_layer = tracing_opentelemetry::layer().with_tracer(tracer);
    let filter_layer =
        EnvFilter::try_from_default_env().or_else(|_| EnvFilter::try_new("debug"))?;
    tracing_subscriber::registry()
        .with(filter_layer)
        .with(fmt_layer)
        .with(otel_layer)
        .init();

    let mut backend = MemoryStorage::new();
    produce_task(&mut backend).await?;

    let avocado_worker = WorkerBuilder::new("tasty-avocado")
        .backend(backend)
        .enable_tracing()
        .build(email_service)
        .run();

    let mut backend = MemoryStorage::new();
    produce_task_with_ctx(&mut backend).await?;

    let pear_worker = WorkerBuilder::new("tasty-pear")
        .backend(backend)
        .layer(TraceLayer::new().make_span_with(ContextualTaskSpan::new()))
        .build(email_service)
        .run();

    tokio::try_join!(avocado_worker, pear_worker)?;

    Ok(())
}
