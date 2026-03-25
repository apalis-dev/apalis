use std::sync::{Arc, Mutex, Once};

use apalis::{
    layers::tracing::{ContextualTaskSpan, TraceLayer, TracingContext},
    prelude::*,
};
use apalis_core::{error::BoxDynError, task::metadata::Meta};
use futures::SinkExt;
use opentelemetry::{
    global,
    propagation::Injector as _,
    trace::{TraceContextExt as _, TracerProvider as _},
};
use opentelemetry_sdk::{propagation::TraceContextPropagator, trace::SdkTracerProvider};
use tracing::Span;
use tracing_opentelemetry::OpenTelemetrySpanExt as _;
use tracing_subscriber::prelude::*;

const ZERO_TRACE_ID: &str = "00000000000000000000000000000000";

#[derive(Debug, Clone)]
struct ObservedSpanContext {
    trace_id: String,
    span_id: String,
}

fn init_otel_for_tests() {
    static INIT: Once = Once::new();
    INIT.call_once(|| {
        let tracer_provider = SdkTracerProvider::builder().build();
        let tracer = tracer_provider.tracer("apalis-otel-tests");
        global::set_tracer_provider(tracer_provider);
        global::set_text_map_propagator(TraceContextPropagator::new());

        let subscriber =
            tracing_subscriber::registry().with(tracing_opentelemetry::layer().with_tracer(tracer));
        let _ = tracing::subscriber::set_global_default(subscriber);
    });
}

fn current_span_context() -> ObservedSpanContext {
    let ctx = Span::current().context();
    let otel_span = ctx.span();
    let sc = otel_span.span_context();
    ObservedSpanContext {
        trace_id: format!("{:032x}", sc.trace_id()),
        span_id: format!("{:016x}", sc.span_id()),
    }
}

#[tokio::test]
async fn otel_context_propagates_producer_to_consumer() {
    init_otel_for_tests();

    let mut backend = MemoryStorage::new();
    let producer = tracing::info_span!("producer");
    let (_producer_guard, producer_observed, metadata) = {
        let _guard = producer.enter();
        let observed = current_span_context();
        let metadata = TracingContext::current();
        (_guard, observed, metadata)
    };

    backend
        .send(Task::builder(1_u8).meta(metadata).build())
        .await
        .unwrap();

    let observed_consumer = Arc::new(Mutex::new(None::<ObservedSpanContext>));
    let observed_consumer_clone = Arc::clone(&observed_consumer);

    let worker = WorkerBuilder::new("otel-producer-consumer")
        .backend(backend)
        .layer(TraceLayer::new().make_span_with(ContextualTaskSpan::new()))
        .build(move |_task: u8, worker: WorkerContext| {
            let observed_consumer_clone = Arc::clone(&observed_consumer_clone);
            async move {
                *observed_consumer_clone.lock().unwrap() = Some(current_span_context());
                worker.stop().unwrap();
                Ok::<(), BoxDynError>(())
            }
        });

    worker.run().await.unwrap();

    let consumer = observed_consumer.lock().unwrap().clone().unwrap();
    assert_eq!(consumer.trace_id, producer_observed.trace_id);
    assert_ne!(consumer.span_id, producer_observed.span_id);
    assert_ne!(consumer.trace_id, ZERO_TRACE_ID);
}

#[tokio::test]
async fn otel_context_missing_metadata_creates_new_root() {
    init_otel_for_tests();

    let mut backend = MemoryStorage::new();
    let producer = tracing::info_span!("producer-no-meta");
    let producer_trace_id = {
        let _guard = producer.enter();
        current_span_context().trace_id
    };

    backend.push(1_u8).await.unwrap();

    let observed_consumer = Arc::new(Mutex::new(None::<ObservedSpanContext>));
    let observed_consumer_clone = Arc::clone(&observed_consumer);

    let worker = WorkerBuilder::new("otel-missing-metadata")
        .backend(backend)
        .layer(TraceLayer::new().make_span_with(ContextualTaskSpan::new()))
        .build(move |_task: u8, worker: WorkerContext| {
            let observed_consumer_clone = Arc::clone(&observed_consumer_clone);
            async move {
                *observed_consumer_clone.lock().unwrap() = Some(current_span_context());
                worker.stop().unwrap();
                Ok::<(), BoxDynError>(())
            }
        });

    worker.run().await.unwrap();

    let consumer = observed_consumer.lock().unwrap().clone().unwrap();
    assert_ne!(consumer.trace_id, ZERO_TRACE_ID);
    assert_ne!(consumer.trace_id, producer_trace_id);
}

#[tokio::test]
async fn otel_context_invalid_traceparent_is_ignored_safely() {
    init_otel_for_tests();

    let mut backend = MemoryStorage::new();
    let mut invalid_context = TracingContext::default();
    invalid_context.set("traceparent", "totally-invalid".to_owned());

    backend
        .send(Task::builder(1_u8).meta(invalid_context).build())
        .await
        .unwrap();

    let observed_consumer = Arc::new(Mutex::new(None::<ObservedSpanContext>));
    let observed_consumer_clone = Arc::clone(&observed_consumer);

    let worker = WorkerBuilder::new("otel-invalid-traceparent")
        .backend(backend)
        .layer(TraceLayer::new().make_span_with(ContextualTaskSpan::new()))
        .build(move |_task: u8, worker: WorkerContext| {
            let observed_consumer_clone = Arc::clone(&observed_consumer_clone);
            async move {
                *observed_consumer_clone.lock().unwrap() = Some(current_span_context());
                worker.stop().unwrap();
                Ok::<(), BoxDynError>(())
            }
        });

    worker.run().await.unwrap();

    let consumer = observed_consumer.lock().unwrap().clone().unwrap();
    assert_ne!(consumer.trace_id, ZERO_TRACE_ID);
}

#[tokio::test]
async fn otel_tracestate_roundtrip_is_preserved() {
    init_otel_for_tests();

    let mut backend = MemoryStorage::new();
    let mut metadata = TracingContext::current();
    metadata.set("tracestate", "vendor=acme".to_owned());

    backend
        .send(Task::builder(1_u8).meta(metadata).build())
        .await
        .unwrap();

    let observed_tracestate = Arc::new(Mutex::new(None::<Option<String>>));
    let observed_tracestate_clone = Arc::clone(&observed_tracestate);

    let worker = WorkerBuilder::new("otel-tracestate-roundtrip")
        .backend(backend)
        .layer(TraceLayer::new().make_span_with(ContextualTaskSpan::new()))
        .build(
            move |_task: u8, tracing_meta: Meta<TracingContext>, worker: WorkerContext| {
                let observed_tracestate_clone = Arc::clone(&observed_tracestate_clone);
                async move {
                    *observed_tracestate_clone.lock().unwrap() =
                        Some(tracing_meta.tracestate().map(ToOwned::to_owned));
                    worker.stop().unwrap();
                    Ok::<(), BoxDynError>(())
                }
            },
        );

    worker.run().await.unwrap();

    let tracestate = observed_tracestate.lock().unwrap().clone().unwrap();
    assert_eq!(tracestate.as_deref(), Some("vendor=acme"));
}
