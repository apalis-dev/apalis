use std::{
    fmt,
    pin::Pin,
    task::{Context, Poll},
    time::Instant,
};

use apalis_core::{task::Task, worker::context::WorkerContext};
use opentelemetry::{
    KeyValue, global,
    metrics::{Counter, Histogram},
};
use tower::{Layer, Service};

/// A layer to support OpenTelemetry metrics
#[derive(Debug, Default)]
#[non_exhaustive]
pub struct OpenTelemetryMetricsLayer {}

impl<S> Layer<S> for OpenTelemetryMetricsLayer {
    type Service = OpenTelemetryMetricsService<S>;

    fn layer(&self, service: S) -> Self::Service {
        let meter = global::meter("apalis");

        let task_counter = meter
            .u64_counter("messaging.client.consumed.messages")
            .build();

        let duration_histogram = meter
            .f64_histogram("messaging.process.duration")
            .with_unit("s")
            .with_boundaries(vec![
                0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 15.0, 20.0, 30.0,
                60.0, 120.0, 300.0, 600.0,
            ])
            .build();

        OpenTelemetryMetricsService {
            service,
            task_counter,
            duration_histogram,
        }
    }
}

/// This service implements the metric collection behavior
#[derive(Debug, Clone)]
pub struct OpenTelemetryMetricsService<S> {
    service: S,
    task_counter: Counter<u64>,
    duration_histogram: Histogram<f64>,
}

impl<Svc, Fut, Args, Ctx, Res, Err, IdType> Service<Task<Args, Ctx, IdType>>
    for OpenTelemetryMetricsService<Svc>
where
    Svc: Service<Task<Args, Ctx, IdType>, Response = Res, Error = Err, Future = Fut>,
    Fut: Future<Output = Result<Res, Err>> + 'static,
{
    type Response = Svc::Response;
    type Error = Svc::Error;
    type Future = ResponseFuture<Fut>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.service.poll_ready(cx)
    }

    fn call(&mut self, request: Task<Args, Ctx, IdType>) -> Self::Future {
        let start = Instant::now();
        let worker = request
            .parts
            .data
            .get::<WorkerContext>()
            .map(|ns| ns.name())
            .cloned()
            .expect("worker context not found in task data");

        let req = self.service.call(request);
        let task_type = std::any::type_name::<Args>().to_string();

        ResponseFuture {
            inner: req,
            start,
            task_type,
            worker,
            task_counter: self.task_counter.clone(),
            duration_histogram: self.duration_histogram.clone(),
        }
    }
}

/// Response for opentelemetry service
#[pin_project::pin_project]
pub struct ResponseFuture<F> {
    #[pin]
    pub(crate) inner: F,
    pub(crate) start: Instant,
    pub(crate) task_type: String,
    pub(crate) worker: String,
    pub(crate) task_counter: Counter<u64>,
    pub(crate) duration_histogram: Histogram<f64>,
}

impl<F> fmt::Debug for ResponseFuture<F>
where
    F: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let elapsed = self.start.elapsed();

        f.debug_struct("ResponseFuture")
            .field("inner", &self.inner)
            .field("elapsed_since_start", &elapsed)
            .field("task_type", &self.task_type)
            .field("worker", &self.worker)
            .field("task_counter", &self.task_counter)
            .field("duration_histogram", &self.duration_histogram)
            .finish()
    }
}

impl<Fut, Res, Err> Future for ResponseFuture<Fut>
where
    Fut: Future<Output = Result<Res, Err>>,
{
    type Output = Result<Res, Err>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();

        let response = std::task::ready!(this.inner.poll(cx));

        let latency = this.start.elapsed().as_secs_f64();
        let status = response
            .as_ref()
            .ok()
            .map(|_res| "Ok".to_string())
            .unwrap_or_else(|| "Err".to_string());

        let attributes = [
            KeyValue::new("messaging.system", "apalis"),
            KeyValue::new("messaging.operation.name", "process"),
            KeyValue::new(
                "messaging.destination.partition.id",
                this.worker.to_string(),
            ),
            KeyValue::new("messaging.destination.name", this.task_type.to_string()),
            KeyValue::new("apalis.status", status),
        ];

        this.task_counter.add(1, &attributes);
        this.duration_histogram.record(latency, &attributes);

        Poll::Ready(response)
    }
}
