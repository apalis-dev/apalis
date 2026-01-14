use std::{
    pin::Pin,
    task::{Context, Poll},
    time::Instant,
};

use apalis_core::{task::Task, worker::context::WorkerContext};
use tower::{Layer, Service};
use opentelemetry::{global, KeyValue, metrics::{Counter, Histogram}};

/// A layer to support OpenTelemetry metrics
#[derive(Debug, Default)]
#[non_exhaustive]
pub struct OpenTelemetryMetricsLayer {}

impl<S> Layer<S> for OpenTelemetryMetricsLayer {
    type Service = OpenTelemetryMetricsService<S>;

    fn layer(&self, service: S) -> Self::Service {
        let meter = global::meter("apalis");

        let task_counter = meter.u64_counter("apalis_task")
                                   .build();

        let duration_histogram = meter
            .f64_histogram("apalis_task_duration")
            .with_unit("s")
            .with_boundaries(vec![0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 15.0, 20.0, 30.0, 60.0, 120.0, 300.0, 600.0])
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
    duration_histogram: Histogram<f64>
}

impl<Svc, Fut, Req, Ctx, Res> Service<Request<Req, Ctx>> for OpenTelemetryMetricsService<Svc>
where
    Svc: Service<Request<Req, Ctx>, Response = Res, Error = Error, Future = Fut>,
    Fut: Future<Output = Result<Res, Error>> + 'static,
{
    type Response = Svc::Response;
    type Error = Svc::Error;
    type Future = ResponseFuture<Fut>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.service.poll_ready(cx)
    }

    fn call(&mut self, request: Request<Req, Ctx>) -> Self::Future {
        let start = Instant::now();
        let namespace = request
            .parts
            .namespace
            .as_ref()
            .map(|ns| ns.0.to_string())
            .unwrap_or(std::any::type_name::<Svc>().to_string());

        let req = self.service.call(request);
        let job_type = std::any::type_name::<Req>().to_string();

        ResponseFuture {
            inner: req,
            start,
            job_type,
            operation: namespace,
            task_counter: self.task_counter.clone(),
            duration_histogram: self.duration_histogram.clone(),
        }
    }
}

pin_project! {
    /// Response for prometheus service
    pub struct ResponseFuture<F> {
        #[pin]
        pub(crate) inner: F,
        pub(crate) start: Instant,
        pub(crate) job_type: String,
        pub(crate) operation: String,
        pub(crate) task_counter: Counter<u64>,
        pub(crate) duration_histogram: Histogram<f64>
    }
}

/// Response for opentelemetry service
#[pin_project::pin_project]
pub struct ResponseFuture<F> {
    #[pin]
    pub(crate) inner: F,
    pub(crate) start: Instant,
    pub(crate) job_type: String,
    pub(crate) worker: String,
}

impl<Fut, Res> Future for ResponseFuture<Fut>
where
    Fut: Future<Output = Result<Res, Error>>,
{
    type Output = Result<Res, Error>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();

        let response = futures::ready!(this.inner.poll(cx));

        let latency = this.start.elapsed().as_secs_f64();
        let status = response
            .as_ref()
            .ok()
            .map(|_res| "Ok".to_string())
            .unwrap_or_else(|| "Err".to_string());

        let attributes = [
            KeyValue::new("name", this.operation.to_string()),
            KeyValue::new("namespace", this.job_type.to_string()),
            KeyValue::new("status", status),
        ];
        this.task_counter.add(1, &attributes);
        this.duration_histogram.record(latency, &attributes);

        Poll::Ready(response)
    }
}
