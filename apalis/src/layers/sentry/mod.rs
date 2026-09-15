use apalis_core::error::BoxDynError;
use sentry_core::protocol;
use std::fmt::{self, Debug};
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use tower::Layer;
use tower::Service;

use apalis_core::task::Task;

/// Sentry integration Layer.
///
/// The Service created by this Layer can also optionally start a new
/// performance monitoring transaction for each incoming task,
/// continuing the trace based on incoming distributed tracing parts.
///
/// This is sometimes not desirable, In which case, users should manually override the transaction name
/// in the task handler using the [`Scope::set_transaction`](sentry_core::Scope::set_transaction)
/// method.
#[derive(Clone, Default, Debug)]
pub struct SentryLayer;

impl SentryLayer {
    /// Creates a new Layer that only logs task details.
    #[must_use]
    pub fn new() -> Self {
        Self
    }
}

/// Task Service for Sentry integration.
///
/// The Service can also optionally start a new performance monitoring transaction
/// for each incoming task, continuing the trace based on the task type.
#[derive(Clone, Debug)]
pub struct SentryTaskService<S> {
    service: S,
}

impl<S> Layer<S> for SentryLayer {
    type Service = SentryTaskService<S>;

    fn layer(&self, service: S) -> Self::Service {
        Self::Service { service }
    }
}

struct Request {
    id: uuid::Uuid,
    current_attempt: i32,
    queue: String,
}

/// The Future returned from [`SentryTaskService`].
#[pin_project::pin_project]
pub struct SentryHttpFuture<F> {
    on_first_poll: Option<(Request, sentry_core::TransactionContext)>,
    transaction: Option<(
        sentry_core::TransactionOrSpan,
        Option<sentry_core::TransactionOrSpan>,
    )>,
    #[pin]
    future: F,
}

/// Error type for Sentry task processing.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum SentryTaskError {
    /// Error occurred in the inner service.
    #[error("Inner service error: {0}")]
    Inner(BoxDynError),
}

impl<F> fmt::Debug for SentryHttpFuture<F>
where
    F: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SentryHttpFuture")
            .field("on_first_poll", &"<Request, TransactionContext>")
            .field(
                "transaction",
                &self.transaction.as_ref().map(|(_, maybe_span)| {
                    let has_child = maybe_span.is_some();
                    format!(
                        "<TransactionOrSpan, child: {}>",
                        if has_child { "Some" } else { "None" }
                    )
                }),
            )
            .field("future", &self.future)
            .finish()
    }
}

impl<F, Res, Err> Future for SentryHttpFuture<F>
where
    F: Future<Output = Result<Res, Err>> + 'static,
    Err: Into<BoxDynError> + 'static,
{
    type Output = Result<Res, BoxDynError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        if let Some((task_details, trx_ctx)) = this.on_first_poll.take() {
            let tid = task_details.id;
            sentry_core::configure_scope(|scope| {
                scope.add_event_processor(move |mut event| {
                    event.event_id = tid;
                    Some(event)
                });
                scope.set_tag("queue", task_details.queue.clone());
                let mut details = std::collections::BTreeMap::new();
                details.insert(String::from("task_id"), task_details.id.to_string().into());
                details.insert(
                    String::from("current_attempt"),
                    task_details.current_attempt.into(),
                );
                scope.set_context("task", sentry_core::protocol::Context::Other(details));

                let transaction: sentry_core::TransactionOrSpan =
                    sentry_core::start_transaction(trx_ctx).into();
                let parent_span = scope.get_span();
                scope.set_span(Some(transaction.clone()));
                *this.transaction = Some((transaction, parent_span));
            });
        }
        match this
            .future
            .poll(cx)
            .map_err(|e| SentryTaskError::Inner(e.into()))
        {
            Poll::Ready(res) => {
                if let Some((transaction, parent_span)) = this.transaction.take() {
                    if transaction.get_status().is_none() {
                        let status = match &res {
                            Ok(_) => protocol::SpanStatus::Ok,
                            Err(err) => {
                                sentry_core::capture_error(&err);
                                protocol::SpanStatus::InternalError
                            }
                        };
                        transaction.set_status(status);
                    }
                    transaction.finish();
                    sentry_core::configure_scope(|scope| scope.set_span(parent_span));
                }
                Poll::Ready(Ok(res?))
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl<Svc, Args, Fut, Res, Err> Service<Task<Args>> for SentryTaskService<Svc>
where
    Svc: Service<Task<Args>, Response = Res, Error = Err, Future = Fut>,
    Fut: Future<Output = Result<Res, BoxDynError>> + 'static,
    Err: Into<BoxDynError> + 'static,
{
    type Response = Svc::Response;
    type Error = BoxDynError;
    type Future = SentryHttpFuture<Svc::Future>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.service.poll_ready(cx).map_err(|e| e.into())
    }

    fn call(&mut self, task: Task<Args>) -> Self::Future {
        let task_type = std::any::type_name::<Args>().to_owned();
        let attempt = task.attempt() as i32;
        let task_id = task.task_id().expect("Task ID is missing").to_uuid();
        let trx_ctx =
            sentry_core::TransactionContext::new(std::any::type_name::<Args>(), "apalis.task");

        let task_details = Request {
            id: task_id,
            current_attempt: attempt,
            queue: task_type,
        };

        SentryHttpFuture {
            on_first_poll: Some((task_details, trx_ctx)),
            transaction: None,
            future: self.service.call(task),
        }
    }
}
