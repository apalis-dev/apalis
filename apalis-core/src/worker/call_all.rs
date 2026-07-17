//! Utilities for executing all tasks from a backend to a service.
//!
//! A combinator for calling all requests from a `Backend` to a service, yielding responses
//! as they arrive. It supports both ordered and unordered response handling, allowing for flexible integration
//! with asynchronous services.
use futures_util::{Stream, ready, stream::FuturesUnordered};
use std::{
    fmt,
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};
use tower_service::Service;

use crate::{
    backend::{Backend, codec::Codec},
    error::BoxDynError,
    task::Task,
    worker::WorkerContext,
};

/// A stream of responses received from the inner service in received order,
/// driven by a `Backend` directly instead of a plain `Stream`.
#[derive(Debug)]
#[pin_project::pin_project]
pub(super) struct CallAllUnordered<Svc, B>
where
    Svc: Service<Task<<B as Backend>::Args, <B as Backend>::Connection, <B as Backend>::Id>>,
    B: Backend,
{
    #[pin]
    inner: CallAll<Svc, B, FuturesUnordered<Svc::Future>>,
}

impl<Svc, B> CallAllUnordered<Svc, B>
where
    Svc: Service<Task<B::Args, B::Connection, B::Id>>,
    B: Backend + Unpin,
{
    /// Create new [`CallAllUnordered`] combinator.
    pub(super) fn new(service: Svc, backend: B, worker: WorkerContext) -> Self {
        Self {
            inner: CallAll::new(service, backend, worker, FuturesUnordered::new()),
        }
    }
}

impl<Svc, B> Stream for CallAllUnordered<Svc, B>
where
    Svc: Service<Task<B::Args, B::Connection, B::Id>>,
    B: Backend + Unpin,
    B::Error: Into<BoxDynError>,
    <B::Codec as Codec<B::Args>>::Error: Into<BoxDynError>,
{
    type Item = Result<Option<Svc::Response>, CallAllError<Svc::Error>>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.project().inner.poll_next(cx)
    }
}

/// Error type that combines backend errors and service errors
#[derive(Debug, thiserror::Error)]
pub enum CallAllError<ServiceError> {
    /// Error originating from `Backend::poll_ready` or `Backend::poll`
    #[error("Backend error: {0}")]
    PollError(BoxDynError),
    /// Error originating from the service
    #[error("Service error: {0}")]
    ServiceError(ServiceError),
    /// Error originating from the decoding of the task
    #[error("Task decoding error: {0}")]
    CodecError(BoxDynError),
}

impl<F: Future> Drive<F> for FuturesUnordered<F> {
    fn is_empty(&self) -> bool {
        Self::is_empty(self)
    }

    fn push(&mut self, future: F) {
        Self::push(self, future)
    }

    fn poll(&mut self, cx: &mut Context<'_>) -> Poll<Option<F::Output>> {
        Stream::poll_next(Pin::new(self), cx)
    }
}

/// The [`Future`]/[`Stream`] that sequences `backend.poll_ready`,
/// `svc.poll_ready`, and `backend.poll` on every iteration, guaranteeing
/// the backend never claims a task the service isn't yet ready to accept.
#[pin_project::pin_project]
pub(crate) struct CallAll<Svc, B, Q>
where
    B: Backend,
{
    service: Option<Svc>,
    #[pin]
    backend: B,
    worker: WorkerContext,
    queue: Q,
    eof: bool,
    curr_req: Option<Task<B::Args, B::Connection, B::Id>>,
}

impl<Svc, B, Q> fmt::Debug for CallAll<Svc, B, Q>
where
    Svc: fmt::Debug,
    B: Backend + fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CallAll")
            .field("service", &self.service)
            .field("backend", &self.backend)
            .field("eof", &self.eof)
            .finish()
    }
}

pub(crate) trait Drive<F: Future> {
    fn is_empty(&self) -> bool;

    fn push(&mut self, future: F);

    fn poll(&mut self, cx: &mut Context<'_>) -> Poll<Option<F::Output>>;
}

impl<Svc, B, Q> CallAll<Svc, B, Q>
where
    Svc: Service<Task<B::Args, B::Connection, B::Id>>,
    B: Backend + Unpin,
    Q: Drive<Svc::Future>,
{
    pub(crate) const fn new(service: Svc, backend: B, worker: WorkerContext, queue: Q) -> Self {
        Self {
            service: Some(service),
            backend,
            worker,
            queue,
            eof: false,
            curr_req: None,
        }
    }
}

impl<Svc, B, Q> Stream for CallAll<Svc, B, Q>
where
    Svc: Service<Task<B::Args, B::Connection, B::Id>>,
    B: Backend + Unpin,
    Q: Drive<Svc::Future>,
    B::Error: Into<BoxDynError>,
    <B::Codec as Codec<B::Args>>::Error: Into<BoxDynError>,
{
    type Item = Result<Option<Svc::Response>, CallAllError<Svc::Error>>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();

        loop {
            // First, see if we have any responses to yield
            if let Poll::Ready(Some(result)) = this.queue.poll(cx) {
                return Poll::Ready(Some(result.map_err(CallAllError::ServiceError).map(Some)));
            }

            // Shutdown requested (or backend naturally exhausted via eof) AND
            // no in-flight work left: time to close.
            let shutting_down = *this.eof || this.worker.is_shutting_down();

            if shutting_down {
                if !this.queue.is_empty() {
                    return Poll::Pending;
                }
                return match this.backend.as_mut().get_mut().poll_close(cx, this.worker) {
                    Poll::Pending => Poll::Pending,
                    Poll::Ready(Err(e)) => {
                        Poll::Ready(Some(Err(CallAllError::PollError(e.into()))))
                    }
                    Poll::Ready(Ok(())) => Poll::Ready(None),
                };
            }

            let svc = this
                .service
                .as_mut()
                .expect("Using CallAll after extracting inner Service");

            // 1. backend.poll_ready — must succeed before we even ask svc.
            match this.backend.as_mut().get_mut().poll_ready(cx, this.worker) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(e)) => {
                    *this.eof = true;
                    return Poll::Ready(Some(Err(CallAllError::PollError(e.into()))));
                }
                Poll::Ready(Ok(())) => {}
            }

            // 2. svc.poll_ready — still gates step 3, same as before.
            if let Err(e) = ready!(svc.poll_ready(cx)) {
                *this.eof = true;
                return Poll::Ready(Some(Err(CallAllError::ServiceError(e))));
            }

            // 3. backend.poll — only now pull a task, having confirmed both
            // the backend and the service are ready.
            if this.curr_req.is_none() {
                match ready!(this.backend.as_mut().get_mut().poll_next(cx, this.worker)) {
                    Some(Ok(next_req)) => {
                        let codec = this.backend.codec();
                        let res = next_req.try_map_args(|args| codec.decode(&args));
                        match res {
                            Err(e) => {
                                *this.eof = true;
                                return Poll::Ready(Some(Err(CallAllError::CodecError(e.into()))));
                            }
                            Ok(next) => {
                                *this.curr_req = Some(next);
                            }
                        }
                    }
                    Some(Err(e)) => {
                        return Poll::Ready(Some(Err(CallAllError::PollError(e.into()))));
                    }
                    None => {
                        *this.eof = true;
                        continue;
                    }
                }
            }

            // Unwrap: The check above always sets `this.curr_req` if none and continues if no request.
            this.queue.push(svc.call(this.curr_req.take().unwrap()));
        }
    }
}
