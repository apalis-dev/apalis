use std::{
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use futures_core::Stream;
use futures_util::FutureExt;
use tower_service::Service;

use crate::{
    backend::Backend,
    error::{BoxDynError, WorkerError},
    worker::{
        CallAllUnordered, call_all::CallAllError, context::WorkerContext, event::Event,
        handle::WorkerHandle,
    },
};

#[pin_project::pin_project]
pub(super) struct WorkerStream<Svc, B>
where
    B: Backend,
    Svc: Service<B::Task>,
{
    phase: WorkerPhase,
    #[pin]
    call_all: CallAllUnordered<Svc, B>,
    handle: WorkerHandle,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WorkerPhase {
    Start,
    Running,
    ShuttingDown,
    Done,
}

impl<Svc, B> WorkerStream<Svc, B>
where
    Svc: Service<B::Task>,
    B: Backend + Unpin,
{
    pub(super) fn new(service: Svc, backend: B, worker: &WorkerContext) -> Self {
        let call_all = CallAllUnordered::new(service, backend, worker.clone());
        let handle = WorkerHandle::new(worker.clone());
        Self {
            phase: WorkerPhase::Start,
            call_all,
            handle,
        }
    }
}

impl<Svc, B> Stream for WorkerStream<Svc, B>
where
    Svc: Service<B::Task>,
    B: Backend + Unpin,
    B::Error: Into<BoxDynError> + Send + 'static,
    Svc::Error: Into<BoxDynError> + Send + 'static,
    Svc::Response: Send + Sync + 'static,
{
    type Item = Result<Event, WorkerError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();
        trace!("worker phase: {:?}", this.phase);
        loop {
            match this.phase {
                WorkerPhase::Start => {
                    this.handle.inner.start()?;
                    debug!("worker started");
                    *this.phase = WorkerPhase::Running;
                    return Poll::Ready(Some(Ok(Event::Start)));
                }

                WorkerPhase::Running => match this.call_all.as_mut().poll_next(cx) {
                    Poll::Ready(Some(res)) => {
                        let event = match res {
                            Ok(Some(res)) => Ok(Event::Success(Box::new(res))),
                            Ok(None) => Ok(Event::Stop),
                            Err(CallAllError::ServiceError(e)) => {
                                Ok(Event::Error(Arc::new(e.into())))
                            }
                            Err(CallAllError::PollError(e)) => Err(WorkerError::PollError(e)),
                            Err(CallAllError::CodecError(e)) => Err(WorkerError::CodecError(e)),
                        };

                        return Poll::Ready(Some(event));
                    }
                    Poll::Ready(None) => {
                        *this.phase = WorkerPhase::ShuttingDown;
                    }
                    Poll::Pending => return Poll::Pending,
                },

                WorkerPhase::ShuttingDown => match this.handle.poll_unpin(cx) {
                    Poll::Ready(Ok(())) => {
                        debug!("worker shutdown complete");
                        this.handle.inner.kill().unwrap();
                        *this.phase = WorkerPhase::Done;

                        return Poll::Ready(Some(Ok(Event::Exit)));
                    }
                    Poll::Ready(Err(e)) => {
                        *this.phase = WorkerPhase::Done;
                        return Poll::Ready(Some(Err(e)));
                    }
                    Poll::Pending => return Poll::Pending,
                },

                WorkerPhase::Done => return Poll::Ready(None),
            }
        }
    }
}
