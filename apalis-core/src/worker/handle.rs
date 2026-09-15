use std::{
    pin::Pin,
    task::{Context, Poll},
};

use crate::{
    error::{WorkerError, WorkerStateError},
    worker::context::WorkerContext,
};

/// Internal handle used to await worker shutdown completion.
///
/// This owns the `Future` impl that used to live on `WorkerContext` directly.
/// `WorkerContext` itself is `Clone` and handed out freely to backends,
/// middleware, and user code — it deliberately does NOT implement `Future`,
/// so nothing outside this module can accidentally spawn a second task
/// awaiting worker completion with its own independent waker.
pub(super) struct WorkerHandle {
    pub(super) inner: WorkerContext,
}

impl WorkerHandle {
    pub(super) fn new(inner: WorkerContext) -> Self {
        Self { inner }
    }
}

impl Future for WorkerHandle {
    type Output = Result<(), WorkerError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = &self.inner;
        let task_count = this.task_count();

        if this.is_pending() {
            return Poll::Ready(Err(WorkerError::StateError(WorkerStateError::NotStarted)));
        }
        if this.is_shutting_down() && task_count == 0 {
            Poll::Ready(Ok(()))
        } else {
            this.register_waker(cx);
            Poll::Pending
        }
    }
}
