use std::{
    pin::Pin,
    task::{Context, Poll, Waker},
};

use futures_sink::Sink;
use futures_util::SinkExt;

use crate::{backend::*, worker::context::WorkerContext};

/// A backend wrapper that wakes the worker when a task is pushed.
///
/// `WakeOnPush` stores the most recent worker waker received through
/// [`Backend::poll_ready`] and wakes it whenever a task is successfully sent
/// through the backend's [`Sink`] implementation.
///
/// This is useful for backends where pushing a task should immediately cause
/// a worker that is waiting for work to poll the backend again.
///
/// # Examples
///
/// ```ignore
/// let backend = backend.wake_on_push();
/// ```
#[derive(Debug, Clone)]
pub struct WakeOnPush<B> {
    pub(super) backend: B,
    pub(super) waker: Option<Waker>,
}

impl<B> WakeOnPush<B> {
    /// Creates a new `WakeOnPush` wrapper around the given backend.
    ///
    /// The worker waker is captured when [`Backend::poll_ready`] is first
    /// called.
    pub fn new(backend: B) -> Self {
        Self {
            backend,
            waker: None,
        }
    }
}

impl<B> Backend for WakeOnPush<B>
where
    B: Backend,
{
    type Task = B::Task;
    type Error = B::Error;

    fn poll_ready(
        &mut self,
        cx: &mut Context<'_>,
        worker: &WorkerContext,
    ) -> Poll<Result<(), Self::Error>> {
        if self.waker.as_ref().is_none_or(|w| !w.will_wake(cx.waker())) {
            self.waker = Some(cx.waker().clone());
        }
        self.backend.poll_ready(cx, worker)
    }

    fn poll_next(
        &mut self,
        cx: &mut Context<'_>,
        worker: &WorkerContext,
    ) -> Poll<Option<Result<Self::Task, Self::Error>>> {
        self.backend.poll_next(cx, worker)
    }

    fn poll_close(
        &mut self,
        cx: &mut Context<'_>,
        worker: &WorkerContext,
    ) -> Poll<Result<(), Self::Error>> {
        self.backend.poll_close(cx, worker)
    }
}

impl<B, T, Err> Sink<T> for WakeOnPush<B>
where
    B: Sink<T, Error = Err> + Unpin,
{
    type Error = Err;
    /// Sends a task to the backend and wakes the worker if the task was
    /// pushed successfully.
    fn start_send(self: Pin<&mut Self>, item: T) -> Result<(), Self::Error> {
        let this = self.get_mut();
        this.backend.start_send_unpin(item).map(|_| {
            if let Some(s) = this.waker.as_ref() {
                s.wake_by_ref()
            }
        })
    }
    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.get_mut().backend.poll_ready_unpin(cx)
    }
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.get_mut().poll_flush_unpin(cx)
    }
    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.get_mut().backend.poll_close_unpin(cx)
    }
}

delegate_config!(WakeOnPush<B>, backend);

delegate_codec!(WakeOnPush<B>, backend);

delegate_deref!(WakeOnPush<B>, backend);

delegate_expose!(
    impl<B> for WakeOnPush<B>
    where {
        B: Send + Sync,
    }
    => backend
);
