use std::{
    future::Future,
    ops::{Deref, DerefMut},
    pin::Pin,
    sync::Arc,
    task::{
        Context,
        Poll::{self},
    },
};

use futures_sink::Sink;
use futures_util::{FutureExt, SinkExt};

use crate::{
    backend::{
        ext::lifecycle::{FnHandler, HookState},
        future::BoxSyncFuture,
        *,
    },
    worker::context::WorkerContext,
};

/// A hook that runs after a backend has stopped.
///
/// `AfterStop` allows an asynchronous callback to perform cleanup or other
/// work on the backend after it has stopped.
///
/// The callback receives a mutable reference to the backend and must return a
/// future resolving to `Result<(), B::Error>`.
pub struct AfterStop<B, FutErr> {
    pub(super) backend: B,
    pub(super) f: FnHandler<B, FutErr>,
    state: HookState<FutErr>,
}

impl<B, FutErr> std::fmt::Debug for AfterStop<B, FutErr>
where
    B: std::fmt::Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AfterStop")
            .field("backend", &self.backend)
            .field("f", &"<function>")
            .field("state", &self.state)
            .finish()
    }
}

impl<B, FutErr> Clone for AfterStop<B, FutErr>
where
    B: Clone,
{
    fn clone(&self) -> Self {
        Self {
            backend: self.backend.clone(),
            f: self.f.clone(),
            state: HookState::Pending,
        }
    }
}

impl<B: Backend> AfterStop<B, B::Error> {
    /// Creates a new `AfterStop` hook.
    ///
    /// The provided function is called with a mutable reference to the
    /// backend after it has stopped.
    ///
    /// # Errors
    ///
    /// If the callback returns an error, the error is propagated to the
    /// backend lifecycle.
    pub fn new<F, Fut>(backend: B, f: F) -> Self
    where
        F: Fn(&mut B) -> Fut + 'static + Send + Sync,
        Fut: Future<Output = Result<(), B::Error>> + Send + 'static,
    {
        Self {
            backend,
            f: Arc::new(move |b: &mut B| BoxSyncFuture::new(f(b).boxed())),
            state: HookState::Pending,
        }
    }
}

impl<B> Backend for AfterStop<B, B::Error>
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
        match self.backend.poll_close(cx, worker) {
            Poll::Ready(Ok(())) => {}
            Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
            Poll::Pending => return Poll::Pending,
        }
        let backend = &mut self.backend;
        let f = &self.f;
        self.state.poll_hook(cx, || f(backend))
    }
}

impl<B, FutErr> Deref for AfterStop<B, FutErr> {
    type Target = B;

    fn deref(&self) -> &Self::Target {
        &self.backend
    }
}

impl<B, FutErr> DerefMut for AfterStop<B, FutErr> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.backend
    }
}

delegate_sink!(AfterStop<B, FutErr>, backend);

delegate_codec!(AfterStop<B, FutErr>, backend);

delegate_config!(AfterStop<B, FutErr>, backend);

delegate_expose!(
    impl<B> for AfterStop<B, B::Error>
    where {
        B: Backend + Send,

    }
    => backend
);
