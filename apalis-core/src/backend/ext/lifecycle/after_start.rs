use std::{
    future::Future,
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

/// A hook that runs after a backend has started.
///
/// `AfterStart` allows an asynchronous callback to perform initialization or
/// other work on the backend after it has started.
///
/// The callback receives a mutable reference to the backend and must return a
/// future resolving to `Result<(), B::Error>`.
pub struct AfterStart<B, FutErr> {
    pub(super) backend: B,
    pub(super) f: FnHandler<B, FutErr>,
    state: HookState<FutErr>,
}

impl<B, FutErr> std::fmt::Debug for AfterStart<B, FutErr>
where
    B: std::fmt::Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AfterStart")
            .field("backend", &self.backend)
            .field("f", &"<function>")
            .field("state", &self.state)
            .finish()
    }
}

impl<B, FutErr> Clone for AfterStart<B, FutErr>
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

impl<B: Backend> AfterStart<B, B::Error> {
    /// Creates a new `AfterStart` hook.
    ///
    /// The provided function is called with a mutable reference to the
    /// backend after it has started.
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

impl<B> Backend for AfterStart<B, B::Error>
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
        match self.backend.poll_ready(cx, worker) {
            Poll::Ready(Ok(())) => {}
            Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
            Poll::Pending => return Poll::Pending,
        }
        let backend = &mut self.backend;
        let f = &self.f;
        self.state.poll_hook(cx, || f(backend))
    }

    fn poll_next(
        &mut self,
        cx: &mut Context<'_>,
        worker: &WorkerContext,
    ) -> Poll<Option<Result<Self::Task, Self::Error>>> {
        match &mut self.state {
            HookState::Done => {}
            _ => unreachable!("This should be resolved in poll_ready"),
        };
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

delegate_sink!(AfterStart<B, FutErr>, backend);
delegate_config!(AfterStart<B, FutErr>, backend);
delegate_deref!(AfterStart<B, FutErr>, backend);
delegate_codec!(AfterStart<B, FutErr>, backend);

delegate_expose!(
    impl<B> for AfterStart<B, B::Error>
    where {
        B: Backend + Send,

    }
    => backend
);
