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

/// A hook that runs before a backend is stopped.
///
/// `BeforeStop` allows an asynchronous callback to perform cleanup or other
/// work on the backend before it is stopped.
///
/// The callback receives a mutable reference to the backend and must return a
/// future resolving to `Result<(), B::Error>`. If the callback returns an
/// error, the error is propagated to the caller.
///
/// # Type Parameters
///
/// * `B` - The backend being stopped.
/// * `FutErr` - The error type returned by the hook future.
pub struct BeforeStop<B, FutErr> {
    pub(super) backend: B,
    pub(super) f: FnHandler<B, FutErr>,
    state: HookState<FutErr>,
}

impl<B, FutErr> std::fmt::Debug for BeforeStop<B, FutErr>
where
    B: std::fmt::Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BeforeStop")
            .field("backend", &self.backend)
            .field("f", &"<function>")
            .field("state", &self.state)
            .finish()
    }
}

impl<B: Backend> BeforeStop<B, B::Error> {
    /// Creates a new `BeforeStop` hook.
    ///
    /// The provided function is called with a mutable reference to the
    /// backend before it is stopped.
    ///
    /// # Examples
    ///
    /// `ignore
    /// let backend = backend.before_stop(|backend| async move {
    ///     backend.flush().await?;
    ///     Ok(())
    /// });
    /// `
    #[must_use]
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

impl<B, FutErr> Clone for BeforeStop<B, FutErr>
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

impl<B> Backend for BeforeStop<B, B::Error>
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
        let backend = &mut self.backend;
        let f = &mut self.f;
        match self.state.poll_hook(cx, || f(backend)) {
            Poll::Ready(Ok(())) => self.backend.poll_close(cx, worker),
            Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
            Poll::Pending => Poll::Pending,
        }
    }
}

delegate_config!(BeforeStop<B, FutErr>, backend);

delegate_sink!(BeforeStop<B, FutErr>, backend);

delegate_deref!(BeforeStop<B, FutErr>, backend);

delegate_codec!(BeforeStop<B, FutErr>, backend);

delegate_expose!(
    impl<B> for BeforeStop<B, B::Error>
    where {
        B: Backend + Send,
    }
    => backend
);
