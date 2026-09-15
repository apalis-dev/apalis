use std::{
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{
        Context,
        Poll::{self},
    },
};

use futures_core::ready;
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

/// A hook that runs before a backend is started.
///
/// `BeforeStart` allows an asynchronous callback to perform initialization or
/// other work on the backend before it is started.
///
/// The callback receives a mutable reference to the backend and must return a
/// future resolving to `Result<(), B::Error>`.
pub struct BeforeStart<B, FutErr> {
    pub(super) backend: B,
    pub(super) f: FnHandler<B, FutErr>,
    state: HookState<FutErr>,
}

impl<B, FutErr> std::fmt::Debug for BeforeStart<B, FutErr>
where
    B: std::fmt::Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BeforeStart")
            .field("backend", &self.backend)
            .field("f", &"<function>")
            .field("state", &self.state)
            .finish()
    }
}

impl<B, FutErr> Clone for BeforeStart<B, FutErr>
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

impl<B: Backend> BeforeStart<B, B::Error> {
    /// Creates a new `BeforeStart` hook.
    ///
    /// The provided function is called with a mutable reference to the
    /// backend before it is started.
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

impl<B> Backend for BeforeStart<B, B::Error>
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
        let backend = &mut self.backend;
        let f = &mut self.f;
        match self.state.poll_hook(cx, || f(backend)) {
            Poll::Ready(Ok(())) => self.backend.poll_ready(cx, worker),
            Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
            Poll::Pending => Poll::Pending,
        }
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

impl<B, FutErr, T> Sink<T> for BeforeStart<B, FutErr>
where
    B: Sink<T, Error = FutErr> + Unpin + Backend,
    FutErr: Unpin,
{
    type Error = FutErr;
    fn start_send(self: Pin<&mut Self>, item: T) -> Result<(), Self::Error> {
        self.get_mut().backend.start_send_unpin(item)
    }
    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let this = self.get_mut();
        match &mut this.state {
            HookState::Done => this.backend.poll_ready_unpin(cx),
            HookState::Running(fut) => {
                ready!(fut.poll_unpin(cx))?;
                this.state = HookState::Done;
                this.backend.poll_ready_unpin(cx)
            }
            HookState::Pending => {
                let f = &mut this.f;
                this.state.poll_hook(cx, || f(&mut this.backend))
            }
        }
    }
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.get_mut().backend.poll_flush_unpin(cx)
    }
    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.get_mut().backend.poll_close_unpin(cx)
    }
}

delegate_deref!(BeforeStart<B, E>, backend);

delegate_codec!(BeforeStart<B, E>, backend);

delegate_config!(BeforeStart<B, E>, backend);

delegate_expose!(
    impl<B> for BeforeStart<B, B::Error>
    where {
        B: Backend + Send,
    }
    => backend
);
