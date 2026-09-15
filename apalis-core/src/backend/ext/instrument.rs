use futures_sink::Sink;
use futures_util::SinkExt;
use std::pin::Pin;
use std::task::{Context, Poll};

use tracing::Span;

use crate::{backend::*, worker::context::WorkerContext};

/// Instruments the inner [Backend] with the provided `Span`, returning an
/// `Instrumented` wrapper.
#[derive(Debug, Clone)]
pub struct Instrumented<B> {
    backend: B,
    span: Span,
}

impl<B> Instrumented<B> {
    /// Create a new `Instrumented` backend with the given tracing span.
    pub fn new(backend: B, span: Span) -> Self {
        Self { backend, span }
    }

    /// Returns the wrapped backend.
    pub fn inner(&self) -> &B {
        &self.backend
    }

    /// Returns a mutable reference to the wrapped backend.
    pub fn inner_mut(&mut self) -> &mut B {
        &mut self.backend
    }

    /// Returns the tracing span.
    pub fn span(&self) -> &Span {
        &self.span
    }

    /// Unwraps the instrumented backend.
    pub fn into_inner(self) -> B {
        self.backend
    }
}

impl<B> Backend for Instrumented<B>
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
        let _entered = self.span.enter();
        self.backend.poll_ready(cx, worker)
    }

    fn poll_next(
        &mut self,
        cx: &mut Context<'_>,
        worker: &WorkerContext,
    ) -> Poll<Option<Result<Self::Task, Self::Error>>> {
        let _entered = self.span.enter();
        self.backend.poll_next(cx, worker)
    }

    fn poll_close(
        &mut self,
        cx: &mut Context<'_>,
        worker: &WorkerContext,
    ) -> Poll<Result<(), Self::Error>> {
        let _entered = self.span.enter();
        self.backend.poll_close(cx, worker)
    }
}

impl<B, T, Err> Sink<T> for Instrumented<B>
where
    B: Sink<T, Error = Err> + Unpin,
{
    type Error = Err;
    fn start_send(self: Pin<&mut Self>, item: T) -> Result<(), Self::Error> {
        let this = self.get_mut();
        let _entered = this.span.enter();
        this.backend.start_send_unpin(item)
    }
    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let this = self.get_mut();
        let _entered = this.span.enter();
        this.backend.poll_ready_unpin(cx)
    }
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let this = self.get_mut();
        let _entered = this.span.enter();
        this.backend.poll_flush_unpin(cx)
    }
    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let this = self.get_mut();
        let _entered = this.span.enter();
        this.backend.poll_close_unpin(cx)
    }
}

delegate_expose!(
    impl<B> for Instrumented<B>
    where {
        B: Send + Sync,
        B::Task: Send + Clone + 'static,
    }
    => backend
);

delegate_config!(Instrumented<B>, backend);
delegate_deref!(Instrumented<B>, backend);
delegate_codec!(Instrumented<B>, backend);
