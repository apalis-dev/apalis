use std::{
    pin::Pin,
    task::{Context, Poll},
};

use futures_core::Stream;
use futures_sink::Sink;
use futures_util::SinkExt;

use crate::{backend::*, worker::context::WorkerContext};

/// A `Backend` wrapper that interleaves tasks from an external/shared `Stream`
/// with tasks produced by the wrapped backend.
///
/// The external stream is polled first on every `poll_next` call, giving it
/// priority; if it's pending or exhausted, control falls through to the
/// underlying backend. This is useful for injecting tasks from a source that
/// isn't itself a `Backend` — e.g. a broadcast channel, webhook listener, or
/// manually-fed queue — without needing to implement the full `Backend` trait
/// for that source.
#[derive(Debug, Clone)]
pub struct Interleave<B, S> {
    pub(super) backend: B,
    pub(super) stream: S,
    /// Set once the external stream yields `None`, so we stop polling it.
    pub(super) stream_done: bool,
}

impl<B, S> Interleave<B, S> {
    /// Wraps a backend with an external stream of tasks.
    ///
    /// The external stream is polled first on every `poll_next` call, giving it
    /// priority; if it's pending or exhausted, control falls through to the
    /// underlying backend.
    pub fn new(backend: B, stream: S) -> Self {
        Self {
            backend,
            stream,
            stream_done: false,
        }
    }
}

impl<B, S> Backend for Interleave<B, S>
where
    B: Backend,
    S: Stream<Item = Result<B::Task, B::Error>> + Unpin,
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
        if !self.stream_done {
            match Pin::new(&mut self.stream).poll_next(cx) {
                Poll::Ready(Some(item)) => return Poll::Ready(Some(item)),
                Poll::Ready(None) => {
                    self.stream_done = true;
                }
                Poll::Pending => {}
            }
        }

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

delegate_sink!(Interleave<B, S>, backend);

delegate_deref!(Interleave<B, S>, backend);

delegate_config!(Interleave<B, S>, backend);

delegate_codec!(Interleave<B, S>, backend);

delegate_expose!(
    impl<B, S> for Interleave<B, S>
    where {
        B: Send + Sync,
        S: Stream<Item = Result<B::Task, B::Error>> + Send + Sync + Unpin,
    }
    => backend
);
