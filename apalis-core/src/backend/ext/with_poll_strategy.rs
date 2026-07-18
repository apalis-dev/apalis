//! A utility module for modifying the codec used by a backend.
//!
//! This is useful for changing the serialization format of task arguments without altering the underlying backend logic.
use std::{
    pin::Pin,
    task::{Context, Poll},
};

use futures_sink::Sink;
use futures_util::SinkExt;

use crate::{
    backend::{
        poll_strategy::{PollMetrics, PollStrategy},
        queue::Queue,
        *,
    },
    task::Task,
    worker::context::WorkerContext,
};

/// A `Backend` wrapper that swaps out the serialization codec entirely (JSON,
/// MessagePack, Protobuf, ...) without touching storage logic.
#[derive(Debug, Clone)]
pub struct WithPollStrategy<B, S> {
    backend: B,
    strategy: S,
    poll_metrics: PollMetrics,
}

impl<B, S> WithPollStrategy<B, S> {
    /// Create a new `WithPollStrategy` wrapping the given backend and strategy.
    pub fn new(backend: B, strategy: S) -> Self {
        Self {
            backend,
            strategy,
            poll_metrics: PollMetrics::default(),
        }
    }
}

impl<B, S> Backend for WithPollStrategy<B, S>
where
    B: Backend,
    S: PollStrategy,
{
    type Args = B::Args;
    type Id = B::Id;
    type Connection = B::Connection;
    type Error = B::Error;
    type Codec = B::Codec;
    type Compact = B::Compact;
    type Layer = B::Layer;

    fn codec(&self) -> &Self::Codec {
        self.backend.codec()
    }

    fn queue(&self) -> Queue {
        self.backend.queue()
    }

    fn middleware(&self) -> Self::Layer {
        self.backend.middleware()
    }

    fn poll_ready(
        &mut self,
        cx: &mut Context<'_>,
        worker: &WorkerContext,
    ) -> Poll<Result<(), Self::Error>> {
        let snapshot = self.poll_metrics.snapshot();
        if self.strategy.poll_gate(cx, &snapshot).is_ready() {
            self.backend.poll_ready(cx, worker) // Poll the inner backend
        } else {
            Poll::Pending
        }
    }

    fn poll_next(
        &mut self,
        cx: &mut Context<'_>,
        worker: &WorkerContext,
    ) -> Poll<Option<Result<Task<Self::Compact, Self::Connection, Self::Id>, Self::Error>>> {
        match self.backend.poll_next(cx, worker) {
            Poll::Ready(task) => {
                self.poll_metrics.on_ready();
                self.strategy.on_poll(&self.poll_metrics.snapshot());
                Poll::Ready(task)
            }

            Poll::Pending => {
                self.poll_metrics.on_pending();
                self.strategy.on_poll(&self.poll_metrics.snapshot());
                Poll::Pending
            }
        }
    }
    fn poll_close(
        &mut self,
        cx: &mut Context<'_>,
        worker: &WorkerContext,
    ) -> Poll<Result<(), Self::Error>> {
        self.backend.poll_close(cx, worker)
    }
}

delegate_sink!(WithPollStrategy<B, S>, backend);

delegate_expose!(
    impl<B, S> for WithPollStrategy<B, S>
    where {
        B: Send + Sync,
        S: PollStrategy + Send + Sync,
        B::Compact: Send
    }
    => backend
);
