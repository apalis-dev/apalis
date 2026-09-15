//! A backend that allows gating the poll procedure
//!
//! This module provides the `PollWith` struct, which wraps a backend and a poll strategy. It allows controlling the polling behavior of the backend based on the provided strategy. The `PollWith` struct implements the `Backend` trait, delegating most of its functionality to the wrapped backend while applying the poll strategy to manage when to poll for new tasks.
use std::{
    pin::Pin,
    task::{Context, Poll},
};

use futures_sink::Sink;
use futures_util::SinkExt;

use crate::{
    backend::{
        ext::poll_strategy::{PollMetrics, PollStrategy},
        *,
    },
    worker::context::WorkerContext,
};

/// A backend that uses a poller to waker the worker
#[derive(Debug, Clone)]
pub struct PollWith<B, S> {
    backend: B,
    strategy: S,
    poll_metrics: PollMetrics,
}

impl<B, S> PollWith<B, S> {
    /// Create a new `PollWith` wrapping the given backend and strategy.
    pub fn new(backend: B, strategy: S) -> Self {
        Self {
            backend,
            strategy,
            poll_metrics: PollMetrics::default(),
        }
    }
}

impl<B, S> Backend for PollWith<B, S>
where
    B: Backend,
    S: PollStrategy,
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
        let snapshot = self.poll_metrics.snapshot();

        let _ = self.strategy.poll_drive(cx, &snapshot);

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

delegate_sink!(PollWith<B, S>, backend);

delegate_codec!(PollWith<B, S>, backend);

delegate_deref!(PollWith<B, S>, backend);

delegate_config!(PollWith<B, S>, backend);

delegate_expose!(
    impl<B, S> for PollWith<B, S>
    where {
        B: Send + Sync,
        S: PollStrategy + Send + Sync,
        B::Task: Send
    }
    => backend
);
