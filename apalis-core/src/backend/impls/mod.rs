use std::task::{Context, Poll};

use futures_core::stream::BoxStream;
use futures_util::StreamExt;
use tower_layer::Identity;

use crate::{
    backend::{Backend, BackendConfig, finalize::Ephemeral},
    task::{
        Task,
        builder::TaskBuilder,
        task_id::{RandomId, TaskId},
    },
    worker::context::WorkerContext,
};

/// Backend implementation based on VecDeque
pub(crate) mod dequeue;
/// In-memory backend implementation
pub(crate) mod memory;

// A boxed stream of tasks that implements the `Backend` trait
impl<T, E> Backend for BoxStream<'_, Result<T, E>>
where
    E: std::error::Error + Send + Sync + 'static,
{
    type Task = Task<T>;

    type Error = E;

    fn poll_ready(
        &mut self,
        _cx: &mut Context<'_>,
        _worker: &WorkerContext,
    ) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn poll_next(
        &mut self,
        cx: &mut Context<'_>,
        _worker: &WorkerContext,
    ) -> Poll<Option<Result<Self::Task, Self::Error>>> {
        self.poll_next_unpin(cx).map_ok(|t| {
            TaskBuilder::new(t)
                .task_id(TaskId::from_string(RandomId::default()))
                .build()
        })
    }

    fn poll_close(
        &mut self,
        _cx: &mut Context<'_>,
        _worker: &WorkerContext,
    ) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
}

impl<T, E> BackendConfig for BoxStream<'_, Result<T, E>>
where
    E: std::error::Error + Send + Sync + 'static,
{
    type Args = T;
    type Kind = Ephemeral;

    type Id = RandomId;

    type Config = ();

    type Layer = Identity;

    fn config(&self) -> &Self::Config {
        &()
    }

    fn middleware(&mut self, _worker: &mut WorkerContext) -> Self::Layer {
        Identity::new()
    }
}

impl<B: Backend> Backend for &mut B {
    type Task = B::Task;

    type Error = B::Error;

    fn poll_ready(
        &mut self,
        cx: &mut Context<'_>,
        worker: &WorkerContext,
    ) -> Poll<Result<(), Self::Error>> {
        (**self).poll_ready(cx, worker)
    }

    fn poll_next(
        &mut self,
        cx: &mut Context<'_>,
        worker: &WorkerContext,
    ) -> Poll<Option<Result<Self::Task, Self::Error>>> {
        (**self).poll_next(cx, worker)
    }

    fn poll_close(
        &mut self,
        cx: &mut Context<'_>,
        worker: &WorkerContext,
    ) -> Poll<Result<(), Self::Error>> {
        (**self).poll_close(cx, worker)
    }
}
