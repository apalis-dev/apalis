use std::task::{Context, Poll};

use apalis_core::{
    backend::{Backend, BackendConfig, WireFormatBackend, finalize::Durable},
    task::Task,
    worker::context::WorkerContext,
};
/// A backend wrapper that provides tasks in compact mode.
#[derive(Debug)]
pub struct WorkflowBackend<B> {
    backend: B,
}

impl<B> WorkflowBackend<B> {
    pub fn new(backend: B) -> Self {
        Self { backend }
    }
}

impl<B, Compact> Backend for WorkflowBackend<B>
where
    B: Backend<Task = Task<Compact>>,
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
        self.backend.poll_close(cx, worker)
    }
}

impl<B, Args> WireFormatBackend for WorkflowBackend<B>
where
    B: WireFormatBackend<Compact = Args> + Backend,
{
    type Compact = B::Compact;
    type Codec = B::Codec;

    fn codec(&self) -> &Self::Codec {
        self.backend.codec()
    }
}

impl<B, Args> BackendConfig for WorkflowBackend<B>
where
    B: BackendConfig + WireFormatBackend<Compact = Args>,
{
    type Id = B::Id;
    type Args = B::Compact;
    type Kind = Durable;
    type Config = B::Config;
    type Layer = B::Layer;

    fn config(&self) -> &Self::Config {
        self.backend.config()
    }

    fn middleware(&mut self, worker: &mut WorkerContext) -> Self::Layer {
        self.backend.middleware(worker)
    }
}
