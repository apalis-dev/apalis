use std::{pin::Pin, task::Context};

use futures_sink::Sink;

use crate::{
    backend::{Backend, BackendExt, TaskSink},
    task::Task,
    worker::builder::WorkerBuilder,
};

/// Allow Backends to partition tasks based on a callback
pub trait Partition: Backend + Sized {
    fn set_partition_with<P>(self, cb: P) -> PartitionedBackend<Self, P>
    where
        P: Fn(&Task<Self::Args, Self::Context, Self::IdType>) -> Option<String>
            + Send
            + Sync
            + 'static;
}

pub struct PartitionedBackend<B, P> {
    inner: B,
    partition_callback: P,
}

impl<B, P> PartitionedBackend<B, P> {
    pub fn new(inner: B, partition_callback: P) -> Self {
        Self {
            inner,
            partition_callback,
        }
    }
}

/// Add a partitioning setup for a worker
pub trait PartitionWithExt<Args, Ctx, Source, Middleware, Res>: Sized
where
    Source: Partition<Args = Args, Context = Ctx>,
{
    /// Add a partitioning callback
    fn partition_with<P>(
        self,
        cb: P,
    ) -> WorkerBuilder<Args, Ctx, PartitionedBackend<Source, P>, Middleware>
    where
        P: Fn(&Task<Self::Args, Self::Context, Self::IdType>) -> Option<String>
            + Send
            + Sync
            + 'static;
}

impl<Args, Ctx, Source, Middleware, Res> PartitionWithExt<Args, Ctx, Source, Middleware, Res>
    for WorkerBuilder<Args, Ctx, Source, Middleware>
{
    fn partition_with<P>(
        self,
        cb: P,
    ) -> WorkerBuilder<Args, Ctx, PartitionedBackend<Source, P>, Middleware>
    where
        P: Fn(&Task<Self::Args, Self::Context, Self::IdType>) -> Option<String>
            + Send
            + Sync
            + 'static,
    {
        let backend = PartitionedBackend::new(self.source, partition_callback);
        WorkerBuilder {
            name: self.name,
            layer: self.layer,
            source: backend,
            event_handler: self.event_handler,
            shutdown: self.shutdown,
            request: self.request,
        }
    }
}

impl<B, P> Sink<Task<B::Compact, B::Context, B::IdType>> for PartitionedBackend<B, P>
where
    B: BackendExt + Sink<Task<B::Compact, B::Context, B::IdType>>,
    P: Fn(&Task<B::Args, B::Context, B::IdType>) -> Option<String> + Send + Sync + 'static,
{
    type Error = B::Error;

    fn poll_ready(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Pin::new(&mut self.inner).poll_ready(cx)
    }

    fn start_send(
        mut self: Pin<&mut Self>,
        item: Task<B::Compact, B::Context, B::IdType>,
    ) -> Result<(), Self::Error> {
        let partition = (self.partition_callback)(&item);

        // Apply partition logic here if needed

        Pin::new(&mut self.inner).start_send(item)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Pin::new(&mut self.inner).poll_close(cx)
    }
}
