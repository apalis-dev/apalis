//! # Pipe streams to backends
//!
//! This backend allows you to pipe tasks from any stream into another backend.
//! It is useful for connecting different backends together, such as piping tasks
//! from a cron stream into a database backend, or transforming and forwarding tasks
//! between systems.
//!
//! ## Example
//!
//! ```rust
//! # use futures_util::stream;
//! # use apalis_core::backend::{ext::pipe::PipeExt, memory::MemoryStorage};
//! # use apalis_core::worker::{builder::WorkerBuilder, context::WorkerContext};
//! # use apalis_core::error::BoxDynError;
//! # use std::time::Duration;
//! # use futures_util::StreamExt;
//! # use crate::apalis_core::worker::ext::event_listener::EventListenerExt;
//! #[tokio::main]
//! async fn main() {
//!     let stm = stream::iter(0..10).map(|s| Ok::<_, std::io::Error>(s));
//!
//!     let in_memory = MemoryStorage::new();
//!     let backend = stm.pipe_to(in_memory);
//!
//!     async fn task(task: u32, worker: WorkerContext) -> Result<(), BoxDynError> {
//!         tokio::time::sleep(Duration::from_secs(1)).await;
//! #        if task == 9 {
//! #            worker.stop().unwrap();
//! #        }
//!         Ok(())
//!     }
//!
//!     let worker = WorkerBuilder::new("rango-tango")
//!         .backend(backend)
//!         .on_event(|_worker, ev| {
//!             println!("On Event = {:?}", ev);
//!         })
//!         .build(task);
//!     worker.run().await.unwrap();
//! }
//! ```
//!
//! This example pipes a stream of numbers into an in-memory backend and processes them with a worker.
//!
//! See also:
//! - [`apalis-cron`](https://docs.rs/apalis-cron)
use std::fmt::Debug;
use std::fmt::{self};
use std::{
    pin::Pin,
    task::{Context, Poll},
};

use crate::backend::*;
use crate::error::BoxDynError;
use crate::task::Task;
use crate::worker::context::WorkerContext;
use futures_core::stream::BoxStream;
use futures_core::{Stream, ready};
use futures_sink::Sink;
use futures_util::SinkExt;
use futures_util::TryStreamExt;

/// A generic pipe that wraps a [`Stream`] and passes it to a backend
#[doc = features_table! {
    setup = "{ unreachable!() }",
    TaskSink => supported("Ability to push new tasks", false),
    InheritsFeatures => limited("Inherits features from the underlying backend", false),
}]
pub struct Pipe<Dst, S> {
    pub(crate) from: S,
    pub(crate) into: Dst,
}

impl<S, Dst> Pipe<Dst, S> {
    /// Create a new `Pipe` from a raw `from` source and an `into` sink.
    /// Prefer [`PipeExt::pipe_to`] or [`BackendExt::pipe_to`] over calling
    /// this directly.
    ///
    /// [`BackendExt::pipe_to`]: crate::backend::ext::BackendExt
    pub fn new(from: S, into: Dst) -> Self {
        Self { from, into }
    }
}

impl<S: fmt::Debug, Dst: fmt::Debug> fmt::Debug for Pipe<Dst, S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Pipe")
            .field("from", &self.from)
            .field("into", &self.into)
            .finish()
    }
}

impl<Dst: Clone, S: Clone> Clone for Pipe<Dst, S> {
    fn clone(&self) -> Self {
        Self {
            from: self.from.clone(),
            into: self.into.clone(),
        }
    }
}

impl<S, TSink, Args, Kind, Err> Backend for Pipe<TSink, S>
where
    S: Backend<Task = Task<Args>, Error = Err> + Send + 'static,
    Err: std::error::Error + Send + Sync + 'static,
    TSink: BackendConfig<Kind = Kind> + Backend + TaskSink<Args, Kind> + Unpin + Send + 'static,
    <TSink as Backend>::Error: std::error::Error + Send + Sync + 'static,
    Args: Send + 'static,
{
    type Task = TSink::Task;

    type Error = PipeError;

    fn poll_ready(
        &mut self,
        cx: &mut Context<'_>,
        wkr: &WorkerContext,
    ) -> Poll<Result<(), Self::Error>> {
        trace!("poll_ready: polling source");

        ready!(S::poll_ready(&mut self.from, cx, wkr)).map_err(|e| {
            trace!(error = ?e, "poll_ready: source returned error");
            PipeError::Inner(e.into())
        })?;

        trace!("poll_ready: source ready, polling destination");

        ready!(Backend::poll_ready(&mut self.into, cx, wkr)).map_err(|e| {
            trace!(error = ?e, "poll_ready: destination returned error");
            PipeError::Inner(e.into())
        })?;

        trace!("poll_ready: ready");

        Poll::Ready(Ok(()))
    }

    fn poll_next(
        &mut self,
        cx: &mut Context<'_>,
        worker: &WorkerContext,
    ) -> Poll<Option<Result<Self::Task, Self::Error>>> {
        loop {
            match TaskSink::poll_ready(Pin::new(&mut self.into), cx) {
                Poll::Ready(Ok(())) => {
                    trace!("poll_next: destination ready");
                }
                Poll::Ready(Err(e)) => {
                    trace!(error = ?e, "poll_next: destination poll_ready failed");
                    return Poll::Ready(Some(Err(PipeError::Inner(e.into()))));
                }
                Poll::Pending => break,
            }

            match self.from.poll_next(cx, worker) {
                Poll::Ready(Some(Ok(task))) => {
                    trace!("poll_next: received task from source");

                    if let Err(e) = TaskSink::start_send(Pin::new(&mut self.into), task) {
                        trace!(error = ?e, "poll_next: destination start_send failed");
                        return Poll::Ready(Some(Err(PipeError::Inner(e.into()))));
                    }

                    trace!("poll_next: task sent to destination");
                }
                Poll::Ready(Some(Err(e))) => {
                    trace!(error = ?e, "poll_next: source returned error");
                    return Poll::Ready(Some(Err(PipeError::Inner(e.into()))));
                }
                Poll::Ready(None) => {
                    trace!("poll_next: source closed");
                    break;
                }
                Poll::Pending => break,
            }
        }

        match TSink::poll_flush(Pin::new(&mut self.into), cx) {
            Poll::Ready(Err(e)) => {
                trace!(error = ?e, "poll_next: destination flush failed");
                return Poll::Ready(Some(Err(PipeError::Inner(e.into()))));
            }
            Poll::Ready(Ok(())) => {
                trace!("poll_next: destination flushed");
            }
            Poll::Pending => {}
        }

        if let Poll::Ready(Err(e)) = self.into.poll_ready(cx, worker) {
            trace!(error = ?e, "poll_next: destination backend poll_ready failed");
            return Poll::Ready(Some(Err(PipeError::Inner(e.into()))));
        }

        self.into.poll_next(cx, worker).map_err(|e| {
            trace!(error = ?e, "poll_next: destination backend returned error");
            PipeError::Inner(e.into())
        })
    }

    fn poll_close(
        &mut self,
        cx: &mut Context<'_>,
        worker: &WorkerContext,
    ) -> Poll<Result<(), Self::Error>> {
        trace!("poll_close: closing destination");

        self.into.poll_close(cx, worker).map_err(|e| {
            trace!(error = ?e, "poll_close: destination returned error");
            PipeError::Inner(e.into())
        })
    }
}

/// Utility for piping a plain stream of `Result<Args, Err>` into a backend.
pub trait PipeExt<B, Args>
where
    B: Backend,
{
    /// Pipe the current stream into the provided sink backend.
    fn pipe_to(self, backend: B) -> Pipe<B, BoxStream<'static, Result<Args, PipeError>>>;
}

impl<B, Args, Err, S> PipeExt<B, Args> for S
where
    B: Backend + Unpin + Send + 'static,
    S: Stream<Item = Result<Args, Err>> + Send + Unpin + 'static,
    Err: Into<BoxDynError> + Send + Sync + 'static,
    Args: 'static,
{
    fn pipe_to(self, backend: B) -> Pipe<B, BoxStream<'static, Result<Args, PipeError>>> {
        Pipe::new(
            Box::pin(self.map_err(|e| PipeError::Inner(e.into()))),
            backend,
        )
    }
}

/// Error encountered while piping streams
#[derive(Debug, thiserror::Error)]
pub enum PipeError {
    /// The cron stream provided a None
    #[error("The inner stream provided a None")]
    EmptyStream,
    /// An inner stream error occurred
    #[error("The inner stream error: {0}")]
    Inner(BoxDynError),
}

impl<Dst, S, T, Err> Sink<T> for Pipe<Dst, S>
where
    Dst: Sink<T, Error = Err> + Unpin,
    S: Unpin,
    Err: Into<BoxDynError> + Send + Sync,
{
    type Error = PipeError;
    fn start_send(self: Pin<&mut Self>, item: T) -> Result<(), Self::Error> {
        self.get_mut()
            .into
            .start_send_unpin(item)
            .map_err(|e| PipeError::Inner(e.into()))
    }
    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.get_mut()
            .into
            .poll_ready_unpin(cx)
            .map_err(|e| PipeError::Inner(e.into()))
    }
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.get_mut()
            .into
            .poll_flush_unpin(cx)
            .map_err(|e| PipeError::Inner(e.into()))
    }
    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.get_mut()
            .into
            .poll_close_unpin(cx)
            .map_err(|e| PipeError::Inner(e.into()))
    }
}

delegate_config!(Pipe<Dst, S>, into);

delegate_deref!(Pipe<Dst, S>, into);

delegate_codec!(Pipe<Dst, S>, into);

delegate_expose!(
    impl<B, S, Args, Kind> for Pipe<B, S>
    where {
        S: Backend<Task = Task<Args>> + Send + Sync + 'static,
        S::Error: std::error::Error + Send + Sync + 'static,
        B: BackendConfig<Kind = Kind>
        + Backend
        + TaskSink<Args, Kind>
        + Unpin
        + Send
        + 'static,
        <B as Backend>::Error: std::error::Error + Send + Sync + 'static,
        Args: Send + 'static,
    }
    => into,
    wrap = |this, result| result.map_err(|e| PipeError::Inner(e.into()))
);

#[cfg(test)]
mod tests {
    use std::{io, time::Duration};

    use futures_util::{StreamExt, stream};

    use crate::{
        backend::{dequeue::VecDequeBackend, ext::BackendExt, memory::MemoryStorage},
        error::BoxDynError,
        worker::{
            builder::WorkerBuilder, context::WorkerContext, ext::event_listener::EventListenerExt,
        },
    };

    use super::*;

    const ITEMS: u32 = 10;

    #[tokio::test]
    async fn basic_worker() {
        let stm = stream::iter(0..ITEMS).map(Ok::<_, io::Error>);
        let in_memory = MemoryStorage::new();

        let backend = stm.pipe_to(in_memory);

        async fn task(task: u32, worker: WorkerContext) -> Result<(), BoxDynError> {
            tokio::time::sleep(Duration::from_secs(1)).await;
            if task == ITEMS - 1 {
                worker.stop().unwrap();
                return Err("Graceful Exit".into());
            }
            Ok(())
        }

        let worker = WorkerBuilder::new("rango-tango")
            .backend(backend)
            .on_event(|_worker, ev| {
                println!("On Event = {ev:?}");
            })
            .build(task);
        worker.run().await.unwrap();
    }

    #[tokio::test]
    async fn dequeue_to_memory_worker() {
        let dequeue = VecDequeBackend::new();

        let mut in_memory = MemoryStorage::new();

        in_memory.push(42).await.unwrap();

        let mut backend = in_memory.pipe_to(dequeue);

        backend.push(43).await.unwrap();

        async fn task(task: u32, worker: WorkerContext) -> Result<(), BoxDynError> {
            tokio::time::sleep(Duration::from_secs(1)).await;
            if task == 42 {
                worker.stop().unwrap();
                return Err("Graceful Exit".into());
            }
            Ok(())
        }

        let worker = WorkerBuilder::new("rango-tango")
            .backend(backend)
            .on_event(|_worker, ev| {
                println!("On Event = {ev:?}");
            })
            .build(task);
        worker.run().await.unwrap();
    }
}
