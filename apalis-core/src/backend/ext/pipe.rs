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
//! # use apalis_core::backend::{ext::pipe::PipeExt, dequeue::VecDequeBackend};
//! # use apalis_core::worker::{builder::WorkerBuilder, context::WorkerContext};
//! # use apalis_core::error::BoxDynError;
//! # use std::time::Duration;
//! # use futures_util::StreamExt;
//! # use crate::apalis_core::worker::ext::event_listener::EventListenerExt;
//! #[tokio::main]
//! async fn main() {
//!     let stm = stream::iter(0..10).map(|s| Ok::<_, std::io::Error>(s));
//!
//!     let in_memory = VecDequeBackend::new();
//!     let backend = stm.pipe_to(in_memory);
//!
//!     async fn task(task: u32, ctx: WorkerContext) -> Result<(), BoxDynError> {
//!         tokio::time::sleep(Duration::from_secs(1)).await;
//! #        if task == 9 {
//! #            ctx.stop().unwrap();
//! #        }
//!         Ok(())
//!     }
//!
//!     let worker = WorkerBuilder::new("rango-tango")
//!         .backend(backend)
//!         .on_event(|_ctx, ev| {
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
use std::fmt::{self, Display};
use std::marker::PhantomData;
use std::ops::{Deref, DerefMut};
use std::{
    pin::Pin,
    task::{Context, Poll},
};

use crate::backend::ext::{BackendExt, PollNextArgsError};
use crate::backend::queue::Queue;
use crate::backend::*;
use crate::error::BoxDynError;
use crate::features_table;
use crate::task::Task;
use crate::task::builder::TaskBuilder;
use crate::{backend::codec::Codec, worker::context::WorkerContext};
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
pub struct Pipe<Dst, S, Args, Conn> {
    pub(crate) from: S,
    pub(crate) into: Dst,
    pub(crate) _req: PhantomData<(Args, Conn)>,
}

/// Adapts a [`Backend`] into an [`IntoArgsStream`] source, so it can be
/// used wherever a plain `Stream<Item = Result<Args, Err>>` would be —
/// specifically, as the `from` side of a [`Pipe`].
///
/// This exists because a blanket `IntoArgsStream` impl can't be written for
/// both "any stream of `Result<Args, Err>`" and "any `Backend`" at once —
/// nothing stops a type from satisfying both bounds simultaneously, so the
/// two blanket impls would conflict under coherence. Wrapping the backend
/// in this newtype sidesteps that: `FromBackend<B>` is a distinct type from
/// `B`, so it only ever matches the backend-flavored impl.
pub struct FromBackend<B>(pub B);

impl<B: Clone> Clone for FromBackend<B> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<B: fmt::Debug> fmt::Debug for FromBackend<B> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("FromBackend").field(&self.0).finish()
    }
}

/// Something that can be converted into a stream of `Result<Args, Err>`,
/// suitable for piping into a sink backend via [`Pipe`].
///
/// Implemented for:
/// - any `Stream<Item = Result<Args, Err>>` directly (the "plain stream"
///   case), and
/// - [`FromBackend<B>`] for any `Backend<Args = Args>` (the "backend as a
///   source" case), which polls the backend and unwraps each task down to
///   its args.
///
/// [`Pipe`] is generic over this trait rather than over `Stream` directly,
/// which is what lets one `Pipe` type serve both streams and backends.
pub trait PipeNextStream<Args, Conn, Id> {
    /// The error type yielded alongside `Args` on failure.
    type Error;

    /// Polls for the next task from the underlying source.
    /// Takes a [`WorkerContext`] since some sources need it to poll
    /// (e.g. backend-backed sources).
    fn poll_pipe_next(
        &mut self,
        cx: &mut Context<'_>,
        worker: &WorkerContext,
    ) -> Poll<Option<Result<Task<Args, Conn, Id>, Self::Error>>>;
}

// Plain-stream case: any stream of `Result<Args, Err>` is already exactly
// what we need, so this is just a passthrough.
impl<S, Args, Conn, Id, Err> PipeNextStream<Args, Conn, Id> for S
where
    S: Stream<Item = Result<Args, Err>> + Send + Unpin + 'static,
    Args: 'static,
    Conn: 'static,
    Id: 'static,
    Err: 'static,
{
    type Error = Err;

    fn poll_pipe_next(
        &mut self,
        cx: &mut Context<'_>,
        _: &WorkerContext,
    ) -> Poll<Option<Result<Task<Args, Conn, Id>, Self::Error>>> {
        let next = Stream::poll_next(Pin::new(self), cx);
        next.map(|item| item.map(|res| res.map(TaskBuilder::new).map(|t| t.build())))
    }
}

// Backend case: poll the backend, drop `None`s (no task available right
// now), and project each `Task` down to its `args`.
impl<B, Conn, Id> PipeNextStream<B::Args, Conn, Id> for FromBackend<B>
where
    B: Backend + Send + 'static,
    B::Error: std::error::Error + Send + Sync + 'static,
    B::Args: Send + 'static,
    B::Id: Display,
    Id: 'static,
    Conn: 'static,
    <B::Codec as Codec<B::Args>>::Error: std::error::Error + Send + Sync + 'static,
{
    type Error = PollNextArgsError<B>;

    fn poll_pipe_next(
        &mut self,
        cx: &mut Context<'_>,
        worker: &WorkerContext,
    ) -> Poll<Option<Result<Task<B::Args, Conn, Id>, Self::Error>>> {
        let next = self.0.poll_next_args(cx, worker);
        next.map(|item| match item {
            Some(Ok(task)) => Some(Ok(task.map_backend::<Conn, Id>())),
            Some(Err(e)) => Some(Err(e)),
            None => None,
        })
    }
}

impl<S, Dst, Args, Conn> Deref for Pipe<Dst, S, Args, Conn> {
    type Target = Dst;

    fn deref(&self) -> &Self::Target {
        &self.into
    }
}

impl<S, Dst, Args, Conn> DerefMut for Pipe<Dst, S, Args, Conn> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.into
    }
}

impl<S, Dst, Args, Conn> Pipe<Dst, S, Args, Conn> {
    /// Create a new `Pipe` from a raw `from` source and an `into` sink.
    /// Prefer [`PipeExt::pipe_to`] or [`BackendExt::pipe_to`] over calling
    /// this directly.
    pub fn new(from: S, into: Dst) -> Self {
        Self {
            from,
            into,
            _req: PhantomData,
        }
    }
}

impl<S: fmt::Debug, Dst: fmt::Debug, Args, Conn> fmt::Debug for Pipe<Dst, S, Args, Conn> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Pipe")
            .field("from", &self.from)
            .field("into", &self.into)
            .finish()
    }
}

impl<Args, Conn, S, TSink, CdcErr> Backend for Pipe<TSink, S, Args, Conn>
where
    S: PipeNextStream<Args, Conn, TSink::Id> + Send + 'static,
    S::Error: std::error::Error + Send + Sync + 'static,
    TSink: Backend<Args = Args>
        + TaskSink<Args>
        + Unpin
        + Send
        + 'static
        + Sink<Task<TSink::Compact, Conn, TSink::Id>>,
    <TSink as Backend>::Error: std::error::Error + Send + Sync + 'static,
    TSink::Id: Display + Send + Sync + 'static,
    TSink::Codec: Codec<Args, Error = CdcErr> + Send + Sync + 'static,
    <TSink as Sink<Task<TSink::Compact, Conn, TSink::Id>>>::Error:
        std::error::Error + Send + Sync + 'static,
    Args: Send + 'static,
    CdcErr: std::error::Error + Send + Sync + 'static,
    TSink::Compact: Send,
    Conn: 'static,
{
    type Args = Args;
    type Id = TSink::Id;
    type Connection = Conn;
    type Layer = TSink::Layer;
    type Error = PipeError;
    type Codec = TSink::Codec;
    type Compact = TSink::Compact;

    fn codec(&self) -> &Self::Codec {
        self.into.codec()
    }

    fn queue(&self) -> Queue {
        self.into.queue()
    }

    fn middleware(&self) -> Self::Layer {
        self.into.middleware()
    }

    fn poll_ready(
        &mut self,
        cx: &mut Context<'_>,
        _: &WorkerContext,
    ) -> Poll<Result<(), Self::Error>> {
        ready!(Sink::poll_ready(Pin::new(&mut self.into), cx))
            .map_err(|e| PipeError::Inner(e.into()))?;

        Poll::Ready(Ok(()))
    }

    fn poll_next(
        &mut self,
        cx: &mut Context<'_>,
        worker: &WorkerContext,
    ) -> Poll<Option<Result<Task<Self::Compact, Self::Connection, Self::Id>, Self::Error>>> {
        let mut source_done = false;

        loop {
            match Sink::poll_ready(Pin::new(&mut self.into), cx) {
                Poll::Ready(Ok(())) => {}
                Poll::Ready(Err(e)) => {
                    return Poll::Ready(Some(Err(PipeError::Inner(e.into()))));
                }
                Poll::Pending => break,
            }

            match self.from.poll_pipe_next(cx, worker) {
                Poll::Ready(Some(Ok(task))) => {
                    let codec = self.into.codec();
                    let encoded = task.try_map_args(|args| {
                        codec.encode(&args).map_err(|e| PipeError::Inner(e.into()))
                    });

                    match encoded {
                        Ok(t) => {
                            if let Err(e) = Sink::start_send(Pin::new(&mut self.into), t) {
                                return Poll::Ready(Some(Err(PipeError::Inner(e.into()))));
                            }
                        }
                        Err(e) => return Poll::Ready(Some(Err(e))),
                    }
                }
                Poll::Ready(Some(Err(e))) => {
                    return Poll::Ready(Some(Err(PipeError::Inner(e.into()))));
                }
                Poll::Ready(None) => {
                    source_done = true;
                    break;
                }
                Poll::Pending => break,
            }
        }

        match Sink::poll_flush(Pin::new(&mut self.into), cx) {
            Poll::Ready(Err(e)) => return Poll::Ready(Some(Err(PipeError::Inner(e.into())))),
            Poll::Ready(Ok(())) | Poll::Pending => {}
        }

        if source_done {
            let _ = Sink::poll_close(Pin::new(&mut self.into), cx);
        }

        if let Poll::Ready(Err(e)) = self.into.poll_ready(cx, worker) {
            return Poll::Ready(Some(Err(PipeError::Inner(e.into()))));
        }

        self.into
            .poll_next(cx, worker)
            .map_ok(|task| task.map_backend())
            .map_err(|e| PipeError::Inner(e.into()))
    }

    fn poll_close(
        &mut self,
        cx: &mut Context<'_>,
        worker: &WorkerContext,
    ) -> Poll<Result<(), Self::Error>> {
        self.into
            .poll_close(cx, worker)
            .map_err(|e| PipeError::Inner(e.into()))
    }
}

/// Utility for piping a plain stream of `Result<Args, Err>` into a backend.
pub trait PipeExt<B, Args, Conn>
where
    B: Backend,
    Self: PipeNextStream<Args, Conn, B::Id> + Sized,
{
    /// Pipe the current stream into the provided sink backend.
    fn pipe_to(self, backend: B) -> Pipe<B, Self, Args, Conn>;
}

impl<B, Args, Conn, Err, S> PipeExt<B, Args, Conn> for S
where
    S: Stream<Item = Result<Args, Err>> + Send + Unpin + 'static,
    B::Error: Into<BoxDynError> + Send + Sync + 'static,
    B: Backend<Args = Args> + TaskSink<Args>,
    Err: 'static,
    Args: 'static,
    Conn: 'static,
{
    fn pipe_to(self, backend: B) -> Pipe<B, Self, Args, Conn> {
        Pipe::new(self, backend)
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

delegate_sink!(Pipe<Dst, S, Args, Conn>, into);

delegate_expose!(
    impl<B, S, Args, Conn, CdcErr> for Pipe<B, S, Args, Conn>
    where {
        B: Send + Sync,
        S: PipeNextStream<Args, Conn, B::Id> + Send + Sync + 'static,
        S::Error: std::error::Error + Send + Sync + 'static,
        B: Backend<Args = Args, Connection = Conn>
            + TaskSink<Args>
            + Unpin
            + Send
            + 'static
            + Sink<Task<B::Compact, Conn, B::Id>>,
        <B as Backend>::Error: std::error::Error + Send + Sync + 'static,
        B::Id: Display + Send + Sync + 'static,
        B::Codec: Codec<Args, Error = CdcErr> + Send + Sync + 'static,
        <B as Sink<Task<B::Compact, Conn, B::Id>>>::Error:
            std::error::Error + Send + Sync + 'static,
        Args: Send + Sync + 'static,
        CdcErr: std::error::Error + Send + Sync + 'static,
        B::Compact: Send,
        Conn: Send + Sync + 'static,
    }
    => into,
    wrap = |this, result| result.map_err(|e| PipeError::Inner(e.into()))
);

#[cfg(test)]
mod tests {
    use std::{io, time::Duration};

    use futures_util::{StreamExt, stream};

    use crate::{
        backend::{
            dequeue::{self, VecDequeBackend},
            ext::BackendExt,
            memory::MemoryStorage,
        },
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
        let in_memory = dequeue::VecDequeBackend::new();

        let backend = stm.pipe_to(in_memory);

        async fn task(task: u32, ctx: WorkerContext) -> Result<(), BoxDynError> {
            tokio::time::sleep(Duration::from_secs(1)).await;
            if task == ITEMS - 1 {
                ctx.stop().unwrap();
                return Err("Graceful Exit".into());
            }
            Ok(())
        }

        let worker = WorkerBuilder::new("rango-tango")
            .backend(backend)
            .on_event(|_ctx, ev| {
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

        async fn task(task: u32, ctx: WorkerContext) -> Result<(), BoxDynError> {
            tokio::time::sleep(Duration::from_secs(1)).await;
            if task == 43 {
                ctx.stop().unwrap();
                return Err("Graceful Exit".into());
            }
            Ok(())
        }

        let worker = WorkerBuilder::new("rango-tango")
            .backend(backend)
            .on_event(|_ctx, ev| {
                println!("On Event = {ev:?}");
            })
            .build(task);
        worker.run().await.unwrap();
    }
}
