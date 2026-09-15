//! Extension traits and combinators for [`Backend`] implementations.
//!
//! This module provides additional functionality for [`Backend`] implementations,
//! including:
//!
//! - [`BackendExt`]: Extension methods for transforming, composing, and managing
//!   backends.
//! - [`InspectErr`]: A wrapper that allows inspection of errors produced by a backend.
//! - [`MapErr`]: A wrapper that maps backend errors from one type to another.
//! - [`Pipe`]: A utility for piping tasks from one backend to another.
//! - [`BeforeStart`]: A lifecycle wrapper that runs an action before the backend starts.
//! - [`AfterStart`]: A lifecycle wrapper that runs an action after the backend starts.
//! - [`BeforeStop`]: A lifecycle wrapper that runs an action before the backend stops.
//! - [`AfterStop`]: A lifecycle wrapper that runs an action after the backend stops.
use std::{
    task::{Context, Poll},
    time::Duration,
};

use futures_core::Stream;

#[cfg(feature = "tracing")]
use crate::backend::ext::instrument::Instrumented;
use crate::{
    backend::{
        Backend, BackendConfig, WireFormatBackend,
        codec::Codec,
        ext::{
            inspect_err::InspectErr,
            interleave::Interleave,
            lifecycle::{AfterStart, AfterStop, BeforeStart, BeforeStop},
            map_err::MapErr,
            pipe::Pipe,
            poll_strategy::{PollStrategy, PollWith, StreamStrategy},
            shared::Shared,
            wake_on_push::WakeOnPush,
            with_codec::WithCodec,
        },
    },
    error::BoxDynError,
    task::Task,
    worker::context::WorkerContext,
};

#[cfg(feature = "sleep")]
use crate::backend::ext::poll_strategy::{BackoffConfig, BackoffStrategy, IntervalStrategy};

#[macro_use]
pub mod delegate;
/// A wrapper that allows inspecting errors produced by a backend.
pub mod inspect_err;

/// Extension allowing backends to be instrumented with a [tracing::Span].
#[cfg(feature = "tracing")]
pub mod instrument;
/// A wrapper that allows merging a backend with a stream
pub mod interleave;

/// A wrapper that allows mapping the error type of a backend from `Self::Error` to another error type `E2`.
pub mod map_err;
pub mod pipe;
pub mod poll_strategy;
/// A wrapper that wakes the worker when a new item is fetched.
pub mod wake_on_push;
pub mod with_codec;

pub mod lifecycle;

/// A wrapper that makes a backend clonable
pub mod shared;

/// A wrapper that allows a backend to be used as a stream of tasks, without needing to know the concrete backend type at compile time.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum PollNextArgsError<B: Backend> {
    /// The backend produced an error while polling for the next task.
    #[error("backend error: {0}")]
    BackendError(B::Error),
    /// The backend produced a task, but the task's arguments could not be decoded.
    #[error("failed to decode task args: {0}")]
    DecodeError(BoxDynError),
}

/// Extension trait for `Backend` that provides additional combinators and utilities.
pub trait BackendExt: Backend {
    /// A convenience method for calling `poll_next` and decoding the `Args` in one step,
    /// returning a `Task<Self::Args, ..>` instead of `Task<Self::Compact, ..>`.
    #[allow(clippy::type_complexity)]
    fn poll_next_args(
        &mut self,
        cx: &mut Context<'_>,
        worker: &WorkerContext,
    ) -> Poll<Option<Result<Task<Self::Args>, PollNextArgsError<Self>>>>
    where
        Self: Sized + BackendConfig + WireFormatBackend + Backend<Task = Task<Self::Compact>>,
        Self::Codec: Codec<Self::Args, Compact = Self::Compact>,
        <Self::Codec as Codec<Self::Args>>::Error: std::error::Error + Send + Sync + 'static,
    {
        let next = self.poll_next(cx, worker);
        let codec = self.codec();
        next.map(move |item| match item {
            Some(Ok(task)) => {
                let task = task.try_map_args(|compact| codec.decode(&compact));
                Some(task.map_err(|e| PollNextArgsError::DecodeError(e.into())))
            }
            Some(Err(e)) => Some(Err(PollNextArgsError::BackendError(e))),
            None => None,
        })
    }

    /// Pipes every task polled from this backend into `sink`
    ///
    /// Useful for bridging two backend implementations — e.g. draining an
    /// ephemeral/legacy queue into a durable one, or fanning a lightweight
    /// source into a shared sink that multiple producers write into.
    fn pipe_to<Dst>(self, backend: Dst) -> Pipe<Dst, Self>
    where
        Self: Sized,
    {
        Pipe::new(self, backend)
    }

    /// Attaches a callback `F` to be run on each error produced while polling the backend.
    fn inspect_err<F>(self, f: F) -> InspectErr<Self, F>
    where
        Self: Sized,
        F: Fn(&Self::Error),
    {
        InspectErr { backend: self, f }
    }

    /// Maps errors produced by the backend from `Self::Error` into `E2`, useful for
    /// heterogeneous composed backends.
    fn map_err<F, E2>(self, f: F) -> MapErr<Self, F>
    where
        Self: Sized,
        F: Fn(Self::Error) -> E2,
    {
        MapErr { backend: self, f }
    }

    /// Swaps out the backend's serialization codec entirely (JSON,
    /// MessagePack, Protobuf, ...) without touching storage logic.
    fn with_codec<NewCodec>(self, codec: NewCodec) -> WithCodec<Self, NewCodec>
    where
        Self: Sized + BackendConfig,
        NewCodec: Codec<Self::Args>,
    {
        WithCodec::new(self, codec)
    }

    /// Wake the worker when a stream receives a new item
    fn poll_with_stream<S>(self, stream: S) -> PollWith<Self, StreamStrategy<S>>
    where
        Self: Sized,
        S: Stream + Unpin + Send + 'static,
    {
        let strategy = StreamStrategy::new(stream);
        PollWith::new(self, strategy)
    }

    /// Wake the worker periodically
    #[cfg(feature = "sleep")]
    fn poll_with_interval(self, duration: Duration) -> PollWith<Self, IntervalStrategy>
    where
        Self: Sized,
    {
        let strategy = IntervalStrategy::new(duration);
        PollWith::new(self, strategy)
    }

    /// Wake the worker periodically with a backoff
    #[cfg(feature = "sleep")]
    fn poll_with_backoff(
        self,
        interval: Duration,
        config: BackoffConfig,
    ) -> PollWith<Self, BackoffStrategy>
    where
        Self: Sized,
    {
        let strategy = IntervalStrategy::new(interval).with_backoff(config);
        PollWith::new(self, strategy)
    }

    /// Wake the worker with a custom strategy
    fn poll_with_strategy<S>(self, strategy: S) -> PollWith<Self, S>
    where
        Self: Sized,
        S: PollStrategy,
    {
        PollWith::new(self, strategy)
    }

    #[cfg(feature = "tracing")]
    /// Provides a span to decorate emitted events
    fn instrumented(self, span: tracing::Span) -> Instrumented<Self>
    where
        Self: Sized,
    {
        Instrumented::new(self, span)
    }

    /// Runs an async callback once, before the backend's first `poll_ready` is delegated.
    fn before_start<F, Fut>(self, f: F) -> BeforeStart<Self, Self::Error>
    where
        Self: Sized,
        F: Fn(&mut Self) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<(), Self::Error>> + Send + 'static,
    {
        BeforeStart::new(self, f)
    }

    /// Runs an async callback once, before the backend's poll_close is called.
    fn before_stop<F, Fut>(self, f: F) -> BeforeStop<Self, Self::Error>
    where
        Self: Sized,
        F: Fn(&mut Self) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<(), Self::Error>> + Send + 'static,
    {
        BeforeStop::new(self, f)
    }

    /// Runs an async callback once, after the backend's first `poll_ready` is successful.
    fn after_start<F, Fut>(self, f: F) -> AfterStart<Self, Self::Error>
    where
        Self: Sized,
        for<'c> F: Fn(&mut Self) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<(), Self::Error>> + Send + 'static,
    {
        AfterStart::new(self, f)
    }

    /// Runs an async callback once, after the worker has stopped and backend has cleaned up.
    fn after_stop<F, Fut>(self, f: F) -> AfterStop<Self, Self::Error>
    where
        Self: Sized,
        F: Fn(&mut Self) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<(), Self::Error>> + Send + 'static,
    {
        AfterStop::new(self, f)
    }

    /// Interleaves the external stream with the backend.
    fn interleave<S>(self, stream: S) -> Interleave<Self, S>
    where
        Self: Sized,
        S: Stream<Item = Result<Self::Task, Self::Error>> + Unpin,
    {
        Interleave::new(self, stream)
    }

    /// Wakes the worker when a new item is pushed
    fn wake_on_push(self) -> WakeOnPush<Self>
    where
        Self: Sized,
    {
        WakeOnPush::new(self)
    }

    /// Create a cloneable handle to the inner backend where all handles are clone.
    fn shared(self) -> Shared<Self>
    where
        Self: WireFormatBackend + Send,
        Self::Codec: Clone,
    {
        Shared::new(self)
    }
}

impl<B: Backend> BackendExt for B {}
