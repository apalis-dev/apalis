//! Extension traits and combinators for `Backend` implementations.
//!
//! This module provides additional functionality for `Backend` implementations, including:
//! - [`BackendExt`]: An extension trait that adds combinators for transforming and composing backends.
//! - [`InspectErr`]: A wrapper that allows inspection of errors produced by a backend
//! - [`MapErr`]: A wrapper that allows mapping errors from one type to another
//! - [`Pipe`]: A utility for piping tasks from one backend to another
//! - [`RawDataBackend`]: A wrapper that allows a backend to be used as a stream of tasks without decoding
use std::task::{Context, Poll};

use crate::{
    backend::{
        Backend,
        codec::Codec,
        ext::{
            inspect_err::InspectErr,
            map_err::MapErr,
            pipe::{FromBackend, Pipe},
            raw::RawDataBackend,
            with_codec::WithCodec,
            with_poll_strategy::WithPollStrategy,
        },
    },
    task::Task,
    worker::context::WorkerContext,
};

#[macro_use]
pub mod delegate;
/// A wrapper that allows inspecting errors produced by a backend.
pub mod inspect_err;
/// A wrapper that allows mapping the error type of a backend from `Self::Error` to another error type `E2`.
pub mod map_err;
pub mod pipe;
pub mod raw;
pub mod with_codec;

pub mod with_poll_strategy;

/// A wrapper that allows a backend to be used as a stream of tasks, without needing to know the concrete backend type at compile time.
#[derive(Debug, thiserror::Error)]
pub enum PollNextArgsError<B: Backend>
where
    <B::Codec as Codec<B::Args>>::Error: std::error::Error,
{
    /// The backend produced an error while polling for the next task.
    #[error("backend error: {0}")]
    BackendError(B::Error),
    /// The backend produced a task, but the task's arguments could not be decoded.
    #[error("failed to decode task args: {0}")]
    DecodeError(<B::Codec as Codec<B::Args>>::Error),
}

/// Extension trait for `Backend` that provides additional combinators and utilities.
pub trait BackendExt: Backend {
    /// Skips decoding entirely: `poll` yields `Task<Self::Compact, ..>`
    /// directly from `poll_compact`, and `Args` becomes `Self::Compact`.
    fn raw(self) -> RawDataBackend<Self>
    where
        Self: Sized,
    {
        RawDataBackend::new(self)
    }

    /// A convenience method for calling `poll_next` and decoding the `Args` in one step,
    /// returning a `Task<Self::Args, ..>` instead of `Task<Self::Compact, ..>`.
    fn poll_next_args(
        &mut self,
        cx: &mut Context<'_>,
        worker: &WorkerContext,
    ) -> Poll<Option<Result<Task<Self::Args, Self::Connection, Self::Id>, PollNextArgsError<Self>>>>
    where
        Self: Sized,
        <Self::Codec as Codec<Self::Args>>::Error: std::error::Error,
    {
        let next = self.poll_next(cx, worker);
        let codec = self.codec();
        next.map(move |item| match item {
            Some(Ok(task)) => {
                let task = task.try_map_args(|compact| codec.decode(&compact));
                Some(task.map_err(|e| PollNextArgsError::DecodeError(e)))
            }
            Some(Err(e)) => Some(Err(PollNextArgsError::BackendError(e))),
            None => None,
        })
    }

    /// Pipes every task polled from this backend into `sink`, then serves
    /// from `sink` going forward. Wraps `self` in [`FromBackend`]
    /// automatically so it can be piped through the same [`Pipe`] used for
    /// plain streams.
    ///
    /// Useful for bridging two backend implementations — e.g. draining an
    /// ephemeral/legacy queue into a durable one, or fanning a lightweight
    /// source into a shared sink that multiple producers write into.
    fn pipe_to<Dst>(self, backend: Dst) -> Pipe<Dst, FromBackend<Self>, Self::Args, Dst::Connection>
    where
        Self: Sized,
        Dst: Backend<Args = Self::Args>,
    {
        Pipe::new(FromBackend(self), backend)
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
        Self: Sized,
        NewCodec: Codec<Self::Args>,
    {
        WithCodec::new(self, codec)
    }

    /// Swaps out the backend's polling strategy entirely (e.g. for rate limiting, backoff, etc.)
    /// without touching storage logic.
    fn with_poll_strategy<S>(self, strategy: S) -> WithPollStrategy<Self, S>
    where
        Self: Sized,
    {
        WithPollStrategy::new(self, strategy)
    }
}

impl<B: Backend> BackendExt for B {}
