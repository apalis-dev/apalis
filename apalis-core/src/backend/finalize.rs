use std::{
    fmt::Debug,
    marker::PhantomData,
    pin::Pin,
    task::{Context, Poll},
};

use futures_sink::Sink;
use futures_util::SinkExt;

use crate::{
    backend::{codec::Codec, *},
    delegate_sink,
    error::BoxDynError,
    task::Task,
    worker::{call_all::CallAllError, context::WorkerContext},
};

/// Finalizes an intermediate backend configuration into a concrete backend.
///
/// `B` represents the backend being built, while `Args` is the type of
/// arguments the finalized backend exposes to workers.
///
/// Implementations of this trait determine how the final backend is composed.
/// For example, [`Ephemeral`] returns the backend unchanged, while
/// [`Durable`] wraps it in [`DurableBackend`] to decode persisted task
/// arguments.
pub trait FinalizeBackend<B, Args> {
    /// The concrete backend produced by finalization.
    type Backend;

    /// Finalizes the current backend.
    fn finalize(current: B) -> Self::Backend;
}

/// Finalization mode for an ephemeral backend.
///
/// An ephemeral backend is used as-is and does not perform any additional
/// persistence-related wrapping.
#[derive(Debug, Clone, Copy, Default)]
pub struct Ephemeral;

impl<B, Args> FinalizeBackend<B, Args> for Ephemeral {
    type Backend = B;

    fn finalize(current: B) -> Self::Backend {
        current
    }
}

/// Finalization mode for a durable backend.
///
/// Durable backends store task arguments in their wire format and decode them
/// when tasks are retrieved.
#[derive(Debug, Clone, Copy, Default)]
pub struct Durable;

impl<B, Args> FinalizeBackend<B, Args> for Durable
where
    B: BackendConfig + WireFormatBackend,
{
    type Backend = DurableBackend<B, Args>;

    fn finalize(current: B) -> Self::Backend {
        DurableBackend {
            backend: current,
            _marker: PhantomData,
        }
    }
}

/// A backend wrapper that decodes persisted task arguments.
///
/// `DurableBackend` wraps a [`WireFormatBackend`] whose task arguments are
/// stored in a compact wire representation. The configured [`Codec`] is used
/// to decode those arguments back into `Args` as tasks are polled.
#[derive(Debug)]
pub struct DurableBackend<B, Args> {
    backend: B,
    _marker: PhantomData<Args>,
}

impl<B: Clone, Args> Clone for DurableBackend<B, Args> {
    fn clone(&self) -> Self {
        Self {
            backend: self.backend.clone(),
            _marker: PhantomData,
        }
    }
}

impl<B, Compact, Args, Err> Backend for DurableBackend<B, Args>
where
    B: Backend<Task = Task<Compact>> + WireFormatBackend,
    B::Codec: Codec<Args, Compact = Compact, Error = Err>,
    Err: Into<BoxDynError>,
    <B::Codec as Codec<Args>>::Error: Into<BoxDynError>,
    B::Error: Into<BoxDynError>,
{
    type Task = Task<Args>;
    type Error = CallAllError<B::Error>;

    fn poll_ready(
        &mut self,
        cx: &mut Context<'_>,
        worker: &WorkerContext,
    ) -> Poll<Result<(), Self::Error>> {
        self.backend
            .poll_ready(cx, worker)
            .map_err(|e| CallAllError::PollError(e.into()))
    }

    fn poll_next(
        &mut self,
        cx: &mut Context<'_>,
        worker: &WorkerContext,
    ) -> Poll<Option<Result<Self::Task, Self::Error>>> {
        self.backend.poll_next(cx, worker).map(|s| match s {
            Some(Ok(task)) => {
                let codec = self.codec();

                let task = task
                    .try_map_args(|s| codec.decode(&s).map_err(Into::into))
                    .map_err(CallAllError::CodecError);

                Some(task)
            }
            Some(Err(e)) => Some(Err(CallAllError::PollError(e.into()))),
            None => None,
        })
    }

    fn poll_close(
        &mut self,
        cx: &mut Context<'_>,
        worker: &WorkerContext,
    ) -> Poll<Result<(), Self::Error>> {
        self.backend
            .poll_close(cx, worker)
            .map_err(|e| CallAllError::PollError(e.into()))
    }
}

impl<B, Args> WireFormatBackend for DurableBackend<B, Args>
where
    B: WireFormatBackend + Backend,
{
    type Compact = B::Compact;
    type Codec = B::Codec;

    fn codec(&self) -> &Self::Codec {
        self.backend.codec()
    }
}

impl<B, Args> BackendConfig for DurableBackend<B, Args>
where
    B: BackendConfig,
{
    type Id = B::Id;
    type Args = Args;
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

delegate_sink!(DurableBackend<B, Args>, backend);

/// Finalization mode for a durable backend.
///
/// Durable backends store task arguments in their wire format and decode them
/// when tasks are retrieved.
#[derive(Debug, Clone, Copy, Default)]
pub struct DurableCompact;

impl<B, Args> FinalizeBackend<B, Args> for DurableCompact
where
    B: BackendConfig + WireFormatBackend,
{
    type Backend = DurableBackend<B, Args>;

    fn finalize(current: B) -> Self::Backend {
        DurableBackend {
            backend: current,
            _marker: PhantomData,
        }
    }
}
