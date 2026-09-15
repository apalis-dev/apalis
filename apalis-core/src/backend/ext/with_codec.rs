//! A utility module for modifying the codec used by a backend.
//!
//! This is useful for changing the serialization format of task arguments without altering the underlying backend logic.
use std::{
    pin::Pin,
    task::{Context, Poll},
};

use futures_sink::Sink;
use futures_util::SinkExt;

use crate::{backend::*, worker::context::WorkerContext};

/// A `Backend` wrapper that swaps out the serialization codec entirely (JSON,
/// MessagePack, Protobuf, ...) without touching storage logic.
#[derive(Debug, Clone)]
#[doc = features_table! {
    setup = r#"
        # {
        #   use apalis_core::backend::memory::MemoryStorage;
        #   use apalis_core::backend::ext::with_codec::WithCodec;
        #   let memory = MemoryStorage::new();
        #.  pub struct MyCodec;
        #   WithCodec::new(memory, MyCodec)
        # };
    "#,
    Backend => supported("Basic Backend functionality", false),
    TaskSink => supported("Ability to push new tasks", false),
    InheritsFeatures => limited("Inherits features from the underlying backend", false),
}]
pub struct WithCodec<B, NewCodec> {
    backend: B,
    codec: NewCodec,
}

impl<B, NewCodec> WithCodec<B, NewCodec> {
    /// Create a new `WithCodec` wrapping the given backend and codec.
    pub fn new(backend: B, codec: NewCodec) -> Self {
        Self { backend, codec }
    }
}

impl<B, NewCodec> Backend for WithCodec<B, NewCodec>
where
    B: Backend,
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

impl<B, NewCodec, Args> WireFormatBackend for WithCodec<B, NewCodec>
where
    NewCodec: Codec<Args> + Send + 'static,
    B: BackendConfig<Args = Args>,
{
    type Codec = NewCodec;

    type Compact = NewCodec::Compact;

    fn codec(&self) -> &Self::Codec {
        &self.codec
    }
}

delegate_sink!(WithCodec<B, NewCodec>, backend);

delegate_deref!(WithCodec<B, NewCodec>, backend);

delegate_config!(WithCodec<B, NewCodec>, backend);

delegate_expose!(
    impl<B, C> for WithCodec<B, C>
    where {
        B: Send + Sync + Backend,
        C: Send + Sync + 'static,

    }
    => backend
);
