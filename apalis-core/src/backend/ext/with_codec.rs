//! A utility module for modifying the codec used by a backend.
//!
//! This is useful for changing the serialization format of task arguments without altering the underlying backend logic.
use std::{
    pin::Pin,
    task::{Context, Poll},
};

use futures_sink::Sink;
use futures_util::SinkExt;

use crate::{
    backend::{queue::Queue, *},
    features_table,
    task::Task,
    worker::context::WorkerContext,
};

/// A `Backend` wrapper that swaps out the serialization codec entirely (JSON,
/// MessagePack, Protobuf, ...) without touching storage logic.
#[derive(Debug, Clone)]
#[doc = features_table! {
    setup = r#"
        # {
        #   use apalis_core::backend::memory::MemoryStorage;
        #   use apalis_core::backend::ext::with_codec::WithCodec;
        #   use apalis_core::backend::codec::IdentityCodec;
        #   let memory = MemoryStorage::new();
        #   WithCodec::new(memory, IdentityCodec)
        # };
    "#,
    Backend => supported("Basic Backend functionality", true),
    TaskSink => supported("Ability to push new tasks", true),
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
    NewCodec: Codec<B::Args, Compact = B::Compact> + Send + 'static,
{
    type Args = B::Args;
    type Id = B::Id;
    type Connection = B::Connection;
    type Error = B::Error;
    type Codec = NewCodec;
    type Compact = B::Compact;
    type Layer = B::Layer;

    fn codec(&self) -> &Self::Codec {
        &self.codec
    }

    fn queue(&self) -> Queue {
        self.backend.queue()
    }

    fn middleware(&self) -> Self::Layer {
        self.backend.middleware()
    }

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
    ) -> Poll<Option<Result<Task<Self::Compact, Self::Connection, Self::Id>, Self::Error>>> {
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

delegate_sink!(WithCodec<B, NewCodec>, backend);

delegate_expose!(
    impl<B, C> for WithCodec<B, C>
    where {
        B: Send + Sync,
        C: Codec<B::Args, Compact = B::Compact> + Send + Sync + 'static,
        B::Compact: Send
    }
    => backend
);
