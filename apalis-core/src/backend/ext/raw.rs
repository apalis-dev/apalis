//! A wrapper that allows a backend to be used as a stream of tasks, without needing to know the concrete backend type at compile time.

use std::{
    pin::Pin,
    task::{Context, Poll},
};

use futures_sink::Sink;
use futures_util::SinkExt;

use crate::{
    backend::{codec::IdentityCodec, *},
    features_table,
    task::Task,
    worker::context::WorkerContext,
};

/// Wrapper that skips decoding and works directly with the compact
/// representation. `Args` becomes `B::Compact`, and `poll` delegates
/// straight to `poll_compact` instead of decoding through `B::Codec`.
///
/// Useful for backends that natively handle compact types and don't know
/// the concrete `Args` type at compile time — e.g. workflow engines or
/// backends manipulating dynamic/raw payloads.
#[doc = features_table! {
    setup = r#"
        # {
        #   use apalis_core::backend::memory::MemoryStorage;
        #   use apalis_core::backend::ext::raw::RawDataBackend;
        #   let memory = MemoryStorage::new();
        #   RawDataBackend::new(memory)
        # };
    "#,
    Backend => supported("Basic Backend functionality", true),
    TaskSink => supported("Ability to push new tasks", true),
    InheritsFeatures => limited("Inherits features from the underlying backend", false),
}]
#[derive(Debug, Clone)]
pub struct RawDataBackend<B> {
    inner: B,
}

impl<B> RawDataBackend<B> {
    /// Create a new `RawDataBackend` wrapping the given backend.
    pub fn new(backend: B) -> Self {
        Self { inner: backend }
    }
}

impl<B> Backend for RawDataBackend<B>
where
    B: Backend,
    B::Compact: Clone + 'static,
{
    type Args = B::Compact;
    type Id = B::Id;
    type Connection = B::Connection;
    type Error = B::Error;
    type Layer = B::Layer;
    type Codec = IdentityCodec;
    type Compact = B::Compact;
    fn codec(&self) -> &Self::Codec {
        &IdentityCodec
    }

    fn queue(&self) -> Queue {
        self.inner.queue()
    }

    fn middleware(&self) -> Self::Layer {
        self.inner.middleware()
    }

    fn poll_ready(
        &mut self,
        cx: &mut Context<'_>,
        worker: &WorkerContext,
    ) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx, worker)
    }

    fn poll_next(
        &mut self,
        cx: &mut Context<'_>,
        worker: &WorkerContext,
    ) -> Poll<Option<Result<Task<Self::Compact, Self::Connection, Self::Id>, Self::Error>>> {
        self.inner.poll_next(cx, worker)
    }
    fn poll_close(
        &mut self,
        cx: &mut Context<'_>,
        worker: &WorkerContext,
    ) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_close(cx, worker)
    }
}

delegate_sink!(RawDataBackend<B>, inner);

delegate_expose!(
    impl<B> for RawDataBackend<B>
    where {
        B: Send + Sync,
        B::Compact: Send + Clone + 'static,
    }
    => inner
);
