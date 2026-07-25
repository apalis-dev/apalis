use std::{
    pin::Pin,
    task::{Context, Poll},
};

use futures_sink::Sink;
use futures_util::SinkExt;
use futures_util::TryStreamExt;

use crate::{backend::*, task::Task, worker::context::WorkerContext};

/// A `Backend` wrapper that maps the backend's error type `Self::Error` into `E2`.
#[derive(Debug, Clone)]
pub struct MapErr<B, F> {
    pub(super) backend: B,
    pub(super) f: F,
}

impl<B, F, E2> Backend for MapErr<B, F>
where
    B: Backend,
    F: Fn(B::Error) -> E2,
    E2: std::error::Error + Send + Sync + 'static,
{
    type Args = B::Args;
    type Id = B::Id;
    type Config = B::Config;
    type Error = E2;
    type Codec = B::Codec;
    type Compact = B::Compact;
    type Layer = B::Layer;

    fn codec(&self) -> &Self::Codec {
        self.backend.codec()
    }

    fn config(&self) -> &Self::Config {
        self.backend.config()
    }

    fn middleware(&self) -> Self::Layer {
        self.backend.middleware()
    }

    fn poll_ready(
        &mut self,
        cx: &mut Context<'_>,
        worker: &WorkerContext,
    ) -> Poll<Result<(), Self::Error>> {
        self.backend
            .poll_ready(cx, worker)
            .map_err(|err| (self.f)(err))
    }

    fn poll_next(
        &mut self,
        cx: &mut Context<'_>,
        worker: &WorkerContext,
    ) -> Poll<Option<Result<Task<Self::Compact, Self::Id>, Self::Error>>> {
        self.backend
            .poll_next(cx, worker)
            .map(|opt| opt.map(|res| res.map_err(|err| (self.f)(err))))
    }

    fn poll_close(
        &mut self,
        cx: &mut Context<'_>,
        worker: &WorkerContext,
    ) -> Poll<Result<(), Self::Error>> {
        self.backend
            .poll_close(cx, worker)
            .map_err(|err| (self.f)(err))
    }
}

delegate_sink!(MapErr<B, F>, backend);

delegate_expose!(
    impl<B, F, E2> for MapErr<B, F>
    where {
            B: Backend + Send,
            F: Fn(B::Error) -> E2 + Send + Sync + Clone + 'static,
            E2: std::error::Error + Send + Sync + 'static,
    }
    => backend,
    wrap = |this, result| {
        let f = this.f.clone();
        #[allow(clippy::manual_inspect, clippy::redundant_closure)]
        result.map_err(move |err| (f)(err))
    }
);
