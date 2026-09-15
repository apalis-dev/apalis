use std::{
    pin::Pin,
    task::{Context, Poll},
};

use futures_sink::Sink;
use futures_util::SinkExt;
use futures_util::TryStreamExt;

use crate::{backend::*, worker::context::WorkerContext};

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
    type Task = B::Task;
    type Error = E2;

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
    ) -> Poll<Option<Result<Self::Task, Self::Error>>> {
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

impl<B, F, T, Err, E2> Sink<T> for MapErr<B, F>
where
    B: Sink<T, Error = Err> + Unpin,
    F: Fn(B::Error) -> E2 + Unpin,
    E2: std::error::Error + Send + Sync + 'static,
{
    type Error = E2;
    fn start_send(self: Pin<&mut Self>, item: T) -> Result<(), Self::Error> {
        let this = self.get_mut();
        this.backend
            .start_send_unpin(item)
            .map_err(|err| (this.f)(err))
    }
    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let this = self.get_mut();
        this.backend
            .poll_ready_unpin(cx)
            .map_err(|err| (this.f)(err))
    }
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let this = self.get_mut();
        this.backend
            .poll_flush_unpin(cx)
            .map_err(|err| (this.f)(err))
    }
    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let this = self.get_mut();
        this.backend
            .poll_close_unpin(cx)
            .map_err(|err| (this.f)(err))
    }
}

delegate_deref!(MapErr<B, F>, backend);

delegate_config!(MapErr<B, F>, backend);

delegate_codec!(MapErr<B, F>, backend);

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
