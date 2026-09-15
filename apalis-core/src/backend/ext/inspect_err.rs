use std::{
    pin::Pin,
    task::{Context, Poll},
};

use futures_sink::Sink;
use futures_util::SinkExt;
use futures_util::TryStreamExt;

use crate::{backend::*, worker::context::WorkerContext};

/// A `Backend` wrapper that runs a callback `F` on each error yielded by the poll stream.
#[derive(Debug, Clone)]
pub struct InspectErr<B, F> {
    pub(super) backend: B,
    pub(super) f: F,
}

impl<B, F> Backend for InspectErr<B, F>
where
    B: Backend,
    F: FnMut(&B::Error),
{
    type Task = B::Task;
    type Error = B::Error;

    fn poll_ready(
        &mut self,
        cx: &mut Context<'_>,
        worker: &WorkerContext,
    ) -> Poll<Result<(), Self::Error>> {
        self.backend.poll_ready(cx, worker).map_err(|err| {
            (self.f)(&err);
            err
        })
    }

    fn poll_next(
        &mut self,
        cx: &mut Context<'_>,
        worker: &WorkerContext,
    ) -> Poll<Option<Result<Self::Task, Self::Error>>> {
        self.backend.poll_next(cx, worker).map(|opt| {
            opt.map(|res| {
                res.inspect_err(|err| {
                    (self.f)(err);
                })
            })
        })
    }

    fn poll_close(
        &mut self,
        cx: &mut Context<'_>,
        worker: &WorkerContext,
    ) -> Poll<Result<(), Self::Error>> {
        self.backend.poll_close(cx, worker).map_err(|err| {
            (self.f)(&err);
            err
        })
    }
}

impl<B, F, T, Err> Sink<T> for InspectErr<B, F>
where
    B: Sink<T, Error = Err> + Unpin,
    F: FnMut(&Err) + Unpin,
{
    type Error = Err;
    fn start_send(self: Pin<&mut Self>, item: T) -> Result<(), Self::Error> {
        let this = self.get_mut();
        this.backend.start_send_unpin(item).inspect_err(|err| {
            (this.f)(err);
        })
    }
    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let this = self.get_mut();
        this.backend.poll_ready_unpin(cx).map_err(|err| {
            (this.f)(&err);
            err
        })
    }
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let this = self.get_mut();
        this.backend.poll_flush_unpin(cx).map_err(|err| {
            (this.f)(&err);
            err
        })
    }
    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let this = self.get_mut();
        this.backend.poll_close_unpin(cx).map_err(|err| {
            (this.f)(&err);
            err
        })
    }
}

delegate_config!(InspectErr<B, F>, backend);

delegate_codec!(InspectErr<B, F>, backend);

delegate_deref!(InspectErr<B, F>, backend);

delegate_expose!(
    impl<B, F> for InspectErr<B, F>
    where {
            B: Backend + Send,
            F: FnMut(&B::Error) + Send + Sync + Clone + 'static,
    }
    => backend,
    wrap = |this, result| { let mut f = this.f.clone(); result.inspect_err(move |err| {(f)(err)}) }
);
