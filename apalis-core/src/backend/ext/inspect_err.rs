use std::{
    pin::Pin,
    task::{Context, Poll},
};

use futures_sink::Sink;
use futures_util::SinkExt;
use futures_util::TryStreamExt;

use crate::{
    backend::{queue::Queue, *},
    task::Task,
    worker::context::WorkerContext,
};

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
    type Args = B::Args;
    type Id = B::Id;
    type Connection = B::Connection;
    type Error = B::Error;
    type Codec = B::Codec;
    type Compact = B::Compact;
    type Layer = B::Layer;

    fn codec(&self) -> &Self::Codec {
        self.backend.codec()
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
        self.backend.poll_ready(cx, worker).map_err(|err| {
            (self.f)(&err);
            err
        })
    }

    fn poll_next(
        &mut self,
        cx: &mut Context<'_>,
        worker: &WorkerContext,
    ) -> Poll<Option<Result<Task<Self::Compact, Self::Connection, Self::Id>, Self::Error>>> {
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

delegate_sink!(InspectErr<B, F>, backend);

delegate_expose!(
    impl<B, F> for InspectErr<B, F>
    where {
            B: Backend + Send,
            F: FnMut(&B::Error) + Send + Sync + Clone + 'static,
    }
    => backend,
    wrap = |this, result| { let mut f = this.f.clone(); result.inspect_err(move |err| {(f)(err)}) }
);
