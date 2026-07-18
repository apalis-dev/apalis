use apalis_core::backend::Backend;
use apalis_core::backend::codec::Codec;
use apalis_core::error::BoxDynError;
use apalis_core::task::Task;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use tower::Service;

use crate::DagExecutor;
use crate::dag::decode::DagCodec;

/// A service that wraps another service to handle encoding and decoding
/// of task inputs and outputs using the backend's codec.
pub struct NodeService<S, B, Input>
where
    S: Service<Task<Input, B::Connection, B::Id>>,
    B: Backend,
{
    inner: S,
    _phantom: std::marker::PhantomData<(B, Input)>,
}

impl<S, B, Input> std::fmt::Debug for NodeService<S, B, Input>
where
    S: Service<Task<Input, B::Connection, B::Id>>,
    B: Backend,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NodeService")
            .field("inner", &"<service>")
            .field("_phantom", &std::any::type_name::<(B, Input)>())
            .finish()
    }
}

impl<S, B, Input> Clone for NodeService<S, B, Input>
where
    S: Service<Task<Input, B::Connection, B::Id>> + Clone,
    B: Backend,
{
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            _phantom: std::marker::PhantomData,
        }
    }
}

impl<S, B, Input> NodeService<S, B, Input>
where
    S: Service<Task<Input, B::Connection, B::Id>>,
    B: Backend,
{
    /// Creates a new `NodeService` wrapping the provided service.
    pub fn new(inner: S) -> Self {
        Self {
            inner,
            _phantom: std::marker::PhantomData,
        }
    }
}

impl<S, B, Input, CdcErr> Service<Task<B::Compact, B::Connection, B::Id>>
    for NodeService<S, B, Input>
where
    S: Service<Task<Input, B::Connection, B::Id>>,
    S::Error: Into<BoxDynError>,
    B: Backend + Send + Sync + 'static,
    B::Codec: Codec<Input, Compact = B::Compact, Error = CdcErr>
        + Codec<S::Response, Compact = B::Compact, Error = CdcErr>
        + Send
        + Clone,
    Input: DagCodec<B, Error = CdcErr>,
    CdcErr: Into<BoxDynError> + Send + 'static,
    S::Future: Send + 'static,
{
    type Response = B::Compact;
    type Error = BoxDynError;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx).map_err(|e| e.into())
    }

    fn call(&mut self, req: Task<B::Compact, B::Connection, B::Id>) -> Self::Future {
        let executor = req
            .ctx
            .data
            .get::<DagExecutor<B>>()
            .expect("DagExecutor should be injected");

        let codec = executor.backend.codec().clone();
        let decoded_req = match Input::decode(&req.args, &codec) {
            Ok(decoded) => req.map_args(|_| decoded),
            Err(e) => {
                return Box::pin(async move { Err(CdcErr::into(e)) });
            }
        };

        let fut = self.inner.call(decoded_req);

        Box::pin(async move {
            let response = fut.await.map_err(|e| e.into())?;
            B::Codec::encode(&codec, &response).map_err(|e| e.into())
        })
    }
}
