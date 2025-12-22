use apalis_core::backend::BackendExt;
use apalis_core::backend::codec::Codec;
use apalis_core::error::BoxDynError;
use apalis_core::task::Task;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use tower::Service;

/// A service that wraps another service to handle encoding and decoding
/// of task inputs and outputs using the backend's codec.
pub struct NodeService<S, B, Input>
where
    S: Service<Task<Input, B::Context, B::IdType>>,
    B: BackendExt,
{
    inner: S,
    _phantom: std::marker::PhantomData<(B, Input)>,
}

impl<S, B, Input> std::fmt::Debug for NodeService<S, B, Input>
where
    S: Service<Task<Input, B::Context, B::IdType>>,
    B: BackendExt,
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
    S: Service<Task<Input, B::Context, B::IdType>> + Clone,
    B: BackendExt,
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
    S: Service<Task<Input, B::Context, B::IdType>>,
    B: BackendExt,
{
    /// Creates a new `NodeService` wrapping the provided service.
    pub fn new(inner: S) -> Self {
        Self {
            inner,
            _phantom: std::marker::PhantomData,
        }
    }
}

impl<S, B, Input, CdcErr> Service<Task<B::Compact, B::Context, B::IdType>>
    for NodeService<S, B, Input>
where
    S: Service<Task<Input, B::Context, B::IdType>>,
    S::Error: Into<BoxDynError>,
    B: BackendExt,
    B::Codec: Codec<Input, Compact = B::Compact, Error = CdcErr>
        + Codec<S::Response, Compact = B::Compact, Error = CdcErr>,
    CdcErr: Into<BoxDynError> + Send + 'static,
    S::Future: Send + 'static,
{
    type Response = B::Compact;
    type Error = BoxDynError;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx).map_err(|e| e.into())
    }

    fn call(&mut self, req: Task<B::Compact, B::Context, B::IdType>) -> Self::Future {
        let decoded_req = match B::Codec::decode(&req.args) {
            Ok(decoded) => req.map(|_| decoded),
            Err(e) => {
                return Box::pin(async move { Err(CdcErr::into(e)) });
            }
        };

        let fut = self.inner.call(decoded_req);

        Box::pin(async move {
            let response = fut.await.map_err(|e| e.into())?;
            B::Codec::encode(&response).map_err(|e| e.into())
        })
    }
}
