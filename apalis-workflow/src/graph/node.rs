use apalis_core::backend::codec::Codec;
use apalis_core::backend::{Backend, BackendConfig, WireFormatBackend};
use apalis_core::error::BoxDynError;
use apalis_core::task::Task;
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use tower::Service;

use crate::graph::NodeInput;
use crate::graph::decode::GraphCodec;

/// A service that wraps another service to handle encoding and decoding
/// of task inputs and outputs using the backend's codec.
pub struct GraphNodeService<S, B, Input>
where
    S: Service<Task<Input>>,
    B: Backend,
{
    inner: S,
    _phantom: std::marker::PhantomData<(B, Input)>,
}

impl<S, B, Input> std::fmt::Debug for GraphNodeService<S, B, Input>
where
    S: Service<Task<Input>>,
    B: Backend,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NodeService")
            .field("inner", &"<service>")
            .field("_phantom", &std::any::type_name::<(B, Input)>())
            .finish()
    }
}

impl<S, B, Input> Clone for GraphNodeService<S, B, Input>
where
    S: Service<Task<Input>> + Clone,
    B: Backend,
{
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            _phantom: std::marker::PhantomData,
        }
    }
}

impl<S, B, Input> GraphNodeService<S, B, Input>
where
    S: Service<Task<Input>>,
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

impl<S, B, Input, CdcErr> Service<Task<NodeInput<B::Compact>>> for GraphNodeService<S, B, Input>
where
    S: Service<Task<Input>>,
    S::Error: Into<BoxDynError>,
    B: Backend + WireFormatBackend + BackendConfig + Send + Sync + 'static,
    B::Codec: Codec<Input, Compact = B::Compact, Error = CdcErr>
        + Codec<S::Response, Compact = B::Compact, Error = CdcErr>
        + Send
        + Sync
        + Clone,
    Input: GraphCodec<B, Error = CdcErr> + DeserializeOwned,
    CdcErr: Into<BoxDynError> + Send + 'static,
    S::Future: Send + 'static,
    S::Response: Serialize,
{
    // Here we return both the encoded version and the json version
    // We push next nodes with the compact version
    // We store results in json - hence why we need the json version
    type Response = (B::Compact, serde_json::Value);
    type Error = BoxDynError;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx).map_err(|e| e.into())
    }

    fn call(&mut self, req: Task<NodeInput<B::Compact>>) -> Self::Future {
        let codec = req
            .data()
            .get::<B::Codec>()
            .cloned()
            .expect("GraphExecutor should be injected");

        let req = req.try_map_args(|args| match args {
            NodeInput::Single(args) => match Input::decode(&args, &codec) {
                Ok(decoded) => Ok(decoded),
                Err(e) => Err(CdcErr::into(e)),
            },
            NodeInput::FanIn(fan_in) => {
                let value = serde_json::Value::Array(fan_in);
                let result: Input = serde_json::from_value(value)?;
                Ok(result)
            }
        });

        let decoded_req = match req {
            Ok(req) => req,
            Err(e) => {
                return Box::pin(async move { Err(e) });
            }
        };

        let fut = self.inner.call(decoded_req);

        Box::pin(async move {
            let response = fut.await.map_err(|e| e.into())?;
            let compact = B::Codec::encode(&codec, &response).map_err(|e| e.into())?;
            let res = serde_json::to_value(&response)?;
            Ok((compact, res))
        })
    }
}
