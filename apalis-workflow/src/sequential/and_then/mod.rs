use std::{marker::PhantomData, task::Context};

use apalis_core::{
    backend::{Backend, BackendConfig, WireFormatBackend, codec::Codec},
    error::BoxDynError,
    task::task_fn::{TaskFn, task_fn},
    task::{Task, task_id::GenerateId},
};
use futures_util::{
    FutureExt, Sink,
    future::{BoxFuture, ready},
};
use serde::Serialize;
use tower::{Service, ServiceBuilder, layer::layer_fn};

use crate::{
    SteppedService,
    sequential::context::StepContext,
    sequential::router::{GoTo, StepResponse, WorkflowRouter},
    sequential::service::handle_step_result,
    sequential::step::{Layer, Stack, Step},
    sequential::workflow::SteppedFlow,
};

/// A layer that represents an `and_then` step in the workflow.
#[derive(Clone, Debug)]
pub struct AndThen<F> {
    then_fn: F,
}

impl<F> AndThen<F> {
    /// Creates a new `AndThen` layer with the provided function.
    pub fn new(then_fn: F) -> Self {
        Self { then_fn }
    }
}

/// The step implementation for the `AndThen` layer.
#[derive(Clone, Debug)]
pub struct AndThenStep<F, S> {
    then_fn: F,
    step: S,
}

impl<S, F> Layer<S> for AndThen<F>
where
    F: Clone,
{
    type Step = AndThenStep<F, S>;

    fn layer(&self, step: S) -> Self::Step {
        AndThenStep {
            then_fn: self.then_fn.clone(),
            step,
        }
    }
}

impl<F, Input, S, B, CodecError, Err> Step<Input, B> for AndThenStep<F, S>
where
    B: Backend<Error = Err>
        + WireFormatBackend
        + BackendConfig
        + Sink<Task<B::Compact>, Error = Err>
        + Send
        + Sync
        + Unpin
        + Clone
        + 'static,
    F: Service<Task<Input>, Error = BoxDynError> + Send + Sync + 'static + Clone,
    S: Step<F::Response, B>,
    Input: Send + Sync + 'static,
    F::Future: Send + 'static,
    F::Error: Into<BoxDynError> + Send + 'static,
    B::Codec: Codec<F::Response, Error = CodecError, Compact = B::Compact>
        + Codec<Input, Error = CodecError, Compact = B::Compact>
        + Codec<S::Response, Error = CodecError, Compact = B::Compact>
        + Send
        + Sync
        + Clone
        + 'static,
    CodecError: std::error::Error + Send + Sync + 'static,
    B::Id: GenerateId + Send + Sync + 'static,
    S::Response: Send + 'static,
    B::Compact: Send + 'static,
    F::Response: Send + Serialize + 'static,
    Err: std::error::Error + Send + Sync + 'static,
{
    type Response = F::Response;
    type Error = F::Error;
    fn register(&mut self, ctx: &mut WorkflowRouter<B>) -> Result<(), BoxDynError> {
        let svc = ServiceBuilder::new()
            .layer(layer_fn(|s| AndThenService {
                service: s,
                _marker: PhantomData::<fn(B, Input) -> ()>,
            }))
            .map_response(|res: F::Response| GoTo::Next(res))
            .service(self.then_fn.clone());
        let svc = SteppedService::<B::Compact>::new(svc);
        let count = ctx.steps.len();
        ctx.steps.insert(count, svc);
        self.step.register(ctx)
    }
}

/// The service implementation for the `AndThen` step.
#[derive(Debug)]
pub struct AndThenService<Svc, Backend, Cur> {
    service: Svc,
    _marker: PhantomData<fn(Backend, Cur) -> ()>,
}

impl<Svc: Clone, Backend, Cur> Clone for AndThenService<Svc, Backend, Cur> {
    fn clone(&self) -> Self {
        Self {
            service: self.service.clone(),
            _marker: PhantomData,
        }
    }
}

impl<Svc, Backend, Cur> AndThenService<Svc, Backend, Cur> {
    /// Creates a new `AndThenService` with the provided service.
    pub fn new(service: Svc) -> Self {
        Self {
            service,
            _marker: PhantomData,
        }
    }
}

impl<S, B, Cur, Res, CodecErr, Err> Service<Task<B::Compact>> for AndThenService<S, B, Cur>
where
    S: Service<Task<Cur>, Response = GoTo<Res>>,
    S::Future: Send + 'static,
    B: Backend<Error = Err>
        + WireFormatBackend
        + Sink<Task<B::Compact>, Error = Err>
        + BackendConfig
        + Clone
        + Send
        + Unpin
        + Sync
        + 'static,
    B::Codec: Codec<Cur, Compact = B::Compact, Error = CodecErr>
        + Codec<Res, Compact = B::Compact, Error = CodecErr>
        + Send
        + Clone
        + Sync,
    S::Error: Into<BoxDynError> + Send + 'static,
    CodecErr: Into<BoxDynError> + Send + 'static,
    Cur: Send + 'static,
    B::Id: GenerateId + Send + Sync + 'static,
    Res: Send + Serialize + 'static,
    B::Compact: Send + 'static,
    Err: std::error::Error + Send + Sync + 'static,
{
    type Response = GoTo<StepResponse>;
    type Error = BoxDynError;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> std::task::Poll<Result<(), Self::Error>> {
        self.service.poll_ready(cx).map_err(|e| e.into())
    }

    fn call(&mut self, request: Task<B::Compact>) -> Self::Future {
        let mut ctx = request.data().get::<StepContext<B>>().cloned().unwrap();
        let codec = ctx.backend.codec();
        let compacted = request.try_map_args(|t| B::Codec::decode(codec, &t));
        match compacted {
            Ok(task) => {
                let fut = self.service.call(task);
                async move {
                    let res = fut.await.map_err(|e| e.into())?;
                    Ok(handle_step_result(&mut ctx, res).await?)
                }
                .boxed()
            }
            Err(e) => ready(Err(e.into())).boxed(),
        }
    }
}

impl<Start, Cur, B, L> SteppedFlow<Start, Cur, B, L>
where
    B: Backend,
{
    /// Adds a transformation step to the workflow that processes the output of the previous step.
    ///
    /// The `and_then` method allows you to chain operations by providing a function that
    /// takes the result of the current workflow step and transforms it into the input
    /// for the next step. This enables building complex processing pipelines with
    /// type-safe transformations between steps.
    /// # Example
    /// ```rust,ignore
    /// workflow
    ///     .and_then(extract)
    ///     .and_then(transform)
    ///     .and_then(load);
    /// ```
    #[allow(clippy::type_complexity)]
    pub fn and_then<F, O, FnArgs>(
        self,
        and_then: F,
    ) -> SteppedFlow<Start, O, B, Stack<AndThen<TaskFn<F, Cur, FnArgs>>, L>>
    where
        TaskFn<F, Cur, FnArgs>: Service<Task<Cur>, Response = O>,
    {
        self.add_step(AndThen {
            then_fn: task_fn(and_then),
        })
    }
}
