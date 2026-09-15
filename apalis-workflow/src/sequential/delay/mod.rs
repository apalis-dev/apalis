use std::time::Duration;

use apalis_core::{
    backend::{Backend, BackendConfig, WireFormatBackend, codec::Codec},
    error::BoxDynError,
    task::{Task, builder::TaskBuilder, task_id::GenerateId},
};
use futures_util::SinkExt;
use futures_util::{FutureExt, Sink, future::BoxFuture};
use serde_json::to_value;
use tower::Service;

use crate::{
    SteppedFlow, SteppedService,
    sequential::{
        context::{StepContext, WorkflowContext},
        router::{GoTo, StepResponse, WorkflowRouter},
        step::{Layer, Stack, Step},
    },
};

/// Layer that delays execution by a specified duration
#[derive(Clone, Debug)]
pub struct DelayFor {
    duration: Duration,
}

impl<S> Layer<S> for DelayFor
where
    S: Clone,
{
    type Step = DelayForStep<S>;

    fn layer(&self, step: S) -> Self::Step {
        DelayForStep {
            inner: step,
            duration: self.duration,
        }
    }
}

/// Step that delays execution by a specified duration
#[derive(Clone, Debug)]
pub struct DelayForStep<S> {
    inner: S,
    duration: Duration,
}

impl<Input, B, S, Err> Step<Input, B> for DelayForStep<S>
where
    B::Id: GenerateId + Send + Sync + 'static,
    B::Compact: Send + 'static,
    B: Sink<Task<B::Compact>, Error = Err>
        + WireFormatBackend
        + BackendConfig
        + Unpin
        + Send
        + Sync
        + Clone
        + 'static,
    Err: std::error::Error + Send + Sync + 'static,
    S: Clone + Send + Sync + 'static,
    S::Response: Send + 'static,
    B::Codec: Codec<Duration, Compact = B::Compact>
        + Codec<Input, Compact = B::Compact>
        + Send
        + Clone
        + 'static,
    <B::Codec as Codec<Duration>>::Error: Into<BoxDynError>,
    Input: Send + Sync + 'static,
    <B::Codec as Codec<Input>>::Error: Into<BoxDynError>,
    B: Backend,
    S: Step<Input, B>,
{
    type Response = Input;
    type Error = BoxDynError;
    fn register(&mut self, ctx: &mut WorkflowRouter<B>) -> Result<(), BoxDynError> {
        let duration = self.duration;
        let svc = SteppedService::new(DelayWithStep {
            f: Box::new(move |_| duration),
            inner: self.inner.clone(),
            _marker: std::marker::PhantomData,
        });
        let count = ctx.steps.len();
        ctx.steps.insert(count, svc);
        self.inner.register(ctx)
    }
}

/// Step that delays execution by a specified duration
#[derive(Clone, Debug)]
pub struct DelayWith<F, B, Input> {
    f: F,
    _marker: std::marker::PhantomData<(B, Input)>,
}

impl<S, F: Clone, B, I> Layer<S> for DelayWith<F, B, I> {
    type Step = DelayWithStep<S, F, B, I>;

    fn layer(&self, step: S) -> Self::Step {
        DelayWithStep {
            f: self.f.clone(),
            inner: step,
            _marker: std::marker::PhantomData,
        }
    }
}

/// Step that delays execution by a specified duration
#[derive(Debug)]
pub struct DelayWithStep<S, F, B, Input> {
    f: F,
    inner: S,
    _marker: std::marker::PhantomData<(B, Input)>,
}

impl<S: Clone, F: Clone, B, Input> Clone for DelayWithStep<S, F, B, Input> {
    fn clone(&self) -> Self {
        Self {
            f: self.f.clone(),
            inner: self.inner.clone(),
            _marker: std::marker::PhantomData,
        }
    }
}

impl<Input, F, B, S, Err> Step<Input, B> for DelayWithStep<S, F, B, Input>
where
    F: FnMut(Task<Input>) -> Duration + Send + Sync + 'static + Clone,
    B::Id: GenerateId + Sync + Send + 'static,
    B::Compact: Send + 'static,
    B: Sink<Task<B::Compact>, Error = Err>
        + BackendConfig
        + WireFormatBackend
        + Unpin
        + Send
        + Sync
        + Clone
        + 'static,
    Err: std::error::Error + Send + Sync + 'static,
    S: Step<Input, B> + Clone + Send + Sync + 'static,
    S::Response: Send + 'static,
    B::Codec: Codec<Duration, Compact = B::Compact>
        + Codec<Input, Compact = B::Compact>
        + Send
        + Clone
        + 'static,
    <B::Codec as Codec<Duration>>::Error: Into<BoxDynError>,
    Input: Send + Sync + 'static,
    <B::Codec as Codec<Input>>::Error: Into<BoxDynError>,
    B: Backend,
{
    type Response = Input;
    type Error = BoxDynError;
    fn register(&mut self, ctx: &mut WorkflowRouter<B>) -> Result<(), BoxDynError> {
        let svc = SteppedService::new(Self {
            f: self.f.clone(),
            inner: self.inner.clone(),
            _marker: std::marker::PhantomData,
        });
        let count = ctx.steps.len();
        ctx.steps.insert(count, svc);
        self.inner.register(ctx)
    }
}

impl<S, F, B: Backend + Send + Sync + 'static + Clone, Input, Err> Service<Task<B::Compact>>
    for DelayWithStep<S, F, B, Input>
where
    F: FnMut(Task<Input>) -> Duration + Send + 'static + Clone,
    S: Step<Input, B> + Send + 'static,
    S::Response: Send + 'static,
    B::Id: GenerateId + Sync + Send + 'static,
    B::Compact: Send + 'static,
    B: Sink<Task<B::Compact>, Error = Err>
        + WireFormatBackend
        + BackendConfig
        + Unpin
        + Send
        + Sync,
    Err: std::error::Error + Send + Sync + 'static,
    B::Codec: Codec<Duration, Compact = B::Compact>
        + Codec<Input, Compact = B::Compact>
        + Send
        + Clone
        + 'static,
    <B::Codec as Codec<Duration>>::Error: Into<BoxDynError>,
    <B::Codec as Codec<Input>>::Error: Into<BoxDynError>,
{
    type Response = GoTo<StepResponse>;
    type Error = BoxDynError;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(
        &mut self,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: Task<B::Compact>) -> Self::Future {
        let mut step_context: StepContext<B> = req.data().get().cloned().unwrap();
        let mut f = self.f.clone();
        let codec = step_context.backend.codec().clone();
        let task_id = B::Id::generate();
        async move {
            let decoded: Input = B::Codec::decode(&codec, &req.args)
                .map_err(|e: <B::Codec as Codec<Input>>::Error| e.into())?;
            let (args, ctx) = req.take();
            let delay_duration = f(Task::new_with_ctx(decoded, ctx));

            let task = TaskBuilder::new(args)
                .task_id(task_id.clone())
                .metadata(&WorkflowContext {
                    step_index: step_context.current_step + 1,
                })
                .run_after(delay_duration)
                .build();
            step_context
                .backend
                .send(task)
                .await
                .map_err(|e| BoxDynError::from(e))?;
            Ok(GoTo::DelayFor(
                delay_duration,
                StepResponse {
                    result: to_value(delay_duration)?,
                    next_task_id: Some(task_id),
                },
            ))
        }
        .boxed()
    }
}

impl<Start, Cur, B, L> SteppedFlow<Start, Cur, B, L> {
    /// Delay the workflow by a fixed duration
    pub fn delay_for(self, delay: Duration) -> SteppedFlow<Start, Cur, B, Stack<DelayFor, L>> {
        self.add_step(DelayFor { duration: delay })
    }
}
impl<Start, Cur, B, L> SteppedFlow<Start, Cur, B, L> {
    /// Delay the workflow by a duration determined by a function
    #[allow(clippy::type_complexity)]
    pub fn delay_with<F>(self, f: F) -> SteppedFlow<Start, Cur, B, Stack<DelayWith<F, B, Cur>, L>>
    where
        F: FnMut(Task<Cur>) -> Duration + Send + 'static,
    {
        self.add_step(DelayWith {
            f,
            _marker: std::marker::PhantomData,
        })
    }
}
