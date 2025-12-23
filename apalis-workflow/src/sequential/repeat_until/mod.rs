use std::convert::Infallible;
use std::marker::PhantomData;
use std::task::Context;

use apalis_core::backend::TaskSinkError;
use apalis_core::backend::codec::Codec;
use apalis_core::error::BoxDynError;
use apalis_core::task::builder::TaskBuilder;
use apalis_core::task::metadata::MetadataExt;
use apalis_core::task::task_id::TaskId;
use apalis_core::task_fn::{FromRequest, TaskFn, task_fn};
use apalis_core::{backend::BackendExt, task::Task};
use futures::future::BoxFuture;
use futures::{FutureExt, Sink, SinkExt};
use serde::{Deserialize, Serialize};
use tower::Service;

use crate::id_generator::GenerateId;
use crate::sequential::router::WorkflowRouter;
use crate::sequential::{GoTo, Layer, Stack, Step, StepContext, StepResult, WorkflowContext};
use crate::{SteppedService, Workflow};

/// A layer that represents a `repeat_until` step in the workflow.
#[derive(Clone, Debug)]
pub struct RepeatUntil<F, Input, Output> {
    repeater: F,
    _marker: PhantomData<(Input, Output)>,
}

impl<F, Input, Output, S> Layer<S> for RepeatUntil<F, Input, Output>
where
    F: Clone,
{
    type Step = RepeatUntilStep<S, F, Input, Output>;

    fn layer(&self, step: S) -> Self::Step {
        RepeatUntilStep {
            inner: step,
            repeater: self.repeater.clone(),
            _marker: std::marker::PhantomData,
        }
    }
}
impl<Start, L, Input, B: BackendExt> Workflow<Start, Input, B, L> {
    /// Folds over a collection of items in the workflow.
    pub fn repeat_until<F, Output, FnArgs>(
        self,
        repeater: F,
    ) -> Workflow<
        Start,
        Output,
        B,
        Stack<RepeatUntil<TaskFn<F, Input, B::Context, FnArgs>, Input, Output>, L>,
    >
    where
        TaskFn<F, Input, B::Context, FnArgs>:
            Service<Task<Input, B::Context, B::IdType>, Response = Option<Output>>,
    {
        self.add_step(RepeatUntil {
            repeater: task_fn(repeater),
            _marker: PhantomData::<(Input, Output)>,
        })
    }
}

/// The step implementation for the `repeat_until` layer.
#[derive(Clone, Debug)]
pub struct RepeatUntilStep<S, R, Input, Output> {
    inner: S,
    repeater: R,
    _marker: PhantomData<(Input, Output)>,
}

/// The service that handles the `repeat_until` logic
#[derive(Debug)]
pub struct RepeatUntilService<F, B, Input, Output> {
    repeater: F,
    _marker: std::marker::PhantomData<(B, Input, Output)>,
}

impl<F, B, Input, Output> Clone for RepeatUntilService<F, B, Input, Output>
where
    F: Clone,
{
    fn clone(&self) -> Self {
        Self {
            repeater: self.repeater.clone(),
            _marker: std::marker::PhantomData,
        }
    }
}

impl<F, Res, B, Input, CodecError, MetaErr, Err> Service<Task<B::Compact, B::Context, B::IdType>>
    for RepeatUntilService<F, B, Input, Res>
where
    F: Service<Task<Input, B::Context, B::IdType>, Response = Option<Res>> + Send + 'static + Clone,
    B: BackendExt<Error = Err>
        + Send
        + Sync
        + Clone
        + Sink<Task<B::Compact, B::Context, B::IdType>, Error = Err>
        + Unpin
        + 'static,
    B::Context: MetadataExt<RepeaterState<B::IdType>, Error = MetaErr>
        + MetadataExt<WorkflowContext, Error = MetaErr>
        + Send
        + 'static,
    B::Codec: Codec<Input, Error = CodecError, Compact = B::Compact>
        + Codec<Res, Error = CodecError, Compact = B::Compact>
        + Codec<Option<Res>, Error = CodecError, Compact = B::Compact>
        + 'static,
    B::IdType: GenerateId + Send + 'static,
    Err: std::error::Error + Send + Sync + 'static,
    CodecError: std::error::Error + Send + Sync + 'static,
    F::Error: Into<BoxDynError> + Send + 'static,
    MetaErr: std::error::Error + Send + Sync + 'static,
    F::Future: Send + 'static,
    B::Compact: Send + 'static,
    Input: Send + 'static, // We don't need Clone because decoding just needs a reference
    Res: Send + 'static,
{
    type Response = GoTo<StepResult<B::Compact, B::IdType>>;
    type Error = BoxDynError;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> std::task::Poll<Result<(), Self::Error>> {
        self.repeater.poll_ready(cx).map_err(|e| e.into())
    }

    fn call(&mut self, task: Task<B::Compact, B::Context, B::IdType>) -> Self::Future {
        let state: RepeaterState<B::IdType> = task.parts.ctx.extract().unwrap_or_default();
        let mut ctx =
            task.parts.data.get::<StepContext<B>>().cloned().expect(
                "StepContext missing, Did you call the repeater outside of a workflow step?",
            );
        let mut repeater = self.repeater.clone();

        (async move {
            let mut compact = None;
            let decoded: Input = B::Codec::decode(&task.args)?;
            let prev_task_id = task.parts.task_id.clone();
            let repeat_task = task.map(|c| {
                compact = Some(c);
                decoded
            });
            let response = repeater.call(repeat_task).await.map_err(|e| e.into())?;
            Ok(match response {
                Some(res) if ctx.has_next => {
                    let task_id = TaskId::new(B::IdType::generate());
                    let next_step = TaskBuilder::new(B::Codec::encode(&res)?)
                        .with_task_id(task_id.clone())
                        .meta(WorkflowContext {
                            step_index: ctx.current_step + 1,
                        })
                        .build();
                    ctx.backend
                        .send(next_step)
                        .await
                        .map_err(|e| TaskSinkError::PushError(e))?;
                    GoTo::Next(StepResult {
                        result: B::Codec::encode(&res)?,
                        next_task_id: Some(task_id),
                    })
                }
                Some(res) => GoTo::Break(StepResult {
                    result: B::Codec::encode(&res)?,
                    next_task_id: None,
                }),
                None => {
                    let task_id = TaskId::new(B::IdType::generate());
                    let next_step =
                        TaskBuilder::new(compact.take().expect("Compact args should be set"))
                            .with_task_id(task_id.clone())
                            .meta(WorkflowContext {
                                step_index: ctx.current_step,
                            })
                            .meta(RepeaterState {
                                iterations: state.iterations + 1,
                                prev_task_id,
                            })
                            .build();
                    ctx.backend
                        .send(next_step)
                        .await
                        .map_err(|e| TaskSinkError::PushError(e))?;
                    GoTo::Break(StepResult {
                        result: B::Codec::encode(&None::<Res>)?,
                        next_task_id: Some(task_id),
                    })
                }
            })
        }
        .boxed()) as _
    }
}

/// The state of the fold operation
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RepeaterState<IdType> {
    iterations: usize,
    prev_task_id: Option<TaskId<IdType>>,
}

impl<IdType> Default for RepeaterState<IdType> {
    fn default() -> Self {
        Self {
            iterations: 0,
            prev_task_id: None,
        }
    }
}

impl<IdType> RepeaterState<IdType> {
    /// Get the number of iterations completed so far.
    pub fn iterations(&self) -> usize {
        self.iterations
    }

    /// Get the previous task id.
    pub fn previous_task_id(&self) -> Option<&TaskId<IdType>> {
        self.prev_task_id.as_ref()
    }
}

impl<Args: Sync, Ctx: MetadataExt<Self> + Sync, IdType: Sync> FromRequest<Task<Args, Ctx, IdType>>
    for RepeaterState<IdType>
{
    type Error = Infallible;
    async fn from_request(task: &Task<Args, Ctx, IdType>) -> Result<Self, Infallible> {
        let state: Self = task.parts.ctx.extract().unwrap_or_default();
        Ok(Self {
            iterations: state.iterations,
            prev_task_id: state.prev_task_id,
        })
    }
}

impl<B, F, Input, Res, S, MetaErr, Err, CodecError> Step<Input, B>
    for RepeatUntilStep<S, F, Input, Res>
where
    F: Service<Task<Input, B::Context, B::IdType>, Response = Option<Res>>
        + Send
        + Sync
        + 'static
        + Clone,
    B: BackendExt<Error = Err>
        + Send
        + Sync
        + Clone
        + Sink<Task<B::Compact, B::Context, B::IdType>, Error = Err>
        + Unpin
        + 'static,
    B::Context: MetadataExt<RepeaterState<B::IdType>, Error = MetaErr>
        + MetadataExt<WorkflowContext, Error = MetaErr>
        + Send
        + 'static,
    B::Codec: Codec<Input, Error = CodecError, Compact = B::Compact>
        + Codec<Res, Error = CodecError, Compact = B::Compact>
        + Codec<Option<Res>, Error = CodecError, Compact = B::Compact>
        + 'static,
    B::IdType: GenerateId + Send + 'static,
    Err: std::error::Error + Send + Sync + 'static,
    CodecError: std::error::Error + Send + Sync + 'static,
    F::Error: Into<BoxDynError> + Send + 'static,
    MetaErr: std::error::Error + Send + Sync + 'static,
    F::Future: Send + 'static,
    B::Compact: Send + 'static,
    Input: Send + Sync + 'static, // We don't need Clone because decoding just needs a reference
    Res: Send + Sync + 'static,
    S: Step<Input, B> + Send + 'static,
{
    type Response = Res;
    type Error = F::Error;
    fn register(&mut self, ctx: &mut WorkflowRouter<B>) -> Result<(), BoxDynError> {
        let svc = SteppedService::new(RepeatUntilService {
            repeater: self.repeater.clone(),
            _marker: PhantomData::<(B, Input, Res)>,
        });
        let count = ctx.steps.len();
        ctx.steps.insert(count, svc);
        self.inner.register(ctx)
    }
}
