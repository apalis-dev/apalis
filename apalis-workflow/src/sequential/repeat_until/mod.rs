use std::fmt::Display;
use std::marker::PhantomData;
use std::num::ParseIntError;
use std::str::FromStr;
use std::task::Context;

use apalis_core::backend::TaskSinkError;
use apalis_core::backend::codec::Codec;
use apalis_core::error::BoxDynError;
use apalis_core::task::builder::TaskBuilder;
use apalis_core::task::metadata::{Metadata, MetadataError, MetadataStore};
use apalis_core::task::task_id::TaskId;
use apalis_core::task_fn::{TaskFn, task_fn};
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
        Stack<RepeatUntil<TaskFn<F, Input, B::Connection, FnArgs>, Input, Output>, L>,
    >
    where
        TaskFn<F, Input, B::Connection, FnArgs>:
            Service<Task<Input, B::Connection, B::IdType>, Response = Option<Output>>,
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

impl<F, Res, B, Input, CodecError, Err> Service<Task<B::Compact, B::Connection, B::IdType>>
    for RepeatUntilService<F, B, Input, Res>
where
    F: Service<Task<Input, B::Connection, B::IdType>, Response = Option<Res>>
        + Send
        + 'static
        + Clone,
    B: BackendExt<Error = Err>
        + Send
        + Sync
        + Clone
        + Sink<Task<B::Compact, B::Connection, B::IdType>, Error = Err>
        + Unpin
        + 'static,
    B::Connection: Send + Sync + 'static,
    B::Codec: Codec<Input, Error = CodecError, Compact = B::Compact>
        + Codec<Res, Error = CodecError, Compact = B::Compact>
        + Codec<Option<Res>, Error = CodecError, Compact = B::Compact>
        + 'static,
    B::IdType: GenerateId + Send + Sync + Display + FromStr + 'static,
    Err: std::error::Error + Send + Sync + 'static,
    CodecError: std::error::Error + Send + Sync + 'static,
    F::Error: Into<BoxDynError> + Send + 'static,
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

    fn call(&mut self, task: Task<B::Compact, B::Connection, B::IdType>) -> Self::Future {
        let state: RepeaterState<B::IdType> =
            Metadata::extract(&task.ctx.metadata).unwrap_or_default();
        let mut ctx =
            task.ctx.data.get::<StepContext<B>>().cloned().expect(
                "StepContext missing, Did you call the repeater outside of a workflow step?",
            );
        let mut repeater = self.repeater.clone();

        (async move {
            let mut compact = None;
            let decoded: Input = B::Codec::decode(&task.args)?;
            let prev_task_id = task.ctx.task_id.clone();
            let repeat_task = task
                .into_builder()
                .map(|c| {
                    compact = Some(c);
                    decoded
                })
                .build();
            let response = repeater.call(repeat_task).await.map_err(|e| e.into())?;
            Ok(match response {
                Some(res) if ctx.has_next => {
                    let task_id = TaskId::new(B::IdType::generate());
                    let next_step = TaskBuilder::new(B::Codec::encode(&res)?)
                        .task_id(task_id.clone())
                        .metadata(&WorkflowContext {
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
                            .task_id(task_id.clone())
                            .metadata(&WorkflowContext {
                                step_index: ctx.current_step,
                            })
                            .metadata(&RepeaterState {
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

/// The state of the repeat operation
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

/// An error representing an invalid [`RepeaterState`]
#[derive(Debug, thiserror::Error)]
pub enum RepeaterStateError {
    /// Missing iterations key
    #[error("the data for key {REPEATER_ITERATIONS_KEY} is missing")]
    MissingIterations,

    /// Could not parse iterations
    #[error("could not parse key {REPEATER_ITERATIONS_KEY}")]
    ParseIterations(#[from] ParseIntError),

    /// Could not parse a task id
    #[error("could not parse key {REPEATER_PREV_TASK_ID_KEY}")]
    ParseTaskId,

    /// Duplicate entry
    #[error("Duplicate entry: {0}")]
    DuplicateEntry(#[from] MetadataError),
}

const REPEATER_ITERATIONS_KEY: &str = "apalis_workflow.repeater.iterations";
const REPEATER_PREV_TASK_ID_KEY: &str = "apalis_workflow.repeater.prev_task_id";

impl<IdType: Display> Metadata for RepeaterState<IdType>
where
    IdType: std::str::FromStr + ToString,
{
    type Error = RepeaterStateError;

    fn extract(map: &MetadataStore) -> Result<Self, Self::Error> {
        let iterations = map
            .get(REPEATER_ITERATIONS_KEY)
            .ok_or(RepeaterStateError::MissingIterations)?
            .parse::<usize>()?;

        let prev_task_id = map
            .get(REPEATER_PREV_TASK_ID_KEY)
            .map(|value| {
                value
                    .parse::<IdType>()
                    .map(TaskId::new)
                    .map_err(|_| RepeaterStateError::ParseTaskId)
            })
            .transpose()?;

        Ok(Self {
            iterations,
            prev_task_id,
        })
    }

    fn inject(&self, map: &mut MetadataStore) -> Result<(), RepeaterStateError> {
        map.insert(REPEATER_ITERATIONS_KEY, self.iterations.to_string())?;

        if let Some(task_id) = &self.prev_task_id {
            map.insert(REPEATER_PREV_TASK_ID_KEY, task_id.to_string())?;
        }
        Ok(())
    }
}

impl<B, F, Input, Res, S, Err, CodecError> Step<Input, B> for RepeatUntilStep<S, F, Input, Res>
where
    F: Service<Task<Input, B::Connection, B::IdType>, Response = Option<Res>>
        + Send
        + Sync
        + 'static
        + Clone,
    B: BackendExt<Error = Err>
        + Send
        + Sync
        + Clone
        + Sink<Task<B::Compact, B::Connection, B::IdType>, Error = Err>
        + Unpin
        + 'static,
    B::Connection: Send + Sync + 'static,
    B::Codec: Codec<Input, Error = CodecError, Compact = B::Compact>
        + Codec<Res, Error = CodecError, Compact = B::Compact>
        + Codec<Option<Res>, Error = CodecError, Compact = B::Compact>
        + 'static,
    B::IdType: GenerateId + Send + 'static,
    Err: std::error::Error + Send + Sync + 'static,
    CodecError: std::error::Error + Send + Sync + 'static,
    F::Error: Into<BoxDynError> + Send + 'static,
    F::Future: Send + 'static,
    B::Compact: Send + 'static,
    Input: Send + Sync + 'static, // We don't need Clone because decoding just needs a reference
    Res: Send + Sync + 'static,
    S: Step<Input, B> + Send + 'static,
    B::IdType: FromStr + Display + Sync,
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
