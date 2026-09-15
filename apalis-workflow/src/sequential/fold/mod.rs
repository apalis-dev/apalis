use std::{marker::PhantomData, task::Context};

use apalis_core::{
    backend::{Backend, BackendConfig, TaskSinkError, WireFormatBackend, codec::Codec},
    error::BoxDynError,
    task::task_fn::{TaskFn, task_fn},
    task::{
        Task,
        builder::TaskBuilder,
        metadata::{Metadata, MetadataError, MetadataStore},
        task_id::GenerateId,
    },
};
use futures_util::{FutureExt, Sink, SinkExt, future::BoxFuture};
use serde::{Deserialize, Serialize};
use serde_json::to_value;
use tower::Service;

use crate::{
    SteppedService,
    sequential::{
        context::{StepContext, WorkflowContext},
        router::{GoTo, StepResponse, WorkflowRouter},
        step::{Layer, Stack, Step},
        workflow::SteppedFlow,
    },
};

/// The fold layer that folds over a collection of items.
#[derive(Clone, Debug)]
pub struct Fold<F, Init> {
    fold: F,
    _marker: std::marker::PhantomData<Init>,
}

impl<F, Init, S> Layer<S> for Fold<F, Init>
where
    F: Clone,
    Init: Clone,
{
    type Step = FoldStep<S, F, Init>;

    fn layer(&self, step: S) -> Self::Step {
        FoldStep {
            inner: step,
            fold: self.fold.clone(),
            _marker: std::marker::PhantomData,
        }
    }
}
impl<Start, C, L, I: IntoIterator<Item = C>, B: Backend> SteppedFlow<Start, I, B, L> {
    /// Folds over a collection of items in the workflow.
    #[allow(clippy::type_complexity)]
    pub fn fold<F, Output, FnArgs, Init>(
        self,
        fold: F,
    ) -> SteppedFlow<Start, Output, B, Stack<Fold<TaskFn<F, (Init, C), FnArgs>, Init>, L>>
    where
        TaskFn<F, (Init, C), FnArgs>: Service<Task<(Init, C)>, Response = Output>,
    {
        self.add_step(Fold {
            fold: task_fn(fold),
            _marker: PhantomData,
        })
    }
}

/// The fold step that folds over a collection of items.
#[derive(Clone, Debug)]
pub struct FoldStep<S, F, Init> {
    inner: S,
    fold: F,
    _marker: std::marker::PhantomData<Init>,
}

impl<S, F, Input, I: IntoIterator<Item = Input>, Init, B, Err, CodecError> Step<I, B>
    for FoldStep<S, F, Init>
where
    F: Service<Task<(Init, Input)>, Response = Init> + Send + Sync + 'static + Clone,
    S: Step<Init, B>,
    B: Backend<Error = Err>
        + WireFormatBackend
        + BackendConfig
        + Send
        + Sync
        + Clone
        + Sink<Task<B::Compact>, Error = Err>
        + Unpin
        + 'static,
    I: IntoIterator<Item = Input> + Send + Sync + 'static,
    B::Codec: Codec<(Init, Vec<Input>), Error = CodecError, Compact = B::Compact>
        + Codec<Init, Error = CodecError, Compact = B::Compact>
        + Codec<I, Error = CodecError, Compact = B::Compact>
        + Codec<(Init, Input), Error = CodecError, Compact = B::Compact>
        + Send
        + Sync
        + Clone
        + 'static,
    B::Id: GenerateId + Sync + Send + 'static + Clone,
    Init: Default + Serialize + Send + Sync + 'static,
    Err: std::error::Error + Send + Sync + 'static,
    CodecError: std::error::Error + Send + Sync + 'static,
    F::Error: Into<BoxDynError> + Send + 'static,
    F::Future: Send + 'static,
    B::Compact: Send + 'static,
    Input: Send + 'static,
{
    type Response = Init;
    type Error = F::Error;
    fn register(&mut self, ctx: &mut WorkflowRouter<B>) -> Result<(), BoxDynError> {
        let svc = SteppedService::new(FoldService {
            fold: self.fold.clone(),
            _marker: PhantomData::<(Init, I, B)>,
        });
        let count = ctx.steps.len();
        ctx.steps.insert(count, svc);
        self.inner.register(ctx)
    }
}

/// The fold service that handles folding over a collection of items.
#[derive(Debug)]
pub struct FoldService<F, Init, I, B> {
    fold: F,
    _marker: std::marker::PhantomData<(Init, I, B)>,
}

impl<F: Clone, Init, I, B> Clone for FoldService<F, Init, I, B> {
    fn clone(&self) -> Self {
        Self {
            fold: self.fold.clone(),
            _marker: std::marker::PhantomData,
        }
    }
}

impl<F, Init, I, B> FoldService<F, Init, I, B> {
    /// Creates a new `FoldService` with the given fold function.
    pub fn new(fold: F) -> Self {
        Self {
            fold,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<F, Init, I, B, Input, CodecError, Err> Service<Task<B::Compact>> for FoldService<F, Init, I, B>
where
    F: Service<Task<(Init, Input)>, Response = Init> + Send + 'static + Clone,
    B: Backend<Error = Err>
        + WireFormatBackend
        + BackendConfig
        + Clone
        + Sink<Task<B::Compact>, Error = Err>
        + Send
        + Sync
        + Unpin
        + 'static,
    I: IntoIterator<Item = Input> + Send + 'static,
    B::Codec: Codec<(Init, Vec<Input>), Error = CodecError, Compact = B::Compact>
        + Codec<Init, Error = CodecError, Compact = B::Compact>
        + Codec<I, Error = CodecError, Compact = B::Compact>
        + Codec<(Init, Input), Error = CodecError, Compact = B::Compact>
        + Send
        + Sync
        + Clone
        + 'static,
    B::Id: GenerateId + Sync + Send + 'static,
    Init: Default + Serialize + Send + 'static,
    Err: std::error::Error + Send + Sync + 'static,
    CodecError: std::error::Error + Send + Sync + 'static,
    F::Error: Into<BoxDynError> + Send + 'static,
    F::Future: Send + 'static,
    B::Compact: Send + 'static,
    Input: Send + 'static,
{
    type Response = GoTo<StepResponse>;
    type Error = BoxDynError;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> std::task::Poll<Result<(), Self::Error>> {
        self.fold.poll_ready(cx).map_err(|e| e.into())
    }

    fn call(&mut self, task: Task<B::Compact>) -> Self::Future {
        let state = FoldState::extract(task.metadata()).unwrap_or(FoldState::Init);
        let mut ctx = task.data().get::<StepContext<B>>().cloned().unwrap();
        let codec = ctx.backend.codec().clone();
        let mut fold = self.fold.clone();

        match state {
            FoldState::Init => async move {
                let task_id = B::Id::generate();
                let steps: Task<I> = task.try_map_args(|arg| B::Codec::decode(&codec, &arg))?;
                let steps = steps.args.into_iter().collect::<Vec<_>>();
                let task = TaskBuilder::new(B::Codec::encode(&codec, &(Init::default(), steps))?)
                    .metadata(&WorkflowContext {
                        step_index: ctx.current_step,
                    })
                    .task_id(task_id.clone())
                    .metadata(&FoldState::Collection)
                    .build();
                ctx.backend
                    .send(task)
                    .await
                    .map_err(TaskSinkError::PushError)?;
                Ok(GoTo::Next(StepResponse {
                    result: to_value(Init::default())?,
                    next_task_id: Some(task_id),
                }))
            }
            .boxed(),
            FoldState::Collection => async move {
                let args: (Init, Vec<Input>) = B::Codec::decode(&codec, &task.args)?;
                let (acc, items) = args;

                let mut items = items.into_iter();
                let next = items.next().unwrap();
                let rest = items.collect::<Vec<_>>();
                let fold_task = task.map_args(|_| (acc, next));
                let response = fold.call(fold_task).await.map_err(|e| e.into())?;

                match rest.len() {
                    0 if ctx.has_next => {
                        let task_id = B::Id::generate();
                        let result = B::Codec::encode(&codec, &response)?;
                        let next_step = TaskBuilder::new(result)
                            .task_id(task_id.clone())
                            .metadata(&WorkflowContext {
                                step_index: ctx.current_step + 1,
                            })
                            .build();
                        ctx.backend
                            .send(next_step)
                            .await
                            .map_err(TaskSinkError::PushError)?;
                        Ok(GoTo::Break(StepResponse {
                            result: to_value(&response)?,
                            next_task_id: Some(task_id),
                        }))
                    }
                    0 => Ok(GoTo::Break(StepResponse {
                        result: to_value(&response)?,
                        next_task_id: None,
                    })),
                    1.. => {
                        // Shouldn't this be limited?
                        let task_id = B::Id::generate();
                        let result = to_value(&response)?;
                        let steps = TaskBuilder::new(B::Codec::encode(&codec, &(response, rest))?)
                            .task_id(task_id.clone())
                            .metadata(&WorkflowContext {
                                step_index: ctx.current_step,
                            })
                            .metadata(&FoldState::Collection)
                            .build();
                        ctx.backend
                            .send(steps)
                            .await
                            .map_err(TaskSinkError::PushError)?;
                        Ok(GoTo::Next(StepResponse {
                            result,
                            next_task_id: Some(task_id),
                        }))
                    }
                }
            }
            .boxed(),
        }
    }
}

/// The state of the fold operation
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[non_exhaustive]
pub enum FoldState {
    /// Initializing state
    Init,
    /// Collection has started
    Collection,
}

const FOLD_STATE_KEY: &str = "apalis_workflow.fold.state";

/// An error representing an invalid [`FoldState`]
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum FoldStateError {
    /// The fold state key is missing
    #[error("the data for key {FOLD_STATE_KEY} is missing")]
    MissingKey,

    /// Duplicate entry
    #[error("Duplicate entry: {0}")]
    DuplicateEntry(#[from] MetadataError),
}

impl Metadata for FoldState {
    type Error = FoldStateError;

    fn extract(map: &MetadataStore) -> Result<Self, Self::Error> {
        let value = map.get(FOLD_STATE_KEY).ok_or(FoldStateError::MissingKey)?;

        match value.as_str() {
            "Collection" => Ok(Self::Collection),
            _ => Ok(Self::Init),
        }
    }

    fn inject(&self, map: &mut MetadataStore) -> Result<(), FoldStateError> {
        let value = match self {
            Self::Init => "Init",
            Self::Collection => "Collection",
        };
        map.insert(FOLD_STATE_KEY, value)?;
        Ok(())
    }
}
