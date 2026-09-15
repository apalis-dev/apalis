use std::{fmt::Display, marker::PhantomData, str::FromStr};

use apalis_core::{
    backend::{
        Backend, BackendConfig, TaskSinkError, WaitForCompletion, WireFormatBackend, codec::Codec,
    },
    error::BoxDynError,
    task::task_fn::{TaskFn, task_fn},
    task::{
        Task,
        builder::TaskBuilder,
        metadata::{Metadata, MetadataError, MetadataStore},
        task_id::{GenerateId, TaskId},
    },
};
use futures_util::{FutureExt, Sink, SinkExt, StreamExt, future::BoxFuture};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::{from_value, to_value};
use tower::Service;

use crate::{
    SteppedService,
    sequential::{
        context::{StepContext, WorkflowContext},
        router::{GoTo, StepResponse, WorkflowRouter},
        service::handle_step_result,
        step::{Layer, Stack, Step},
        workflow::SteppedFlow,
    },
};

/// A layer that filters and maps task inputs to outputs.
#[derive(Debug)]
pub struct FilterMap<F, I> {
    filter_map: F,
    _marker: PhantomData<I>,
}

impl<F: Clone, I> Clone for FilterMap<F, I> {
    fn clone(&self) -> Self {
        Self {
            filter_map: self.filter_map.clone(),
            _marker: PhantomData,
        }
    }
}

impl<F, I> FilterMap<F, I> {
    /// Creates a new `FilterMap` layer with the given filter and map function.
    pub fn new(filter_map: F) -> Self {
        Self {
            filter_map,
            _marker: PhantomData,
        }
    }
}

/// The filter map step that applies filtering and mapping to task inputs.
#[derive(Debug)]
pub struct FilterMapStep<F, S, I> {
    filter_map: F,
    step: S,
    _marker: PhantomData<I>,
}

impl<F: Clone, S: Clone, I> Clone for FilterMapStep<F, S, I> {
    fn clone(&self) -> Self {
        Self {
            filter_map: self.filter_map.clone(),
            step: self.step.clone(),
            _marker: PhantomData,
        }
    }
}

impl<S, F, I> Layer<S> for FilterMap<F, I>
where
    F: Clone,
{
    type Step = FilterMapStep<F, S, I>;

    fn layer(&self, step: S) -> Self::Step {
        FilterMapStep {
            filter_map: self.filter_map.clone(),
            step,
            _marker: PhantomData,
        }
    }
}

/// The filter service that handles filtering and mapping of task inputs to outputs.
#[derive(Debug)]
pub struct FilterService<F, Backend, Input, Iter> {
    service: F,
    _marker: PhantomData<(Backend, Input, Iter)>,
}

impl<F: Clone, Backend, Input, Iter> Clone for FilterService<F, Backend, Input, Iter> {
    fn clone(&self) -> Self {
        Self {
            service: self.service.clone(),
            _marker: PhantomData,
        }
    }
}

/// The state of the filter operation
#[derive(Debug, Clone, Deserialize, Serialize)]
#[non_exhaustive]
pub enum FilterState {
    /// Initializing state
    Init,
    /// Collector state to process a single step
    SingleStep,
    /// Collector state to gather results
    Collector,
}

const FILTER_STATE_KEY: &str = "apalis_workflow.filter.state";

impl std::fmt::Display for FilterState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Init => write!(f, "Init"),
            Self::SingleStep => write!(f, "SingleStep"),
            Self::Collector => write!(f, "Collector"),
        }
    }
}

/// Represents an invalid FilterState
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum FilterStateParseError {
    /// Invalid filter state
    #[error("invalid filter state: {0}")]
    InvalidState(String),
}

impl std::str::FromStr for FilterState {
    type Err = FilterStateParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "Init" => Ok(Self::Init),
            "SingleStep" => Ok(Self::SingleStep),
            "Collector" => Ok(Self::Collector),
            _ => Err(FilterStateParseError::InvalidState(s.to_owned())),
        }
    }
}

/// Represents an invalid [`FilterState`]
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum FilterStateError {
    /// The filter state is missing
    #[error("the data for key {FILTER_STATE_KEY} is missing")]
    MissingKey,

    /// Could not parse the filter state
    #[error(transparent)]
    Parse(#[from] FilterStateParseError),

    /// Duplicate entry
    #[error("Duplicate entry: {0}")]
    DuplicateEntry(#[from] MetadataError),
}

impl Metadata for FilterState {
    type Error = FilterStateError;

    fn extract(map: &MetadataStore) -> Result<Self, Self::Error> {
        let value = map
            .get(FILTER_STATE_KEY)
            .ok_or(FilterStateError::MissingKey)?;

        Ok(value.parse::<Self>()?)
    }

    fn inject(&self, map: &mut MetadataStore) -> Result<(), FilterStateError> {
        map.insert(FILTER_STATE_KEY, self.to_string())?;
        Ok(())
    }
}

/// The context for the filter operation
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct FilterContext {
    task_ids: Vec<TaskId>,
}

const FILTER_CONTEXT_TASK_IDS_KEY: &str = "apalis_workflow.filter.task_ids";

/// Error representing an invalid [`FilterContext`] state
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum FilterContextError {
    /// The entry for key {FILTER_CONTEXT_TASK_IDS_KEY} is missing"
    #[error("the entry for key {FILTER_CONTEXT_TASK_IDS_KEY} is missing")]
    MissingKey,

    /// Could not parse the provided task_id
    #[error("could not parse task id")]
    ParseTaskId,

    /// Duplicate entry
    #[error("Duplicate entry: {0}")]
    DuplicateEntry(#[from] MetadataError),
}

impl Metadata for FilterContext {
    type Error = FilterContextError;

    fn extract(map: &MetadataStore) -> Result<Self, Self::Error> {
        let value = map
            .get(FILTER_CONTEXT_TASK_IDS_KEY)
            .ok_or(FilterContextError::MissingKey)?;

        let task_ids = if value.is_empty() {
            Vec::new()
        } else {
            value
                .split(',')
                .map(|id| {
                    id.parse::<TaskId>()
                        .map_err(|_| FilterContextError::ParseTaskId)
                })
                .collect::<Result<Vec<_>, _>>()?
        };

        Ok(Self { task_ids })
    }

    fn inject(&self, map: &mut MetadataStore) -> Result<(), FilterContextError> {
        let value = self
            .task_ids
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join(",");

        map.insert(FILTER_CONTEXT_TASK_IDS_KEY, value)?;

        Ok(())
    }
}

impl<F, B, Input, CodecError, Err, Output, Iter, Compact> Service<Task<Compact>>
    for FilterService<F, B, Input, Iter>
where
    F: Service<Task<Input>, Response = Option<Output>>,
    B: Backend<Error = Err>
        + WireFormatBackend<Compact = Compact>
        + BackendConfig
        + Send
        + Sync
        + 'static
        + Clone
        + Sink<Task<B::Compact>, Error = Err>
        + WaitForCompletion<GoTo<StepResponse>>
        + Unpin,
    B::Codec: Codec<Vec<Input>, Error = CodecError, Compact = B::Compact>
        + Codec<Iter, Error = CodecError, Compact = B::Compact>
        + Codec<F::Response, Error = CodecError, Compact = B::Compact>
        + Codec<Input, Error = CodecError, Compact = B::Compact>
        + Codec<Vec<Output>, Error = CodecError, Compact = B::Compact>
        + Send
        + Clone
        + 'static,
    B::Id: GenerateId + Send + 'static,
    Err: std::error::Error + Send + Sync + 'static,
    CodecError: std::error::Error + Send + Sync + 'static,
    F::Error: Into<BoxDynError> + Send + 'static,
    F::Future: Send + 'static,
    B::Compact: Send + 'static,
    Input: Send + 'static,
    Output: Send + 'static,
    Iter: IntoIterator<Item = Input> + Send + 'static,
    Output: Serialize + DeserializeOwned,
{
    type Response = GoTo<StepResponse>;
    type Error = BoxDynError;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.service.poll_ready(cx).map_err(|e| e.into())
    }

    fn call(&mut self, request: Task<B::Compact>) -> Self::Future {
        let filter_state: FilterState =
            Metadata::extract(request.metadata()).unwrap_or(FilterState::Init);
        let mut ctx = request.data().get::<StepContext<B>>().cloned().unwrap();
        let codec = ctx.backend.codec().clone();
        match filter_state {
            FilterState::Init => {
                // Handle unknown state
                async move {
                    let main_args: Vec<Input> = vec![];
                    let steps: Task<Iter> =
                        request.try_map_args(|arg| B::Codec::decode(&codec, &arg))?;
                    let steps = steps.args.into_iter().collect::<Vec<_>>();
                    #[cfg(feature = "tracing")]
                    tracing::debug!(step_count = ?steps.len(), "Enqueuing FilterMap steps");
                    let mut task_ids = Vec::new();
                    for step in steps {
                        let task_id = B::Id::generate();

                        let task = TaskBuilder::new(B::Codec::encode(&codec, &step)?)
                            .metadata(&WorkflowContext {
                                step_index: ctx.current_step,
                            })
                            .task_id(task_id.clone())
                            .metadata(&FilterState::SingleStep)
                            .build();
                        ctx.backend.send(task).await?;

                        task_ids.push(task_id);
                    }
                    let task_id = B::Id::generate();
                    let task = TaskBuilder::new(B::Codec::encode(&codec, &main_args)?)
                        .task_id(task_id.clone())
                        .metadata(&WorkflowContext {
                            step_index: ctx.current_step,
                        })
                        .metadata(&FilterContext { task_ids })
                        .metadata(&FilterState::Collector)
                        .build();

                    ctx.backend.send(task).await?;

                    Ok(GoTo::Done)
                }
                .boxed()
            }
            FilterState::SingleStep => {
                let step: Task<Input> = request
                    .try_map_args(|arg| B::Codec::decode(&codec, &arg))
                    .unwrap();
                let fut = self.service.call(step);
                async move {
                    let res = fut.await.map_err(|e| e.into())?;
                    Ok(GoTo::Break(StepResponse {
                        result: to_value(&res)
                            .map_err(|e| TaskSinkError::CodecError::<Err>(e.into()))?,
                        next_task_id: None,
                    }))
                }
                .boxed()
            }
            FilterState::Collector => {
                // Handle collector state
                async move {
                    let filter_ctx: FilterContext = Metadata::extract(request.metadata())?;
                    let res: Vec<Output> = ctx
                        .backend
                        .wait_for(filter_ctx.task_ids)
                        .collect::<Vec<_>>()
                        .await
                        .into_iter()
                        .collect::<Result<Vec<_>, _>>()?
                        .into_iter()
                        .filter_map(|res| {
                            let res = res.take().ok();
                            match res {
                                Some(GoTo::Break(val)) => {
                                    let opt: Result<Option<Output>, _> = from_value(val.result);
                                    opt.ok().flatten()
                                }
                                _ => None,
                            }
                        })
                        .collect();
                    if res.is_empty() {
                        return Ok(GoTo::Break(StepResponse {
                            result: to_value(&res)
                                .map_err(|e| TaskSinkError::CodecError::<Err>(e.into()))?,
                            next_task_id: None,
                        }));
                    }

                    let next = handle_step_result(&mut ctx, GoTo::Next(res)).await?;
                    Ok(next)
                }
                .boxed()
            }
        }
    }
}

impl<F, Input, S, B, CodecError, SinkError, I, Output, Compact> Step<I, B>
    for FilterMapStep<F, S, I>
where
    I: IntoIterator<Item = Input> + Send + Sync + 'static,
    B: Backend<Error = SinkError>
        + WireFormatBackend<Compact = Compact>
        + BackendConfig
        + Send
        + Sync
        + 'static
        + Sink<Task<Compact>, Error = SinkError>
        + WaitForCompletion<GoTo<StepResponse>>
        + Unpin
        + Clone,
    F: Service<Task<Input>, Error = BoxDynError, Response = Option<Output>>
        + Send
        + Sync
        + 'static
        + Clone,
    S: Step<Vec<Output>, B>,
    Input: Send + Sync + 'static,
    F::Future: Send + 'static,
    F::Error: Into<BoxDynError> + Send + 'static,
    B::Codec: Codec<F::Response, Error = CodecError, Compact = B::Compact>
        + Codec<Input, Error = CodecError, Compact = B::Compact>
        + Send
        + Clone
        + 'static,
    CodecError: std::error::Error + Send + Sync + 'static,
    B::Id: GenerateId + Send + 'static + Clone,
    S::Response: Send + 'static,
    B::Compact: Send + 'static,
    SinkError: std::error::Error + Send + Sync + 'static,
    F::Response: Send + Serialize + 'static,
    B::Codec: Codec<Vec<Input>, Error = CodecError, Compact = B::Compact>
        + Codec<I, Error = CodecError, Compact = B::Compact>
        + Codec<F::Response, Error = CodecError, Compact = B::Compact>
        + Codec<Input, Error = CodecError, Compact = B::Compact>
        + Codec<Vec<Output>, Error = CodecError, Compact = B::Compact>
        + Sync
        + 'static,
    B::Id: GenerateId + Send + Sync + 'static,
    CodecError: std::error::Error + Send + Sync + 'static,
    F::Future: Send + 'static,
    B::Compact: Send + 'static,
    Output: Send + Serialize + 'static,
    B::Id: FromStr + Display,
    Output: Serialize + DeserializeOwned + 'static,
{
    type Response = Vec<F::Response>;
    type Error = F::Error;
    fn register(&mut self, ctx: &mut WorkflowRouter<B>) -> Result<(), BoxDynError> {
        let svc = SteppedService::new(FilterService {
            service: self.filter_map.clone(),
            _marker: PhantomData::<(B, Input, I)>,
        });
        let count = ctx.steps.len();
        ctx.steps.insert(count, svc);
        self.step.register(ctx)
    }
}

impl<Start, C, L, I: IntoIterator<Item = C>, B: Backend> SteppedFlow<Start, I, B, L> {
    /// Adds a filter and map step to the workflow.
    #[allow(clippy::type_complexity)]
    pub fn filter_map<F, Output, FnArgs>(
        self,
        filter_map: F,
    ) -> SteppedFlow<Start, Vec<Output>, B, Stack<FilterMap<TaskFn<F, C, FnArgs>, I>, L>>
    where
        TaskFn<F, C, FnArgs>: Service<Task<C>, Response = Option<Output>>,
    {
        self.add_step(FilterMap {
            filter_map: task_fn(filter_map),
            _marker: PhantomData,
        })
    }
}
