use apalis_core::{
    backend::{Backend, BackendConfig, TaskSinkError, WireFormatBackend, codec::Codec},
    error::BoxDynError,
    task::{Task, builder::TaskBuilder, metadata::Metadata, task_id::GenerateId},
};
use futures_util::{FutureExt, Sink, SinkExt, future::BoxFuture};
use serde::Serialize;
use serde_json::to_value;
use std::{
    collections::{HashMap, VecDeque},
    marker::PhantomData,
    task::{Context, Poll},
};
use tower::Service;

use crate::{
    SteppedService,
    sequential::{
        context::{StepContext, WorkflowContext},
        router::{GoTo, StepResponse},
    },
};

/// The main workflow service that orchestrates the execution of workflow steps.
#[derive(Debug, Clone)]
pub struct WorkflowService<B, Input, Output>
where
    B: Backend + WireFormatBackend,
{
    services: HashMap<usize, SteppedService<B::Compact>>,
    not_ready: VecDeque<usize>,
    backend: B,
    _marker: PhantomData<(Input, Output)>,
}
impl<B, Input, Output> WorkflowService<B, Input, Output>
where
    B: Backend + WireFormatBackend,
{
    /// Creates a new `WorkflowService` with the given services and backend.
    pub fn new(services: HashMap<usize, SteppedService<B::Compact>>, backend: B) -> Self {
        Self {
            services,
            not_ready: VecDeque::new(),
            backend,
            _marker: PhantomData,
        }
    }
}

impl<B, Err, Input, Output> Service<Task<B::Compact>> for WorkflowService<B, Input, Output>
where
    B: Sink<Task<B::Compact>, Error = Err>
        + Unpin
        + WireFormatBackend
        + BackendConfig<Args = Input>
        + Clone
        + Send
        + Sync
        + 'static
        + Backend<Error = Err>,
    B::Compact: Send + 'static,
    Err: std::error::Error + Send + Sync + 'static,
    B::Id: GenerateId + Send + 'static,
{
    type Response = GoTo<StepResponse>;
    type Error = BoxDynError;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        loop {
            // must wait for *all* services to be ready.
            // this will cause head-of-line blocking unless the underlying services are always ready.
            if self.not_ready.is_empty() {
                return Poll::Ready(Ok(()));
            } else {
                if self
                    .services
                    .get_mut(&self.not_ready[0])
                    .unwrap()
                    .poll_ready(cx)?
                    .is_pending()
                {
                    return Poll::Pending;
                }

                self.not_ready.pop_front();
            }
        }
    }

    fn call(&mut self, mut req: Task<B::Compact>) -> Self::Future {
        assert!(
            self.not_ready.is_empty(),
            "Workflow must wait for all services to be ready. Did you forget to call poll_ready()?"
        );
        let meta = WorkflowContext::extract(req.metadata()).unwrap_or_default();
        let idx = meta.step_index;

        let has_next = self.services.contains_key(&(idx + 1));
        let step_ctx: StepContext<B> = StepContext::new(self.backend.clone(), idx, has_next);

        let svc = self
            .services
            .get_mut(&idx)
            .expect("Attempted to run a step that doesn't exist");

        req.inject_data(step_ctx);

        self.not_ready.push_back(idx);
        svc.call(req).boxed()
    }
}

/// Handle the result of a workflow step, scheduling the next step if necessary
pub async fn handle_step_result<N, Compact, B, Err>(
    ctx: &mut StepContext<B>,
    result: GoTo<N>,
) -> Result<GoTo<StepResponse>, TaskSinkError<Err>>
where
    B: Sink<Task<Compact>, Error = Err>
        + Backend<Error = Err>
        + WireFormatBackend<Compact = Compact>
        + BackendConfig
        + Send
        + Unpin,
    Err: Into<BoxDynError>,
    B::Codec: Codec<N, Compact = Compact> + Clone,
    <B::Codec as Codec<N>>::Error: Into<BoxDynError>,
    N: Serialize,
    Compact: 'static,
    N: 'static,
    B::Id: GenerateId + Send + 'static,
{
    let codec = ctx.backend.codec().clone();
    match result {
        GoTo::Next(next) if ctx.has_next => {
            let task_id = B::Id::generate();
            let task = TaskBuilder::new(
                B::Codec::encode(&codec, &next).map_err(|e| TaskSinkError::CodecError(e.into()))?,
            )
            .task_id(task_id.clone())
            .metadata(&WorkflowContext {
                step_index: ctx.current_step + 1,
            })
            .build();
            ctx.backend.send(task).await?;
            Ok(GoTo::Next(StepResponse {
                result: to_value(&next).map_err(|e| TaskSinkError::CodecError(e.into()))?,
                next_task_id: Some(task_id),
            }))
        }
        GoTo::DelayFor(delay, next) if ctx.has_next => {
            let task_id = B::Id::generate();

            let task = TaskBuilder::new(
                B::Codec::encode(&codec, &next).map_err(|e| TaskSinkError::CodecError(e.into()))?,
            )
            .run_after(delay)
            .task_id(task_id.clone())
            .metadata(&WorkflowContext {
                step_index: ctx.current_step + 1,
            })
            .build();
            ctx.backend.send(task).await?;
            Ok(GoTo::DelayFor(
                delay,
                StepResponse {
                    result: to_value(&next).map_err(|e| TaskSinkError::CodecError(e.into()))?,
                    next_task_id: Some(task_id),
                },
            ))
        }
        #[allow(clippy::match_same_arms)]
        GoTo::Done => Ok(GoTo::Done),
        GoTo::Break(res) => Ok(GoTo::Break(StepResponse {
            result: to_value(&res).map_err(|e| TaskSinkError::CodecError(e.into()))?,
            next_task_id: None,
        })),
        _ => Ok(GoTo::Done),
    }
}
