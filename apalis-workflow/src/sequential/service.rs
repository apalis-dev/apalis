use apalis_core::{
    backend::{BackendExt, TaskSinkError, codec::Codec},
    error::BoxDynError,
    task::{Task, builder::TaskBuilder, metadata::Metadata, task_id::TaskId},
};
use futures::SinkExt;
use futures::{FutureExt, Sink, future::BoxFuture};
use std::{
    collections::{HashMap, VecDeque},
    marker::PhantomData,
    task::{Context, Poll},
};
use tower::Service;

use crate::{
    SteppedService,
    id_generator::GenerateId,
    sequential::context::{StepContext, WorkflowContext},
    sequential::router::{GoTo, StepResult},
};

/// The main workflow service that orchestrates the execution of workflow steps.
#[derive(Debug, Clone)]
pub struct WorkflowService<B, Input>
where
    B: BackendExt,
{
    services: HashMap<usize, SteppedService<B::Compact, B::Connection, B::IdType>>,
    not_ready: VecDeque<usize>,
    backend: B,
    _marker: PhantomData<Input>,
}
impl<B, Input> WorkflowService<B, Input>
where
    B: BackendExt,
{
    /// Creates a new `WorkflowService` with the given services and backend.
    pub fn new(
        services: HashMap<usize, SteppedService<B::Compact, B::Connection, B::IdType>>,
        backend: B,
    ) -> Self {
        Self {
            services,
            not_ready: VecDeque::new(),
            backend,
            _marker: PhantomData,
        }
    }
}

impl<B, Err, Input> Service<Task<B::Compact, B::Connection, B::IdType>>
    for WorkflowService<B, Input>
where
    B::Compact: Send + 'static,
    B: Sync,
    B::Connection: Send,
    Err: std::error::Error + Send + Sync + 'static,
    B::IdType: GenerateId + Send + 'static,
    B: Sink<Task<B::Compact, B::Connection, B::IdType>, Error = Err> + Unpin,
    B: Clone + Send + Sync + 'static + BackendExt<Error = Err>,
{
    type Response = GoTo<StepResult<B::Compact, B::IdType>>;
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

    fn call(&mut self, req: Task<B::Compact, B::Connection, B::IdType>) -> Self::Future {
        assert!(
            self.not_ready.is_empty(),
            "Workflow must wait for all services to be ready. Did you forget to call poll_ready()?"
        );
        let meta = WorkflowContext::extract(&req.ctx.metadata).unwrap_or_default();
        let idx = meta.step_index;

        let has_next = self.services.contains_key(&(idx + 1));
        let step_ctx: StepContext<B> = StepContext::new(self.backend.clone(), idx, has_next);

        let svc = self
            .services
            .get_mut(&idx)
            .expect("Attempted to run a step that doesn't exist");

        let mut task = req.into_builder();
        task.ctx.data.insert(step_ctx);

        self.not_ready.push_back(idx);
        svc.call(task.build()).boxed()
    }
}

/// Handle the result of a workflow step, scheduling the next step if necessary
pub async fn handle_step_result<N, Compact, B, Err>(
    ctx: &mut StepContext<B>,
    result: GoTo<N>,
) -> Result<GoTo<StepResult<B::Compact, B::IdType>>, TaskSinkError<Err>>
where
    B: Sink<Task<Compact, B::Connection, B::IdType>, Error = Err>
        + BackendExt<Error = Err, Compact = Compact>
        + Send
        + Unpin,
    B::Codec: Codec<N, Compact = Compact>,
    <B::Codec as Codec<N>>::Error: Into<BoxDynError>,
    Compact: 'static,
    N: 'static,
    B::IdType: GenerateId + Send + 'static,
{
    match result {
        GoTo::Next(next) if ctx.has_next => {
            let task_id = B::IdType::generate();
            let task_id = TaskId::new(task_id);
            let task = TaskBuilder::new(
                B::Codec::encode(&next).map_err(|e| TaskSinkError::CodecError(e.into()))?,
            )
            .task_id(task_id.clone())
            .metadata(&WorkflowContext {
                step_index: ctx.current_step + 1,
            })
            .build();
            ctx.backend.send(task).await?;
            Ok(GoTo::Next(StepResult {
                result: B::Codec::encode(&next).map_err(|e| TaskSinkError::CodecError(e.into()))?,
                next_task_id: Some(task_id),
            }))
        }
        GoTo::DelayFor(delay, next) if ctx.has_next => {
            let task_id = B::IdType::generate();
            let task_id = TaskId::new(task_id);
            let task = TaskBuilder::new(
                B::Codec::encode(&next).map_err(|e| TaskSinkError::CodecError(e.into()))?,
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
                StepResult {
                    result: B::Codec::encode(&next)
                        .map_err(|e| TaskSinkError::CodecError(e.into()))?,
                    next_task_id: Some(task_id),
                },
            ))
        }
        #[allow(clippy::match_same_arms)]
        GoTo::Done => Ok(GoTo::Done),
        GoTo::Break(res) => Ok(GoTo::Break(StepResult {
            result: B::Codec::encode(&res).map_err(|e| TaskSinkError::CodecError(e.into()))?,
            next_task_id: None,
        })),
        _ => Ok(GoTo::Done),
    }
}
