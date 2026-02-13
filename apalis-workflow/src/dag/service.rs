use apalis_core::backend::codec::Codec;
use apalis_core::backend::BackendExt;
use apalis_core::task::builder::TaskBuilder;
use apalis_core::task::metadata::Meta;
use apalis_core::task::status::Status;
use apalis_core::{
    backend::WaitForCompletion,
    error::BoxDynError,
    task::{metadata::MetadataExt, task_id::TaskId, Task},
};
use futures::future::BoxFuture;
use futures::{FutureExt, Sink, SinkExt, StreamExt, pin_mut};
use petgraph::graph::NodeIndex;
use petgraph::Direction;
use std::collections::HashMap;
use std::fmt::Debug;
use tower::Service;

use crate::dag::context::DagFlowContext;
use crate::dag::response::DagExecutionResponse;
use crate::id_generator::GenerateId;
use crate::DagExecutor;

#[derive(Debug, Clone, Copy)]
enum FanInAction {
    Execute,
    Wait,
    Exit,
}

/// Determine if the previous node is the designated predecessor in a fan-in scenario
fn is_designated_fan_in_predecessor(
    incoming_nodes: &[NodeIndex],
    prev_node: Option<NodeIndex>,
) -> bool {
    if incoming_nodes.is_empty() {
        return false;
    }

    let designated_parent = incoming_nodes.iter().min_by_key(|n| n.index()).copied();
    prev_node == designated_parent
}

fn fan_in_action(
    incoming_nodes: &[NodeIndex],
    prev_node: Option<NodeIndex>,
    all_deps_done: bool,
    fan_in_completed: bool,
) -> FanInAction {
    if fan_in_completed {
        return FanInAction::Exit;
    }

    let is_designated = is_designated_fan_in_predecessor(incoming_nodes, prev_node);

    match (all_deps_done, is_designated) {
        (true, true) => FanInAction::Execute,
        (false, true) => FanInAction::Wait,
        _ => FanInAction::Exit,
    }
}

/// Service that manages the execution of a DAG workflow
pub struct RootDagService<B>
where
    B: BackendExt,
{
    executor: DagExecutor<B>,
    backend: B,
}

impl<B> std::fmt::Debug for RootDagService<B>
where
    B: BackendExt,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RootDagService")
            .field("executor", &"<DagExecutor>")
            .field("backend", &"<Backend>")
            .finish()
    }
}

impl<B> RootDagService<B>
where
    B: BackendExt,
{
    pub(crate) fn new(executor: DagExecutor<B>, backend: B) -> Self {
        Self { executor, backend }
    }
}

impl<B> Clone for RootDagService<B>
where
    B: BackendExt + Clone,
{
    fn clone(&self) -> Self {
        Self {
            executor: self.executor.clone(),
            backend: self.backend.clone(),
        }
    }
}

impl<B, Err, CdcErr, MetaError, IdType> Service<Task<B::Compact, B::Context, B::IdType>>
    for RootDagService<B>
where
    B: BackendExt<Error = Err, IdType = IdType>
        + Send
        + Sync
        + 'static
        + Clone
        + WaitForCompletion<DagExecutionResponse<B::Compact, IdType>>,
    IdType: GenerateId + Send + Sync + 'static + PartialEq + Debug + Clone,
    B::Compact: Send + Sync + 'static + Clone + Debug,
    B::Context:
        Send + Sync + Default + MetadataExt<DagFlowContext<B::IdType>, Error = MetaError> + 'static,
    Err: std::error::Error + Send + Sync + 'static,
    B: Sink<Task<B::Compact, B::Context, B::IdType>, Error = Err> + Unpin,
    B::Codec: Codec<Vec<B::Compact>, Compact = B::Compact, Error = CdcErr>
        + 'static
        + Codec<DagExecutionResponse<B::Compact, B::IdType>, Compact = B::Compact, Error = CdcErr>,
    CdcErr: Into<BoxDynError>,
    MetaError: Into<BoxDynError> + Send + Sync + 'static,
{
    type Response = DagExecutionResponse<B::Compact, B::IdType>;
    type Error = BoxDynError;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.executor.poll_ready(cx).map_err(|e| e.into())
    }

    

    fn call(&mut self, mut req: Task<B::Compact, B::Context, B::IdType>) -> Self::Future {
        let mut executor = self.executor.clone();
        let mut backend = self.backend.clone();
        let start_nodes = executor.start_nodes.clone();
        let end_nodes = executor.end_nodes.clone();

        // Make sure the task has a concrete TaskId (not Option<TaskId>)
        let this_task_id: TaskId<B::IdType> = match req.parts.task_id.clone() {
            Some(id) => id,
            None => {
                let id = TaskId::new(B::IdType::generate());
                req.parts.task_id = Some(id.clone());
                id
            }
        };

        async move {
            let ctx = req.extract::<Meta<DagFlowContext<B::IdType>>>().await;

            let (response, mut context) = if let Ok(Meta(context)) = ctx {
                #[cfg(feature = "tracing")]
                tracing::debug!(
                    node = ?context.current_node,
                    "Extracted DagFlowContext for task"
                );

                let incoming_nodes = executor
                    .graph
                    .neighbors_directed(context.current_node, Direction::Incoming)
                    .collect::<Vec<_>>();

                match incoming_nodes.len() {
                    // Entry node
                    0 if start_nodes.len() == 1 => {
                        let response = executor.call(req).await?;
                        (response, context)
                    }
                    // Entry node with multiple start nodes
                    0 if start_nodes.len() > 1 => {
                        let response = executor.call(req).await?;
                        (response, context)
                    }
                    // Single incoming node, proceed normally
                    1 => {
                        let response = executor.call(req).await?;
                        (response, context)
                    }
                    // Fan-in node: wait until deps done, then merge+execute
                    _ => {
                        let dependency_task_ids = context.get_dependency_task_ids(&incoming_nodes);

                        #[cfg(feature = "tracing")]
                        tracing::debug!(
                            prev_node = ?context.prev_node,
                            node = ?context.current_node,
                            deps = ?dependency_task_ids,
                            "Fanning in from multiple dependencies",
                        );

                        let mut results = backend
                            .check_status(dependency_task_ids.values().cloned().collect::<Vec<_>>())
                            .await?;

                        let mut all_done = dependency_task_ids.len() == incoming_nodes.len()
                            && results.len() == dependency_task_ids.len()
                            && results.iter().all(|s| matches!(s.status, Status::Done));

                        let fan_in_completed = context.completed_nodes.contains(&context.current_node);
                        let action = fan_in_action(
                            &incoming_nodes,
                            context.prev_node,
                            all_done,
                            fan_in_completed,
                        );

                        match action {
                            FanInAction::Wait => {
                                let pending_dependency_ids = dependency_task_ids
                                    .values()
                                    .filter(|task_id| {
                                        !results.iter().any(|s| {
                                            &s.task_id == *task_id
                                                && matches!(s.status, Status::Done)
                                        })
                                    })
                                    .cloned()
                                    .collect::<Vec<_>>();

                                if !pending_dependency_ids.is_empty() {
                                    let pending_stream = backend.wait_for(pending_dependency_ids);
                                    pin_mut!(pending_stream);
                                    while let Some(status) = pending_stream.next().await {
                                        let _ = status.map_err(BoxDynError::from)?;
                                    }
                                }

                                results = backend
                                    .check_status(
                                        dependency_task_ids.values().cloned().collect::<Vec<_>>(),
                                    )
                                    .await?;
                                all_done = dependency_task_ids.len() == incoming_nodes.len()
                                    && results.len() == dependency_task_ids.len()
                                    && results
                                        .iter()
                                        .all(|s| matches!(s.status, Status::Done));

                                if !all_done {
                                    return Ok(DagExecutionResponse::WaitingForDependencies {
                                        pending_dependencies: dependency_task_ids,
                                    });
                                }
                            }
                            FanInAction::Exit => {
                                return Ok(DagExecutionResponse::EnqueuedNext {
                                    result: req.args.clone(),
                                });
                            }
                            FanInAction::Execute => {}
                        }

                        // Merge dependency results in a deterministic order (matching incoming_nodes)
                        let sorted_results = {
                            let res = incoming_nodes
                                .iter()
                                .rev()
                                .map(|node_index| {
                                    let task_id = context
                                        .node_task_ids
                                        .get(node_index)
                                        .ok_or(BoxDynError::from("TaskId for incoming node not found"))?;

                                    let task_result = results
                                        .iter()
                                        .find(|r| &r.task_id == task_id)
                                        .ok_or(BoxDynError::from(format!(
                                            "TaskResult for task_id {task_id:?} not found"
                                        )))?;

                                    Ok(task_result)
                                })
                                .collect::<Result<Vec<_>, BoxDynError>>();

                            match res {
                                Ok(v) => v,
                                Err(_) => {
                                    return Ok(DagExecutionResponse::WaitingForDependencies {
                                        pending_dependencies: dependency_task_ids,
                                    });
                                }
                            }
                        };

                        let res = sorted_results
                            .iter()
                            .map(|s| match &s.result {
                                Ok(val) => match val {
                                    DagExecutionResponse::FanOut { response, .. } => Ok(response.clone()),
                                    DagExecutionResponse::EnqueuedNext { result }
                                    | DagExecutionResponse::Complete { result } => Ok(result.clone()),
                                    _ => Err(
                                        "Dependency task returned invalid response, which is unexpected during fan-in"
                                            .to_owned(),
                                    ),
                                },
                                Err(e) => Err(format!("Dependency task failed: {e:?}")),
                            })
                            .collect::<Result<Vec<_>, String>>()?;

                        let encoded_input = B::Codec::encode(&res).map_err(|e| e.into())?;
                        let req = req.map(|_| encoded_input); // Replace args with merged fan-in input
                        let response = executor.call(req).await?;
                        (response, context)
                    }
                }
            } else {
                #[cfg(feature = "tracing")]
                tracing::debug!("Extracting DagFlowContext for task without meta");

                // if no metadata, we assume its an entry task
                if start_nodes.len() == 1 {
                    #[cfg(feature = "tracing")]
                    tracing::debug!("Single start node detected, proceeding with execution");

                    let mut context = DagFlowContext::new(Some(this_task_id.clone()));
                    // CRITICAL: seed node_task_ids so fan-in can resolve dependency ids
                    context
                        .node_task_ids
                        .insert(context.current_node, this_task_id.clone());

                    req.parts.ctx.inject(context.clone()).map_err(|e| e.into())?;
                    let response = executor.call(req).await?;

                    #[cfg(feature = "tracing")]
                    tracing::debug!(node = ?context.current_node, "Execution complete at node");

                    (response, context)
                } else {
                    let new_node_task_ids = fan_out_entry_nodes(
                        &executor,
                        &backend,
                        &DagFlowContext::new(Some(this_task_id.clone())),
                        &req.args,
                    )
                    .await?;

                    return Ok(DagExecutionResponse::EntryFanOut {
                        node_task_ids: new_node_task_ids,
                    });
                }
            };

            // Persist that this node has completed so subsequent fan-in contenders can short-circuit.
            context.completed_nodes.insert(context.current_node);

            // Figure out outgoing nodes and enqueue next tasks
            let current_node = context.current_node;
            let outgoing_nodes = executor
                .graph
                .neighbors_directed(current_node, Direction::Outgoing)
                .collect::<Vec<_>>();

            match outgoing_nodes.len() {
                0 => {
                    assert!(
                        end_nodes.contains(&current_node),
                        "Current node is not an end node"
                    );
                    return Ok(DagExecutionResponse::Complete { result: response });
                }

                1 => {
                    let next_node = outgoing_nodes[0];

                    let mut new_context = context.clone();
                    new_context.prev_node = Some(current_node);
                    new_context.current_node = next_node;
                    new_context.current_position += 1;
                    new_context.is_initial = false;

                    let next_task_id = TaskId::new(B::IdType::generate());
                    new_context
                        .node_task_ids
                        .insert(next_node, next_task_id.clone());

                    let task = TaskBuilder::new(response.clone())
                        .with_task_id(next_task_id)
                        .meta(new_context)
                        .build();

                    backend.send(task).await.map_err(BoxDynError::from)?;
                }

                _ => {
                    let mut new_context = context.clone();
                    new_context.prev_node = Some(current_node);
                    new_context.current_position += 1;
                    new_context.is_initial = false;

                    let next_task_ids = fan_out_next_nodes(
                        &executor,
                        outgoing_nodes,
                        &backend,
                        &new_context,
                        &response,
                    )
                    .await?;

                    return Ok(DagExecutionResponse::FanOut {
                        response,
                        node_task_ids: next_task_ids,
                    });
                }
            }

            Ok(DagExecutionResponse::EnqueuedNext { result: response })
        }
        .boxed()
    }

}

async fn fan_out_next_nodes<B, Err, CdcErr>(
    _executor: &DagExecutor<B>,
    outgoing_nodes: Vec<NodeIndex>,
    backend: &B,
    context: &DagFlowContext<B::IdType>,
    input: &B::Compact,
) -> Result<HashMap<NodeIndex, TaskId<B::IdType>>, BoxDynError>
where
    B::IdType: GenerateId + Send + Sync + 'static + PartialEq + Debug,
    B::Compact: Send + Sync + 'static + Clone,
    B::Context: Send + Sync + Default + MetadataExt<DagFlowContext<B::IdType>> + 'static,
    B: Sink<Task<B::Compact, B::Context, B::IdType>, Error = Err> + Unpin,
    Err: std::error::Error + Send + Sync + 'static,
    B: BackendExt<Error = Err> + Send + Sync + 'static + Clone,
    B::Codec: Codec<Vec<B::Compact>, Compact = B::Compact, Error = CdcErr>,
    CdcErr: Into<BoxDynError>,
{
    let mut enqueue_futures = vec![];
    let next_nodes = outgoing_nodes
        .iter()
        .map(|node| (*node, TaskId::new(B::IdType::generate())))
        .collect::<HashMap<NodeIndex, TaskId<B::IdType>>>();

    let mut node_task_ids = next_nodes.clone();
    node_task_ids.extend(context.node_task_ids.clone());

    for outgoing_node in outgoing_nodes.into_iter() {
        let task_id = next_nodes
            .get(&outgoing_node)
            .expect("TaskId for start node not found")
            .clone();

        let task = TaskBuilder::new(input.clone())
            .with_task_id(task_id.clone())
            .meta(DagFlowContext {
                prev_node: context.prev_node,
                current_node: outgoing_node,
                completed_nodes: context.completed_nodes.clone(),
                node_task_ids: node_task_ids.clone(),
                current_position: context.current_position + 1,
                is_initial: context.is_initial,
                root_task_id: context.root_task_id.clone(),
            })
            .build();

        let mut b = backend.clone();
        enqueue_futures.push(
            async move {
                b.send(task).await.map_err(BoxDynError::from)?;
                Ok::<(), BoxDynError>(())
            }
            .boxed(),
        );
    }

    futures::future::try_join_all(enqueue_futures).await?;
    Ok(next_nodes)
}

async fn fan_out_entry_nodes<B, Err, CdcErr>(
    executor: &DagExecutor<B>,
    backend: &B,
    context: &DagFlowContext<B::IdType>,
    input: &B::Compact,
) -> Result<HashMap<NodeIndex, TaskId<B::IdType>>, BoxDynError>
where
    B::IdType: GenerateId + Send + Sync + 'static + PartialEq + Debug,
    B::Compact: Send + Sync + 'static + Clone,
    B::Context: Send + Sync + Default + MetadataExt<DagFlowContext<B::IdType>> + 'static,
    B: Sink<Task<B::Compact, B::Context, B::IdType>, Error = Err> + Unpin,
    Err: std::error::Error + Send + Sync + 'static,
    B: BackendExt<Error = Err> + Send + Sync + 'static + Clone,
    B::Codec: Codec<Vec<B::Compact>, Compact = B::Compact, Error = CdcErr>,
    CdcErr: Into<BoxDynError>,
{
    let values: Vec<B::Compact> = B::Codec::decode(input).map_err(|e: CdcErr| e.into())?;
    let start_nodes = executor.start_nodes.clone();

    if values.len() != start_nodes.len() {
        return Err(BoxDynError::from(format!(
            "Expected {} inputs for fan-in, got {}",
            start_nodes.len(),
            values.len()
        )));
    }

    let mut enqueue_futures = vec![];
    let next_nodes = start_nodes
        .iter()
        .map(|node| (*node, TaskId::new(B::IdType::generate())))
        .collect::<HashMap<NodeIndex, TaskId<B::IdType>>>();

    let mut node_task_ids = next_nodes.clone();
    node_task_ids.extend(context.node_task_ids.clone());

    for (outgoing_node, input) in start_nodes.into_iter().zip(values) {
        let task_id = next_nodes
            .get(&outgoing_node)
            .expect("TaskId for start node not found")
            .clone();

        let task = TaskBuilder::new(input)
            .with_task_id(task_id.clone())
            .meta(DagFlowContext {
                prev_node: None,
                current_node: outgoing_node,
                completed_nodes: Default::default(),
                node_task_ids: node_task_ids.clone(),
                current_position: context.current_position,
                is_initial: true,
                root_task_id: context.root_task_id.clone(),
            })
            .build();

        let mut b = backend.clone();
        enqueue_futures.push(
            async move {
                b.send(task).await.map_err(BoxDynError::from)?;
                Ok::<(), BoxDynError>(())
            }
            .boxed(),
        );
    }

    futures::future::try_join_all(enqueue_futures).await?;
    Ok(next_nodes)
}
