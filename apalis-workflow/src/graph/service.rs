use apalis_core::backend::Backend;
use apalis_core::backend::BackendConfig;
use apalis_core::backend::WireFormatBackend;
use apalis_core::backend::codec::Codec;
use apalis_core::task::builder::TaskBuilder;
use apalis_core::task::metadata::Meta;
use apalis_core::task::status::Status;
use apalis_core::task::task_id::GenerateId;
use apalis_core::worker::service::IntoWorkerService;
use apalis_core::worker::service::WorkerService;
use apalis_core::{
    backend::WaitForCompletion,
    error::BoxDynError,
    task::{Task, task_id::TaskId},
};
use futures_util::future::BoxFuture;
use futures_util::future::try_join_all;
use futures_util::{FutureExt, Sink, SinkExt, StreamExt};
use petgraph::Direction;
use petgraph::graph::DiGraph;
use petgraph::graph::NodeIndex;
use serde_json::Value;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::fmt::{Debug, Display};
use std::str::FromStr;
use std::task::Poll;
use tower::Service;

use crate::GraphFlow;
use crate::NodeService;
use crate::graph::NodeInput;
use crate::graph::context::GraphFlowContext;
use crate::graph::error::{GraphFlowError, GraphServiceError};
use crate::graph::response::GraphNodeResponse;

/// Service that manages the execution of a Graph workflow
pub struct RootGraphService<B>
where
    B: Backend + WireFormatBackend,
{
    pub(super) graph: DiGraph<NodeService<B::Compact>, ()>,
    pub(super) node_mapping: HashMap<String, NodeIndex>,
    pub(super) topological_order: Vec<NodeIndex>,
    pub(super) start_nodes: Vec<NodeIndex>,
    pub(super) end_nodes: Vec<NodeIndex>,
    pub(super) not_ready: VecDeque<NodeIndex>,
    pub(super) backend: B,
}

impl<B> std::fmt::Debug for RootGraphService<B>
where
    B: Backend + WireFormatBackend,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RootGraphService")
            .field("executor", &"<GraphExecutor>")
            .field("backend", &"<Backend>")
            .finish()
    }
}

impl<B> RootGraphService<B>
where
    B: Backend + WireFormatBackend,
{
    /// Get a node by name
    pub fn get_node_by_name_mut(&mut self, name: &str) -> Option<&mut NodeService<B::Compact>> {
        self.node_mapping
            .get(name)
            .and_then(|&idx| self.graph.node_weight_mut(idx))
    }

    async fn handle_task(
        graph: &mut DiGraph<NodeService<B::Compact>, ()>,
        task: Task<B::Compact>,
    ) -> Result<(B::Compact, Value), GraphFlowError>
    where
        B::Compact: Send + Sync,
    {
        let context = task
            .extract::<Meta<GraphFlowContext>>()
            .await
            .map_err(|e| GraphFlowError::Metadata(e.into()))?
            .0;
        // Get the service for this node
        let service = graph
            .node_weight_mut(context.current_node)
            .ok_or_else(|| GraphFlowError::MissingService(context.current_node))?;

        let result = service
            .call(task.map_args(NodeInput::Single))
            .await
            .map_err(GraphFlowError::NodeExecutionError)?;
        Ok(result)
    }

    async fn handle_fan_in(
        graph: &mut DiGraph<NodeService<B::Compact>, ()>,
        task: Task<Vec<Value>>,
    ) -> Result<(B::Compact, Value), GraphFlowError>
    where
        B::Compact: Send + Sync,
    {
        let context = task
            .extract::<Meta<GraphFlowContext>>()
            .await
            .map_err(|e| GraphFlowError::Metadata(e.into()))?
            .0;
        // Get the service for this node
        let service = graph
            .node_weight_mut(context.current_node)
            .ok_or_else(|| GraphFlowError::MissingService(context.current_node))?;

        let result = service
            .call(task.map_args(NodeInput::FanIn))
            .await
            .map_err(GraphFlowError::NodeExecutionError)?;
        Ok(result)
    }
}

impl<B> Clone for RootGraphService<B>
where
    B: Backend + WireFormatBackend + Clone,
{
    fn clone(&self) -> Self {
        Self {
            graph: self.graph.clone(),
            node_mapping: self.node_mapping.clone(),
            topological_order: self.topological_order.clone(),
            start_nodes: self.start_nodes.clone(),
            end_nodes: self.end_nodes.clone(),
            not_ready: self.not_ready.clone(),
            backend: self.backend.clone(),
        }
    }
}

/// Determine if the previous node is the designated predecessor in a fan-in scenario
fn find_designated_fan_in_handler(
    incoming_nodes: &[NodeIndex],
) -> Result<&NodeIndex, GraphFlowError> {
    let designated_handler = incoming_nodes.iter().max_by_key(|n| n.index());
    designated_handler.ok_or(GraphFlowError::Service(
        GraphServiceError::MissingFaninHandler,
    ))
}

impl<B, Err, CdcErr, Id, Compact> Service<Task<Compact>> for RootGraphService<B>
where
    B: Backend<Error = Err>
        + BackendConfig<Id = Id>
        + WireFormatBackend<Compact = Compact>
        + Send
        + Sync
        + 'static
        + Clone
        + WaitForCompletion<GraphNodeResponse>,
    Id: GenerateId + Send + Sync + 'static + PartialEq + Debug + FromStr + Display,
    Compact: Send + Sync + 'static + Clone,
    Err: std::error::Error + Send + Sync + 'static,
    B: Sink<Task<B::Compact>, Error = Err> + Unpin,
    B::Codec:
        Codec<Vec<Compact>, Compact = Compact, Error = CdcErr> + Send + Sync + Clone + 'static,
    CdcErr: Into<BoxDynError>,
    <Id as FromStr>::Err: std::error::Error + Send + Sync + 'static,
{
    type Response = GraphNodeResponse;
    type Error = GraphFlowError;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        loop {
            // must wait for *all* services to be ready.
            // this will cause head-of-line blocking unless the underlying services are always ready.
            if self.not_ready.is_empty() {
                return Poll::Ready(Ok(()));
            } else {
                if self
                    .graph
                    .node_weight_mut(self.not_ready[0])
                    .ok_or(GraphFlowError::MissingService(self.not_ready[0]))?
                    .poll_ready(cx)
                    .map_err(GraphServiceError::PollError)
                    .map_err(GraphFlowError::Service)?
                    .is_pending()
                {
                    return Poll::Pending;
                }

                self.not_ready.pop_front();
            }
        }
    }

    fn call(&mut self, mut req: Task<B::Compact>) -> Self::Future {
        let backend = self.backend.clone();
        let codec = backend.codec().clone();
        let start_nodes = self.start_nodes.clone();
        let end_nodes = self.end_nodes.clone();
        let mut graph = self.graph.clone();
        let mut backend = self.backend.clone();

        req.inject_data(codec); // Inject codec

        async move {
            let ctx = req.extract::<Meta<GraphFlowContext>>().await;
            let ((compact, response), context) = if let Ok(Meta(context)) = ctx {
                #[cfg(feature = "tracing")]
                tracing::debug!(
                    node = ?context.current_node,
                    "Extracted GraphFlowContext for task"
                );
                let incoming_nodes = graph
                    .neighbors_directed(context.current_node, Direction::Incoming)
                    .collect::<Vec<_>>();
                match incoming_nodes.len() {
                    // Single entry node
                    0 if start_nodes.len() == 1 => {
                        #[cfg(feature = "tracing")]
                        tracing::trace!(
                            node = ?context.current_node,
                            "Found a single entry node"
                        );
                        let response = Self::handle_task(&mut graph, req).await?;
                        #[cfg(feature = "tracing")]
                        tracing::trace!(
                            node = ?context.current_node,
                            "Found a single entry node: done"
                        );
                        (response, context)
                    }
                    // Entry node with multiple start nodes
                    0 if start_nodes.len() > 1 => {
                        #[cfg(feature = "tracing")]
                        tracing::trace!(
                            node = ?context.current_node,
                            "Entry node with multiple start node"
                        );
                        let response = Self::handle_task(&mut graph, req).await?;
                        #[cfg(feature = "tracing")]
                        tracing::trace!(
                            node = ?context.current_node,
                            "Entry node with multiple start node: done"
                        );
                        (response, context)
                    }
                    // Single incoming node, proceed normally
                    1 => {
                        #[cfg(feature = "tracing")]
                        tracing::trace!(
                            node = ?context.current_node,
                            "Single incoming node"
                        );
                        let response = Self::handle_task(&mut graph, req).await?;
                        #[cfg(feature = "tracing")]
                        tracing::trace!(
                            node = ?context.current_node,
                            "Single incoming node complete"
                        );
                        (response, context)
                    }
                    // Multiple incoming nodes, fan-in scenario
                    _ => {
                        #[cfg(feature = "tracing")]
                        tracing::debug!(
                            node = ?context.current_node,
                            "Multiple incoming nodes, fan-in scenario"
                        );
                        let dependency_task_ids = context.get_dependency_task_ids(&incoming_nodes);

                        let prev_node = context.prev_node.ok_or(GraphFlowError::Service(
                            GraphServiceError::MissingPreviousNode,
                        ))?;

                        let fan_in_node = *find_designated_fan_in_handler(&incoming_nodes)?;
                        #[cfg(feature = "tracing")]
                        tracing::debug!(
                            prev_node = ?prev_node,
                            node = ?context.current_node,
                            deps = ?dependency_task_ids.len(),
                            fan_in = ?fan_in_node,
                            "Fanning in from multiple dependencies",
                        );
                        if fan_in_node != prev_node {
                            return Ok(GraphNodeResponse::WaitingForDependencies {
                                pending_dependencies: dependency_task_ids,
                            });
                        }

                        #[cfg(feature = "tracing")]
                        tracing::debug!(
                            prev_node = ?prev_node,
                            node = ?context.current_node,
                            deps = ?dependency_task_ids.len(),
                            fan_in = ?fan_in_node,
                            "designated_fan_in_handler: Waiting for other dependencies",
                        );

                        let results = backend
                            .wait_for(dependency_task_ids.values().cloned().collect::<Vec<_>>())
                            .collect::<Vec<_>>()
                            .await
                            .into_iter()
                            .collect::<Result<Vec<_>, _>>()
                            .map_err(|e| GraphFlowError::Backend(e.into()))?;

                        #[cfg(feature = "tracing")]
                        tracing::debug!(
                            prev_node = ?prev_node,
                            node = ?context.current_node,
                            deps = ?dependency_task_ids.len(),
                            results = ?results.len(),
                            fan_in = ?fan_in_node,
                            "Found results",
                        );
                        if results.iter().all(|s| matches!(s.status, Status::Done)) {
                            let sorted_results = {
                                // Match the order of incoming_nodes by matching NodeIndex
                                let res = incoming_nodes
                                    .iter()
                                    .rev()
                                    .map(|node_index| {
                                        let task_id = context
                                            .node_task_ids
                                            .iter()
                                            .find(|(n, _)| *n == node_index)
                                            .map(|(_, task_id)| task_id)
                                            .ok_or(GraphFlowError::Service(
                                                GraphServiceError::MissingIncomingTaskId,
                                            ))?;
                                        let task_result = results
                                            .iter()
                                            .find(|r| &r.task_id == task_id)
                                            .ok_or(GraphFlowError::Service(
                                                GraphServiceError::MissingTaskIdResult(format!(
                                                    "{task_id:?}"
                                                )),
                                            ))?;
                                        Ok(task_result)
                                    })
                                    .collect::<Result<Vec<_>, GraphFlowError>>();
                                match res {
                                    Ok(v) => v,
                                    Err(e) => {
                                        #[cfg(feature = "tracing")]
                                        tracing::error!(
                                            node = ?context.current_node,
                                            error = ?e,
                                            "Encountered an error resolving result",
                                        );
                                        return Ok(GraphNodeResponse::WaitingForDependencies {
                                            pending_dependencies: dependency_task_ids,
                                        });
                                    }
                                }
                            };
                            let res = sorted_results
                                .iter()
                                .map(|s| match &s.result {
                                    Ok(val) => match val {
                                        GraphNodeResponse::FanOut { response, .. } => {
                                            Ok(response.clone())
                                        }
                                        GraphNodeResponse::EnqueuedNext { result }
                                        | GraphNodeResponse::Complete { result } => {
                                            Ok(result.clone())
                                        }
                                        _ => Err(GraphFlowError::Service(
                                            GraphServiceError::InvalidFanInDependencyResult,
                                        )),
                                    },
                                    Err(e) => Err(GraphFlowError::Service(
                                        GraphServiceError::DependencyTaskFailed(e.as_str().into()),
                                    )),
                                })
                                .collect::<Result<Vec<_>, _>>()?;

                            let req = req.map_args(|_| res); // Replace args with fan-in input
                            let response = Self::handle_fan_in(&mut graph, req).await?;
                            (response, context)
                        } else {
                            return Err(GraphFlowError::Service(
                                GraphServiceError::DependencyTaskFailed(
                                    "An adjacent node failed. Terminating".into(),
                                ),
                            ));
                        }
                    }
                }
            } else {
                #[cfg(feature = "tracing")]
                tracing::debug!("Extracting GraphFlowContext for task without meta");
                // if no metadata, we assume its an entry task
                if start_nodes.len() == 1 {
                    #[cfg(feature = "tracing")]
                    tracing::debug!("Single start node detected, proceeding with execution");
                    let context = GraphFlowContext::new(req.task_id().cloned());
                    req.inject_metadata(&context)?;
                    let response = Self::handle_task(&mut graph, req).await?;
                    #[cfg(feature = "tracing")]
                    tracing::debug!(node = ?context.current_node, "Execution complete at node");
                    (response, context)
                } else {
                    #[cfg(feature = "tracing")]
                    tracing::debug!("Multiple nodes detected, proceeding with fan_out_entry_nodes");
                    let new_node_task_ids = fan_out_entry_nodes(
                        &backend,
                        &start_nodes,
                        &GraphFlowContext::new(req.task_id().cloned()),
                        &req.args,
                    )
                    .await?;
                    return Ok(GraphNodeResponse::EntryFanOut {
                        node_task_ids: new_node_task_ids,
                    });
                }
            };
            // At this point we know a node was executed and we have its context
            // We need to figure out the outgoing nodes and enqueue tasks for them
            let current_node = context.current_node;
            let outgoing_nodes = graph
                .neighbors_directed(current_node, Direction::Outgoing)
                .collect::<Vec<_>>();

            match outgoing_nodes.len() {
                0 => {
                    assert!(
                        end_nodes.contains(&current_node),
                        "Current node is not an end node"
                    );
                    // This was an end node
                    return Ok(GraphNodeResponse::Complete { result: response });
                }
                1 => {
                    // Single outgoing node, enqueue task for it
                    let next_node = outgoing_nodes[0];
                    let mut new_context = context.clone();
                    new_context.prev_node = Some(current_node);
                    new_context.current_node = next_node;
                    new_context.current_position += 1;
                    new_context.is_initial = false;

                    let task = TaskBuilder::new(compact)
                        .task_id(B::Id::generate())
                        .metadata(&new_context)
                        .build();

                    backend
                        .send(task)
                        .await
                        .map_err(|e| GraphFlowError::Backend(e.into()))?;
                }
                _ => {
                    // Multiple outgoing nodes, fan out
                    let mut new_context = context.clone();
                    new_context.prev_node = Some(current_node);
                    new_context.current_position += 1;
                    new_context.is_initial = false;

                    let next_task_ids =
                        fan_out_next_nodes(&backend, outgoing_nodes, &new_context, &compact)
                            .await?;
                    return Ok(GraphNodeResponse::FanOut {
                        response,
                        node_task_ids: next_task_ids,
                    });
                }
            }
            Ok(GraphNodeResponse::EnqueuedNext { result: response })
        }
        .boxed()
    }
}

async fn fan_out_next_nodes<B, Err, CdcErr>(
    backend: &B,
    outgoing_nodes: Vec<NodeIndex>,
    context: &GraphFlowContext,
    input: &B::Compact,
) -> Result<HashMap<NodeIndex, TaskId>, GraphFlowError>
where
    B::Id: GenerateId + Send + Sync + 'static + PartialEq,
    B::Compact: Send + Sync + 'static + Clone,
    B: Sink<Task<B::Compact>, Error = Err> + BackendConfig + Unpin,
    Err: std::error::Error + Send + Sync + 'static,
    B: Backend<Error = Err> + WireFormatBackend + Send + Sync + 'static + Clone,
    B::Codec: Codec<Vec<B::Compact>, Compact = B::Compact, Error = CdcErr>,
    CdcErr: Into<BoxDynError>,
    B::Id: FromStr + Display,
    <B::Id as FromStr>::Err: std::error::Error + Send + Sync + 'static,
{
    let mut enqueue_futures = vec![];
    let next_nodes = outgoing_nodes
        .iter()
        .map(|node| (*node, B::Id::generate()))
        .collect::<HashMap<NodeIndex, TaskId>>();
    let mut node_task_ids = next_nodes.clone();
    node_task_ids.extend(context.node_task_ids.clone());
    for outgoing_node in outgoing_nodes.into_iter() {
        let task_id = next_nodes
            .get(&outgoing_node)
            .ok_or(GraphFlowError::Service(GraphServiceError::MissingNextNode))?
            .clone();
        let task = TaskBuilder::new(input.clone())
            .task_id(task_id)
            .metadata(&GraphFlowContext {
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
                b.send(task)
                    .await
                    .map_err(|e| GraphFlowError::Backend(e.into()))?;
                Ok::<(), GraphFlowError>(())
            }
            .boxed(),
        );
    }
    try_join_all(enqueue_futures).await?;
    Ok(next_nodes)
}

async fn fan_out_entry_nodes<B, Err, CdcErr>(
    backend: &B,
    start_nodes: &[NodeIndex],
    context: &GraphFlowContext,
    input: &B::Compact,
) -> Result<HashMap<NodeIndex, TaskId>, GraphFlowError>
where
    B::Id: GenerateId + Send + Sync + 'static + PartialEq + Debug,
    B::Compact: Send + Sync + 'static + Clone,
    B: Sink<Task<B::Compact>, Error = Err> + Unpin,
    Err: std::error::Error + Send + Sync + 'static,
    B: Backend<Error = Err> + WireFormatBackend + BackendConfig + Send + Sync + 'static + Clone,
    B::Codec: Codec<Vec<B::Compact>, Compact = B::Compact, Error = CdcErr> + Clone,
    CdcErr: Into<BoxDynError>,
    B::Id: FromStr + Display,
    <B::Id as FromStr>::Err: std::error::Error + Send + Sync + 'static,
{
    let codec = backend.codec().clone();
    let values: Vec<B::Compact> =
        B::Codec::decode(&codec, input).map_err(|e: CdcErr| GraphFlowError::Codec(e.into()))?;
    if values.len() != start_nodes.len() {
        return Err(GraphFlowError::InputCountMismatch {
            expected: start_nodes.len(),
            actual: values.len(),
        });
    }
    let mut enqueue_futures = vec![];
    let next_nodes = start_nodes
        .iter()
        .map(|node| (*node, B::Id::generate()))
        .collect::<HashMap<NodeIndex, TaskId>>();
    let mut node_task_ids = next_nodes.clone();
    node_task_ids.extend(context.node_task_ids.clone());
    for (outgoing_node, input) in start_nodes.iter().zip(values) {
        let task_id = next_nodes
            .get(outgoing_node)
            .ok_or(GraphFlowError::Service(GraphServiceError::MissingNextNode))?;
        let task = TaskBuilder::new(input)
            .task_id(task_id.clone())
            .metadata(&GraphFlowContext {
                prev_node: None,
                current_node: *outgoing_node,
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
                b.send(task)
                    .await
                    .map_err(|e| GraphFlowError::Backend(BoxDynError::from(e)))?;
                Ok::<(), GraphFlowError>(())
            }
            .boxed(),
        );
    }
    try_join_all(enqueue_futures).await?;
    Ok(next_nodes)
}

impl<B, Compact, Err> IntoWorkerService<B, RootGraphService<B>> for GraphFlow<B>
where
    B: Backend<Error = Err, Task = Task<Compact>> + WireFormatBackend<Compact = Compact> + Clone,
    Err: std::error::Error + Send + Sync + 'static,
    B::Compact: Send + Sync + 'static + Clone,
    RootGraphService<B>: Service<Task<Compact>>,
{
    type Task = Task<Compact>;
    type Backend = B;
    fn into_service(self, b: B) -> WorkerService<B, RootGraphService<B>> {
        let service = self.build(b.clone()).expect("Execution should be valid");
        WorkerService {
            backend: b,
            service,
        }
    }
}
