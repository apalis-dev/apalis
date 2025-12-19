use apalis_core::backend::codec::Codec;
use apalis_core::backend::{self, BackendExt, TaskResult};
use apalis_core::task;
use apalis_core::task::builder::TaskBuilder;
use apalis_core::task::metadata::Meta;
use apalis_core::task::status::Status;
use apalis_core::{
    backend::{Backend, WaitForCompletion},
    error::BoxDynError,
    task::{Task, metadata::MetadataExt, task_id::TaskId},
};
use futures::future::BoxFuture;
use futures::stream::StreamExt;
use futures::{FutureExt, Sink, SinkExt};
use petgraph::graph::{DiGraph, NodeIndex};
use petgraph::{Direction, graph};
use serde::{Deserialize, Serialize, de};
use std::collections::{HashMap, HashSet, VecDeque};
use std::fmt::Debug;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use tower::Service;

use crate::id_generator::GenerateId;
use crate::{DagExecutor, DagService};

/// Metadata stored in each task for workflow processing
#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct DagflowContext<Ctx, IdType> {
    /// The current node being executed in the DAG
    pub current_node: NodeIndex,

    /// All nodes that have been completed in this execution
    pub completed_nodes: HashSet<NodeIndex>,

    /// Map of node indices to their task IDs for result lookup
    pub node_task_ids: HashMap<NodeIndex, TaskId<IdType>>,

    /// Current position in the topological order
    pub current_position: usize,

    /// Whether this is the initial execution
    pub is_initial: bool,

    /// The original task ID that started this DAG execution
    pub root_task_id: Option<TaskId<IdType>>,

    _phantom: std::marker::PhantomData<Ctx>,
}

impl<Ctx, IdType: Clone> DagflowContext<Ctx, IdType> {
    /// Create initial context for DAG execution
    pub fn new(root_task_id: Option<TaskId<IdType>>) -> Self {
        Self {
            current_node: NodeIndex::new(0),
            completed_nodes: HashSet::new(),
            node_task_ids: HashMap::new(),
            current_position: 0,
            is_initial: true,
            root_task_id,
            _phantom: std::marker::PhantomData,
        }
    }

    /// Mark a node as completed and store its task ID
    pub fn mark_completed(&mut self, node: NodeIndex, task_id: TaskId<IdType>) {
        self.completed_nodes.insert(node);
        self.node_task_ids.insert(node, task_id);
    }

    /// Check if all dependencies of a node are completed
    pub fn are_dependencies_complete(&self, dependencies: &[NodeIndex]) -> bool {
        dependencies
            .iter()
            .all(|dep| self.completed_nodes.contains(dep))
    }

    /// Check if the DAG execution is complete
    pub fn is_complete(&self, end_nodes: &Vec<NodeIndex>) -> bool {
        end_nodes
            .iter()
            .all(|node| self.completed_nodes.contains(node))
    }

    /// Get task IDs for dependencies of a given node
    pub fn get_dependency_task_ids(&self, dependencies: &[NodeIndex]) -> Vec<TaskId<IdType>> {
        dependencies
            .iter()
            .filter_map(|dep| self.node_task_ids.get(dep).cloned())
            .collect()
    }
}

/// Response from DAG execution step
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DagExecutionResponse<Compact, Ctx, IdType> {
    InitializeFanIn {
        input: Compact,
        context: DagflowContext<Ctx, IdType>,
    },
    EnqueueNext {
        result: Compact,
        context: DagflowContext<Ctx, IdType>,
    },
    /// Waiting for dependencies to complete
    WaitingForDependencies {
        node: NodeIndex,
        pending_dependencies: Vec<NodeIndex>,
        context: DagflowContext<Ctx, IdType>,
    },

    /// DAG execution is complete
    Complete {
        end_node_task_ids: Vec<TaskId<IdType>>,
        context: DagflowContext<Ctx, IdType>,
    },

    /// No more nodes can execute (shouldn't happen and should be an error)
    Stuck {
        context: DagflowContext<Ctx, IdType>,
    },
}

impl<B> Service<Task<B::Compact, B::Context, B::IdType>> for DagExecutor<B>
where
    B: BackendExt,
    B::Context:
        Send + Sync + 'static + MetadataExt<DagflowContext<B::Context, B::IdType>> + Default,
    B::IdType: Clone + Send + Sync + 'static + GenerateId,
    B::Compact: Send + Sync + 'static,
{
    type Response = DagExecutionResponse<B::Compact, B::Context, B::IdType>;
    type Error = BoxDynError;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        loop {
            // must wait for *all* services to be ready.
            // this will cause head-of-line blocking unless the underlying services are always ready.
            if self.not_ready.is_empty() {
                return Poll::Ready(Ok(()));
            } else {
                if self
                    .graph
                    .node_weight_mut(self.not_ready[0])
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

    fn call(&mut self, req: Task<B::Compact, B::Context, B::IdType>) -> Self::Future {
        // Clone what we need for the async block
        let mut graph = self.graph.clone();
        let node_mapping = self.node_mapping.clone();
        let topological_order = self.topological_order.clone();
        let task_id = req.parts.task_id.as_ref().unwrap().clone();
        let start_nodes = self.start_nodes.clone();
        let end_nodes = self.end_nodes.clone();

        Box::pin(async move {
            let context_result = req.extract::<Meta<DagflowContext<B::Context, B::IdType>>>();
            let mut context = match context_result.await {
                Ok(ctx) => ctx.0,
                Err(_) => {
                    if start_nodes.len() == 1 {
                        DagflowContext::new(req.parts.task_id.clone())
                    } else {
                        println!("Initializing fan-in for multiple start nodes");
                        return Ok(DagExecutionResponse::InitializeFanIn {
                            input: req.args,
                            context: DagflowContext::new(req.parts.task_id.clone()),
                        });
                    }
                }
            };

            // Check if execution is complete
            if context.is_complete(&end_nodes) {
                let end_task_ids = end_nodes
                    .iter()
                    .filter_map(|node| context.node_task_ids.get(node).cloned())
                    .collect();

                return Ok(DagExecutionResponse::Complete {
                    end_node_task_ids: end_task_ids,
                    context,
                });
            }

            // Get dependencies for this node
            let dependencies: Vec<NodeIndex> = graph
                .neighbors_directed(context.current_node, Direction::Incoming)
                .collect();

            // Check if dependencies are ready
            if !context.are_dependencies_complete(&dependencies) {
                let pending: Vec<NodeIndex> = dependencies
                    .iter()
                    .copied()
                    .filter(|dep| !context.completed_nodes.contains(dep))
                    .collect();

                return Ok(DagExecutionResponse::WaitingForDependencies {
                    node: context.current_node,
                    pending_dependencies: pending,
                    context,
                });
            }

            // Get the service for this node
            let service = graph
                .node_weight_mut(context.current_node)
                .ok_or_else(|| BoxDynError::from("Node not found in graph"))?;

            let result = service.call(req).await.unwrap();

            // Mark this node as completed (or in-progress)
            context.mark_completed(context.current_node, task_id);

            Ok(DagExecutionResponse::EnqueueNext { result, context })
        })
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

impl<B, Err, CdcErr> Service<Task<B::Compact, B::Context, B::IdType>> for RootDagService<B>
where
    B: BackendExt<Error = Err> + Send + Sync + 'static + Clone + WaitForCompletion<B::Compact>,
    B::IdType: GenerateId + Send + Sync + 'static + PartialEq,
    B::Compact: Send + Sync + 'static + Clone,
    B::Context:
        Send + Sync + Default + MetadataExt<DagflowContext<B::Context, B::IdType>> + 'static,
    Err: std::error::Error + Send + Sync + 'static,
    B: Sink<Task<B::Compact, B::Context, B::IdType>, Error = Err> + Unpin,
    B::Codec: Codec<Vec<B::Compact>, Compact = B::Compact, Error = CdcErr>
        + 'static
        + Codec<
            DagExecutionResponse<B::Compact, B::Context, B::IdType>,
            Compact = B::Compact,
            Error = CdcErr,
        >,
    CdcErr: Into<BoxDynError>,
{
    type Response = DagExecutionResponse<B::Compact, B::Context, B::IdType>;
    type Error = BoxDynError;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.executor.poll_ready(cx).map_err(|e| e.into())
    }

    fn call(&mut self, req: Task<B::Compact, B::Context, B::IdType>) -> Self::Future {
        let mut executor = self.executor.clone();
        let backend = self.backend.clone();

        async move {
            let response = executor.call(req).await?;
            match response {
                DagExecutionResponse::EnqueueNext {
                    ref result,
                    ref context,
                } => {
                    // Enqueue next tasks for downstream nodes
                    let mut graph = executor.graph.clone();
                    let mut enqueue_futures = vec![];

                    for neighbor in executor
                        .graph
                        .neighbors_directed(context.current_node, Direction::Outgoing)
                    {
                        let service = graph
                            .node_weight_mut(neighbor)
                            .ok_or_else(|| BoxDynError::from("Node not found in graph"))?;

                        let dependencies: Vec<NodeIndex> = graph
                            .neighbors_directed(neighbor, Direction::Incoming)
                            .collect();

                        let dependency_task_ids = context.get_dependency_task_ids(&dependencies);

                        let task = TaskBuilder::new(result.clone())
                            .with_task_id(TaskId::new(B::IdType::generate()))
                            .meta(DagflowContext {
                                current_node: neighbor,
                                completed_nodes: context.completed_nodes.clone(),
                                node_task_ids: dependency_task_ids
                                    .iter()
                                    .enumerate()
                                    .map(|(i, task_id)| (dependencies[i], task_id.clone()))
                                    .collect(),
                                current_position: context.current_position + 1,
                                is_initial: false,
                                root_task_id: context.root_task_id.clone(),
                                _phantom: std::marker::PhantomData,
                            })
                            .build();

                        let mut b = backend.clone();

                        enqueue_futures.push(async move {
                            b.send(task).await.map_err(|e| BoxDynError::from(e))?;
                            Ok::<(), BoxDynError>(())
                        });
                    }

                    // Await all enqueue operations
                    futures::future::try_join_all(enqueue_futures).await?;
                }
                DagExecutionResponse::InitializeFanIn {
                    ref input,
                    ref context,
                } => {
                    use apalis_core::backend::codec::Codec;
                    let values: Vec<B::Compact> =
                        B::Codec::decode(input).map_err(|e: CdcErr| e.into())?;
                    let start_nodes = executor.start_nodes.clone();
                    assert_eq!(values.len(), start_nodes.len());

                    let mut collector_tasks = vec![];

                    let mut enqueue_futures = vec![];
                    for (node_input, start_node) in values.into_iter().zip(start_nodes) {
                        let task_id = TaskId::new(B::IdType::generate());
                        let task = TaskBuilder::new(node_input)
                            .with_task_id(task_id.clone())
                            .meta(DagflowContext {
                                current_node: start_node,
                                completed_nodes: Default::default(),
                                node_task_ids: Default::default(),
                                current_position: context.current_position,
                                is_initial: true,
                                root_task_id: context.root_task_id.clone(),
                                _phantom: std::marker::PhantomData,
                            })
                            .build();
                        let mut b = backend.clone();
                        collector_tasks.push((start_node, task_id));
                        enqueue_futures.push(
                            async move {
                                b.send(task).await.map_err(|e| BoxDynError::from(e))?;
                                Ok::<(), BoxDynError>(())
                            }
                            .boxed(),
                        );
                    }
                    let collector_future = {
                        let mut b = backend.clone();
                        let graph = executor.graph.clone();
                        let collector_tasks = collector_tasks.clone();

                        async move {
                            let res: Vec<TaskResult<B::Compact, B::IdType>> = b
                                .wait_for(
                                    collector_tasks.iter().map(|(_, task_id)| task_id.clone()),
                                )
                                .collect::<Vec<_>>()
                                .await
                                .into_iter()
                                .collect::<Result<Vec<_>, _>>()?;
                            let outgoing_nodes =
                                graph.neighbors_directed(context.current_node, Direction::Outgoing);
                            for outgoing_node in outgoing_nodes {
                                let incoming_nodes = graph
                                    .neighbors_directed(outgoing_node, Direction::Incoming)
                                    .collect::<Vec<_>>();

                                let all_good = res.iter().all(|r| matches!(r.status, Status::Done));

                                if !all_good {
                                    return Err(BoxDynError::from(
                                        "One or more collector tasks failed",
                                    ));
                                }
                                let sorted_results = {
                                    // Match the order of incoming_nodes by matching NodeIndex
                                    incoming_nodes
                                        .iter()
                                        .rev()
                                        .map(|node_index| {
                                            let task_id = collector_tasks
                                                .iter()
                                                .find(|(n, _)| n == node_index)
                                                .map(|(_, task_id)| task_id)
                                                .expect("TaskId for incoming node not found");
                                            res.iter().find(|r| &r.task_id == task_id).expect(
                                                "TaskResult for incoming node's task_id not found",
                                            )
                                        })
                                        .collect::<Vec<_>>()
                                };

                                let args = sorted_results
                                    .into_iter()
                                    .map(|s| {
                                        let inner = s.result.as_ref().unwrap();
                                        let decoded: DagExecutionResponse<
                                            B::Compact,
                                            B::Context,
                                            B::IdType,
                                        > = B::Codec::decode(inner)
                                            .map_err(|e: CdcErr| e.into())?;
                                        match decoded {
                                            DagExecutionResponse::EnqueueNext {
                                                result,
                                                context,
                                            } => Ok(result),
                                            _ => Err(BoxDynError::from(
                                                "Unexpected response type from collector task",
                                            )),
                                        }
                                    })
                                    .collect::<Result<Vec<_>, BoxDynError>>()?;
                                let mut completed_nodes = collector_tasks
                                    .iter()
                                    .map(|(node, _)| *node)
                                    .collect::<HashSet<_>>();
                                completed_nodes.insert(context.current_node);

                                let task = TaskBuilder::new(
                                    B::Codec::encode(&args).map_err(|e| e.into())?,
                                )
                                .with_task_id(TaskId::new(B::IdType::generate()))
                                .meta(DagflowContext {
                                    current_node: outgoing_node,
                                    completed_nodes,
                                    node_task_ids: collector_tasks.clone().into_iter().collect(),
                                    current_position: context.current_position + 1,
                                    is_initial: false,
                                    root_task_id: context.root_task_id.clone(),
                                    _phantom: std::marker::PhantomData,
                                })
                                .build();
                                b.send(task).await.map_err(|e| BoxDynError::from(e))?;
                            }

                            println!("Collector results enqueued for next nodes");

                            Ok::<(), BoxDynError>(())
                        }
                    }
                    .boxed();
                    // Await all enqueue operations
                    enqueue_futures.push(collector_future);
                    futures::future::try_join_all(enqueue_futures).await?;
                }
                _ => { /* No action needed for other variants */ }
            }
            Ok(response)
        }
        .boxed()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_dag_executor_service() {
        // This would test the Service implementation
        // You would create a DagExecutor, create a Task, and call the service
    }
}
