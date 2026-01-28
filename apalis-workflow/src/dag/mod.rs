use std::{
    collections::{HashMap, VecDeque},
    fmt::{self, Debug},
    marker::PhantomData,
    sync::Mutex,
};

use apalis_core::{
    backend::{BackendExt, codec::Codec},
    error::BoxDynError,
    task::Task,
    task_fn::{TaskFn, task_fn},
};
use petgraph::{
    Direction,
    algo::toposort,
    dot::Config,
    graph::{DiGraph, EdgeIndex, NodeIndex},
};
/// DAG executor implementations
pub mod executor;
/// DAG service implementations
pub mod service;

/// DAG error definitions
pub mod error;
/// DAG node implementations
pub mod node;

/// DAG context implementations
pub mod context;

/// DAG response implementations
pub mod response;

/// DAG Decoding and encoding utilities
pub mod decode;

use serde::{Deserialize, Serialize};
use tower::{Service, util::BoxCloneSyncService};

use crate::{
    DagService,
    dag::{decode::DagCodec, error::DagFlowError, executor::DagExecutor, node::NodeService},
};

pub use context::DagFlowContext;
pub use service::RootDagService;

/// Directed Acyclic Graph (DAG) workflow builder
#[derive(Debug)]
pub struct DagFlow<B>
where
    B: BackendExt,
{
    name: String,
    graph: Mutex<DiGraph<DagService<B::Compact, B::Context, B::IdType>, ()>>,
    node_mapping: Mutex<HashMap<String, NodeIndex>>,
}

impl<B> fmt::Display for DagFlow<B>
where
    B: BackendExt,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "DAG name: {}", self.name)?;
        writeln!(f, "Dot format:")?;
        f.write_str(&self.to_dot())
    }
}

impl<B> DagFlow<B>
where
    B: BackendExt,
{
    /// Create a new DAG workflow builder
    #[must_use]
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_owned(),
            graph: Mutex::new(DiGraph::new()),
            node_mapping: Mutex::new(HashMap::new()),
        }
    }

    /// Add a node to the DAG
    #[must_use]
    pub fn add_node<S, Input, CodecError>(
        &self,
        name: &str,
        service: S,
    ) -> NodeBuilder<'_, Input, S::Response, B>
    where
        S: Service<Task<Input, B::Context, B::IdType>> + Send + 'static + Sync + Clone,
        S::Future: Send + 'static,
        B::Codec: Codec<Input, Compact = B::Compact, Error = CodecError>
            + Codec<S::Response, Compact = B::Compact, Error = CodecError>
            + 'static,
        CodecError: Into<BoxDynError> + Send + 'static,
        S::Error: Into<BoxDynError>,
        B: Send + Sync + 'static,
        Input: DagCodec<B, Error = CodecError> + Send + Sync + 'static,
    {
        let svc: NodeService<S, B, Input> = NodeService::new(service);
        let node = self
            .graph
            .lock()
            .expect("Failed to lock graph mutex")
            .add_node(BoxCloneSyncService::new(svc));
        self.node_mapping
            .lock()
            .expect("Failed to lock node_mapping mutex")
            .insert(name.to_owned(), node);
        NodeBuilder {
            id: node,
            dag: self,
            _phantom: PhantomData,
        }
    }

    /// Add a task function node to the DAG
    pub fn node<F, Input, O, FnArgs, Err, CodecError>(&self, node: F) -> NodeBuilder<'_, Input, O, B>
    where
        TaskFn<F, Input, B::Context, FnArgs>: Service<Task<Input, B::Context, B::IdType>, Response = O, Error = Err> + Clone,
        F: Send + 'static + Sync,
        Input: Send + 'static + Sync,
        FnArgs: Send + 'static + Sync,
        B::Context: Send + Sync + 'static,
        <TaskFn<F, Input, B::Context, FnArgs> as Service<Task<Input, B::Context, B::IdType>>>::Future:
            Send + 'static,
        B::Codec: Codec<Input, Compact = B::Compact, Error = CodecError> + 'static,
        B::Codec: Codec<O, Compact = B::Compact, Error = CodecError> + 'static,
        CodecError: Into<BoxDynError> + Send + 'static,
        Err: Into<BoxDynError>,
        B: Send + Sync + 'static,
        Input: DagCodec<B, Error = CodecError> + Send + Sync + 'static,

    {
        self.add_node(std::any::type_name::<F>(), task_fn(node))
    }

    /// Add a routing node to the DAG
    pub fn route<F, Input, O, FnArgs, Err, CodecError>(
        &self,
        router: F,
    ) -> NodeBuilder<'_, Input, O, B>
    where
        TaskFn<F, Input, B::Context, FnArgs>: Service<Task<Input, B::Context, B::IdType>, Response = O, Error = Err> + Clone,
        F: Send + 'static + Sync,
        Input: Send + 'static + Sync,
        FnArgs: Send + 'static + Sync,
        <TaskFn<F, Input, B::Context, FnArgs> as Service<Task<Input, B::Context, B::IdType>>>::Future:
            Send + 'static,
        O: Into<NodeIndex>,
        B::Context: Send + Sync + 'static,
        B::Codec: Codec<Input, Compact = B::Compact, Error = CodecError> + 'static,
        B::Codec: Codec<O, Compact = B::Compact, Error = CodecError> + 'static,
        CodecError: Into<BoxDynError> + Send + 'static,
        Err: Into<BoxDynError>,
        B: Send + Sync + 'static,
        Input: DagCodec<B, Error = CodecError> + Send + Sync + 'static,

    {
        self.add_node::<TaskFn<F, Input, B::Context, FnArgs>, Input, CodecError>(
            std::any::type_name::<F>(),
            task_fn(router),
        )
    }

    /// Validate the DAG for cycles
    pub fn validate(&self) -> Result<(), DagFlowError> {
        // Validate DAG (check for cycles)
        toposort(
            &*self.graph.lock().expect("Failed to lock graph mutex"),
            None,
        )
        .map_err(DagFlowError::CyclicDAG)?;
        Ok(())
    }

    /// Export the DAG to DOT format
    pub fn to_dot(&self) -> String {
        let names = self
            .node_mapping
            .lock()
            .expect("could not lock nodes")
            .iter()
            .map(|(name, &idx)| (idx, name.clone()))
            .collect::<HashMap<_, _>>();
        let get_node_attributes = |_, (index, _)| {
            format!(
                "label=\"{}\"",
                names.get(&index).cloned().unwrap_or_default()
            )
        };
        let graph = self.graph.lock().expect("could not lock graph");
        let dot = petgraph::dot::Dot::with_attr_getters(
            &*graph,
            &[Config::NodeNoLabel, Config::EdgeNoLabel],
            &|_, _| String::new(),
            &get_node_attributes,
        );
        format!("{dot:?}")
    }

    /// Build the DAG executor
    pub(crate) fn build(self) -> Result<DagExecutor<B>, DagFlowError> {
        // Validate DAG (check for cycles)
        let sorted = toposort(
            &*self.graph.lock().expect("Failed to lock graph mutex"),
            None,
        )
        .map_err(DagFlowError::CyclicDAG)?;

        fn find_edge_nodes<N, E>(graph: &DiGraph<N, E>, direction: Direction) -> Vec<NodeIndex> {
            graph
                .node_indices()
                .filter(|&n| graph.neighbors_directed(n, direction).count() == 0)
                .collect()
        }

        let graph = self
            .graph
            .into_inner()
            .expect("Failed to unlock graph mutex");

        Ok(DagExecutor {
            start_nodes: find_edge_nodes(&graph, Direction::Incoming),
            end_nodes: find_edge_nodes(&graph, Direction::Outgoing),
            graph,
            node_mapping: self
                .node_mapping
                .into_inner()
                .expect("Failed to unlock node_mapping mutex"),
            topological_order: sorted,
            not_ready: VecDeque::new(),
        })
    }
}

/// Builder for a node in the DAG
pub struct NodeBuilder<'a, Input, Output, B>
where
    B: BackendExt,
{
    pub(crate) id: NodeIndex,
    pub(crate) dag: &'a DagFlow<B>,
    _phantom: PhantomData<(Input, Output)>,
}

impl<'a, Input, Output, B> std::fmt::Debug for NodeBuilder<'a, Input, Output, B>
where
    B: BackendExt,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NodeBuilder")
            .field("id", &self.id)
            .finish_non_exhaustive()
    }
}

impl<'a, Input, Output, B> Clone for NodeBuilder<'a, Input, Output, B>
where
    B: BackendExt,
{
    fn clone(&self) -> Self {
        Self {
            id: self.id,
            dag: self.dag,
            _phantom: PhantomData,
        }
    }
}

impl<Input, Output, B> NodeBuilder<'_, Input, Output, B>
where
    B: BackendExt,
{
    /// Specify dependencies for this node
    #[allow(clippy::needless_pass_by_value)]
    pub fn depends_on<D>(self, deps: D) -> NodeHandle<Input, Output>
    where
        D: DepsCheck<Input>,
    {
        let mut edges = Vec::new();
        for dep in deps.to_node_indices() {
            edges.push(self.dag.graph.lock().unwrap().add_edge(dep, self.id, ()));
        }
        NodeHandle {
            id: self.id,
            edges,
            _phantom: PhantomData,
        }
    }
}

/// Handle for a node in the DAG
#[derive(Clone, Debug)]
pub struct NodeHandle<Input, Output> {
    pub(crate) id: NodeIndex,
    pub(crate) edges: Vec<EdgeIndex>,
    pub(crate) _phantom: PhantomData<(Input, Output)>,
}

impl<Input, Output> NodeHandle<Input, Output> {
    /// Get the node ID
    #[must_use]
    pub fn id(&self) -> NodeIndex {
        self.id
    }

    /// Get the edge IDs
    #[must_use]
    pub fn edges(&self) -> &[EdgeIndex] {
        &self.edges
    }
}

/// Trait for converting dependencies into node IDs
pub trait DepsCheck<Input> {
    /// Convert dependencies to node indices
    fn to_node_indices(&self) -> Vec<NodeIndex>;
}

impl DepsCheck<()> for () {
    fn to_node_indices(&self) -> Vec<NodeIndex> {
        Vec::new()
    }
}

impl<'a, Input, Output, B> DepsCheck<Output> for &NodeBuilder<'a, Input, Output, B>
where
    B: BackendExt,
{
    fn to_node_indices(&self) -> Vec<NodeIndex> {
        vec![self.id]
    }
}

impl<Input, Output> DepsCheck<Output> for &NodeHandle<Input, Output> {
    fn to_node_indices(&self) -> Vec<NodeIndex> {
        vec![self.id]
    }
}

impl<Input, Output> DepsCheck<Output> for (&NodeHandle<Input, Output>,) {
    fn to_node_indices(&self) -> Vec<NodeIndex> {
        vec![self.0.id]
    }
}

impl<'a, Input, Output, B> DepsCheck<Output> for (&NodeBuilder<'a, Input, Output, B>,)
where
    B: BackendExt,
{
    fn to_node_indices(&self) -> Vec<NodeIndex> {
        vec![self.0.id]
    }
}

impl<Output, T: DepsCheck<Output>> DepsCheck<Vec<Output>> for Vec<T> {
    fn to_node_indices(&self) -> Vec<NodeIndex> {
        self.iter()
            .flat_map(|item| item.to_node_indices())
            .collect()
    }
}

macro_rules! impl_deps_check {
    ($( $len:literal => ( $( $in:ident $out:ident $idx:tt ),+ ) ),+ $(,)?) => {
        $(
            impl<'a, $( $in, )+ $( $out, )+ B> DepsCheck<( $( $out, )+ )>
                for ( $( &NodeBuilder<'a, $in, $out, B>, )+ ) where B: BackendExt
            {
                fn to_node_indices(&self) -> Vec<NodeIndex> {
                    vec![ $( self.$idx.id ),+ ]
                }
            }

            impl<$( $in, )+ $( $out, )+> DepsCheck<( $( $out, )+ )>
                for ( $( &NodeHandle<$in, $out>, )+ )
            {
                fn to_node_indices(&self) -> Vec<NodeIndex> {
                    vec![ $( self.$idx.id ),+ ]
                }
            }
        )+
    };
}

impl_deps_check! {
    1 => (Input1 Output1 0),
    2 => (Input1 Output1 0, Input2 Output2 1),
    3 => (Input1 Output1 0, Input2 Output2 1, Input3 Output3 2),
    4 => (Input1 Output1 0, Input2 Output2 1, Input3 Output3 2, Input4 Output4 3),
    5 => (Input1 Output1 0, Input2 Output2 1, Input3 Output3 2, Input4 Output4 3, Input5 Output5 4),
    6 => (Input1 Output1 0, Input2 Output2 1, Input3 Output3 2, Input4 Output4 3, Input5 Output5 4, Input6 Output6 5),
    7 => (Input1 Output1 0, Input2 Output2 1, Input3 Output3 2, Input4 Output4 3, Input5 Output5 4, Input6 Output6 5, Input7 Output7 6),
    8 => (Input1 Output1 0, Input2 Output2 1, Input3 Output3 2, Input4 Output4 3, Input5 Output5 4, Input6 Output6 5, Input7 Output7 6, Input8 Output8 7),
}

/// State of the node in DAG execution
#[derive(Debug, Clone, Deserialize, Serialize)]
pub enum DagState {
    /// Unknown state
    Unknown,
    /// State for a single node
    SingleNode,
    /// Fan-in state to gather inputs
    FanIn,
    /// Fan-out state to distribute inputs
    FanOut,
}

#[cfg(test)]
mod tests {
    use std::num::ParseIntError;

    use apalis_core::{
        error::BoxDynError,
        task_fn::task_fn,
        worker::{
            builder::WorkerBuilder, context::WorkerContext, event::Event,
            ext::event_listener::EventListenerExt,
        },
    };
    use apalis_file_storage::JsonStorage;
    use petgraph::graph::NodeIndex;
    use serde::{Deserialize, Serialize};
    use serde_json::Value;

    use crate::WorkflowSink;

    use super::*;

    #[tokio::test]
    async fn test_basic_workflow() {
        let dag = DagFlow::new("sequential-workflow");
        let start = dag.add_node("start", task_fn(|task: u32| async move { task as usize }));
        let middle = dag
            .add_node(
                "middle",
                task_fn(|task: usize| async move { task.to_string() }),
            )
            .depends_on(&start);

        let _end = dag
            .add_node(
                "end",
                task_fn(|task: String, worker: WorkerContext| async move {
                    worker.stop().unwrap();
                    task.parse::<usize>()
                }),
            )
            .depends_on(&middle);

        println!("DAG in DOT format:\n{}", dag.to_dot());

        let mut backend = JsonStorage::new_temp().unwrap();

        backend.push_start(42).await.unwrap();

        let worker = WorkerBuilder::new("rango-tango")
            .backend(backend)
            .on_event(|ctx, ev| {
                println!("On Event = {ev:?}");
                if matches!(ev, Event::Error(_)) {
                    ctx.stop().unwrap();
                }
            })
            .build(dag);
        worker.run().await.unwrap();
    }

    #[tokio::test]
    async fn test_fan_out_workflow() {
        let dag = DagFlow::new("fan-out-workflow");
        let source = dag.add_node("source", task_fn(|task: u32| async move { task as usize }));
        let plus_one = dag
            .add_node("plus_one", task_fn(|task: usize| async move { task + 1 }))
            .depends_on(&source);

        let multiply = dag
            .add_node("multiply", task_fn(|task: usize| async move { task * 2 }))
            .depends_on(&source);
        let squared = dag
            .add_node("squared", task_fn(|task: usize| async move { task * task }))
            .depends_on(&source);

        let _collector = dag
            .add_node(
                "collector",
                task_fn(|task: (usize, usize, usize), w: WorkerContext| async move {
                    w.stop().unwrap();
                    task.0 + task.1 + task.2
                }),
            )
            .depends_on((&plus_one, &multiply, &squared));

        println!("DAG in DOT format:\n{}", dag.to_dot());

        let mut backend: JsonStorage<Value> = JsonStorage::new_temp().unwrap();

        backend.push_start(42).await.unwrap();

        let worker = WorkerBuilder::new("rango-tango")
            .backend(backend)
            .on_event(|ctx, ev| {
                println!("On Event = {ev:?}");
                if matches!(ev, Event::Error(_)) {
                    ctx.stop().unwrap();
                }
            })
            .build(dag);
        worker.run().await.unwrap();
    }

    #[tokio::test]
    async fn test_fan_in_workflow() {
        let dag = DagFlow::new("fan-in-workflow");
        let get_name = dag.add_node(
            "get_name",
            task_fn(|task: usize| async move { task as usize }),
        );
        let get_age = dag.add_node(
            "get_age",
            task_fn(|task: u32| async move { task.to_string() }),
        );
        let get_address = dag.add_node(
            "get_address",
            task_fn(|task: i32| async move { task as usize }),
        );
        let main_collector = dag
            .add_node(
                "main_collector",
                task_fn(|task: (String, usize, usize)| async move {
                    task.2 + task.1 + task.0.parse::<usize>().unwrap()
                }),
            )
            .depends_on((&get_age, &get_name, &get_address));

        let side_collector = dag
            .add_node(
                "side_collector",
                task_fn(
                    |task: (usize, usize)| async move { [task.0, task.1].iter().sum::<usize>() },
                ),
            )
            .depends_on((&get_name, &get_address));

        let _final_node = dag
            .add_node(
                "final_node",
                task_fn(|task: (usize, usize), w: WorkerContext| async move {
                    w.stop().unwrap();
                    task.0 + task.1
                }),
            )
            .depends_on((&main_collector, &side_collector));

        println!("DAG in DOT format:\n{}", dag.to_dot());

        let mut backend: JsonStorage<Value> = JsonStorage::new_temp().unwrap();

        backend
            .start_fan_out((42usize, 43u32, 44i32))
            .await
            .unwrap();

        let worker = WorkerBuilder::new("rango-tango")
            .backend(backend)
            .on_event(|ctx, ev| {
                println!("On Event = {ev:?}");
                if matches!(ev, Event::Error(_)) {
                    ctx.stop().unwrap();
                }
            })
            .build(dag);
        worker.run().await.unwrap();
    }

    #[tokio::test]
    async fn test_routed_workflow() {
        let dag = DagFlow::new("routed-workflow");

        let entry1 = dag.add_node("entry1", task_fn(|task: u32| async move { task as usize }));
        let entry2 = dag.add_node("entry2", task_fn(|task: u32| async move { task as usize }));
        let entry3 = dag.add_node("entry3", task_fn(|task: u32| async move { task as usize }));

        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
        enum EntryRoute {
            Entry1(NodeIndex),
            Entry2(NodeIndex),
            Entry3(NodeIndex),
        }

        impl Into<NodeIndex> for EntryRoute {
            fn into(self) -> NodeIndex {
                match self {
                    EntryRoute::Entry1(idx) => idx,
                    EntryRoute::Entry2(idx) => idx,
                    EntryRoute::Entry3(idx) => idx,
                }
            }
        }

        impl DepsCheck<usize> for EntryRoute {
            fn to_node_indices(&self) -> Vec<NodeIndex> {
                vec![(*self).into()]
            }
        }

        async fn collect(task: (usize, usize, usize)) -> usize {
            task.0 + task.1 + task.2
        }
        let collector = dag.node(collect).depends_on((&entry1, &entry2, &entry3));

        async fn vec_collect(task: Vec<usize>, _wrk: WorkerContext) -> usize {
            task.iter().sum::<usize>()
        }

        let vec_collector = dag
            .node(vec_collect)
            .depends_on(vec![&entry1, &entry2, &entry3]);

        async fn exit(task: (usize, usize)) -> Result<u32, ParseIntError> {
            (task.0.to_string() + &task.1.to_string()).parse()
        }

        let on_collect = dag.node(exit).depends_on((&collector, &vec_collector));

        async fn check_approval(
            task: u32,
            worker: WorkerContext,
        ) -> Result<EntryRoute, BoxDynError> {
            println!("Approval check for task: {}", task);
            worker.stop().unwrap();
            match task % 3 {
                0 => Ok(EntryRoute::Entry1(NodeIndex::new(0))),
                1 => Ok(EntryRoute::Entry2(NodeIndex::new(1))),
                2 => Ok(EntryRoute::Entry3(NodeIndex::new(2))),
                _ => Err(BoxDynError::from("Invalid task")),
            }
        }

        dag.route(check_approval).depends_on(&on_collect);

        println!("DAG in DOT format:\n{}", dag.to_dot());

        let mut backend = JsonStorage::new_temp().unwrap();

        backend.start_fan_out(vec![17, 18, 19]).await.unwrap();

        let worker = WorkerBuilder::new("rango-tango")
            .backend(backend)
            .on_event(|ctx, ev| {
                println!("On Event = {ev:?}");
                if matches!(ev, Event::Error(_)) {
                    ctx.stop().unwrap();
                }
            })
            .build(dag);
        worker.run().await.unwrap();
    }
}
