use std::{
    collections::{HashMap, VecDeque},
    fmt::{self, Debug},
    marker::PhantomData,
    sync::Mutex,
};

use apalis_core::{
    backend::{Backend, BackendConfig, WireFormatBackend, codec::Codec},
    error::BoxDynError,
    task::Task,
    task::task_fn::{TaskFn, task_fn},
};
use petgraph::{
    Direction,
    algo::toposort,
    dot::Config,
    graph::{DiGraph, EdgeIndex, NodeIndex},
};
/// Graph service implementations
pub mod service;

/// Graph error definitions
pub mod error;
/// Graph node implementations
pub mod node;

/// Graph context implementations
pub mod context;

/// Graph response implementations
pub mod response;

/// Graph Decoding and encoding utilities
pub mod decode;

use serde::{Serialize, de::DeserializeOwned};
use serde_json::Value;
use tower::{Service, util::BoxCloneSyncService};

use crate::{
    NodeService,
    graph::{decode::GraphCodec, error::GraphFlowError, node::GraphNodeService},
};

pub use context::GraphFlowContext;
pub use service::RootGraphService;

/// Directed Acyclic Graph (Graph) workflow builder
#[derive(Debug)]
pub struct GraphFlow<B>
where
    B: WireFormatBackend,
{
    pub(crate) name: String,
    graph: Mutex<DiGraph<NodeService<B::Compact>, ()>>,
    node_mapping: Mutex<HashMap<String, NodeIndex>>,
}

impl<B> fmt::Display for GraphFlow<B>
where
    B: Backend + WireFormatBackend,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "Graph name: {}", self.name)?;
        writeln!(f, "Dot format:")?;
        f.write_str(&self.to_dot())
    }
}

impl<B> GraphFlow<B>
where
    B: Backend + WireFormatBackend,
{
    /// Create a new Graph workflow builder
    #[must_use]
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_owned(),
            graph: Mutex::new(DiGraph::new()),
            node_mapping: Mutex::new(HashMap::new()),
        }
    }
    /// Create a new Graph workflow builder with typed inputs and output
    #[must_use]
    pub fn new_typed(name: &str) -> Self {
        Self {
            name: name.to_owned(),
            graph: Mutex::new(DiGraph::new()),
            node_mapping: Mutex::new(HashMap::new()),
        }
    }

    /// Export the Graph to DOT format
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
}

impl<B> GraphFlow<B>
where
    B: Backend + WireFormatBackend,
{
    /// Add a named node to the Graph
    #[must_use]
    pub fn add_node<S, Input, CodecError>(
        &self,
        name: &str,
        service: S,
    ) -> NodeBuilder<'_, Input, S::Response, B>
    where
        S: Service<Task<Input>> + Send + 'static + Sync + Clone,
        S::Future: Send + 'static,
        B::Codec: Codec<Input, Compact = B::Compact, Error = CodecError>
            + Codec<S::Response, Compact = B::Compact, Error = CodecError>
            + Send
            + Sync
            + Clone
            + 'static,
        CodecError: Into<BoxDynError> + Send + 'static,
        S::Error: Into<BoxDynError>,
        B: BackendConfig + Send + Sync + 'static,
        Input: GraphCodec<B, Error = CodecError> + Send + Sync + 'static + DeserializeOwned,
        S::Response: Serialize,
    {
        let svc: GraphNodeService<S, B, Input> = GraphNodeService::new(service);
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
            graph: self,
            _phantom: PhantomData,
        }
    }

    /// Add a task function node to the Graph
    pub fn add_task<F, Input, O, FnArgs, Err, CodecError>(
        &self,
        task: F,
    ) -> NodeBuilder<'_, Input, O, B>
    where
        TaskFn<F, Input, FnArgs>: Service<Task<Input>, Response = O, Error = Err> + Clone,
        F: Send + 'static + Sync,
        Input: Send + 'static + Sync,
        FnArgs: Send + 'static + Sync,
        <TaskFn<F, Input, FnArgs> as Service<Task<Input>>>::Future: Send + 'static,
        B::Codec: Codec<Input, Compact = B::Compact, Error = CodecError> + 'static,
        B::Codec:
            Codec<O, Compact = B::Compact, Error = CodecError> + Send + Sync + Clone + 'static,
        CodecError: Into<BoxDynError> + Send + 'static,
        Err: Into<BoxDynError>,
        B: BackendConfig + Send + Sync + 'static,
        Input: GraphCodec<B, Error = CodecError> + Send + Sync + DeserializeOwned + 'static,
        O: Serialize,
    {
        self.add_node(std::any::type_name::<F>(), task_fn(task))
    }

    /// Add a routing node to the Graph
    pub fn route<F, Input, O, FnArgs, Err, CodecError>(
        &self,
        router: F,
    ) -> NodeBuilder<'_, Input, O, B>
    where
        TaskFn<F, Input, FnArgs>: Service<Task<Input>, Response = O, Error = Err> + Clone,
        F: Send + 'static + Sync,
        Input: Send + 'static + Sync,
        FnArgs: Send + 'static + Sync,
        <TaskFn<F, Input, FnArgs> as Service<Task<Input>>>::Future: Send + 'static,
        O: Into<NodeIndex> + Serialize,
        B::Codec: Codec<Input, Compact = B::Compact, Error = CodecError> + 'static,
        B::Codec:
            Codec<O, Compact = B::Compact, Error = CodecError> + Send + Sync + Clone + 'static,
        CodecError: Into<BoxDynError> + Send + 'static,
        Err: Into<BoxDynError>,
        B: BackendConfig + Send + Sync + 'static,
        Input: GraphCodec<B, Error = CodecError> + Send + Sync + 'static + DeserializeOwned,
    {
        self.add_node::<TaskFn<F, Input, FnArgs>, Input, CodecError>(
            std::any::type_name::<F>(),
            task_fn(router),
        )
    }

    /// Validate the Graph for cycles
    pub fn validate(&self) -> Result<(), GraphFlowError> {
        // Validate Graph (check for cycles)
        toposort(
            &*self.graph.lock().expect("Failed to lock graph mutex"),
            None,
        )
        .map_err(GraphFlowError::CyclicDAG)?;
        Ok(())
    }

    /// Build the Graph executor
    pub(crate) fn build(self, backend: B) -> Result<RootGraphService<B>, GraphFlowError> {
        // Validate Graph (check for cycles)
        let sorted = toposort(
            &*self.graph.lock().expect("Failed to lock graph mutex"),
            None,
        )
        .map_err(GraphFlowError::CyclicDAG)?;

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

        Ok(RootGraphService {
            start_nodes: find_edge_nodes(&graph, Direction::Incoming),
            end_nodes: find_edge_nodes(&graph, Direction::Outgoing),
            graph,
            node_mapping: self
                .node_mapping
                .into_inner()
                .expect("Failed to unlock node_mapping mutex"),
            topological_order: sorted,
            not_ready: VecDeque::new(),
            backend,
        })
    }
}

/// Builder for a node in the Graph
pub struct NodeBuilder<'a, Input, Output, B>
where
    B: Backend + WireFormatBackend,
{
    pub(crate) id: NodeIndex,
    pub(crate) graph: &'a GraphFlow<B>,
    _phantom: PhantomData<(Input, Output)>,
}

impl<'a, Input, Output, B> std::fmt::Debug for NodeBuilder<'a, Input, Output, B>
where
    B: Backend + WireFormatBackend,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NodeBuilder")
            .field("id", &self.id)
            .finish_non_exhaustive()
    }
}

impl<'a, Input, Output, B> Clone for NodeBuilder<'a, Input, Output, B>
where
    B: Backend + WireFormatBackend,
{
    fn clone(&self) -> Self {
        Self {
            id: self.id,
            graph: self.graph,
            _phantom: PhantomData,
        }
    }
}

impl<Input, Output, B> NodeBuilder<'_, Input, Output, B>
where
    B: Backend + WireFormatBackend,
{
    /// Specify dependencies for this node
    #[allow(clippy::needless_pass_by_value)]
    pub fn depends_on<D>(self, deps: D) -> NodeHandle<Input, Output>
    where
        D: DepsCheck<Input>,
    {
        let mut edges = Vec::new();
        for dep in deps.to_node_indices() {
            edges.push(self.graph.graph.lock().unwrap().add_edge(dep, self.id, ()));
        }
        NodeHandle {
            id: self.id,
            edges,
            _phantom: PhantomData,
        }
    }
}

/// Handle for a node in the Graph
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

impl<'a, Input, Output, B> DepsCheck<Output> for &NodeBuilder<'a, Input, Output, B>
where
    B: Backend + WireFormatBackend,
{
    fn to_node_indices(&self) -> Vec<NodeIndex> {
        vec![self.id]
    }
}

impl<'a, Input, Output, B> DepsCheck<Output> for NodeBuilder<'a, Input, Output, B>
where
    B: Backend + WireFormatBackend,
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

impl<Input, Output> DepsCheck<Output> for NodeHandle<Input, Output> {
    fn to_node_indices(&self) -> Vec<NodeIndex> {
        vec![self.id]
    }
}

impl<Input, Output> DepsCheck<Output> for (&NodeHandle<Input, Output>,) {
    fn to_node_indices(&self) -> Vec<NodeIndex> {
        vec![self.0.id]
    }
}

impl<Input, Output> DepsCheck<Output> for (NodeHandle<Input, Output>,) {
    fn to_node_indices(&self) -> Vec<NodeIndex> {
        vec![self.0.id]
    }
}

impl<'a, Input, Output, B> DepsCheck<Output> for (NodeBuilder<'a, Input, Output, B>,)
where
    B: Backend + WireFormatBackend,
{
    fn to_node_indices(&self) -> Vec<NodeIndex> {
        vec![self.0.id]
    }
}

impl<'a, Input, Output, B> DepsCheck<Output> for (&NodeBuilder<'a, Input, Output, B>,)
where
    B: Backend + WireFormatBackend,
{
    fn to_node_indices(&self) -> Vec<NodeIndex> {
        vec![self.0.id]
    }
}

impl<Output, T: DepsCheck<Output>> DepsCheck<Vec<Output>> for Vec<T> {
    fn to_node_indices(&self) -> Vec<NodeIndex> {
        assert!(self.len() > 1, "dependencies should be more than one");
        self.iter()
            .flat_map(|item| item.to_node_indices())
            .collect()
    }
}

macro_rules! impl_deps_check {
    ($( $len:literal => ( $( $in:ident $out:ident $idx:tt ),+ ) ),+ $(,)?) => {
        $(
            impl<'a, $( $in, )+ $( $out, )+ B> DepsCheck<( $( $out, )+ )>
                for ( $( &NodeBuilder<'a, $in, $out, B>, )+ ) where B: Backend + WireFormatBackend
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

/// Input for graph nodes
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum NodeInput<Compact> {
    /// A single node with compact data
    Single(Compact),
    /// Fan in input from previous nodes
    FanIn(Vec<Value>),
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, num::ParseIntError};

    use apalis_core::{
        error::BoxDynError,
        task::task_fn::task_fn,
        worker::{
            builder::WorkerBuilder, context::WorkerContext, event::Event,
            ext::event_listener::EventListenerExt,
        },
    };
    use apalis_file_storage::JsonStorage;
    use futures_util::StreamExt;
    use petgraph::graph::NodeIndex;
    use serde::{Deserialize, Serialize};
    use serde_json::Value;

    use crate::{WorkflowSink, graph::response::GraphNodeResponse, in_memory::InMemoryWorkflow};

    use super::*;

    #[tokio::test]
    async fn test_basic_workflow() {
        let graph = GraphFlow::new("sequential-workflow");
        let start = graph.add_node("start", task_fn(|task: u32| async move { task as usize }));
        let middle = graph
            .add_node(
                "middle",
                task_fn(|task: usize| async move { task.to_string() }),
            )
            .depends_on(&start);

        let _end = graph
            .add_node(
                "end",
                task_fn(|task: String, worker: WorkerContext| async move {
                    worker.stop().unwrap();
                    task.parse::<usize>()
                }),
            )
            .depends_on(&middle);

        println!("Graph in DOT format:\n{}", graph.to_dot());

        let mut backend = JsonStorage::new_temp().unwrap();

        backend.push_start(42).await.unwrap();

        let worker = WorkerBuilder::new("rango-tango")
            .backend(backend)
            .on_event(|worker, ev| {
                println!("On Event = {ev:?}");
                if matches!(ev, Event::Error(_)) {
                    worker.stop().unwrap();
                }
            })
            .build(graph);
        worker.run().await.unwrap();
    }

    #[tokio::test]
    async fn test_fan_out_workflow() {
        let graph = GraphFlow::new("fan-out-workflow");
        let source = graph.add_node("source", task_fn(|task: u32| async move { task as usize }));
        let plus_one = graph
            .add_node("plus_one", task_fn(|task: usize| async move { task + 1 }))
            .depends_on(&source);

        let multiply = graph
            .add_node("multiply", task_fn(|task: usize| async move { task * 2 }))
            .depends_on(&source);
        let squared = graph
            .add_node("squared", task_fn(|task: usize| async move { task * task }))
            .depends_on(&source);

        let _collector = graph
            .add_node(
                "collector",
                task_fn(|task: (usize, usize, usize), w: WorkerContext| async move {
                    w.stop().unwrap();
                    task.0 + task.1 + task.2
                }),
            )
            .depends_on((&plus_one, &multiply, &squared));

        println!("Graph in DOT format:\n{}", graph.to_dot());

        let mut backend = InMemoryWorkflow::create();

        backend.push_start(42).await.unwrap();

        let worker = WorkerBuilder::new("rango-tango")
            .backend(backend)
            .on_event(|_w, ev| {
                println!("On Event = {ev:?}");
                if matches!(ev, Event::Error(_)) {
                    panic!("{}", ev);
                }
            })
            .build(graph);
        worker.run().await.unwrap();
    }

    #[tokio::test]
    async fn test_fan_in_workflow() {
        let graph = GraphFlow::new("fan-in-workflow");
        let get_name = graph.add_node(
            "get_name",
            task_fn(|task: usize| async move { task as usize }),
        );
        let get_age = graph.add_node(
            "get_age",
            task_fn(|task: u32| async move { task.to_string() }),
        );
        let get_address = graph.add_node(
            "get_address",
            task_fn(|task: i32| async move { task as usize }),
        );
        let main_collector = graph
            .add_node(
                "main_collector",
                task_fn(|task: (String, usize, usize)| async move {
                    task.2 + task.1 + task.0.parse::<usize>().unwrap()
                }),
            )
            .depends_on((&get_age, &get_name, &get_address));

        let side_collector = graph
            .add_node(
                "side_collector",
                task_fn(
                    |task: (usize, usize)| async move { [task.0, task.1].iter().sum::<usize>() },
                ),
            )
            .depends_on((&get_name, &get_address));

        let _final_node = graph
            .add_node(
                "final_node",
                task_fn(|task: (usize, usize), w: WorkerContext| async move {
                    w.stop().unwrap();
                    task.0 + task.1
                }),
            )
            .depends_on((&main_collector, &side_collector));

        println!("Graph in DOT format:\n{}", graph.to_dot());

        let mut backend: JsonStorage<Value> = JsonStorage::new_temp().unwrap();

        backend
            .start_fan_out((42usize, 43u32, 44i32))
            .await
            .unwrap();

        let worker = WorkerBuilder::new("rango-tango")
            .backend(backend)
            .on_event(|_w, ev| {
                println!("On Event = {ev:?}");
                if matches!(ev, Event::Error(_)) {
                    panic!("{}", ev);
                }
            })
            .build(graph);
        worker.run().await.unwrap();
    }

    #[tokio::test]
    async fn fan_in_completes_once_with_testworker() {
        use apalis_core::task::task_fn::task_fn;
        use apalis_file_storage::JsonStorage;
        use serde_json::Value;

        let graph = GraphFlow::new("fan-in-testworker");

        let a = graph.add_node("a", task_fn(|t: u32| async move { t }));
        let b = graph.add_node("b", task_fn(|t: u32| async move { t }));

        let _fan_in = graph
            .add_node("fan_in", task_fn(|t: (u32, u32)| async move { t.0 + t.1 }))
            .depends_on((&a, &b));

        let mut backend: JsonStorage<Value> = JsonStorage::new_temp().unwrap();
        backend.start_fan_out((1u32, 2u32)).await.unwrap();

        let worker = WorkerBuilder::new("test-worker")
            .backend(backend)
            .build(graph)
            .stream();

        let (res_map, final_res) = worker.take(6).collect::<Vec<_>>().await.into_iter().fold(
            (BTreeMap::new(), None),
            |(mut res_map, final_res), item| {
                let resp = match item {
                    Ok(Event::Success(res)) => res.downcast::<GraphNodeResponse>().unwrap(),
                    Ok(Event::Error(e)) => panic!("execution error {e}"),
                    Ok(_) => return (res_map, final_res),
                    Err(e) => panic!("worker error: {e:?}"),
                };

                let kind = match *resp {
                    GraphNodeResponse::EntryFanOut { .. } => "EntryFanOut",
                    GraphNodeResponse::FanOut { .. } => "FanOut",
                    GraphNodeResponse::EnqueuedNext { .. } => "EnqueuedNext",
                    GraphNodeResponse::WaitingForDependencies { .. } => "WaitingForDependencies",
                    GraphNodeResponse::Complete { .. } => "Complete",
                };
                *res_map.entry(kind).or_insert(0) += 1;

                if let GraphNodeResponse::Complete { result } = *resp {
                    return (res_map, Some(result));
                }
                (res_map, final_res)
            },
        );

        assert_eq!(final_res, Some(Value::from(3)));
        assert_eq!(res_map["EntryFanOut"], 1); // Entry
        assert_eq!(res_map["EnqueuedNext"], 2); // Node Results
        assert_eq!(res_map["WaitingForDependencies"], 1); // Non Handler node
        assert_eq!(res_map["Complete"], 1); // Handler Node
    }

    #[tokio::test]
    async fn test_routed_workflow() {
        let graph = GraphFlow::new("routed-workflow");

        let entry1 = graph.add_node("entry1", task_fn(|task: u32| async move { task as usize }));
        let entry2 = graph.add_node("entry2", task_fn(|task: u32| async move { task as usize }));
        let entry3 = graph.add_node("entry3", task_fn(|task: u32| async move { task as usize }));

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
        let collector = graph
            .add_task(collect)
            .depends_on((&entry1, &entry2, &entry3));

        async fn vec_collect(task: Vec<usize>, _wrk: WorkerContext) -> usize {
            task.iter().sum::<usize>()
        }

        let vec_collector = graph
            .add_task(vec_collect)
            .depends_on(vec![&entry1, &entry2, &entry3]);

        async fn exit(task: (usize, usize)) -> Result<u32, ParseIntError> {
            (task.0.to_string() + &task.1.to_string()).parse()
        }

        let on_collect = graph
            .add_task(exit)
            .depends_on((&collector, &vec_collector));

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

        let _ = graph.route(check_approval).depends_on(&on_collect);

        println!("Graph in DOT format:\n{}", graph.to_dot());

        let mut backend = JsonStorage::new_temp().unwrap();

        backend.start_fan_out(vec![17, 18, 19]).await.unwrap();

        let worker = WorkerBuilder::new("rango-tango")
            .backend(backend)
            .on_event(|_, ev| {
                println!("On Event = {ev:?}");
                if matches!(ev, Event::Error(_)) {
                    panic!("{}", ev);
                }
            })
            .build(graph);
        worker.run().await.unwrap();
    }
}
