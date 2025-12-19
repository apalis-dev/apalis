use std::{
    collections::{HashMap, VecDeque},
    fmt::Debug,
    marker::PhantomData,
    sync::Mutex,
};

use apalis_core::{
    backend::{
        Backend, BackendExt, WaitForCompletion,
        codec::{Codec, RawDataBackend},
    },
    error::BoxDynError,
    task::{Task, metadata::MetadataExt},
    task_fn::{TaskFn, task_fn},
    worker::builder::{IntoWorkerService, WorkerService},
};
use futures::Sink;
use petgraph::{
    Direction,
    algo::toposort,
    dot::Config,
    graph::{DiGraph, EdgeIndex, NodeIndex},
};
mod service;
use tower::{Service, ServiceBuilder, util::BoxCloneSyncService};

use crate::{
    BoxedService, DagService, dag::service::DagExecutionResponse, id_generator::GenerateId,
};

pub use service::DagflowContext;
pub use service::RootDagService;

/// Directed Acyclic Graph (DAG) workflow builder
#[derive(Debug)]
pub struct DagFlow<B>
where
    B: BackendExt,
{
    graph: Mutex<DiGraph<DagService<B::Compact, B::Context, B::IdType>, ()>>,
    node_mapping: Mutex<HashMap<String, NodeIndex>>,
}

impl<B> Default for DagFlow<B>
where
    B: BackendExt,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<B> DagFlow<B>
where
    B: BackendExt,
{
    /// Create a new DAG workflow builder
    #[must_use]
    pub fn new() -> Self {
        Self {
            graph: Mutex::new(DiGraph::new()),
            node_mapping: Mutex::new(HashMap::new()),
        }
    }

    /// Add a node to the DAG
    #[must_use]
    #[allow(clippy::todo)]
    pub fn add_node<S, Input>(
        &self,
        name: &str,
        service: S,
    ) -> NodeBuilder<'_, Input, S::Response, B>
    where
        S: Service<Task<Input, B::Context, B::IdType>> + Send + 'static + Sync + Clone,
        S::Future: Send + 'static,
        B::Codec:
            Codec<Input, Compact = B::Compact> + Codec<S::Response, Compact = B::Compact> + 'static,
        <B::Codec as Codec<Input>>::Error: Debug,
        <B::Codec as Codec<S::Response>>::Error: Debug,
        S::Error: Into<BoxDynError>,
        B::Compact: Debug,
    {
        let svc = ServiceBuilder::new()
            .map_request(|r: Task<B::Compact, B::Context, B::IdType>| {
                r.map(|r| B::Codec::decode(&r).unwrap())
            })
            .map_response(|r: S::Response| B::Codec::encode(&r).unwrap())
            .map_err(|e: S::Error| {
                let boxed: BoxDynError = e.into();
                boxed
            })
            .service(service);
        let node = self
            .graph
            .lock()
            .unwrap()
            .add_node(BoxCloneSyncService::new(svc));
        self.node_mapping
            .lock()
            .unwrap()
            .insert(name.to_owned(), node);
        NodeBuilder {
            id: node,
            dag: self,
            _phantom: PhantomData,
        }
    }

    /// Add a task function node to the DAG
    pub fn node<F, Input, O, FnArgs, Err>(&self, node: F) -> NodeBuilder<'_, Input, O, B>
    where
        TaskFn<F, Input, B::Context, FnArgs>: Service<Task<Input, B::Context, B::IdType>, Response = O, Error = Err> + Clone,
        F: Send + 'static + Sync,
        Input: Send + 'static + Sync,
        FnArgs: Send + 'static + Sync,
        B::Context: Send + Sync + 'static,
        <TaskFn<F, Input, B::Context, FnArgs> as Service<Task<Input, B::Context, B::IdType>>>::Future:
            Send + 'static,
            B::Codec: Codec<Input, Compact = B::Compact> + 'static,
        <B::Codec as Codec<Input>>::Error: Debug,
        B::Codec: Codec<Input, Compact = B::Compact> + 'static,
        B::Codec: Codec<O, Compact = B::Compact> + 'static,
        <B::Codec as Codec<Input>>::Error: Debug,
        <B::Codec as Codec<O>>::Error: Debug,
        Err: Into<BoxDynError>,
        B::Compact: Debug

    {
        self.add_node(std::any::type_name::<F>(), task_fn(node))
    }

    /// Add a routing node to the DAG
    pub fn route<F, Input, O, FnArgs, Err>(
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
        B::Codec: Codec<Input, Compact = B::Compact> + 'static,
        B::Codec: Codec<O, Compact = B::Compact> + 'static,
        <B::Codec as Codec<Input>>::Error: Debug,
        <B::Codec as Codec<O>>::Error: Debug,
        Err: Into<BoxDynError>,
        B::Compact: Debug

    {
        self.add_node::<TaskFn<F, Input, B::Context, FnArgs>, Input>(
            std::any::type_name::<F>(),
            task_fn(router),
        )
    }

    /// Build the DAG executor
    pub fn build(self) -> Result<DagExecutor<B>, String> {
        // Validate DAG (check for cycles)
        let sorted =
            toposort(&*self.graph.lock().unwrap(), None).map_err(|_| "DAG contains cycles")?;

        fn find_edge_nodes<N, E>(graph: &DiGraph<N, E>, direction: Direction) -> Vec<NodeIndex> {
            graph
                .node_indices()
                .filter(|&n| graph.neighbors_directed(n, direction).count() == 0)
                .collect()
        }

        let graph = self.graph.into_inner().unwrap();

        Ok(DagExecutor {
            start_nodes: find_edge_nodes(&graph, Direction::Incoming),
            end_nodes: find_edge_nodes(&graph, Direction::Outgoing),
            graph,
            node_mapping: self.node_mapping.into_inner().unwrap(),
            topological_order: sorted,
            not_ready: VecDeque::new(),
        })
    }
}

/// Executor for DAG workflows

#[derive(Debug)]
pub struct DagExecutor<B>
where
    B: BackendExt,
{
    graph: DiGraph<DagService<B::Compact, B::Context, B::IdType>, ()>,
    node_mapping: HashMap<String, NodeIndex>,
    topological_order: Vec<NodeIndex>,
    start_nodes: Vec<NodeIndex>,
    end_nodes: Vec<NodeIndex>,
    not_ready: VecDeque<NodeIndex>,
}

impl<B> Clone for DagExecutor<B>
where
    B: BackendExt,
{
    fn clone(&self) -> Self {
        Self {
            graph: self.graph.clone(),
            node_mapping: self.node_mapping.clone(),
            topological_order: self.topological_order.clone(),
            start_nodes: self.start_nodes.clone(),
            end_nodes: self.end_nodes.clone(),
            not_ready: self.not_ready.clone(),
        }
    }
}

impl<B> DagExecutor<B>
where
    B: BackendExt,
{
    /// Get a node by name
    pub fn get_node_by_name_mut(
        &mut self,
        name: &str,
    ) -> Option<&mut DagService<B::Compact, B::Context, B::IdType>> {
        self.node_mapping
            .get(name)
            .and_then(|&idx| self.graph.node_weight_mut(idx))
    }

    /// Export the DAG to DOT format
    #[must_use]
    pub fn to_dot(&self) -> String {
        let names = self
            .node_mapping
            .iter()
            .map(|(name, &idx)| (idx, name.clone()))
            .collect::<HashMap<_, _>>();
        let get_node_attributes = |_, (index, _)| {
            format!(
                "label=\"{}\"",
                names.get(&index).cloned().unwrap_or_default()
            )
        };
        let dot = petgraph::dot::Dot::with_attr_getters(
            &self.graph,
            &[Config::NodeNoLabel, Config::EdgeNoLabel],
            &|_, _| String::new(),
            &get_node_attributes,
        );
        format!("{dot:?}")
    }
}

impl<B, Compact, Err, CdcErr> IntoWorkerService<B, RootDagService<B>, B::Compact, B::Context>
    for DagExecutor<B>
where
    B: BackendExt<Compact = Compact>
        + Send
        + Sync
        + 'static
        + Sink<Task<Compact, B::Context, B::IdType>, Error = Err>
        + Unpin
        + Clone
        + WaitForCompletion<Compact>,
    Err: std::error::Error + Send + Sync + 'static,
    B::Context: MetadataExt<DagflowContext<B::Context, B::IdType>> + Send + Sync + 'static,
    B::IdType: Send + Sync + 'static + Default + GenerateId + PartialEq,
    B: Sync + Backend<Args = Compact, Error = Err>,
    B::Compact: Send + Sync + 'static + Clone, // Remove on compact
    // B::Context: Clone,
    <B::Context as MetadataExt<DagflowContext<B::Context, B::IdType>>>::Error: Into<BoxDynError>,
    B::Codec: Codec<Vec<Compact>, Compact = Compact, Error = CdcErr> + 'static,
    CdcErr: Into<BoxDynError>,
    <B as BackendExt>::Codec: Codec<
            DagExecutionResponse<Compact, <B as Backend>::Context, <B as Backend>::IdType>,
            Compact = Compact,
            Error = CdcErr,
        >,
{
    type Backend = RawDataBackend<B>;
    fn into_service(self, b: B) -> WorkerService<RawDataBackend<B>, RootDagService<B>> {
        WorkerService {
            backend: RawDataBackend::new(b.clone()),
            service: RootDagService::new(self, b),
        }
    }
}

/// Builder for a node in the DAG
#[derive(Clone)]
pub struct NodeBuilder<'a, Input, Output, B>
where
    B: BackendExt,
{
    pub(crate) id: NodeIndex,
    pub(crate) dag: &'a DagFlow<B>,
    _phantom: PhantomData<(Input, Output)>,
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
        for dep in deps.to_node_ids() {
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

/// Trait for converting dependencies into node IDs
pub trait DepsCheck<Input> {
    /// Convert dependencies to node IDs
    fn to_node_ids(&self) -> Vec<NodeIndex>;
}

impl DepsCheck<()> for () {
    fn to_node_ids(&self) -> Vec<NodeIndex> {
        Vec::new()
    }
}

impl<'a, Input, Output, B> DepsCheck<Output> for &NodeBuilder<'a, Input, Output, B>
where
    B: BackendExt,
{
    fn to_node_ids(&self) -> Vec<NodeIndex> {
        vec![self.id]
    }
}

impl<Input, Output> DepsCheck<Output> for &NodeHandle<Input, Output> {
    fn to_node_ids(&self) -> Vec<NodeIndex> {
        vec![self.id]
    }
}

impl<Input, Output> DepsCheck<Output> for (&NodeHandle<Input, Output>,) {
    fn to_node_ids(&self) -> Vec<NodeIndex> {
        vec![self.0.id]
    }
}

impl<'a, Input, Output, B> DepsCheck<Output> for (&NodeBuilder<'a, Input, Output, B>,)
where
    B: BackendExt,
{
    fn to_node_ids(&self) -> Vec<NodeIndex> {
        vec![self.0.id]
    }
}

impl<Output, T: DepsCheck<Output>> DepsCheck<Vec<Output>> for Vec<T> {
    fn to_node_ids(&self) -> Vec<NodeIndex> {
        self.iter().flat_map(|item| item.to_node_ids()).collect()
    }
}

macro_rules! impl_deps_check {
    ($( $len:literal => ( $( $in:ident $out:ident $idx:tt ),+ ) ),+ $(,)?) => {
        $(
            impl<'a, $( $in, )+ $( $out, )+ B> DepsCheck<( $( $out, )+ )>
                for ( $( &NodeBuilder<'a, $in, $out, B>, )+ ) where B: BackendExt
            {
                fn to_node_ids(&self) -> Vec<NodeIndex> {
                    vec![ $( self.$idx.id ),+ ]
                }
            }

            impl<$( $in, )+ $( $out, )+> DepsCheck<( $( $out, )+ )>
                for ( $( &NodeHandle<$in, $out>, )+ )
            {
                fn to_node_ids(&self) -> Vec<NodeIndex> {
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

#[cfg(test)]
mod tests {
    use std::{
        collections::HashMap, marker::PhantomData, num::ParseIntError, ops::Range, time::Duration,
    };

    use apalis_core::{
        error::BoxDynError,
        task::{Task, builder::TaskBuilder, task_id::RandomId},
        task_fn::task_fn,
        worker::{
            builder::WorkerBuilder, context::WorkerContext, event::Event,
            ext::event_listener::EventListenerExt,
        },
    };
    use apalis_file_storage::{JsonMapMetadata, JsonStorage};
    use petgraph::graph::NodeIndex;
    use serde::{Deserialize, Serialize};
    use serde_json::Value;

    use crate::{WorkflowSink, step::Identity, workflow::Workflow};

    use super::*;

    #[tokio::test]
    async fn test_basic_workflow() {
        let dag = DagFlow::new();
        let start = dag.add_node("start", task_fn(|task: u32| async move { task as usize }));
        let middle = dag
            .add_node(
                "middle",
                task_fn(|task: usize| async move { task.to_string() }),
            )
            .depends_on(&start);

        let end = dag
            .add_node(
                "end",
                task_fn(|task: String, worker: WorkerContext| async move {
                    worker.stop().unwrap();
                    task.parse::<usize>()
                }),
            )
            .depends_on(&middle);
        let dag_executor = dag.build().unwrap();
        assert_eq!(dag_executor.topological_order.len(), 3);

        println!("DAG in DOT format:\n{}", dag_executor.to_dot());

        let mut backend: JsonStorage<Value> = JsonStorage::new_temp().unwrap();

        backend.push_start(Value::from(42)).await.unwrap();

        let worker = WorkerBuilder::new("rango-tango")
            .backend(backend)
            .on_event(|ctx, ev| {
                println!("On Event = {ev:?}");
                if matches!(ev, Event::Error(_)) {
                    ctx.stop().unwrap();
                }
            })
            .build::<DagExecutor<JsonStorage<Value>>, RootDagService<JsonStorage<Value>>>(
                dag_executor,
            );
        worker.run().await.unwrap();
    }

    #[tokio::test]
    async fn test_fan_out_workflow() {
        let dag = DagFlow::new();
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
        let dag_executor = dag.build().unwrap();
        assert_eq!(dag_executor.topological_order.len(), 4);

        println!("DAG in DOT format:\n{}", dag_executor.to_dot());

        let mut backend: JsonStorage<Value> = JsonStorage::new_temp().unwrap();

        backend.push_start(Value::from(42)).await.unwrap();

        let worker = WorkerBuilder::new("rango-tango")
            .backend(backend)
            .on_event(|ctx, ev| {
                println!("On Event = {ev:?}");
                if matches!(ev, Event::Error(_)) {
                    ctx.stop().unwrap();
                }
            })
            .build::<DagExecutor<JsonStorage<Value>>, RootDagService<JsonStorage<Value>>>(
                dag_executor,
            );
        worker.run().await.unwrap();
    }

    #[tokio::test]
    async fn test_fan_in_workflow() {
        let dag = DagFlow::new();
        let get_name = dag.add_node(
            "get_name",
            task_fn(|task: u32| async move { task as usize }),
        );
        let get_age = dag.add_node(
            "get_age",
            task_fn(|task: u32| async move { task.to_string() }),
        );
        let get_address = dag.add_node(
            "get_address",
            task_fn(|task: u32| async move { task as usize }),
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
                task_fn(|task: Vec<usize>| async move { task.iter().sum::<usize>() }),
            )
            .depends_on(vec![&get_name, &get_address]);

        let final_node = dag
            .add_node(
                "final_node",
                task_fn(|task: (usize, usize), w: WorkerContext| async move {
                    w.stop().unwrap();
                    task.0 + task.1
                }),
            )
            .depends_on((&main_collector, &side_collector));
        let dag_executor = dag.build().unwrap();
        assert_eq!(dag_executor.topological_order.len(), 6);

        println!("DAG in DOT format:\n{}", dag_executor.to_dot());

        let mut backend: JsonStorage<Value> = JsonStorage::new_temp().unwrap();

        backend
            .push_start(Value::from(vec![42, 43, 44]))
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
            .build::<DagExecutor<JsonStorage<Value>>, RootDagService<JsonStorage<Value>>>(
                dag_executor,
            );
        worker.run().await.unwrap();
    }

    #[tokio::test]
    async fn test_routed_workflow() {
        let dag = DagFlow::new();

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
            fn to_node_ids(&self) -> Vec<NodeIndex> {
                vec![(*self).into()]
            }
        }

        async fn collect(task: (usize, usize, usize)) -> usize {
            task.0 + task.1 + task.2
        }
        let collector = dag.node(collect).depends_on((&entry1, &entry2, &entry3));

        async fn vec_collect(task: Vec<usize>, worker: WorkerContext) -> usize {
            task.iter().sum::<usize>()
        }

        let vec_collector = dag
            .node(vec_collect)
            .depends_on(vec![&entry1, &entry2, &entry3]);

        async fn exit(task: (usize, usize)) -> Result<u32, ParseIntError> {
            (task.0.to_string() + &task.1.to_string()).parse()
        }

        let on_collect = dag.node(exit).depends_on((&collector, &vec_collector));

        async fn check_approval(task: u32) -> Result<EntryRoute, BoxDynError> {
            println!("Approval check for task: {}", task);
            match task % 3 {
                0 => Ok(EntryRoute::Entry1(NodeIndex::new(0))),
                1 => Ok(EntryRoute::Entry2(NodeIndex::new(1))),
                2 => Ok(EntryRoute::Entry3(NodeIndex::new(2))),
                _ => Err(BoxDynError::from("Invalid task")),
            }
        }

        dag.route(check_approval).depends_on(&on_collect);

        let dag_executor = dag.build().unwrap();
        assert_eq!(dag_executor.topological_order.len(), 7);

        println!("Start nodes: {:?}", dag_executor.start_nodes);
        println!("End nodes: {:?}", dag_executor.end_nodes);

        println!(
            "DAG Topological Order: {:?}",
            dag_executor.topological_order
        );

        println!("DAG in DOT format:\n{}", dag_executor.to_dot());

        let mut backend: JsonStorage<Value> = JsonStorage::new_temp().unwrap();

        backend.push_start(Value::from(vec![17, 18, 19])).await.unwrap();

        let worker = WorkerBuilder::new("rango-tango")
            .backend(backend)
            .on_event(|ctx, ev| {
                println!("On Event = {ev:?}");
                if matches!(ev, Event::Error(_)) {
                    ctx.stop().unwrap();
                }
            })
            .build::<DagExecutor<JsonStorage<Value>>, RootDagService<JsonStorage<Value>>>(
                dag_executor,
            );
        worker.run().await.unwrap();

        // let inner_basic: Workflow<u32, usize, JsonStorage<Value>, _> = Workflow::new("basic")
        //     .and_then(async |input: u32| (input + 1) as usize)
        //     .and_then(async |input: usize| input.to_string())
        //     .and_then(async |input: String| input.parse::<usize>());

        // let workflow = Workflow::new("example_workflow")
        //     .and_then(async |input: u32| Ok::<Range<u32>, BoxDynError>(input..100))
        //     .filter_map(
        //         async |input: u32| {
        //             if input > 50 { Some(input) } else { None }
        //         },
        //     )
        //     .and_then(async |items: Vec<u32>| Ok::<_, BoxDynError>(items))
        //     .fold(0, async |(acc, item): (u32, u32)| {
        //         Ok::<_, BoxDynError>(item + acc)
        //     })
        //     // .delay_for(Duration::from_secs(2))
        //     // .delay_with(|_| Duration::from_secs(1))
        //     // .and_then(async |items: Range<u32>| Ok::<_, BoxDynError>(items.sum::<u32>()))
        //     // .repeat_until(async |i: u32| {
        //     //     if i < 20 {
        //     //         Ok::<_, BoxDynError>(Some(i))
        //     //     } else {
        //     //         Ok(None)
        //     //     }
        //     // })
        //     // .chain(inner_basic)
        //     // .chain(
        //     //     Workflow::new("sub_workflow")
        //     //         .and_then(async |input: usize| Ok::<_, BoxDynError>(input as u32 * 2))
        //     //         .and_then(async |input: u32| Ok::<_, BoxDynError>(input + 10)),
        //     // )
        //     // .chain(dag_executor)
        //     .build();
    }
}
