use std::{
    collections::{HashMap, VecDeque},
    fmt::Debug,
    pin::Pin,
    task::{Context, Poll},
};

use apalis_core::{
    backend::{
        Backend, BackendExt, WaitForCompletion,
        codec::{Codec, RawDataBackend},
    },
    error::BoxDynError,
    task::{
        Task,
        metadata::{Meta, MetadataExt},
    },
    worker::builder::{IntoWorkerService, WorkerService},
};
use futures::Sink;
use petgraph::{
    dot::Config,
    graph::{DiGraph, NodeIndex},
};
use tower::Service;

use crate::{
    DagService,
    dag::{DagflowContext, RootDagService, error::DagflowError, response::DagExecutionResponse},
    id_generator::GenerateId,
};

/// Executor for DAG workflows
#[derive(Debug)]
pub struct DagExecutor<B>
where
    B: BackendExt,
{
    pub(super) graph: DiGraph<DagService<B::Compact, B::Context, B::IdType>, ()>,
    pub(super) node_mapping: HashMap<String, NodeIndex>,
    pub(super) topological_order: Vec<NodeIndex>,
    pub(super) start_nodes: Vec<NodeIndex>,
    pub(super) end_nodes: Vec<NodeIndex>,
    pub(super) not_ready: VecDeque<NodeIndex>,
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

impl<B, MetaError> Service<Task<B::Compact, B::Context, B::IdType>> for DagExecutor<B>
where
    B: BackendExt,
    B::Context:
        Send + Sync + 'static + MetadataExt<DagflowContext<B::IdType>, Error = MetaError> + Default,
    B::IdType: Clone + Send + Sync + 'static + GenerateId + Debug,
    B::Compact: Send + Sync + 'static,
    MetaError: Into<BoxDynError>,
{
    type Response = B::Compact;
    type Error = DagflowError;
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
                    .poll_ready(cx)
                    .map_err(|e| DagflowError::Service(e))?
                    .is_pending()
                {
                    return Poll::Pending;
                }

                self.not_ready.pop_front();
            }
        }
    }

    fn call(&mut self, req: Task<B::Compact, B::Context, B::IdType>) -> Self::Future {
        let mut graph = self.graph.clone();

        Box::pin(async move {
            let context = req
                .extract::<Meta<DagflowContext<B::IdType>>>()
                .await
                .map_err(|e| DagflowError::Metadata(e.into()))?
                .0;

            // Get the service for this node
            let service = graph
                .node_weight_mut(context.current_node)
                .ok_or_else(|| DagflowError::MissingService(context.current_node))?;

            let result = service.call(req).await.map_err(|e| DagflowError::Node(e))?;

            Ok(result)
        })
    }
}

impl<B, Compact, Err, CdcErr, MetaError>
    IntoWorkerService<B, RootDagService<B>, B::Compact, B::Context> for DagExecutor<B>
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
    B::Context: MetadataExt<DagflowContext<B::IdType>, Error = MetaError> + Send + Sync + 'static,
    B::IdType: Send + Sync + 'static + Default + GenerateId + PartialEq + Debug,
    B: Sync + Backend<Args = Compact, Error = Err>,
    B::Compact: Send + Sync + 'static + Clone,
    <B::Context as MetadataExt<DagflowContext<B::IdType>>>::Error: Into<BoxDynError>,
    B::Codec: Codec<Vec<Compact>, Compact = Compact, Error = CdcErr> + 'static,
    CdcErr: Into<BoxDynError>,
    <B as BackendExt>::Codec: Codec<
            DagExecutionResponse<Compact, <B as Backend>::IdType>,
            Compact = Compact,
            Error = CdcErr,
        >,
    MetaError: Send + Sync + 'static + Into<BoxDynError>,
{
    type Backend = RawDataBackend<B>;
    fn into_service(self, b: B) -> WorkerService<RawDataBackend<B>, RootDagService<B>> {
        WorkerService {
            backend: RawDataBackend::new(b.clone()),
            service: RootDagService::new(self, b),
        }
    }
}
