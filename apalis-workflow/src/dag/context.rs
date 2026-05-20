use std::{
    collections::{HashMap, HashSet},
    fmt::Display,
    num::ParseIntError,
    str::FromStr,
};

use apalis_core::{
    error::BoxDynError,
    task::{
        metadata::{Metadata, MetadataError, MetadataStore},
        task_id::TaskId,
    },
};
use petgraph::graph::NodeIndex;
use serde::{Deserialize, Serialize};

/// Metadata stored in each task for workflow processing
#[derive(Debug, Deserialize, Serialize, Default)]
pub struct DagFlowContext<IdType> {
    /// Previous node executed in the DAG
    /// This is the source node that led to the current node's execution
    pub prev_node: Option<NodeIndex>,
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
}

impl<IdType: Clone> Clone for DagFlowContext<IdType> {
    fn clone(&self) -> Self {
        Self {
            prev_node: self.prev_node,
            current_node: self.current_node,
            completed_nodes: self.completed_nodes.clone(),
            node_task_ids: self.node_task_ids.clone(),
            current_position: self.current_position,
            is_initial: self.is_initial,
            root_task_id: self.root_task_id.clone(),
        }
    }
}

impl<IdType: Clone> DagFlowContext<IdType> {
    /// Create initial context for DAG execution
    pub fn new(root_task_id: Option<TaskId<IdType>>) -> Self {
        Self {
            prev_node: None,
            current_node: NodeIndex::new(0),
            completed_nodes: HashSet::new(),
            node_task_ids: HashMap::new(),
            current_position: 0,
            is_initial: true,
            root_task_id,
        }
    }
    /// Get task IDs for dependencies of a given node
    pub fn get_dependency_task_ids(
        &self,
        dependencies: &[NodeIndex],
    ) -> HashMap<NodeIndex, TaskId<IdType>> {
        dependencies
            .iter()
            .filter_map(|dep| {
                self.node_task_ids
                    .get(dep)
                    .cloned()
                    .map(|task_id| (*dep, task_id))
            })
            .collect()
    }
}

const DAG_FLOW_PREV_NODE_KEY: &str = "apalis_workflow.dag.prev_node";

const DAG_FLOW_CURRENT_NODE_KEY: &str = "apalis_workflow.dag.current_node";

const DAG_FLOW_COMPLETED_NODES_KEY: &str = "apalis_workflow.dag.completed_nodes";

const DAG_FLOW_NODE_TASK_IDS_KEY: &str = "apalis_workflow.dag.node_task_ids";

const DAG_FLOW_CURRENT_POSITION_KEY: &str = "apalis_workflow.dag.current_position";

const DAG_FLOW_IS_INITIAL_KEY: &str = "apalis_workflow.dag.is_initial";

const DAG_FLOW_ROOT_TASK_ID_KEY: &str = "apalis_workflow.dag.root_task_id";

/// An error representing an invalid [`DagFlowContext`]
#[derive(Debug, thiserror::Error)]
pub enum DagFlowContextError {
    /// Missing current node key
    #[error("missing key {DAG_FLOW_CURRENT_NODE_KEY}")]
    MissingCurrentNode,

    /// Missing current position key
    #[error("missing key {DAG_FLOW_CURRENT_POSITION_KEY}")]
    MissingCurrentPosition,

    /// Could not parse a node index
    #[error("could not parse node index")]
    ParseNodeIndex(#[from] ParseIntError),

    /// Could not parse a task_id
    #[error("could not parse task id: {0}")]
    ParseTaskId(BoxDynError),

    /// Duplicate entry
    #[error("Duplicate entry: {0}")]
    DuplicateEntry(#[from] MetadataError),
}

impl<IdType> Metadata for DagFlowContext<IdType>
where
    IdType: FromStr + Display,
    <IdType as FromStr>::Err: std::error::Error + Send + Sync + 'static,
{
    type Error = DagFlowContextError;

    fn extract(map: &MetadataStore) -> Result<Self, Self::Error> {
        let prev_node = map
            .get(DAG_FLOW_PREV_NODE_KEY)
            .map(|v| v.parse::<usize>())
            .transpose()?
            .map(NodeIndex::new);

        let current_node = map
            .get(DAG_FLOW_CURRENT_NODE_KEY)
            .ok_or(DagFlowContextError::MissingCurrentNode)?
            .parse::<usize>()?;

        let completed_nodes = map
            .get(DAG_FLOW_COMPLETED_NODES_KEY)
            .map(|v| {
                v.split(',')
                    .filter(|s| !s.is_empty())
                    .map(|s| s.parse::<usize>().map(NodeIndex::new))
                    .collect::<Result<HashSet<_>, _>>()
            })
            .transpose()?
            .unwrap_or_default();

        let node_task_ids = map
            .get(DAG_FLOW_NODE_TASK_IDS_KEY)
            .map(|v| {
                v.split(',')
                    .filter(|s| !s.is_empty())
                    .map(|s| {
                        s.split_once('=')
                            .ok_or(DagFlowContextError::ParseTaskId("Invalid delimiter".into()))
                            .and_then(|(k, v)| {
                                let node = k
                                    .parse::<usize>()
                                    .map(NodeIndex::new)
                                    .map_err(DagFlowContextError::ParseNodeIndex)?;
                                let task_id = v
                                    .parse::<TaskId<IdType>>()
                                    .map_err(|e| DagFlowContextError::ParseTaskId(e.into()))?;
                                Ok((node, task_id))
                            })
                    })
                    .collect::<Result<HashMap<_, _>, _>>()
            })
            .transpose()?
            .unwrap_or_default();

        let current_position = map
            .get(DAG_FLOW_CURRENT_POSITION_KEY)
            .ok_or(DagFlowContextError::MissingCurrentPosition)?
            .parse::<usize>()?;

        let is_initial = map
            .get(DAG_FLOW_IS_INITIAL_KEY)
            .map(|v| v.parse::<bool>())
            .transpose()
            .unwrap_or(None)
            .unwrap_or(true);

        let root_task_id = map
            .get(DAG_FLOW_ROOT_TASK_ID_KEY)
            .map(|v| v.parse::<IdType>().map(TaskId::new))
            .transpose()
            .map_err(|e| DagFlowContextError::ParseTaskId(e.into()))?;

        Ok(Self {
            prev_node,
            current_node: NodeIndex::new(current_node),
            completed_nodes,
            node_task_ids,
            current_position,
            is_initial,
            root_task_id,
        })
    }

    fn inject(&self, map: &mut MetadataStore) -> Result<(), DagFlowContextError> {
        if let Some(prev_node) = self.prev_node {
            map.insert(DAG_FLOW_PREV_NODE_KEY, prev_node.index().to_string())?;
        }

        map.insert(
            DAG_FLOW_CURRENT_NODE_KEY,
            self.current_node.index().to_string(),
        )?;

        let completed_nodes = self
            .completed_nodes
            .iter()
            .map(|n| n.index())
            .collect::<Vec<_>>();

        map.insert(
            DAG_FLOW_COMPLETED_NODES_KEY,
            completed_nodes
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join(","),
        )?;

        let node_task_ids = self
            .node_task_ids
            .iter()
            .map(|(k, v)| format!("{}={v}", k.index()))
            .collect::<Vec<_>>()
            .join(",");

        map.insert(DAG_FLOW_NODE_TASK_IDS_KEY, node_task_ids)?;

        map.insert(
            DAG_FLOW_CURRENT_POSITION_KEY,
            self.current_position.to_string(),
        )?;

        map.insert(DAG_FLOW_IS_INITIAL_KEY, self.is_initial.to_string())
            .expect("A value already exists");

        if let Some(root_task_id) = &self.root_task_id {
            map.insert(DAG_FLOW_ROOT_TASK_ID_KEY, root_task_id.to_string())?;
        }
        Ok(())
    }
}
