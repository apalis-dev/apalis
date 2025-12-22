use std::collections::{HashMap, HashSet};

use apalis_core::task::task_id::TaskId;
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
