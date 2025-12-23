use std::collections::HashMap;

use apalis_core::task::task_id::TaskId;
use petgraph::graph::NodeIndex;
use serde::{Deserialize, Serialize};

/// Response from DAG execution step
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DagExecutionResponse<Compact, IdType> {
    /// Entry nodes have been fanned out
    EntryFanOut {
        /// Map of node indices to their task IDs
        node_task_ids: HashMap<NodeIndex, TaskId<IdType>>,
    },
    /// Next tasks have been fanned out
    FanOut {
        /// Result of the current task
        response: Compact,
        /// Map of node indices to their task IDs
        node_task_ids: HashMap<NodeIndex, TaskId<IdType>>,
    },
    /// Next task has been enqueued
    EnqueuedNext {
        /// Result of the current task
        result: Compact,
    },
    /// Waiting for dependencies to complete
    WaitingForDependencies {
        /// Map of pending dependency node indices to their task IDs
        pending_dependencies: HashMap<NodeIndex, TaskId<IdType>>,
    },

    /// DAG execution is complete
    Complete {
        /// Result of the final task
        result: Compact,
    },
}
