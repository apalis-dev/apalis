use std::collections::HashMap;

use apalis_core::task::task_id::TaskId;
use petgraph::graph::NodeIndex;
use serde::{Deserialize, Serialize};

/// Response from Graph execution step
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[non_exhaustive]
pub enum GraphNodeResponse {
    /// Entry nodes have been fanned out
    EntryFanOut {
        /// Map of node indices to their task IDs
        node_task_ids: HashMap<NodeIndex, TaskId>,
    },
    /// Next tasks have been fanned out
    FanOut {
        /// Result of the current task
        response: serde_json::Value,
        /// Map of node indices to their task IDs
        node_task_ids: HashMap<NodeIndex, TaskId>,
    },
    /// Next task has been enqueued
    EnqueuedNext {
        /// Result of the current task
        result: serde_json::Value,
    },
    /// Waiting for dependencies to complete
    WaitingForDependencies {
        /// Map of pending dependency node indices to their task IDs
        pending_dependencies: HashMap<NodeIndex, TaskId>,
    },

    /// Graph execution is complete
    Complete {
        /// Result of the final task
        result: serde_json::Value,
    },
}
