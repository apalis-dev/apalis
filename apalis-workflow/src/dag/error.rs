use apalis_core::error::BoxDynError;
use petgraph::{algo::Cycle, graph::NodeIndex};
use std::fmt::Debug;
use thiserror::Error;

/// Errors that can occur during DAG workflow execution.
#[derive(Error, Debug)]
pub enum DagFlowError {
    /// An error originating from the actual node execution.
    #[error("Node execution error: {0}")]
    NodeExecutionError(#[source] BoxDynError),
    /// An error originating from the backend.
    #[error("Backend error: {0}")]
    Backend(#[source] BoxDynError),

    /// An error originating from the service.
    #[error("MissingService error: {0:?}")]
    MissingService(petgraph::graph::NodeIndex),

    /// An error originating from the service.
    #[error("Service error: {0}")]
    Service(#[source] BoxDynError),

    /// An error related to codec operations.
    #[error("Codec error: {0}")]
    Codec(#[source] BoxDynError),

    /// An error related to metadata operations.
    #[error("Metadata error: {0}")]
    Metadata(#[source] BoxDynError),

    /// An error indicating that dependencies are not ready.
    #[error("Dependencies not ready")]
    DependenciesNotReady,

    /// An error indicating a missing task ID for a dependency node.
    #[error("Missing task ID for dependency node {0:?}")]
    MissingDependencyTaskId(petgraph::graph::NodeIndex),

    /// An error indicating that a task result was not found for a node.
    #[error("Task result not found for node {0:?}")]
    TaskResultNotFound(petgraph::graph::NodeIndex),

    /// An error indicating that a dependency task has failed.
    #[error("Dependency task failed: {0}")]
    DependencyTaskFailed(String),

    /// An error indicating an unexpected response type during fan-in.
    #[error("Unexpected response type during fan-in")]
    UnexpectedResponseType,

    /// An error indicating a mismatch in the number of inputs during fan-in.
    #[error("Input count mismatch: expected {expected} inputs for fan-in, got {actual}")]
    InputCountMismatch {
        /// The expected number of inputs.
        expected: usize,
        /// The actual number of inputs received.
        actual: usize,
    },
    /// An error indicating that entry fan-out is not completed.
    #[error("Entry fan-out not completed")]
    EntryFanOutIncomplete,

    /// DAG contains cycles.
    #[error("DAG contains cycles involving nodes: {0:?}")]
    CyclicDAG(Cycle<NodeIndex>),
}
