use std::num::ParseIntError;

use apalis_core::task::metadata::{Metadata, MetadataError, MetadataStore};
use serde::{Deserialize, Serialize};

/// Context information for the current step in the workflow
#[derive(Debug, Clone)]
pub struct StepContext<Backend> {
    /// Index of the current step
    pub current_step: usize,
    /// Backend associated with the current step
    pub backend: Backend,
    /// Indicates if there is a next step
    pub has_next: bool,
}
impl<B> StepContext<B> {
    /// Creates a new StepContext
    pub fn new(backend: B, idx: usize, has_next: bool) -> Self {
        Self {
            current_step: idx,
            backend,
            has_next,
        }
    }
}

/// Metadata stored in each task for workflow processing
#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct WorkflowContext {
    /// Index of the step in the workflow
    pub step_index: usize,
}

/// Represents an invalid [`WorkflowContext`] state
#[derive(Debug, thiserror::Error)]
pub enum WorkflowContextError {
    /// An entry for the key is missing
    #[error("the data for key {WORKFLOW_CONTEXT_KEY} is missing")]
    MissingKey,
    /// Could not parse the value provided
    #[error("Could not parse key {WORKFLOW_CONTEXT_KEY}")]
    Parse(#[from] ParseIntError),

    /// Duplicate entry
    #[error("Duplicate entry: {0}")]
    DuplicateEntry(#[from] MetadataError),
}

const WORKFLOW_CONTEXT_KEY: &str = "apalis_workflow.context.step_index";

impl Metadata for WorkflowContext {
    type Error = WorkflowContextError;

    fn extract(map: &MetadataStore) -> Result<Self, Self::Error> {
        let step_index = map
            .get(WORKFLOW_CONTEXT_KEY)
            .ok_or(WorkflowContextError::MissingKey)?
            .parse::<usize>()
            .map_err(WorkflowContextError::Parse)?;

        Ok(Self { step_index })
    }

    fn inject(&self, map: &mut MetadataStore) -> Result<(), WorkflowContextError> {
        map.insert(WORKFLOW_CONTEXT_KEY, self.step_index.to_string())?;
        Ok(())
    }
}
