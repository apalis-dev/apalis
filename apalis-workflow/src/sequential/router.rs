use std::{collections::HashMap, time::Duration};

use apalis_core::{
    backend::{Backend, WireFormatBackend},
    task::task_id::TaskId,
};
use serde::{Deserialize, Serialize};

use crate::SteppedService;

/// Router for workflow steps
#[derive(Debug, Default)]
pub struct WorkflowRouter<B>
where
    B: Backend + WireFormatBackend,
{
    pub(crate) steps: HashMap<usize, SteppedService<B::Compact>>,
}

impl<B> WorkflowRouter<B>
where
    B: Backend + WireFormatBackend,
{
    /// Create a new workflow router
    #[must_use]
    pub fn new() -> Self {
        Self {
            steps: HashMap::new(),
        }
    }
}
/// Result information for workflow steps
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct StepResponse {
    /// Result produced by the step
    pub result: serde_json::Value,
    /// Optional ID of the next task to execute
    pub next_task_id: Option<TaskId>,
}

/// Enum representing the possible transitions in a workflow
#[derive(Debug, Clone, Serialize, Deserialize)]
#[non_exhaustive]
pub enum GoTo<T = ()> {
    /// Proceed to the next step with the given value
    Next(T),
    /// Delay the execution for the specified duration
    DelayFor(Duration, T),
    /// Break the workflow with the given value
    Break(T),
    /// Marks the workflow as done
    Done,
}
