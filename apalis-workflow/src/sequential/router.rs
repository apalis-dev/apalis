use std::{collections::HashMap, time::Duration};

use apalis_core::{backend::Backend, task::task_id::TaskId};
use serde::{Deserialize, Serialize};

use crate::SteppedService;

/// Router for workflow steps
#[derive(Debug, Default)]
pub struct WorkflowRouter<B>
where
    B: Backend,
{
    pub(super) steps: HashMap<usize, SteppedService<B::Compact, B::Id>>,
}

impl<B> WorkflowRouter<B>
where
    B: Backend,
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
#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct StepResult<Res, Id> {
    /// Result produced by the step
    pub result: Res,
    /// Optional ID of the next task to execute
    pub next_task_id: Option<TaskId<Id>>,
}

/// Enum representing the possible transitions in a workflow
#[derive(Debug, Clone, Serialize, Deserialize)]
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
