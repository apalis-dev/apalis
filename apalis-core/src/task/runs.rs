//! Represents a single execution attempt of a task.
//!
//! It includes information about the attempt number, the serialized value of the run, timestamps for when the task was locked and completed, and the identifier of the worker that executed the task.

use crate::backend::TaskResult;

/// Represents a run of a task
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Debug, Clone)]
pub struct Run<T = Vec<u8>> {
    /// The serialized result of the run
    pub result: Option<TaskResult<T>>,
    /// The time at which the task was locked for execution
    pub lock_at: i64,
    /// The identifier of the worker that locked the task
    pub lock_by: String,
    /// The time at which the run was completed
    pub done_at: Option<i64>,
}
