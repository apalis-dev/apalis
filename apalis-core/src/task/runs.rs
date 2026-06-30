//! This module contains the definition of a `Run` struct, which represents a single execution attempt of a task.
//! It includes information about the attempt number, the serialized value of the run, timestamps for when the task was locked and completed, and the identifier of the worker that executed the task.

use bytes::Bytes;

/// Represents a run of a task
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Debug, Clone)]
pub struct Run<T = Bytes> {
    /// The attempt number of the run
    pub attempt: usize,
    /// The serialized value of the run
    pub value: Option<Result<T, String>>,
    /// The time at which the task was locked for execution
    pub lock_at: i64,
    /// The identifier of the worker that locked the task
    pub lock_by: String,
    /// The time at which the run was completed
    pub done_at: Option<i64>,
}
