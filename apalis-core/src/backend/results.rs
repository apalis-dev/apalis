use futures_core::Stream;

use crate::{
    backend::Backend,
    task::{status::Status, task_id::TaskId},
};

/// Represents the result of a task execution
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Debug, Clone)]
pub struct TaskResult<T> {
    /// The unique identifier of the task
    pub task_id: TaskId,
    /// The most recent result
    pub attempt: usize,
    /// The status of the task
    pub status: Status,
    /// The result of the task execution
    pub result: Result<T, String>,
}

impl<T> TaskResult<T> {
    /// Get the ID of the task
    pub fn task_id(&self) -> &TaskId {
        &self.task_id
    }

    /// Get the status of the task
    pub fn status(&self) -> &Status {
        &self.status
    }

    /// Get the result of the task
    pub fn result(&self) -> &Result<T, String> {
        &self.result
    }

    /// Take the result of the task
    pub fn take(self) -> Result<T, String> {
        self.result
    }
}

/// Allows waiting for tasks to complete and checking their status
pub trait WaitForCompletion<Output>: Backend {
    /// The result stream type yielding task results
    type ResultStream: Stream<Item = Result<TaskResult<Output>, Self::Error>> + Send + 'static;

    /// Wait for multiple tasks to complete, yielding results as they become available
    fn wait_for(&self, task_ids: impl IntoIterator<Item = TaskId>) -> Self::ResultStream;

    /// Wait for a single task to complete, yielding its result
    fn wait_for_single(&self, task_id: TaskId) -> Self::ResultStream {
        self.wait_for(std::iter::once(task_id))
    }

    /// Check current status of tasks without waiting
    fn check_status(
        &self,
        task_ids: impl IntoIterator<Item = TaskId> + Send,
    ) -> impl Future<Output = Result<Vec<TaskResult<Output>>, Self::Error>> + Send;
}
