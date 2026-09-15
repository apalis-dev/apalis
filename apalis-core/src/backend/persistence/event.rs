use crate::{backend::TaskResult, task::task_id::TaskId};

/// A message representing a state transition or event for a task within
/// the task processing system.
///
/// `Res` is the task's associated output/payload type, and `Id` is the
/// underlying identifier type used to uniquely reference a task.
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum TaskEvent<Res> {
    /// Signals that a task has finished processing, carrying its result.
    ///
    /// This is sent once a worker has finished executing a task,
    /// regardless of whether it succeeded or failed — the outcome itself
    /// is encoded in [`TaskResult`].
    Complete(TaskResult<Res>),

    /// Requests that a lock has been acquired on the given
    /// task, preventing other workers from picking it up concurrently.
    Lock {
        /// The identifier of the task being locked.
        task_id: TaskId,
    },

    /// Releases a previously acquired lock on the given task, making it
    /// available for other workers to claim.
    ///
    /// This is typically sent after a task completes, fails, or its lock
    /// needs to be freed without marking the task as complete (e.g. on
    /// worker shutdown).
    Release {
        /// The identifier of the task whose lock is being released.
        task_id: TaskId,
    },

    /// Requests that the given task be cancelled.
    ///
    /// This may be sent before a task has started, or while it is in
    /// progress, depending on what the consuming system supports.
    Cancel {
        /// The identifier of the task being cancelled.
        task_id: TaskId,
    },
}
