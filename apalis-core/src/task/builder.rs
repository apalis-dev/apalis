//! # Task Builder
//!
//! The `TaskBuilder` module provides a flexible builder pattern for constructing [`Task`] instances
//! with customizable configuration options. It allows users to specify arguments, context, extensions,
//! task identifiers, attempt information, status, and scheduling details for tasks.
//!
//! ## Features
//! - Create tasks with required arguments and optional context.
//! - Attach custom extensions/data to tasks.
//! - Assign unique task identifiers.
//! - Configure attempt and status information.
//! - Schedule tasks to run at specific times, after delays, or at intervals (seconds, minutes, hours).
//! - Build tasks with sensible defaults for omitted fields.
//!
//! ## Usage
//! Use [`TaskBuilder`] to incrementally configure a task, then call `.build()` to obtain a [`Task`] instance.
//! Convenience methods are provided for common scheduling scenarios.
//!
//! ### Example
//! ```rust,ignore
//! let task = TaskBuilder::new(args)
//!     .attempts(3)
//!     .run_in_minutes(10)
//!     .build();
//! ```
//!
use crate::{
    backend::queue::Queue,
    task::{
        ExecutionContext, Task,
        attempt::Attempt,
        extensions::Extensions,
        metadata::{Metadata, MetadataStore},
        status::Status,
        task_id::TaskId,
    },
};
use std::{
    fmt::Debug,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

/// Builder for creating [`Task`] instances with optional configuration
#[derive(Debug)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct TaskBuilder<Args, Conn, IdType> {
    /// The arguments for the task
    pub args: Args,
    /// Execution context for the task, including metadata and extensions
    pub ctx: ExecutionContext<Conn, IdType>,
}

impl<Args, Conn, IdType> TaskBuilder<Args, Conn, IdType> {
    /// Create a new TaskBuilder with the required args
    #[must_use]
    pub fn new(args: Args) -> Self {
        Self {
            args,
            ctx: ExecutionContext::default(),
        }
    }

    /// Set the task's metadata
    #[must_use]
    pub fn with_metadata(mut self, metadata: MetadataStore) -> Self {
        self.ctx.metadata = metadata;
        self
    }

    /// Set the task's runtime data
    #[must_use]
    pub fn with_data(mut self, data: Extensions) -> Self {
        self.ctx.data = data;
        self
    }

    /// Insert a value into the task's data context
    #[must_use]
    pub fn data<D: Clone + Send + Sync + 'static>(mut self, value: D) -> Self {
        self.ctx.data.insert(value);
        self
    }

    /// Insert a value into the task's metadata
    #[must_use]
    pub fn metadata<M>(mut self, value: &M) -> Self
    where
        M: Metadata,
        M::Error: Debug,
    {
        value
            .inject(&mut self.ctx.metadata)
            .expect("Could not add Metadata");
        self
    }

    /// Set the task ID
    #[must_use]
    pub fn task_id(mut self, task_id: TaskId<IdType>) -> Self {
        self.ctx.task_id = Some(task_id);
        self
    }

    /// Set the attempt information
    #[must_use]
    pub fn attempt(mut self, attempt: Attempt) -> Self {
        self.ctx.attempt = attempt;
        self
    }

    /// Set the task status
    #[must_use]
    pub fn status(mut self, status: Status) -> Self {
        self.ctx.status = status.into();
        self
    }

    /// Set the maximum number of attempts allowed for the task
    #[must_use]
    pub fn max_attempts(mut self, max_attempts: usize) -> Self {
        self.ctx.max_attempts = Some(max_attempts);
        self
    }

    /// Set the priority of the task
    #[must_use]
    pub fn priority(mut self, priority: usize) -> Self {
        self.ctx.priority = Some(priority);
        self
    }

    /// Set the queue this task belongs to
    #[must_use]
    pub fn queue(mut self, queue: Queue) -> Self {
        self.ctx.queue = Some(queue);
        self
    }

    /// Schedule the task to run at a specific Unix timestamp
    #[must_use]
    pub fn run_at_timestamp(mut self, timestamp: u64) -> Self {
        self.ctx.run_at = Some(timestamp);
        self
    }

    /// Schedule the task to run at a specific SystemTime
    #[must_use]
    pub fn run_at_time(mut self, time: SystemTime) -> Self {
        let timestamp = time
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards")
            .as_secs();
        self.ctx.run_at = Some(timestamp);
        self
    }

    /// Schedule the task to run after a delay from now
    #[must_use]
    pub fn run_after(mut self, delay: Duration) -> Self {
        let now = SystemTime::now();
        let run_time = now + delay;
        let timestamp = run_time
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards")
            .as_secs();
        self.ctx.run_at = Some(timestamp);
        self
    }

    /// Schedule the task to run in the specified number of seconds
    #[must_use]
    pub fn run_in_seconds(self, seconds: u64) -> Self {
        self.run_after(Duration::from_secs(seconds))
    }

    /// Schedule the task to run in the specified number of minutes
    #[must_use]
    pub fn run_in_minutes(self, minutes: u64) -> Self {
        self.run_after(Duration::from_secs(minutes * 60))
    }

    /// Schedule the task to run in the specified number of hours
    #[must_use]
    pub fn run_in_hours(self, hours: u64) -> Self {
        self.run_after(Duration::from_secs(hours * 3600))
    }

    /// Set the idempotency key
    #[must_use]
    pub fn idempotency_key<S: AsRef<str>>(mut self, idempotency_key: S) -> Self {
        self.ctx.idempotency_key = Some(idempotency_key.as_ref().to_owned());
        self
    }

    /// Set the time the task was completed
    #[must_use]
    pub fn done_at(mut self, done_at: Option<u64>) -> Self {
        self.ctx.done_at = done_at;
        self
    }

    /// Set the time the task was locked
    #[must_use]
    pub fn lock_at(mut self, lock_at: Option<u64>) -> Self {
        self.ctx.lock_at = lock_at;
        self
    }

    /// Set the worker/process identifier holding the lock on this task
    #[must_use]
    pub fn lock_by(mut self, lock_by: Option<String>) -> Self {
        self.ctx.lock_by = lock_by;
        self
    }

    /// Build the Task with default context
    #[must_use]
    pub fn build(self) -> Task<Args, Conn, IdType> {
        Task {
            args: self.args,
            ctx: Arc::new(self.ctx),
        }
    }
}

impl<Args, Conn, IdType> TaskBuilder<Args, Conn, IdType> {
    /// Maps the `args` field using the provided function, consuming the task.
    pub fn try_map<F, NewArgs, Err>(self, f: F) -> Result<TaskBuilder<NewArgs, Conn, IdType>, Err>
    where
        F: FnOnce(Args) -> Result<NewArgs, Err>,
    {
        Ok(TaskBuilder {
            args: f(self.args)?,
            ctx: self.ctx,
        })
    }
    /// Maps the `args` field using the provided function, consuming the task.
    pub fn map<F, NewArgs>(self, f: F) -> TaskBuilder<NewArgs, Conn, IdType>
    where
        F: FnOnce(Args) -> NewArgs,
    {
        TaskBuilder {
            args: f(self.args),
            ctx: self.ctx,
        }
    }

    /// Maps both `args` and `execution_context` together.
    pub fn map_all<F, NewArgs, NewCtx>(self, f: F) -> TaskBuilder<NewArgs, NewCtx, IdType>
    where
        F: FnOnce(
            Args,
            ExecutionContext<Conn, IdType>,
        ) -> (NewArgs, ExecutionContext<NewCtx, IdType>),
    {
        let (args, execution_context) = f(self.args, self.ctx);
        TaskBuilder {
            args,
            ctx: execution_context,
        }
    }

    /// Maps only the `execution_context` field.
    pub fn map_ctx<F, NewCtx>(self, f: F) -> TaskBuilder<Args, NewCtx, IdType>
    where
        F: FnOnce(ExecutionContext<Conn, IdType>) -> ExecutionContext<NewCtx, IdType>,
    {
        TaskBuilder {
            args: self.args,
            ctx: f(self.ctx),
        }
    }
}
