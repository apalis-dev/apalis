//! Utilities for creating and managing tasks.
//!
//! The [`Task`] component encapsulates a unit of work to be executed,
//! along with its associated context, metadata, and execution status. The [`ExecutionContext`]
//! struct contains metadata, attempt tracking, extensions, and scheduling information for each task.
//!
//! # Overview
//!
//! In `apalis`, tasks are designed to represent discrete units of work that can be scheduled, retried, and tracked
//! throughout their lifecycle. Each task consists of arguments (`args`) describing the work to be performed,
//! and an [`ExecutionContext`] containing metadata and control information.
//!
//! ## [`Task`]
//!
//! The [`Task`] struct is generic over:
//! - `Args`: The type of arguments or payload for the task.
//! - `Conn`: Backend-specific marker for a task.
//! - `IdType`: The type used for uniquely identifying the task (defaults to [`RandomId`]).
//!
//! ## [`ExecutionContext`]
//!
//! The [`ExecutionContext`] struct provides the following:
//! - `task_id`: Optionally stores a unique identifier for the task.
//! - `data`: An [`Extensions`] container for storing arbitrary per-task data (e.g., middleware extensions).
//! - `attempt`: Tracks how many times the task has been attempted.
//! - `metadata`: Custom metadata for the task, provided by the backend or user.
//! - `status`: The current [`Status`] of the task (e.g., Pending, Running, Completed, Failed).
//! - `run_at`: The UNIX timestamp (in seconds) when the task should be run.
//! - `done_at`: The UNIX timestamp (in seconds) when the task completed, if it has.
//! - `lock_at`: The UNIX timestamp (in seconds) when the task was locked for processing, if applicable.
//! - `lock_by`: An identifier for the worker or process currently holding the lock on the task.
//! - `idempotency_key`: An optional key used to enforce job uniqueness.
//! - `max_attempts`: The maximum number of attempts allowed before the task is considered failed.
//! - `priority`: An optional priority value used to influence scheduling order.
//! - `queue`: The queue the task belongs to, if applicable.
//! - `runs`: A history of all runs recorded for the task.
//!
//! The execution context is essential for tracking the state and metadata of a task as it moves through
//! the system. It enables features such as retries, scheduling, locking, prioritization, and extensibility
//! via the `Extensions` type.
//!
//! # Modules
//!
//! - [`attempt`]: Tracks the number of attempts a task has been executed.
//! - [`builder`]: Utilities for constructing tasks.
//! - [`data`]: Data types for task payloads.
//! - [`extensions`]: Extension storage for tasks.
//! - [`metadata`]: Ctxdata types for tasks.
//! - [`status`]: Status tracking for tasks.
//! - [`task_id`]: Types for uniquely identifying tasks.
//!
//! # Examples
//!
//! ## Creating a new task with default metadata
//!
//! ```rust
//! # use apalis_core::task::{Task, ExecutionContext};
//! # use apalis_core::task::builder::TaskBuilder;
//! # use apalis_core::task::task_id::RandomId;
//! let task: Task<String, (), RandomId> = TaskBuilder::new("my work".to_string()).build();
//! ```
//!
//! ## Creating a task with custom metadata
//!
//! ```rust
//! # use apalis_core::task::{Task, ExecutionContext};
//! # use apalis_core::task::builder::TaskBuilder;
//! # use apalis_core::task::task_id::RandomId;
//! # use apalis_core::task::metadata::Metadata;
//! # use apalis_core::task::metadata::MetadataStore;
//! # use apalis_core::backend::memory::MemoryContext;
//! #
//! #[derive(Debug, PartialEq)]
//! struct RequestId(String);
//!
//! impl Metadata for RequestId {
//!     type Error = std::convert::Infallible;
//!
//!     fn inject(&self, metadata: &mut MetadataStore) -> Result<(), Self::Error> {
//!         let _ = metadata.insert("request_id", self.0.clone());
//!         Ok(())
//!     }
//!
//!     fn extract(metadata: &MetadataStore) -> Result<Self, Self::Error> {
//!         Ok(Self(
//!             metadata
//!                 .get("request_id")
//!                 .cloned()
//!                 .unwrap_or_default(),
//!         ))
//!     }
//! }
//!
//! let task: Task<String, MemoryContext, RandomId> = TaskBuilder::new("important work".to_string())
//!     .metadata(&RequestId("user_id".to_string()))
//!     .build();
//! ```
//!
//! ## Accessing and modifying the execution context
//!
//! ```rust
//! # use apalis_core::task::builder::TaskBuilder;
//! # use apalis_core::task::task_id::RandomId;
//! # use apalis_core::backend::memory::MemoryContext;
//! use apalis_core::task::{Task, ExecutionContext, status::Status};
//! let mut task: TaskBuilder<_, MemoryContext, RandomId> = TaskBuilder::new("work".to_string());
//! task.ctx.status = Status::Running.into();
//! task.ctx.attempt.increment();
//! ```
//!
//! ## Using Extensions for per-task data
//!
//! ```rust
//! # use apalis_core::task::builder::TaskBuilder;
//! # use apalis_core::task::task_id::RandomId;
//! use apalis_core::task::{Task, extensions::Extensions};
//! #[derive(Debug, Clone, PartialEq)]
//! pub struct TracingId(String);
//! let mut extensions = Extensions::default();
//! extensions.insert(TracingId("abc123".to_owned()));
//! let task: Task<String, (), RandomId> = TaskBuilder::new("work".to_string()).with_data(extensions).build();
//! assert_eq!(task.ctx.data.get::<TracingId>(), Some(&TracingId("abc123".to_owned())));
//! ```
//!
//! # See Also
//!
//! - [`Task`]: Represents a unit of work to be executed.
//! - [`ExecutionContext`]: Holds metadata, status, and control information for a task.
//! - [`Extensions`]: Type-safe storage for per-task data.
//! - [`Status`]: Enum representing the lifecycle state of a task.
//! - [`Attempt`]: Tracks the number of execution attempts for a task.
//! - [`TaskId`]: Unique identifier type for tasks.
//! - [`FromRequest`]: Trait for extracting data from task contexts.
//! - [`IntoResponse`]: Trait for converting tasks into response types.
//! - [`TaskBuilder`]: Fluent builder for constructing tasks with optional configuration.
//! - [`RandomId`]: Default unique identifier type for tasks.
//!
//! [`TaskBuilder`]: crate::task::builder::TaskBuilder
//! [`IntoResponse`]: crate::task_fn::into_response::IntoResponse
//! [`FromRequest`]: crate::task_fn::from_request::FromRequest

use std::{fmt::Debug, marker::PhantomData, sync::Arc};

use crate::{
    backend::queue::Queue,
    task::{
        attempt::Attempt,
        builder::TaskBuilder,
        extensions::Extensions,
        metadata::MetadataStore,
        runs::Run,
        status::{AtomicStatus, Status},
        task_id::TaskId,
    },
    task_fn::FromRequest,
};

pub mod attempt;
pub mod builder;
pub mod data;
pub mod extensions;
pub mod metadata;
pub mod runs;
pub mod status;
pub mod task_id;

/// Represents a task which will be executed
/// Should be considered a single unit of work
#[derive(Debug, Clone, Default)]
pub struct Task<Args, Connection, IdType> {
    /// The argument task part
    pub args: Args,
    /// ExecutionContext of the task eg id, attempts and context
    pub ctx: Arc<ExecutionContext<Connection, IdType>>,
}

/// Execution context of a `Task`
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct ExecutionContext<Connection, IdType> {
    /// The task's id if allocated
    pub task_id: Option<TaskId<IdType>>,

    /// The tasks's extensions
    #[cfg_attr(feature = "serde", serde(skip))]
    pub data: Extensions,

    /// The tasks's attempts
    /// Keeps track of the number of attempts a task has been worked on
    pub attempt: Attempt,

    /// The task status that is wrapped in an atomic status
    pub status: AtomicStatus,

    /// The time a task should be run
    pub run_at: Option<u64>,

    /// The time the task was completed, if applicable
    pub done_at: Option<u64>,

    /// The time the task was locked, if applicable
    pub lock_at: Option<u64>,

    /// Identifier of the worker/process that currently holds the lock on this task
    pub lock_by: Option<String>,

    /// Adds a unique key to enforce job uniqueness when used
    pub idempotency_key: Option<String>,

    /// Metadata associated with the task
    pub metadata: MetadataStore,

    /// The maximum number of attempts allowed for the task
    pub max_attempts: Option<usize>,

    /// The priority of the task, which can be used for scheduling
    pub priority: Option<usize>,

    /// The queue to which the task belongs, if applicable
    pub queue: Option<Queue>,

    /// A list of all runs for this task
    pub runs: Vec<Run>,

    /// A marker to indicate the type of connection used by the backend.
    pub connection: PhantomData<Connection>,
}

impl<Conn, IdType> Default for ExecutionContext<Conn, IdType> {
    fn default() -> Self {
        Self {
            task_id: None,
            data: Extensions::default(),
            attempt: Attempt::default(),
            status: AtomicStatus::new(Status::Pending),
            run_at: None,
            done_at: None,
            lock_at: None,
            lock_by: None,
            idempotency_key: None,
            metadata: MetadataStore::default(),
            max_attempts: None,
            priority: None,
            queue: None,
            runs: Vec::new(),
            connection: PhantomData,
        }
    }
}

impl<Conn: Debug, IdType: Debug> Debug for ExecutionContext<Conn, IdType> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ExecutionContext")
            .field("task_id", &self.task_id)
            .field("data", &"<Extensions>")
            .field("attempt", &self.attempt)
            .field("connection", &self.connection)
            .field("status", &self.status.load())
            .field("run_at", &self.run_at)
            .field("done_at", &self.done_at)
            .field("lock_at", &self.lock_at)
            .field("lock_by", &self.lock_by)
            .field("idempotency_key", &self.idempotency_key)
            .field("metadata", &self.metadata)
            .field("max_attempts", &self.max_attempts)
            .field("runs", &self.runs)
            .field("priority", &self.priority)
            .field("queue", &self.queue)
            .finish()
    }
}

impl<Conn, IdType: Clone> Clone for ExecutionContext<Conn, IdType> {
    fn clone(&self) -> Self {
        Self {
            task_id: self.task_id.clone(),
            data: self.data.clone(),
            attempt: self.attempt.clone(),
            connection: self.connection,
            status: self.status.clone(),
            run_at: self.run_at,
            done_at: self.done_at,
            lock_at: self.lock_at,
            lock_by: self.lock_by.clone(),
            idempotency_key: self.idempotency_key.clone(),
            metadata: self.metadata.clone(),
            runs: self.runs.clone(),
            max_attempts: self.max_attempts,
            priority: self.priority,
            queue: self.queue.clone(),
        }
    }
}

impl<Args, Conn, IdType> Task<Args, Conn, IdType> {
    /// Take the task into its parts
    pub fn take(self) -> (Args, Arc<ExecutionContext<Conn, IdType>>) {
        (self.args, self.ctx)
    }

    /// Extract a value of type `T` from the task's context
    ///
    /// Uses [FromRequest] trait to extract the value.
    pub async fn extract<T: FromRequest<Self>>(&self) -> Result<T, T::Error> {
        T::from_request(self).await
    }

    /// Converts the task into a [`TaskBuilder`]
    pub fn into_builder(self) -> TaskBuilder<Args, Conn, IdType>
    where
        IdType: Clone,
    {
        TaskBuilder {
            args: self.args,
            ctx: Arc::unwrap_or_clone(self.ctx),
        }
    }
}
