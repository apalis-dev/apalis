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
//! - `runs`: A history of all runs recorded for the task. (*experimental*)
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
//! let task: Task<String> = TaskBuilder::new("my work".to_string()).build();
//! ```
//!
//! ## Creating a task with custom metadata
//!
//! ```rust
//! # use apalis_core::task::{Task, ExecutionContext};
//! # use apalis_core::task::builder::TaskBuilder;
//! # use apalis_core::task::metadata::Metadata;
//! # use apalis_core::task::metadata::MetadataStore;
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
//! let task: Task<String> = TaskBuilder::new("important work".to_string())
//!     .metadata(&RequestId("user_id".to_string()))
//!     .build();
//! ```
//!
//! ## Accessing and modifying the execution context
//!
//! ```rust
//! # use apalis_core::task::builder::TaskBuilder;
//! use apalis_core::task::{Task, ExecutionContext, status::Status};
//! let mut task: TaskBuilder<_> = TaskBuilder::new("work".to_string());
//! task = task.status(Status::Running);
//! ```
//!
//! ## Using Extensions for per-task data
//!
//! ```rust
//! # use apalis_core::task::builder::TaskBuilder;
//! use apalis_core::task::{Task, extensions::Extensions};
//! #[derive(Debug, Clone, PartialEq)]
//! pub struct TracingId(String);
//! let mut extensions = Extensions::default();
//! extensions.insert(TracingId("abc123".to_owned()));
//! let task: Task<String> = TaskBuilder::new("work".to_string()).with_data(extensions).build();
//! assert_eq!(task.data().get::<TracingId>(), Some(&TracingId("abc123".to_owned())));
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
//!
//! [`TaskBuilder`]: crate::task::builder::TaskBuilder
//! [`IntoResponse`]: crate::task::into_response::IntoResponse
//! [`FromRequest`]: crate::task::from_request::FromRequest

use std::{fmt::Debug, ops::Deref, sync::Arc};

use crate::{
    backend::queue::Queue,
    task::{
        attempt::Attempt,
        builder::TaskBuilder,
        extensions::Extensions,
        from_request::FromRequest,
        metadata::{Metadata, MetadataStore},
        runs::Run,
        status::{AtomicStatus, Status},
        task_id::TaskId,
    },
};

pub mod attempt;
pub mod builder;
pub mod context;
pub mod data;
pub mod extensions;
pub mod from_request;
pub mod into_response;
pub mod metadata;
pub mod runs;
pub mod status;
pub mod task_fn;
pub mod task_id;

/// Represents a task which will be executed
/// Should be considered a single unit of work
#[derive(Debug, Clone, Default)]
pub struct Task<Args> {
    /// The argument task part
    pub args: Args,
    /// ExecutionContext of the task eg id, attempts and context
    ctx: Arc<ExecutionContext>,
}

/// Execution context of a `Task`
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct ExecutionContext {
    /// The task's id if allocated
    task_id: Option<TaskId>,

    /// The tasks's extensions
    #[cfg_attr(feature = "serde", serde(skip))]
    data: Extensions,

    /// The tasks's attempts
    /// Keeps track of the number of attempts a task has been worked on
    attempt: Attempt,

    /// The task status that is wrapped in an atomic status
    status: AtomicStatus,

    /// The time a task should be run
    run_at: Option<u64>,

    /// The time the task was completed, if applicable
    done_at: Option<u64>,

    /// The time the task was locked, if applicable
    lock_at: Option<u64>,

    /// Identifier of the worker/process that currently holds the lock on this task
    lock_by: Option<String>,

    /// Adds a unique key to enforce job uniqueness when used
    idempotency_key: Option<String>,

    /// Metadata associated with the task
    metadata: MetadataStore,

    /// The maximum number of attempts allowed for the task
    max_attempts: Option<usize>,

    /// The priority of the task, which can be used for scheduling
    priority: Option<usize>,

    /// The queue to which the task belongs, if applicable
    queue: Option<Queue>,

    /// A list of all runs for this task
    runs: Vec<Run>,
}

impl ExecutionContext {
    /// Returns the task's ID, if one has been allocated.
    #[must_use]
    pub fn task_id(&self) -> Option<&TaskId> {
        self.task_id.as_ref()
    }

    /// Returns the extensions associated with the task.
    #[must_use]
    pub fn data(&self) -> &Extensions {
        &self.data
    }

    /// Returns the number of attempts made to execute the task.
    #[must_use]
    pub fn attempt(&self) -> usize {
        self.attempt.current()
    }

    /// Get the atomic attempt
    #[must_use]
    pub fn raw_attempt(&self) -> &Attempt {
        &self.attempt
    }

    /// Returns the current status of the task.
    #[must_use]
    pub fn status(&self) -> Status {
        self.status.load()
    }

    /// Get the atomic status
    #[must_use]
    pub fn raw_status(&self) -> &AtomicStatus {
        &self.status
    }

    /// Returns the time at which the task is scheduled to run.
    ///
    /// Returns `None` if no run time has been specified.
    #[must_use]
    pub fn run_at(&self) -> Option<u64> {
        self.run_at
    }

    /// Returns the time at which the task was completed.
    ///
    /// Returns `None` if the task has not been completed.
    #[must_use]
    pub fn done_at(&self) -> Option<u64> {
        self.done_at
    }

    /// Returns the time at which the task was locked.
    ///
    /// Returns `None` if the task is not locked.
    #[must_use]
    pub fn lock_at(&self) -> Option<u64> {
        self.lock_at
    }

    /// Returns the identifier of the worker or process currently holding
    /// the lock on this task.
    ///
    /// Returns `None` if the task is not currently locked.
    #[must_use]
    pub fn lock_by(&self) -> Option<&str> {
        self.lock_by.as_deref()
    }

    /// Returns the idempotency key associated with the task.
    ///
    /// Returns `None` if no idempotency key has been specified.
    #[must_use]
    pub fn idempotency_key(&self) -> Option<&str> {
        self.idempotency_key.as_deref()
    }

    /// Returns the metadata associated with the task.
    #[must_use]
    pub fn metadata(&self) -> &MetadataStore {
        &self.metadata
    }

    /// Returns the maximum number of attempts allowed for the task.
    ///
    /// Returns `None` if no maximum has been specified.
    #[must_use]
    pub fn max_attempts(&self) -> Option<usize> {
        self.max_attempts
    }

    /// Returns the priority of the task.
    ///
    /// Returns `None` if no priority has been specified.
    #[must_use]
    pub fn priority(&self) -> Option<usize> {
        self.priority
    }

    /// Returns the queue to which the task belongs.
    ///
    /// Returns `None` if the task is not associated with a queue.
    #[must_use]
    pub fn queue(&self) -> Option<&Queue> {
        self.queue.as_ref()
    }

    /// Returns the runs recorded for this task.
    #[must_use]
    pub fn runs(&self) -> &[Run] {
        &self.runs
    }
}

impl<Args> Task<Args> {
    /// Creates a new task given some args
    ///
    /// Used to easily create a ready task.
    /// Please prefer to use [`TaskBuilder`] if you need to modify [`ExecutionContext`]
    pub fn new(args: Args) -> Self {
        Self {
            args,
            ctx: Default::default(),
        }
    }
    /// Returns the execution context associated with the task.
    #[must_use]
    pub fn ctx(&self) -> &Arc<ExecutionContext> {
        &self.ctx
    }
}

impl Default for ExecutionContext {
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
        }
    }
}

impl Debug for ExecutionContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ExecutionContext")
            .field("task_id", &self.task_id)
            .field("data", &"<Extensions>")
            .field("attempt", &self.attempt)
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

impl Clone for ExecutionContext {
    fn clone(&self) -> Self {
        Self {
            task_id: self.task_id.clone(),
            data: self.data.clone(),
            attempt: self.attempt.clone(),
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

impl<Args> Task<Args> {
    /// Take the task into its parts
    #[must_use]
    pub fn take(self) -> (Args, Arc<ExecutionContext>) {
        (self.args, self.ctx)
    }

    /// Extract a value of type `T` from the task's context
    ///
    /// Uses [FromRequest] trait to extract the value.
    #[must_use = "An extracted value should be used or handled to avoid unused value warnings."]
    pub async fn extract<T: FromRequest<Self>>(&self) -> Result<T, T::Error> {
        T::from_request(self).await
    }

    /// Maps the `args` field using the provided function, consuming the task.
    pub fn map_args<F, NewArgs>(self, f: F) -> Task<NewArgs>
    where
        F: FnOnce(Args) -> NewArgs,
    {
        Task {
            args: f(self.args),
            ctx: self.ctx,
        }
    }

    /// Maps the `args` field using the provided function, consuming the task.
    #[must_use = "A mapped task should be used or handled to avoid unused value warnings."]
    pub fn try_map_args<F, NewArgs, Err>(self, f: F) -> Result<Task<NewArgs>, Err>
    where
        F: FnOnce(Args) -> Result<NewArgs, Err>,
    {
        Ok(Task {
            args: f(self.args)?,
            ctx: self.ctx,
        })
    }

    /// Maps the `execution_context` using the provided function, consuming the task
    #[must_use]
    pub fn map_context<F>(self, f: F) -> Self
    where
        F: FnOnce(Arc<ExecutionContext>) -> Arc<ExecutionContext>,
    {
        Self {
            args: self.args,
            ctx: f(self.ctx),
        }
    }

    /// Modifies the id type of the task, consuming the task
    ///
    /// See [`crate::backend::ext::pipe`]
    #[must_use]
    #[doc(hidden)]
    pub fn map_id_type(self) -> Self {
        let mut ctx = Arc::unwrap_or_clone(self.ctx);
        let _ = ctx.metadata.insert(
            "apalis_core.transform.old_id",
            ctx.task_id.map(|id| id.to_string()).unwrap_or_default(),
        );
        Self {
            args: self.args,
            ctx: Arc::new(ExecutionContext {
                task_id: None,
                data: ctx.data,
                attempt: ctx.attempt,
                status: ctx.status,
                run_at: ctx.run_at,
                done_at: ctx.done_at,
                lock_at: ctx.lock_at,
                lock_by: ctx.lock_by,
                idempotency_key: ctx.idempotency_key,
                metadata: ctx.metadata,
                max_attempts: ctx.max_attempts,
                priority: ctx.priority,
                queue: ctx.queue,
                runs: ctx.runs,
            }),
        }
    }

    /// Converts the task into a [`TaskBuilder`]
    #[must_use = "Converting a task into a builder allows for further modifications before rebuilding the task."]
    pub fn into_builder(self) -> TaskBuilder<Args> {
        TaskBuilder {
            args: self.args,
            ctx: Arc::unwrap_or_clone(self.ctx),
        }
    }

    /// Inject data into the execution context via [`Arc::make_mut`]
    ///
    /// If an extension of this type already existed, it will
    /// be returned.
    pub fn inject_data<D>(&mut self, data: D) -> Option<D>
    where
        D: Send + Clone + Sync + 'static,
    {
        let ctx = Arc::make_mut(&mut self.ctx);
        ctx.data.insert(data)
    }

    /// Inject metadata into the execution context via [`Arc::make_mut`]
    pub fn inject_metadata<M>(&mut self, value: &M) -> Result<(), M::Error>
    where
        M: Metadata,
    {
        let ctx = Arc::make_mut(&mut self.ctx);
        value.inject(&mut ctx.metadata)?;
        Ok(())
    }

    /// Build a new task with ready parts
    ///
    /// Usually you wanna use [TaskBuilder] so this is considered an internal api
    #[doc(hidden)]
    pub fn new_with_ctx(args: Args, ctx: Arc<ExecutionContext>) -> Self {
        Self { args, ctx }
    }
}

impl<Args> Deref for Task<Args> {
    type Target = ExecutionContext;

    fn deref(&self) -> &Self::Target {
        &self.ctx
    }
}

impl<A: Send + 'static> From<A> for Task<A> {
    fn from(value: A) -> Self {
        Self {
            args: value,
            ctx: Default::default(),
        }
    }
}
