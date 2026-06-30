//! Core traits for interacting with backends
//!
//! The core traits and types for backends, responsible for providing sources of tasks, handling their lifecycle, and exposing middleware for internal processing.
//! The traits here abstract over different backend implementations, allowing for extensibility and interoperability.
//!
//! # Overview
//! - [`Backend`]: The primary trait representing a task source, defining methods for polling tasks, heartbeats, and middleware.
//! - [`TaskSink`]: An extension trait for backends that support pushing tasks.
//! - [`FetchById`], [`Update`], [`Reschedule`]: Additional traits for managing tasks.
//! - [`Vacuum`], [`ResumeById`], [`ResumeAbandoned`]: Traits for backend maintenance and task recovery.
//! - [`RegisterWorker`], [`ListWorkers`], [`ListTasks`]: Traits for worker management and task listing.
//! - [`WaitForCompletion`]: A trait for waiting on task completion and checking their status.
//!
//!
//! ## Default Implementations
//!
//! The module includes several default backend implementations, such as:
//! - [`MemoryStorage`](memory::MemoryStorage): An in-memory backend for testing and lightweight use cases
//! - [`Pipe`](pipe::Pipe): A simple pipe-based backend for inter-thread communication
//! - [`CustomBackend`](custom::CustomBackend): A flexible backend allowing custom functions for task management
use std::{future::Future, time::Duration};

use futures_util::{Stream, stream::BoxStream};

use crate::{
    backend::{codec::Codec, queue::Queue},
    error::BoxDynError,
    task::{Task, status::Status, task_id::TaskId},
    worker::context::WorkerContext,
};

pub mod codec;
pub mod custom;
pub mod pipe;
pub mod poll_strategy;
pub mod queue;
pub mod shared;

mod expose;
mod impls;
mod sink;

pub use expose::*;
pub use sink::*;

pub use impls::guide;

/// In-memory backend based on channels
pub mod memory {
    pub use crate::backend::impls::memory::*;
}

/// In-memory dequeue backend
#[cfg(feature = "sleep")]
pub mod dequeue {
    pub use crate::backend::impls::dequeue::*;
}

/// The `Backend` trait defines how workers get and manage tasks from a backend.
///
/// In other languages, this might be called a "Queue", "Broker", etc.
pub trait Backend {
    /// The type of arguments the backend handles.
    type Args;
    /// The type used to uniquely identify tasks.
    type IdType: Clone;
    /// The type of connection used by the backend.
    type Connection;
    /// The error type returned by backend operations
    type Error;
    /// A stream of tasks provided by the backend.
    type Stream: Stream<
        Item = Result<Option<Task<Self::Args, Self::Connection, Self::IdType>>, Self::Error>,
    >;
    /// A stream representing heartbeat signals.
    type Beat: Stream<Item = Result<(), Self::Error>>;
    /// The type representing backend middleware layer.
    type Layer;

    /// Returns a heartbeat stream for the given worker.
    fn heartbeat(&self, worker: &WorkerContext) -> Self::Beat;
    /// Returns the backend's middleware layer.
    fn middleware(&self) -> Self::Layer;
    /// Polls the backend for tasks for the given worker.
    fn poll(self, worker: &WorkerContext) -> Self::Stream;
}

/// Defines the encoding/serialization aspects of a backend.
pub trait BackendExt: Backend {
    /// The codec used for serialization/deserialization of tasks.
    type Codec: Codec<Self::Args, Compact = Self::Compact>;
    /// The compact representation of task arguments.
    type Compact;
    /// A stream of encoded tasks provided by the backend.
    type CompactStream: Stream<
        Item = Result<Option<Task<Self::Compact, Self::Connection, Self::IdType>>, Self::Error>,
    >;

    /// Returns the queue associated with the backend.
    fn get_queue(&self) -> Queue;

    /// Polls the backend for encoded tasks for the given worker.
    fn poll_compact(self, worker: &WorkerContext) -> Self::CompactStream;
}

/// Represents a stream for T.
pub type TaskStream<T, E = BoxDynError> = BoxStream<'static, Result<Option<T>, E>>;
/// Allows fetching a task by its ID
pub trait FetchById<Args>: Backend {
    /// Fetch a task by its unique identifier
    #[allow(clippy::type_complexity)]
    fn fetch_by_id(
        &mut self,
        task_id: &TaskId<Self::IdType>,
    ) -> impl Future<
        Output = Result<Option<Task<Args, Self::Connection, Self::IdType>>, Self::Error>,
    > + Send;
}

/// Allows updating an existing task
pub trait Update: Backend {
    /// Update the given task
    fn update(
        &mut self,
        task: Task<Self::Args, Self::Connection, Self::IdType>,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;
}

/// Allows rescheduling a task for later execution
pub trait Reschedule: Backend {
    /// Reschedule the task after a specified duration
    fn reschedule(
        &mut self,
        task: Task<Self::Args, Self::Connection, Self::IdType>,
        wait: Duration,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;
}

/// Allows cleaning up resources in the backend
pub trait Vacuum: Backend {
    /// Cleans up resources and returns the number of items vacuumed
    fn vacuum(&mut self) -> impl Future<Output = Result<usize, Self::Error>> + Send;
}

/// Allows resuming a task by its ID
pub trait ResumeById: Backend {
    /// Resume a task by its ID
    fn resume_by_id(
        &mut self,
        id: TaskId<Self::IdType>,
    ) -> impl Future<Output = Result<bool, Self::Error>> + Send;
}

/// Allows fetching multiple tasks by their IDs
pub trait ResumeAbandoned: Backend {
    /// Resume all abandoned tasks
    fn resume_abandoned(&mut self) -> impl Future<Output = Result<usize, Self::Error>> + Send;
}

/// Allows registering a worker with the backend
pub trait RegisterWorker: Backend {
    /// Registers a worker
    fn register_worker(
        &mut self,
        worker_id: String,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;
}

/// Represents the result of a task execution
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Debug, Clone)]
pub struct TaskResult<T, IdType> {
    /// The unique identifier of the task
    pub task_id: TaskId<IdType>,
    /// The status of the task
    pub status: Status,
    /// The result of the task execution
    pub result: Result<T, String>,
}

impl<T, IdType> TaskResult<T, IdType> {
    /// Create a new TaskResult
    pub fn new(task_id: TaskId<IdType>, status: Status, result: Result<T, String>) -> Self {
        Self {
            task_id,
            status,
            result,
        }
    }
    /// Get the ID of the task
    pub fn task_id(&self) -> &TaskId<IdType> {
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
pub trait WaitForCompletion<T>: Backend {
    /// The result stream type yielding task results
    type ResultStream: Stream<Item = Result<TaskResult<T, Self::IdType>, Self::Error>>
        + Send
        + 'static;

    /// Wait for multiple tasks to complete, yielding results as they become available
    fn wait_for(
        &self,
        task_ids: impl IntoIterator<Item = TaskId<Self::IdType>>,
    ) -> Self::ResultStream;

    /// Wait for a single task to complete, yielding its result
    fn wait_for_single(&self, task_id: TaskId<Self::IdType>) -> Self::ResultStream {
        self.wait_for(std::iter::once(task_id))
    }

    /// Check current status of tasks without waiting
    fn check_status(
        &self,
        task_ids: impl IntoIterator<Item = TaskId<Self::IdType>> + Send,
    ) -> impl Future<Output = Result<Vec<TaskResult<T, Self::IdType>>, Self::Error>> + Send;
}

/// A helper trait to build connections and pollers for backends
/// This should be used in crates implementing Backend rather than end users.
pub trait TryIntoConnectionParts {
    /// The config for the backend
    type Config;
    /// The connection for the backend
    type Connection;
    /// The poller to be used by the backend
    type Fetcher;
    /// The error emitted during creation
    type Error: std::error::Error + Send + Sync + 'static;
    /// Generate the parts needed to build a Backend
    fn try_into_parts(
        self,
        config: &Self::Config,
    ) -> Result<(Self::Connection, Self::Fetcher), Self::Error>;
}

/// A helper to standardize building new backends
pub trait TryNewBackend: Backend + Sized {
    /// Build a new backend given a connection source and a config
    fn try_new<P>(src: P, config: P::Config) -> Result<Self, P::Error>
    where
        P: TryIntoConnectionParts<Connection = Self::Connection>;
}
