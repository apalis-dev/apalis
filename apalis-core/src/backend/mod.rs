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
//! ## Provided Implementations
//!
//! The module includes several default backend implementations, such as:
//! - [`MemoryStorage`](memory::MemoryStorage): An in-memory backend for testing and lightweight use cases
//! - [`CustomBackend`](custom::CustomBackend): A flexible backend allowing custom functions for task management
//! - [`VecDequeBackend`](dequeue::VecDequeBackend): A simple in-memory backend that uses a `VecDeque` to store tasks.
//! - [`Persisted`](persistence::Persisted): A backend that drives a [`Persistence`](persistence::Persistence) instance to emit tasks
use std::{
    fmt::Debug,
    future::Future,
    task::{Context, Poll},
    time::Duration,
};

use crate::{backend::codec::Codec, task::task_id::TaskId, worker::context::WorkerContext};

pub mod codec;
pub mod custom;
pub mod ext;
pub mod factory;
/// Finalizes an intermediate backend configuration into a concrete backend.
///
/// Used internally by workers
pub mod finalize;
pub mod queue;

mod expose;
/// Provides utilities for working with futures
pub mod future;
mod impls;
/// Persistence layer for managing workers, tasks and events
///
/// Provides the `Persistence` trait which eases making backends with predictable persistence
#[cfg(feature = "sleep")]
pub mod persistence;
mod results;
mod sink;
mod worker;

pub use expose::*;
pub use results::*;
pub use sink::*;
pub use worker::*;
/// In-memory backend based on channels
pub mod memory {
    pub use crate::backend::impls::memory::*;
}

/// In-memory dequeue backend
pub mod dequeue {
    pub use crate::backend::impls::dequeue::*;
}

/// A task backend that drives task acquisition and lifecycle management for a worker.
///
/// A backend is responsible for:
///
/// - determining when it is ready to perform work;
/// - yielding available tasks;
/// - registering the worker's waker when no work is currently available; and
/// - completing any pending cleanup or acknowledgement work during shutdown.
///
/// In other systems, this abstraction may be called a queue, broker, or
/// consumer. The Backend abstraction is intentionally broader than a queue:
/// it may also manage acknowledgements, leases, heartbeats, subscriptions, and
/// other state associated with processing tasks.
///
/// The worker drives a backend by repeatedly polling [poll_ready], [poll_next],
/// and [poll_close] as appropriate. Implementations must not block inside these
/// methods.
///
/// # Polling
///
/// Backend methods follow the same conventions as [Future] and [Stream]:
///
/// - Poll::Ready means the operation can make progress immediately.
/// - Poll::Pending means the operation cannot make progress yet and the
///   implementation has registered the supplied waker.
/// - The implementation must arrange for the waker to be woken when polling
///   again may make progress.
///
/// A backend should avoid busy-polling. In particular, [poll_next] should
/// return Poll::Pending once all currently available tasks have been drained
/// and wait for its underlying source to signal that more work is available.
///
/// # Shutdown
///
/// [poll_close] is called when the worker is shutting down. It should continue
/// making progress until all backend-owned resources and pending operations
/// have been released. A backend must not report completion from poll_close
/// while work that it is responsible for flushing or releasing remains.
///
/// [poll_next]: Backend::poll_next
/// [poll_ready]: Backend::poll_ready
/// [poll_close]: Backend::poll_close
/// [Future]: std::future::Future
/// [Stream]: futures_core::Stream
#[must_use = "a Backend does nothing unless it is polled"]
pub trait Backend: Sized {
    /// The type of task the backend emits.
    type Task;

    /// The error type returned by backend operations
    type Error: std::error::Error + Send + Sync + 'static;

    /// Polls whether the backend is ready for the worker to request more work.
    ///
    /// This method may be used to flush pending backend operations or perform
    /// other work required before [`poll_next`] can be called.
    /// Returning `Poll::Ready(Ok(()))` indicates that the worker may proceed.
    /// Returning `Poll::Pending` indicates that the backend cannot make progress
    /// yet. The implementation must register `cx.waker()` and wake it when the
    /// backend becomes ready.
    ///
    /// [`poll_next`]: Backend::poll_next
    fn poll_ready(
        &mut self,
        cx: &mut Context<'_>,
        worker: &WorkerContext,
    ) -> Poll<Result<(), Self::Error>>;

    /// Polls the backend for the next available task for this worker.
    ///
    /// Like `poll_ready`, this is lazy: a backend should only produce a
    /// task when one is actually available, not busy-poll its source.
    /// Once the buffer of ready tasks is drained and this returns
    /// `Poll::Pending`, the backend is expected to register the waker and
    /// go to sleep until new work arrives rather than being polled again
    /// right away. Returns `None` when the backend is exhausted and will
    /// never produce another task.
    ///
    /// # Example
    ///
    /// A minimal in-memory backend might look like this:
    ///
    /// ```ignore
    /// fn poll_next(
    ///     &mut self,
    ///     cx: &mut Context<'_>,
    ///     _worker: &WorkerContext,
    /// ) -> Poll<Option<Result<Task, Self::Error>>> {
    ///     match self.queue.pop_front() {
    ///         // A task was ready, return it immediately.
    ///         Some(task) => Poll::Ready(Some(Ok(task))),
    ///         // Nothing to do right now: register the waker and sleep
    ///         // until `wake()` is called (e.g. when a task is pushed).
    ///         None => {
    ///             self.waker = Some(cx.waker().clone());
    ///             Poll::Pending
    ///         }
    ///     }
    /// }
    /// ```
    fn poll_next(
        &mut self,
        cx: &mut Context<'_>,
        worker: &WorkerContext,
    ) -> Poll<Option<Result<Self::Task, Self::Error>>>;

    /// Flushes/releases any resources the backend holds (pending acks,
    /// open subscriptions, connections) before the worker fully shuts down.
    fn poll_close(
        &mut self,
        cx: &mut Context<'_>,
        worker: &WorkerContext,
    ) -> Poll<Result<(), Self::Error>>;
}

/// Decorates a backend with middleware, configuration and associated types
pub trait BackendConfig {
    /// The type of argument this backend emits
    type Args;

    /// The internal type of this Backend's [`TaskId`]
    type Id;

    /// This defines the kind of Backend
    type Kind;

    /// The config for the backend
    type Config;

    /// The type representing backend middleware layer.
    type Layer;

    /// Returns the config associated with the backend.
    fn config(&self) -> &Self::Config;

    /// Returns the backend's middleware layer.
    fn middleware(&mut self, worker: &mut WorkerContext) -> Self::Layer;
}

/// Provides wire-format support for a backend.
///
/// A `WireFormatBackend` associates a backend with a codec used to encode and
/// decode tasks and with the compact representation used when tasks are
/// transmitted or persisted in their encoded form.
pub trait WireFormatBackend {
    /// The codec used to encode and decode tasks.
    type Codec: Send + 'static;

    /// The compact representation of task arguments.
    type Compact;

    /// Returns a reference to the codec used by the backend.
    fn codec(&self) -> &Self::Codec;
}

/// Allows fetching a task by its ID
pub trait FetchById: Backend {
    /// Fetch a task by its unique identifier
    #[allow(clippy::type_complexity)]
    fn fetch_by_id(
        &mut self,
        task_id: &TaskId,
    ) -> impl Future<Output = Result<Option<Self::Task>, Self::Error>> + Send;
}

/// Allows updating an existing task
pub trait Update: Backend {
    /// Update the given task
    fn update(&mut self, task: Self::Task) -> impl Future<Output = Result<(), Self::Error>> + Send;
}

/// Allows rescheduling a task for later execution
pub trait Reschedule: Backend {
    /// Reschedule the task after a specified duration
    fn reschedule(
        &mut self,
        task: Self::Task,
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
        id: TaskId,
    ) -> impl Future<Output = Result<bool, Self::Error>> + Send;
}

/// A [`Backend`] that can be fallibly constructed from its own configuration.
pub trait TryNewBackend: BackendConfig + Backend + Sized {
    /// The final backend after decoration
    ///
    /// This value may easily be Self;
    /// But in some cases we want to add some `BackendExt` compositions
    type Backend;
    /// Attempt to construct a new backend instance from its configuration.
    fn try_new(config: Self::Config) -> Result<Self::Backend, Self::Error>;
}
