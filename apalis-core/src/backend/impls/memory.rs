//! # In-memory backend based on channels
//!
//! An in-memory backend suitable for testing, prototyping, or lightweight task processing scenarios where persistence is not required.
//!
//! ## Features
//! - Generic in-memory queue for any task type.
//! - Implements [`Backend`] for integration with workers.
//! - Sink support: Ability to push new tasks.
//!
//! A detailed feature list can be found in the [capabilities](crate::backend::memory::MemoryStorage#capabilities) section.
//!
//! ## Example
//!
//! ```rust
//! # use apalis_core::backend::memory::MemoryStorage;
//! # use apalis_core::worker::context::WorkerContext;
//! # use apalis_core::worker::builder::WorkerBuilder;
//! # use apalis_core::backend::TaskSink;
//!
//! async fn handler(_: u32, worker: WorkerContext) {
//!     worker.stop().unwrap();
//! }
//!
//! #[tokio::main]
//! async fn main() {
//!     let mut store = MemoryStorage::new();
//!     store.push(42).await.unwrap();
//!
//!     let worker = WorkerBuilder::new("int-worker")
//!         .backend(store)
//!         .build(handler);
//!
//!     worker.run().await.unwrap();
//! }
//! ```
//!
//! ## Note
//! This backend is not persistent and is intended for use cases where durability is not required.
//! For production workloads, consider using a persistent backend such as PostgreSQL or Redis.
//!
//! ## See Also
//! - [`Backend`]
//! - [`WorkerContext`]
use crate::backend::finalize::Ephemeral;
use crate::backend::{Backend, BackendConfig, TryNewBackend};
use crate::error::BoxDynError;
use crate::{
    task::{
        Task,
        task_id::{RandomId, TaskId},
    },
    worker::context::WorkerContext,
};
use futures_channel::mpsc::{SendError, unbounded};
use futures_core::ready;
use futures_sink::Sink;
use futures_util::lock::Mutex;
use futures_util::{FutureExt, SinkExt, Stream, StreamExt};
use std::collections::HashSet;
use std::{
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};
use tower_layer::Identity;

/// A boxed in-memory task receiver stream
pub type BoxedReceiver<Args> = Pin<Box<dyn Stream<Item = Task<Args>> + Send>>;

/// In-memory queue that is based on channels
///
///
/// ## Example
/// ```rust
/// # use apalis_core::backend::memory::MemoryStorage;
/// # fn setup() -> MemoryStorage<u32> {
/// let mut backend = MemoryStorage::new();
/// # backend
/// # }
/// ```
///
#[doc = features_table! {
    setup = r#"
        # {
        #   use apalis_core::backend::memory::MemoryStorage;
        #   MemoryStorage::new()
        # };
    "#,
    Backend => supported("Basic Backend functionality", true),
    TaskSink => supported("Ability to push new tasks", true),
    Serialization => not_supported("Serialization support for arguments"),

    PipeExt => not_implemented("Allow other backends to pipe to this backend"),
    BackendFactory => not_supported("Share the same storage across multiple workers"),

    Update => not_supported("Allow updating a task"),
    FetchById => not_supported("Allow fetching a task by its ID"),
    Reschedule => not_supported("Reschedule a task"),

    ResumeById => not_supported("Resume a task by its ID"),
    ResumeAbandoned => not_supported("Resume abandoned tasks"),
    Vacuum => not_supported("Vacuum the task storage"),

    Workflow => not_implemented("Flexible enough to support workflows"),
    WaitForCompletion => not_implemented("Wait for tasks to complete without blocking"), // Requires Clone

    RegisterWorker => not_supported("Allow registering a worker with the backend"),
    ListWorkers => not_supported("List all workers registered with the backend"),
    ListTasks => not_supported("List all tasks in the backend"),
}]
pub struct MemoryStorage<Args> {
    pub(super) sender: MemorySink<Args>,
    pub(super) receiver: std::sync::Mutex<BoxedReceiver<Args>>,
}

impl<Args: Send + 'static> Default for MemoryStorage<Args> {
    fn default() -> Self {
        Self::new()
    }
}

/// Error type for MemoryStorage operations
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum MemoryStorageError {
    /// Error occurred while sending a task to the in-memory channel
    #[error("Failed to send task: {0}")]
    SendError(#[from] SendError),
    /// Error occurred while flushing the in-memory channel
    #[error("Failed to add task to storage: {0}")]
    Other(BoxDynError),
}

impl<Args: Send + 'static> MemoryStorage<Args> {
    /// Create a new in-memory storage
    #[must_use]
    pub fn new() -> Self {
        let (sender, receiver) = unbounded();
        let sender = Box::new(sender.sink_map_err(|e| e.into()))
            as Box<dyn Sink<Task<Args>, Error = MemoryStorageError> + Send + Sync + Unpin>;
        Self {
            sender: MemorySink {
                inner: Arc::new(futures_util::lock::Mutex::new(sender)),
                idempotency_keys: Default::default(),
            },
            receiver: receiver.boxed().into(),
        }
    }
    /// Create a storage given a sender and receiver
    #[must_use]
    pub fn new_with(sender: MemorySink<Args>, receiver: BoxedReceiver<Args>) -> Self {
        Self {
            sender,
            receiver: receiver.into(),
        }
    }
}

impl<Args> Sink<Task<Args>> for MemoryStorage<Args> {
    type Error = MemoryStorageError;

    fn poll_ready(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.as_mut().sender.poll_ready_unpin(cx)
    }

    fn start_send(mut self: Pin<&mut Self>, item: Task<Args>) -> Result<(), Self::Error> {
        self.as_mut().sender.start_send_unpin(item)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.as_mut().sender.poll_flush_unpin(cx)
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.as_mut().sender.poll_close_unpin(cx)
    }
}

type ArcMemorySink<Args> = Arc<
    Mutex<Box<dyn Sink<Task<Args>, Error = MemoryStorageError> + Send + Sync + Unpin + 'static>>,
>;

type ArcIdempotencySet = Arc<Mutex<HashSet<String>>>;

/// Memory sink for sending tasks to the in-memory backend
pub struct MemorySink<Args> {
    pub(super) inner: ArcMemorySink<Args>,
    pub(super) idempotency_keys: ArcIdempotencySet,
}

impl<Args> MemorySink<Args> {
    /// Build a new memory sink given a sink
    pub fn new(sink: ArcMemorySink<Args>) -> Self {
        Self {
            inner: sink,
            idempotency_keys: Arc::new(Mutex::new(HashSet::new())),
        }
    }
}

impl<Args> std::fmt::Debug for MemorySink<Args> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MemorySink")
            .field("inner", &"<Sink>")
            .field("idempotency_keys", &self.idempotency_keys.lock())
            .finish()
    }
}

impl<Args> Clone for MemorySink<Args> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
            idempotency_keys: Arc::clone(&self.idempotency_keys),
        }
    }
}

impl<Args> Sink<Task<Args>> for MemorySink<Args> {
    type Error = MemoryStorageError;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let mut lock = ready!(self.inner.lock().poll_unpin(cx));
        Pin::new(&mut *lock).poll_ready_unpin(cx)
    }

    fn start_send(self: Pin<&mut Self>, mut item: Task<Args>) -> Result<(), Self::Error> {
        let this = self.get_mut();
        if let Some(key) = item.idempotency_key() {
            let mut keys = this.idempotency_keys.try_lock().unwrap();

            if keys.contains(key) {
                return Ok(());
            }

            keys.insert(key.to_owned());
        }

        if item.task_id().is_none() {
            let task = item
                .into_builder()
                .task_id(TaskId::from_string(RandomId::default()));
            item = task.build();
        }

        let mut sink = this.inner.try_lock().unwrap();
        Pin::new(&mut *sink).start_send_unpin(item)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let mut lock = ready!(self.inner.lock().poll_unpin(cx));
        Pin::new(&mut *lock).poll_flush_unpin(cx)
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let mut lock = ready!(self.inner.lock().poll_unpin(cx));
        Pin::new(&mut *lock).poll_close_unpin(cx)
    }
}

impl<Args> std::fmt::Debug for MemoryStorage<Args> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MemoryStorage")
            .field("sender", &self.sender)
            .field("receiver", &"<Stream>")
            .finish()
    }
}

impl<Args> Stream for MemoryStorage<Args> {
    type Item = Task<Args>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.receiver.lock().unwrap().poll_next_unpin(cx)
    }
}

// MemoryStorage as a Backend
impl<Args> Backend for MemoryStorage<Args> {
    type Task = Task<Args>;

    type Error = MemoryStorageError;

    fn poll_ready(
        &mut self,
        cx: &mut Context<'_>,
        _: &WorkerContext,
    ) -> Poll<Result<(), Self::Error>> {
        self.sender.poll_ready_unpin(cx)
    }

    fn poll_next(
        &mut self,
        cx: &mut Context<'_>,
        _: &WorkerContext,
    ) -> Poll<Option<Result<Self::Task, Self::Error>>> {
        self.receiver
            .lock()
            .unwrap()
            .poll_next_unpin(cx)
            .map(|item| item.map(Ok))
    }

    fn poll_close(
        &mut self,
        cx: &mut Context<'_>,
        _: &WorkerContext,
    ) -> Poll<Result<(), Self::Error>> {
        self.sender.poll_close_unpin(cx)
    }
}

impl<Args> BackendConfig for MemoryStorage<Args> {
    type Id = RandomId;

    type Args = Args;

    type Kind = Ephemeral;

    type Config = ();

    type Layer = Identity;

    fn config(&self) -> &Self::Config {
        &()
    }

    fn middleware(&mut self, _: &mut WorkerContext) -> Self::Layer {
        Identity::new()
    }
}

impl<T: Send + 'static> TryNewBackend for MemoryStorage<T> {
    type Backend = Self;
    fn try_new(_: Self::Config) -> Result<Self::Backend, Self::Error> {
        Ok(Self::new())
    }
}
