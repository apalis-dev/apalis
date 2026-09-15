use apalis_codec::json::JsonCodec;
use apalis_core::backend::ext::shared::Shared;
use apalis_core::backend::finalize::Durable;
use apalis_core::backend::memory::{BoxedReceiver, MemorySink, MemoryStorage, MemoryStorageError};
use apalis_core::backend::{
    Backend, BackendConfig, TaskResult, WaitForCompletion, WireFormatBackend,
};
use apalis_core::features_table;
use apalis_core::{
    task::{
        Task,
        task_id::{RandomId, TaskId},
    },
    worker::context::WorkerContext,
};
use futures_sink::Sink;
use futures_util::SinkExt;
use serde::de::DeserializeOwned;
use std::marker::PhantomData;
use std::{
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use crate::in_memory::result_store::ResultStore;
use crate::in_memory::service::StoreResultsLayer;
use crate::in_memory::stream::WaitForStream;

mod result_store;

mod service;

mod stream;

/// In-memory queue that is based on channels
///
///
/// ## Example
/// ```rust
/// # use apalis_workflow::in_memory::InMemoryWorkflow;
/// # use apalis_core::backend::ext::shared::Shared;
/// # fn setup() -> Shared<InMemoryWorkflow<u32>> {
/// let mut backend = InMemoryWorkflow::create();
/// # backend
/// # }
/// ```
///
#[doc = features_table! {
    setup = r#"
        # {
        #   use apalis_workflow::in_memory::InMemoryWorkflow;
        #   InMemoryWorkflow::create()
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
pub struct InMemoryWorkflow<Args> {
    pub(super) inner: MemoryStorage<Vec<u8>>,
    _marker: PhantomData<Args>,
    codec: JsonCodec,
    store: Arc<ResultStore>,
}

impl<Args> InMemoryWorkflow<Args> {
    /// Create a new in-memory storage
    #[must_use]
    pub fn create() -> Shared<Self> {
        Shared::new(Self {
            _marker: PhantomData,
            codec: JsonCodec::default(),
            inner: MemoryStorage::new(),
            store: Arc::default(),
        })
    }
}

impl<Args> InMemoryWorkflow<Args> {
    /// Create a storage given a sender and receiver
    #[must_use]
    pub fn new_with(sender: MemorySink<Vec<u8>>, receiver: BoxedReceiver<Vec<u8>>) -> Shared<Self> {
        Shared::new(Self {
            inner: MemoryStorage::new_with(sender, receiver),
            _marker: PhantomData,
            codec: JsonCodec::default(),
            store: Arc::default(),
        })
    }
}

impl<Args> Sink<Task<Vec<u8>>> for InMemoryWorkflow<Args>
where
    Args: Unpin,
{
    type Error = MemoryStorageError;

    fn poll_ready(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.as_mut().inner.poll_ready_unpin(cx)
    }

    fn start_send(mut self: Pin<&mut Self>, item: Task<Vec<u8>>) -> Result<(), Self::Error> {
        self.as_mut().inner.start_send_unpin(item)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.as_mut().inner.poll_flush_unpin(cx)
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.as_mut().inner.poll_close_unpin(cx)
    }
}

impl<Args> std::fmt::Debug for InMemoryWorkflow<Args> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InMemoryWorkflow")
            .field("inner", &self.inner)
            .finish()
    }
}

// InMemoryWorkflow as a Backend
impl<Args> Backend for InMemoryWorkflow<Args> {
    type Task = Task<Vec<u8>>;

    type Error = MemoryStorageError;

    fn poll_ready(
        &mut self,
        cx: &mut Context<'_>,
        worker: &WorkerContext,
    ) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx, worker)
    }

    fn poll_next(
        &mut self,
        cx: &mut Context<'_>,
        worker: &WorkerContext,
    ) -> Poll<Option<Result<Self::Task, Self::Error>>> {
        self.inner.poll_next(cx, worker)
    }

    fn poll_close(
        &mut self,
        cx: &mut Context<'_>,
        worker: &WorkerContext,
    ) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_close(cx, worker)
    }
}

impl<Args> BackendConfig for InMemoryWorkflow<Args> {
    type Id = RandomId;

    type Args = Args;

    type Kind = Durable;

    type Config = ();

    type Layer = StoreResultsLayer;

    fn config(&self) -> &Self::Config {
        &()
    }

    fn middleware(&mut self, _: &mut WorkerContext) -> Self::Layer {
        StoreResultsLayer::new(Arc::clone(&self.store))
    }
}

impl<Args> WireFormatBackend for InMemoryWorkflow<Args> {
    type Codec = JsonCodec;

    type Compact = Vec<u8>;

    fn codec(&self) -> &Self::Codec {
        &self.codec
    }
}

impl<Args, Output> WaitForCompletion<Output> for InMemoryWorkflow<Args>
where
    Output: DeserializeOwned + Unpin + Send + 'static,
{
    type ResultStream = WaitForStream<Output>;

    fn wait_for(&self, task_ids: impl IntoIterator<Item = TaskId>) -> Self::ResultStream {
        WaitForStream::new(task_ids, Arc::clone(&self.store))
    }

    fn check_status(
        &self,
        task_ids: impl IntoIterator<Item = TaskId> + Send,
    ) -> impl Future<Output = Result<Vec<TaskResult<Output>>, Self::Error>> + Send {
        let store = Arc::clone(&self.store);
        let task_ids: Vec<_> = task_ids.into_iter().collect();

        async move {
            let results = &store.results;

            task_ids
                .iter()
                .filter_map(|id| results.get(id))
                .map(|result| {
                    let result = result.value();
                    let decoded = result
                        .result
                        .as_ref()
                        .map(|a| Output::deserialize(a).map_err(|e| e.to_string()))
                        .map_err(|e| MemoryStorageError::Other(e.as_str().into()))?;
                    Ok(TaskResult {
                        task_id: result.task_id.clone(),
                        attempt: result.attempt,
                        status: result.status.clone(),
                        result: decoded,
                    })
                })
                .collect()
        }
    }
}
