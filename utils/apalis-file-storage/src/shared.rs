use futures_core::stream::BoxStream;
use futures_sink::Sink;
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use serde_json::Value;
/// Sharable JSON based backend.
///
/// The [`SharedJsonStore`] allows multiple task types to be stored
/// and processed concurrently using a single JSON-based in-memory backend.
/// It is useful for testing, prototyping,
/// or sharing state between workers in a single process.
///
/// # Example
///
/// ```rust,no_run
/// # use apalis_core::backend::shared::MakeShared;
/// # use apalis_core::task::Task;
/// # use apalis_core::worker::context::WorkerContext;
/// # use apalis_core::worker::builder::WorkerBuilder;
/// # use apalis_file_storage::SharedJsonStore;
/// # use apalis_core::error::BoxDynError;
/// # use std::time::Duration;
/// # use apalis_core::backend::TaskSink;
///
/// #[tokio::main]
/// async fn main() {
///     let mut store = SharedJsonStore::new();
///     let mut int_store = store.make_shared().unwrap();
///     int_store.push(42).await.unwrap();
///
///     async fn task(
///         task: u32,
///         ctx: WorkerContext,
///     ) -> Result<(), BoxDynError> {
///         tokio::time::sleep(Duration::from_millis(2)).await;
///         ctx.stop()?;
///         Ok(())
///     }
///
///     let int_worker = WorkerBuilder::new("int-worker")
///         .backend(int_store)
///         .build(task)
///         .run();
///
///     int_worker.await.unwrap();
/// }
/// ```
///
/// See the tests for more advanced usage with multiple types and event listeners.
use std::{fmt::Debug, sync::Arc};

use apalis_core::{
    backend::{
        memory::{MemorySink, MemoryStorage, MemoryStorageError},
        shared::MakeShared,
    },
    task::{Task, builder::TaskBuilder, metadata::MetadataStore, task_id::RandomId},
};

use crate::JsonStorage;
/// Sharable JSON based backend.
///
/// # Features
///
/// - Concurrent processing of multiple task types
/// - In-memory storage with optional disk persistence
/// - Metadata support for tasks
#[derive(Debug, Clone)]
pub struct SharedJsonStore {
    inner: JsonStorage<Value>,
}

impl Default for SharedJsonStore {
    fn default() -> Self {
        Self::new()
    }
}

impl SharedJsonStore {
    /// Create a new instance of the shared JSON store.
    #[must_use]
    pub fn new() -> Self {
        Self {
            inner: JsonStorage::new_temp().unwrap(),
        }
    }
}

impl<Args: Send + Serialize + for<'de> Deserialize<'de> + Unpin + 'static> MakeShared<Args>
    for SharedJsonStore
{
    type Backend = MemoryStorage<Args, MetadataStore>;

    type Config = String;

    type MakeError = MemoryStorageError;

    fn make_shared(&mut self) -> Result<Self::Backend, Self::MakeError>
    where
        Self::Config: Default,
    {
        self.make_shared_with_config(std::any::type_name::<Args>().to_owned())
    }

    fn make_shared_with_config(
        &mut self,
        queue: Self::Config,
    ) -> Result<Self::Backend, Self::MakeError> {
        let (sender, receiver) = self.create_channel::<Args>(&queue);
        let sender = MemorySink::new(Arc::new(futures_util::lock::Mutex::new(sender)));
        Ok(MemoryStorage::new_with(sender, receiver))
    }
}

type BoxSink<Args> = Box<
    dyn Sink<Task<Args, MetadataStore, RandomId>, Error = MemoryStorageError>
        + Send
        + Sync
        + Unpin
        + 'static,
>;

impl SharedJsonStore {
    fn create_channel<Args: 'static + for<'de> Deserialize<'de> + Serialize + Send + Unpin>(
        &self,
        queue: &str,
    ) -> (
        BoxSink<Args>,
        BoxStream<'static, Task<Args, MetadataStore, RandomId>>,
    ) {
        // Create a channel for communication
        let sender = self.inner.clone();

        let queue_config = queue.to_owned();

        // Create a wrapped sender that will insert into the in-memory store
        let wrapped_sender = {
            let sender = sender.clone();

            sender
                .sink_map_err(|e| MemoryStorageError::Other(e.into()))
                .with_flat_map(move |task: Task<Args, MetadataStore, RandomId>| {
                    let mut task = task.into_builder();
                    task.ctx
                        .metadata
                        .insert("queue", queue_config.clone())
                        .unwrap();

                    let res = task
                        .try_map(|s| {
                            serde_json::to_value(s).map_err(|e| MemoryStorageError::Other(e.into()))
                        })
                        .map(|t| t.build());

                    futures_util::stream::iter(vec![res])
                })
        };

        // Create a stream that filters by type T
        let filtered_stream = {
            let queue_config = queue.to_owned();
            sender.map(|s| s.unwrap()).filter_map(move |(_, job)| {
                let queue_config = queue_config.clone();
                async move {
                    let queue = job.ctx.get("queue").cloned().unwrap_or_default();
                    if queue == queue_config {
                        let args = Args::deserialize(&job.args).ok()?;
                        let task = TaskBuilder::new(args)
                            .with_metadata(job.ctx)
                            .task_id(job.task_id.unwrap())
                            .build();
                        Some(task)
                    } else {
                        None
                    }
                }
            })
        };

        // Combine the sender and receiver
        let sender = Box::new(wrapped_sender)
            as Box<
                dyn Sink<Task<Args, MetadataStore, RandomId>, Error = MemoryStorageError>
                    + Send
                    + Sync
                    + Unpin,
            >;
        let receiver = filtered_stream.boxed();

        (sender, receiver)
    }
}
#[cfg(test)]
mod tests {
    use std::time::Duration;

    use apalis_core::error::BoxDynError;

    use apalis_core::worker::context::WorkerContext;
    use apalis_core::{
        backend::{TaskSink, shared::MakeShared},
        worker::{builder::WorkerBuilder, ext::event_listener::EventListenerExt},
    };

    use super::*;

    const ITEMS: u32 = 10;

    #[tokio::test]
    async fn basic_shared() {
        let mut store = SharedJsonStore::new();
        let mut string_store = store.make_shared().unwrap();
        let mut int_store = store.make_shared_with_config("int".into()).unwrap();
        for i in 0..ITEMS {
            string_store.push(format!("ITEM: {i}")).await.unwrap();
            int_store.push(i).await.unwrap();
        }

        async fn task(task: u32, ctx: WorkerContext) -> Result<(), BoxDynError> {
            tokio::time::sleep(Duration::from_millis(2)).await;
            if task == ITEMS - 1 {
                ctx.stop()?;
                return Err("Worker stopped!")?;
            }
            Ok(())
        }

        let string_worker = WorkerBuilder::new("rango-tango-string")
            .backend(string_store)
            .on_event(|ctx, ev| {
                println!("CTX {:?}, On Event = {ev:?}", ctx.name());
            })
            .build(|req: String, ctx: WorkerContext| async move {
                tokio::time::sleep(Duration::from_millis(2)).await;
                println!("{req}");
                if req.ends_with(&(ITEMS - 1).to_string()) {
                    ctx.stop().unwrap();
                }
            })
            .run();

        let int_worker = WorkerBuilder::new("rango-tango-int")
            .backend(int_store)
            .on_event(|ctx, ev| {
                println!("CTX {:?}, On Event = {ev:?}", ctx.name());
            })
            .build(task)
            .run();

        let _ = futures_util::future::join(int_worker, string_worker).await;
    }
}
