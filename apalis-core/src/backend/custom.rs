//! # Custom Backend
//!
//! A highly customizable backend for task processing that allows integration with any persistence engine by providing custom fetcher and sink functions.
//!
//! ## Overview
//!
//! The [`CustomBackend`] struct enables you to define how tasks are fetched from and persisted to
//! your storage engine.
//!
//! You can use the [`BackendBuilder`] to construct a [`CustomBackend`] by
//! providing the required database, fetcher, sink, and optional configuration and codec.
//!
//! ## Usage
//!
//! Use [`BackendBuilder`] to configure and build your custom backend:
//!
//! ## Example: CustomBackend with Worker
//!
//! ```rust
//! # use std::collections::VecDeque;
//! # use std::sync::Arc;
//! # use futures_util::{lock::Mutex, sink, stream, sink::SinkExt};
//! # use apalis_core::backend::custom::{BackendBuilder, CustomBackend};
//! # use apalis_core::task::Task;
//! # use apalis_core::task::task_id::{RandomId,TaskId};
//! # use apalis_core::task::builder::TaskBuilder;
//! # use apalis_core::worker::builder::WorkerBuilder;
//! # use apalis_core::worker::context::WorkerContext;
//! # use apalis_core::error::BoxDynError;
//! # use std::time::Duration;
//! # use futures_util::StreamExt;
//! # use futures_util::FutureExt;
//! # use apalis_core::backend::TaskSink;
//! #[tokio::main]
//! async fn main() {
//!     // Create a memory-backed VecDeque
//!     let memory = Arc::new(Mutex::new(VecDeque::<Task<u32>>::new()));
//!
//!     // Build the custom backend
//!     let mut backend = BackendBuilder::new()
//!         .database(memory)
//!         .fetcher(|memory, _, _| {
//!             stream::unfold(memory.clone(), |p| async move {
//!                 let mut memory = p.lock().await;
//!                 let item = memory.pop_front();
//!                 drop(memory);
//!                 match item {
//!                     Some(item) => Some((Ok::<_, BoxDynError>(Some(item)), p)),
//!                     None => Some((Ok::<_, BoxDynError>(None), p)),
//!                 }
//!             })
//!             .boxed()
//!         })
//!         .sink(|memory, _| {
//!             sink::unfold(memory.clone(), move |p, item: Task<_>| {
//!                 async move {
//!                     let mut memory = p.lock().await;
//!                     let item = item
//!                            .into_builder()
//!                            .task_id(TaskId::String(RandomId::default().to_string()))
//!                            .build();
//!                     memory.push_back(item);
//!                     drop(memory);
//!                     Ok::<_, BoxDynError>(p)
//!                 }
//!                 .boxed()
//!             })
//!         })
//!         .build()
//!         .unwrap();
//!
//!     // Add a task to the backend;
//!     backend.push(42).await.unwrap();
//!
//!     // Define the task handler
//!     async fn task(task: u32, worker: WorkerContext) -> Result<(), BoxDynError> {
//!         tokio::time::sleep(Duration::from_secs(1)).await;
//! #       worker.stop().unwrap();
//!         Ok(())
//!     }
//!
//!     // Build and run the worker
//!     let worker = WorkerBuilder::new("custom-worker")
//!         .backend(backend)
//!         .build(task);
//!     worker.run().await.unwrap();
//! }
//! ```
//!
//! ## Features
//!
//! - **Custom Fetcher**: Define how jobs are fetched from your storage.
//! - **Custom Sink**: Define how jobs are persisted to your storage.
//! - **Configurable**: Pass custom configuration to your backend.
//!
use futures_sink::Sink;
use futures_util::SinkExt;
use futures_util::{Stream, StreamExt};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::{fmt, marker::PhantomData};
use thiserror::Error;
use tower_layer::Identity;

use crate::backend::BackendConfig;
use crate::backend::finalize::Ephemeral;
use crate::error::BoxDynError;
use crate::task::task_id::RandomId;
use crate::{backend::Backend, task::Task, worker::context::WorkerContext};

type Fetcher<DB, Config, Fetch> =
    Arc<Box<dyn Fn(&mut DB, &Config, &WorkerContext) -> Fetch + Send + Sync>>;

type Sinker<DB, Config, Sink> = Arc<Box<dyn Fn(&mut DB, &Config) -> Sink + Send + Sync>>;

/// A highly customizable backend for integration with any persistence engine
///
/// This backend allows you to define how tasks are fetched from and persisted to your storage,
/// meaning you can use it to integrate with existing systems.
///
/// # Example
/// ```rust,ignore
/// let backend = BackendBuilder::new()
///     .database(my_db)
///     .fetcher(my_fetcher_fn)
///     .sink(my_sink_fn)
///     .build()
///     .unwrap();
/// ```
#[doc = features_table! {
    setup = "{ unreachable!() }",
    TaskSink => supported("Ability to push new tasks", false),
    Serialization => supported("Serialization support for arguments", false),
    FetchById => not_supported("Allow fetching a task by its ID"),
    RegisterWorker => not_implemented("Allow registering a worker with the backend"),
    PipeExt => limited("Allow other backends to pipe to this backend", false), // Would require Clone,
    BackendFactory => not_implemented("Share the same [`CustomBackend`] across multiple workers", false),
    Workflow => not_implemented("Flexible enough to support workflows"),
    WaitForCompletion => not_implemented("Wait for tasks to complete without blocking"), // Would require Clone
    ResumeById => not_supported("Resume a task by its ID"),
    ResumeAbandoned => not_supported("Resume abandoned tasks"),
    ListWorkers => not_implemented("List all workers registered with the backend"),
    ListTasks => not_implemented("List all tasks in the backend"),
}]
#[pin_project::pin_project]
#[must_use = "Custom backends must be polled or used as a sink"]
pub struct CustomBackend<Args, DB, Fetch, Sink, Config = ()> {
    _marker: PhantomData<Args>,
    db: DB,
    fetcher: Fetcher<DB, Config, Fetch>,
    sinker: Sinker<DB, Config, Sink>,
    #[pin]
    current_sink: Sink,
    config: Config,

    stream: Option<Fetch>,
}

impl<Args, DB, Fetch, Sink, Config> Clone for CustomBackend<Args, DB, Fetch, Sink, Config>
where
    DB: Clone,
    Config: Clone,
{
    fn clone(&self) -> Self {
        let mut db = self.db.clone();
        let current_sink = (self.sinker)(&mut db, &self.config);
        Self {
            _marker: PhantomData,
            db,
            fetcher: Arc::clone(&self.fetcher),
            sinker: Arc::clone(&self.sinker),
            current_sink,
            config: self.config.clone(),

            stream: None,
        }
    }
}

impl<Args, DB, Fetch, Sink, Config> fmt::Debug for CustomBackend<Args, DB, Fetch, Sink, Config>
where
    DB: fmt::Debug,
    Config: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CustomBackend")
            .field(
                "_marker",
                &format_args!("PhantomData<({})>", std::any::type_name::<Args>()),
            )
            .field("db", &self.db)
            .field("fetcher", &"Fn(&mut DB, &Config, &WorkerContext) -> Fetch")
            .field("sink", &"Fn(&mut DB, &Config) -> Sink")
            .field("config", &self.config)
            .finish()
    }
}

type FetcherBuilder<DB, Config, Fetch> =
    Box<dyn Fn(&mut DB, &Config, &WorkerContext) -> Fetch + Send + Sync + 'static>;

type SinkerBuilder<DB, Config, Sink> =
    Box<dyn Fn(&mut DB, &Config) -> Sink + Send + Sync + 'static>;

/// Builder for [`CustomBackend`]
///
/// Lets you set the database, fetcher, sink, codec, and config
pub struct BackendBuilder<Args, DB, Fetch, Sink, Config = ()> {
    _marker: PhantomData<Args>,
    database: Option<DB>,
    fetcher: Option<FetcherBuilder<DB, Config, Fetch>>,
    sink: Option<SinkerBuilder<DB, Config, Sink>>,
    config: Option<Config>,
}

impl<Args, DB, Fetch, Sink, Config> fmt::Debug for BackendBuilder<Args, DB, Fetch, Sink, Config>
where
    DB: fmt::Debug,
    Config: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BackendBuilder")
            .field(
                "_marker",
                &format_args!("PhantomData<({})>", std::any::type_name::<Args>(),),
            )
            .field("database", &self.database)
            .field("fetcher", &self.fetcher.as_ref().map(|_| "Some(fn)"))
            .field("sink", &self.sink.as_ref().map(|_| "Some(fn)"))
            .field("config", &self.config)
            .finish()
    }
}

impl<Args, DB, Fetch, Sink, Config> Default for BackendBuilder<Args, DB, Fetch, Sink, Config> {
    fn default() -> Self {
        Self {
            _marker: PhantomData,
            database: None,
            fetcher: None,
            sink: None,
            config: None,
        }
    }
}

impl<Args, DB, Fetch, Sink> BackendBuilder<Args, DB, Fetch, Sink, ()> {
    /// Create a new `BackendBuilder` instance
    #[must_use]
    pub fn new() -> Self {
        Self::new_with_cfg(())
    }

    /// Create a new `BackendBuilder` instance with custom configuration
    pub fn new_with_cfg<Config>(config: Config) -> BackendBuilder<Args, DB, Fetch, Sink, Config> {
        BackendBuilder {
            config: Some(config),
            ..Default::default()
        }
    }
}

impl<Args, DB, Fetch, Sink, Config> BackendBuilder<Args, DB, Fetch, Sink, Config> {
    /// The custom backend persistence engine
    #[must_use]
    pub fn database(mut self, db: DB) -> Self {
        self.database = Some(db);
        self
    }

    /// The fetcher function to retrieve tasks from the database
    #[must_use]
    pub fn fetcher<F: Fn(&mut DB, &Config, &WorkerContext) -> Fetch + Send + Sync + 'static>(
        mut self,
        fetcher: F,
    ) -> Self {
        self.fetcher = Some(Box::new(fetcher));
        self
    }

    /// The sink function to persist tasks to the database
    #[must_use]
    pub fn sink<F: Fn(&mut DB, &Config) -> Sink + Send + Sync + 'static>(
        mut self,
        sink: F,
    ) -> Self {
        self.sink = Some(Box::new(sink));
        self
    }

    #[allow(clippy::type_complexity)]
    /// Build the `CustomBackend` instance
    pub fn build(self) -> Result<CustomBackend<Args, DB, Fetch, Sink, Config>, BuildError> {
        let mut db = self.database.ok_or(BuildError::MissingDb)?;
        let config = self.config.ok_or(BuildError::MissingConfig)?;
        let sink_fn = self.sink.ok_or(BuildError::MissingSink)?;
        let sink = sink_fn(&mut db, &config);

        Ok(CustomBackend {
            _marker: PhantomData,
            db,
            fetcher: self
                .fetcher
                .map(Arc::new)
                .ok_or(BuildError::MissingFetcher)?,
            current_sink: sink,
            sinker: Arc::new(sink_fn),
            config,
            stream: None,
        })
    }
}

/// Errors encountered building a `CustomBackend`
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum BuildError {
    /// Missing database db
    #[error("Database db is required")]
    MissingDb,
    /// Missing fetcher function
    #[error("Fetcher is required")]
    MissingFetcher,
    /// Missing sink function
    #[error("Sink is required")]
    MissingSink,
    /// Missing configuration
    #[error("Config is required")]
    MissingConfig,
}

/// Errors encountered while using the `CustomBackend`
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum CustomBackendError {
    /// Inner error
    #[error("Inner error: {0}")]
    Inner(#[from] BoxDynError),
}

impl<Args: 'static, DB, Fetch, S, E, Config> Backend for CustomBackend<Args, DB, Fetch, S, Config>
where
    Fetch: Stream<Item = Result<Option<Task<Args>>, E>> + Unpin + Send + 'static,
    S: Sink<Task<Args>, Error = E> + Unpin + Send + 'static,
    E: Into<BoxDynError>,
{
    type Task = Task<Args>;

    type Error = CustomBackendError;

    fn poll_ready(
        &mut self,
        _: &mut Context<'_>,
        _: &WorkerContext,
    ) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn poll_next(
        &mut self,
        cx: &mut Context<'_>,
        worker: &WorkerContext,
    ) -> Poll<Option<Result<Self::Task, Self::Error>>> {
        if self.stream.is_none() {
            self.stream = Some((self.fetcher)(&mut self.db, &self.config, worker));
        }
        let stream = self.stream.as_mut().unwrap();
        stream.poll_next_unpin(cx).map(|item| {
            item.transpose()
                .map(|res| res.flatten())
                .map_err(|e| CustomBackendError::Inner(e.into()))
                .transpose()
        })
    }

    fn poll_close(
        &mut self,
        cx: &mut Context<'_>,
        _: &WorkerContext,
    ) -> Poll<Result<(), Self::Error>> {
        self.current_sink
            .poll_close_unpin(cx)
            .map_err(|e| CustomBackendError::Inner(e.into()))
    }
}

impl<Args, DB, Fetch, S, Config, E> BackendConfig for CustomBackend<Args, DB, Fetch, S, Config>
where
    Fetch: Stream<Item = Result<Option<Task<Args>>, E>> + Unpin + Send + 'static,
    S: Sink<Task<Args>, Error = E> + Unpin + Send + 'static,
    E: Into<BoxDynError>,
    Args: 'static,
{
    type Id = RandomId;
    type Args = Args;
    type Kind = Ephemeral;

    type Layer = Identity;

    type Config = Config;

    fn config(&self) -> &Self::Config {
        &self.config
    }

    fn middleware(&mut self, _: &mut WorkerContext) -> Self::Layer {
        Identity::new()
    }
}

impl<Args, DB, Fetch, S, Config> Sink<Task<Args>> for CustomBackend<Args, DB, Fetch, S, Config>
where
    S: Sink<Task<Args>>,
    S::Error: Into<BoxDynError>,
{
    type Error = CustomBackendError;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.project()
            .current_sink
            .poll_ready_unpin(cx)
            .map_err(|e| CustomBackendError::Inner(e.into()))
    }

    fn start_send(self: Pin<&mut Self>, item: Task<Args>) -> Result<(), Self::Error> {
        self.project()
            .current_sink
            .start_send_unpin(item)
            .map_err(|e| CustomBackendError::Inner(e.into()))
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.project()
            .current_sink
            .poll_flush_unpin(cx)
            .map_err(|e| CustomBackendError::Inner(e.into()))
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.project()
            .current_sink
            .poll_close_unpin(cx)
            .map_err(|e| CustomBackendError::Inner(e.into()))
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::VecDeque, time::Duration};

    use futures_util::{FutureExt, lock::Mutex, sink, stream};

    use crate::{
        backend::TaskSink,
        error::BoxDynError,
        task::task_id::{RandomId, TaskId},
        worker::{builder::WorkerBuilder, ext::event_listener::EventListenerExt},
    };

    use super::*;

    const ITEMS: u32 = 10;

    #[tokio::test]
    async fn basic_custom_backend() {
        let memory: Arc<Mutex<VecDeque<Task<u32>>>> = Arc::new(Mutex::new(VecDeque::new()));

        let mut backend = BackendBuilder::new()
            .database(memory)
            .fetcher(|db, _, _| {
                stream::unfold(db.clone(), |p| async move {
                    tokio::time::sleep(Duration::from_millis(100)).await; // Debounce
                    let mut db = p.try_lock().unwrap();
                    let item = db.pop_front();
                    drop(db);
                    match item {
                        Some(item) => Some((Ok::<_, CustomBackendError>(Some(item)), p)),
                        None => Some((Ok::<_, CustomBackendError>(None), p)),
                    }
                })
                .boxed()
            })
            .sink(|db, _| {
                sink::unfold(db.clone(), move |p, item: Task<u32>| {
                    async move {
                        let mut db = p.try_lock().unwrap();
                        let item = item
                            .into_builder()
                            .task_id(TaskId::String(RandomId::default().to_string()))
                            .build();
                        db.push_back(item);
                        drop(db);
                        Ok::<_, CustomBackendError>(p)
                    }
                    .boxed()
                })
            })
            .build()
            .unwrap();

        for i in 0..ITEMS {
            backend.push(i).await.unwrap();
        }

        async fn task(task: u32, worker: WorkerContext) -> Result<(), BoxDynError> {
            tokio::time::sleep(Duration::from_secs(1)).await;
            if task == ITEMS - 1 {
                worker.stop().unwrap();
                return Err("Worker stopped!")?;
            }
            Ok(())
        }

        let worker = WorkerBuilder::new("rango-tango")
            .backend(backend)
            .on_event(|worker, ev| {
                println!("On Event = {ev:?} from {}", worker.name());
            })
            .build(task);
        worker.run().await.unwrap();
    }
}
