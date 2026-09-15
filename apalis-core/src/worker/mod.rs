//! Utilities for building and running workers.
//!
//! A `Worker` polls tasks from a backend, executes them using
//! a service, emits lifecycle events, and handles graceful shutdowns. A worker is typically
//! constructed using a [`WorkerBuilder`](crate::worker::builder).
//!
//! # Features
//! - Pluggable backends for task queues (e.g., in-memory, Redis).
//! - Middleware support for task processing.
//! - Stream or future-based worker execution modes.
//! - Built-in event system for logging or metrics.
//! - Task tracking and controlled worker readiness.
//!
//! # Lifecycle
//!
//! ```mermaid
//! graph TD
//!     A[Start Worker] --> B[Initialize Context & Heartbeat]
//!     B --> C[Poll Backend for Tasks]
//!     C --> D{Task Available?}
//!     D -- Yes --> E[Execute Task via Service Stack]
//!     E --> F[Emit Events]
//!     F --> C
//!     D -- No --> F
//!     F --> G{Shutdown Signal?}
//!     G -- Yes --> H[Graceful Shutdown]
//!     G -- No --> C
//! ```
//! Worker lifecycle is composed of several stages:
//! - Initialize context and heartbeat
//! - Poll backend for tasks
//! - Execute tasks via service stack
//! - Emit events (Idle, Success, Error, HeartBeat)
//! - Graceful shutdown on signal or stop
//!
//! # Examples
//!
//! ## Run as a future
//! ```rust,no_run
//! # use apalis_core::{worker::builder::WorkerBuilder, backend::memory::MemoryStorage};
//! # use apalis_core::error::BoxDynError;
//! # use apalis_core::backend::TaskSink;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), BoxDynError> {
//!     let mut storage = MemoryStorage::new();
//!     for i in 0..5 {
//!         storage.push(i).await?;
//!     }
//!
//!     async fn handler(task: u32) {
//!         println!("Processing task: {task}");
//!     }
//!
//!     let worker = WorkerBuilder::new("worker-1")
//!         .backend(storage)
//!         .build(handler);
//!
//!     worker.run().await?;
//!     Ok(())
//! }
//! ```
//!
//! ## Runner as a stream
//! The `stream` interface yields worker events (e.g., `Success`, `Error`) while running:
//! ```rust,no_run
//! # use apalis_core::worker::builder::WorkerBuilder;
//! # use apalis_core::backend::memory::MemoryStorage;
//! # use futures_util::StreamExt;
//! # #[tokio::main]
//! # async fn main() {
//! #   let mut storage = MemoryStorage::new();
//! #   async fn handler(task: u32) {
//! #        println!("Processing task: {task}");
//! #    }
//! #   let worker = WorkerBuilder::new("worker-1")
//! #        .backend(storage)
//! #        .build(handler);
//! let mut stream = worker.stream();
//! while let Some(evt) = stream.next().await {
//!     println!("Event: {:?}", evt);
//! }
//! # }
//! ```
use crate::backend::{Backend, BackendConfig};
use crate::error::{BoxDynError, WorkerError};
use crate::monitor::shutdown::Shutdown;
use crate::task::Task;
use crate::task::data::Data;
use crate::worker::call_all::CallAllUnordered;
use crate::worker::context::WorkerContext;
use crate::worker::event::{Event, RawEventListener};
use crate::worker::lifecycle::{LifecycleLayer, LifecycleService};
use crate::worker::stream::WorkerStream;
use futures_util::{Future, FutureExt, Stream, StreamExt, TryFutureExt};
use std::fmt::{self};
use std::marker::PhantomData;
use tower_layer::{Layer, Stack};
use tower_service::Service;

pub mod builder;
pub mod call_all;
pub mod context;
pub mod event;
pub mod ext;
pub(crate) mod handle;
pub mod lifecycle;
pub mod service;
mod state;
mod stream;

/// A worker polls a backend and processes tasks using a service.
///
/// Its the core component responsible for task polling, execution, and lifecycle management.
///
/// # Example
/// Basic example:
/// ```rust,no_run
/// # use apalis_core::error::BoxDynError;
/// # use apalis_core::backend::memory::MemoryStorage;
/// # use apalis_core::worker::builder::WorkerBuilder;
/// # use apalis_core::backend::TaskSink;
///
/// #[tokio::main]
/// async fn main() -> Result<(), BoxDynError> {
///     let mut storage = MemoryStorage::new();
///     for i in 0..5 {
///         storage.push(i).await?;
///     }
///
///     async fn handler(task: u32) {
///         println!("Processing task: {task}");
///     }
///
///     let worker = WorkerBuilder::new("worker-1")
///         .backend(storage)
///         .build(handler);
///
///     worker.run().await?;
///     Ok(())
/// }
/// ```
/// See [module level documentation](self) for more details.
#[must_use = "Workers must be run or streamed to execute tasks"]
pub struct Worker<Args, Backend, Svc, Middleware> {
    pub(crate) context: WorkerContext,
    pub(crate) backend: Backend,
    pub(crate) service: Svc,
    pub(crate) middleware: Middleware,
    pub(crate) task_marker: PhantomData<Args>,
    pub(crate) shutdown: Option<Shutdown>,
    pub(crate) event_handler: RawEventListener,
}

impl<Args, B, Svc, Middleware> fmt::Debug for Worker<Args, B, Svc, Middleware>
where
    Svc: fmt::Debug,
    B: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Worker")
            .field("service", &self.service)
            .field("backend", &self.backend)
            .finish()
    }
}

impl<Args, B, Svc, M> Worker<Args, B, Svc, M> {
    /// Build a worker that is ready for execution
    pub fn new(context: WorkerContext, backend: B, service: Svc, layers: M) -> Self {
        Self {
            context,
            backend,
            service,
            middleware: layers,
            task_marker: PhantomData,
            shutdown: None,
            event_handler: Box::new(|_, _| {}),
        }
    }
}

impl<Args, S, M, FB> Worker<Args, FB, S, M>
where
    FB: BackendConfig + Backend<Task = Task<FB::Args>> + Send + Unpin + 'static,
    S: Service<Task<FB::Args>> + Send + 'static,
    FB::Args: Send + 'static,
    Args: Send + 'static,
    FB::Error: Into<BoxDynError> + Send + 'static,
    M: Layer<LifecycleService<S>>,
    FB::Layer: Layer<<M as Layer<LifecycleService<S>>>::Service>,
    <FB::Layer as Layer<<M as Layer<LifecycleService<S>>>::Service>>::Service:
        Service<Task<FB::Args>>,
    <FB::Layer as Layer<<M as Layer<LifecycleService<S>>>::Service>>::Service: Send + 'static,
    <<FB::Layer as Layer<<M as Layer<LifecycleService<S>>>::Service>>::Service as Service<
        Task<<FB as BackendConfig>::Args>,
    >>::Future: Send,
    <<FB::Layer as Layer<<M as Layer<LifecycleService<S>>>::Service>>::Service as Service<
        Task<<FB as BackendConfig>::Args>,
    >>::Error: Into<BoxDynError> + Send + Sync + 'static,
    <<FB::Layer as Layer<<M as Layer<LifecycleService<S>>>::Service>>::Service as Service<
        Task<<FB as BackendConfig>::Args>,
    >>::Response: Send + Sync + 'static,
{
    /// Run the worker until completion
    ///
    /// # Example
    /// ```no_run
    /// # use apalis_core::error::BoxDynError;
    /// # use apalis_core::backend::memory::MemoryStorage;
    /// # use apalis_core::backend::TaskSink;
    /// # use apalis_core::worker::builder::WorkerBuilder;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), BoxDynError> {
    ///     let mut storage = MemoryStorage::new();
    ///     for i in 0..5 {
    ///         storage.push(i).await?;
    ///     }
    ///
    ///     async fn handler(task: u32) {
    ///         println!("Processing task: {task}");
    ///     }
    ///
    ///     let worker = WorkerBuilder::new("worker-1")
    ///         .backend(storage)
    ///         .build(handler);
    ///
    ///     worker.run().await?;
    ///     Ok(())
    /// }
    /// ```
    pub async fn run(self) -> Result<(), WorkerError> {
        let mut stream = self.stream();
        while let Some(res) = stream.next().await {
            match res {
                Ok(_) => {}
                Err(e) => return Err(e),
            }
        }
        Ok(())
    }

    /// Run the worker until a shutdown signal future is complete.
    pub async fn run_until<Fut, Err>(mut self, signal: Fut) -> Result<(), WorkerError>
    where
        Fut: Future<Output = Result<(), Err>> + Send + 'static,
        FB: Send,
        M: Send,
        Err: Into<WorkerError> + Send + 'static,
    {
        let shutdown = self.shutdown.take().unwrap_or_default();
        let terminator = shutdown.shutdown_after(signal);
        let c = self.context.clone();
        let worker = self.run();
        futures_util::try_join!(
            worker,
            terminator.map_ok(|_| c.stop()).map_err(|e| e.into()),
        )
        .map(|_| ())
    }

    /// Run the worker until a shutdown signal future is complete.
    ///
    /// *Note*: Using this function requires you to call `ctx.stop()` in the future to completely stop the worker.
    ///
    /// This can also be very powerful with pausing and resuming the worker using the context.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    ///
    /// # use apalis_core::{worker::builder::WorkerBuilder, backend::memory::MemoryStorage};
    /// # use apalis_core::error::BoxDynError;
    /// # use apalis_core::backend::TaskSink;
    /// # use std::time::Duration;
    /// # use tokio::time::sleep;
    /// # use apalis_core::error::WorkerError;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), BoxDynError> {
    ///     let mut storage = MemoryStorage::new();
    ///     for i in 0..5 {
    ///         storage.push(i).await?;
    ///     }
    ///     async fn handler(task: u32) {
    ///         println!("Processing task: {task}");
    ///     }
    ///     let worker = WorkerBuilder::new("worker-1")
    ///         .backend(storage)
    ///         .build(handler);
    ///     worker.run_with_ctx(|ctx| async move {
    ///         sleep(Duration::from_secs(1)).await;
    ///         ctx.stop()?;
    ///         Ok(())
    ///     }).await?;
    ///     Ok(())
    /// }
    /// ```
    pub async fn run_with_ctx<F, Fut>(mut self, mut fut: F) -> Result<(), WorkerError>
    where
        F: FnMut(WorkerContext) -> Fut + Send + 'static,
        Fut: Future<Output = Result<(), WorkerError>> + Send,
        FB: Send,
        M: Send,
    {
        let shutdown = self.shutdown.take().unwrap_or_default();
        let terminator = shutdown.shutdown_after(fut(self.context.clone()));
        let worker = self.run().boxed();
        futures_util::try_join!(terminator.map_ok(|_| ()), worker).map(|_| ())
    }

    /// Returns a stream that will yield events as they occur within the worker's lifecycle
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use apalis_core::error::BoxDynError;
    /// # use apalis_core::backend::memory::MemoryStorage;
    /// # use apalis_core::worker::builder::WorkerBuilder;
    /// # use apalis_core::backend::TaskSink;
    /// # use futures_util::StreamExt;
    /// #[tokio::main]
    /// async fn main() -> Result<(), BoxDynError> {
    ///     let mut storage = MemoryStorage::new();
    ///     for i in 0..5 {
    ///         storage.push(i).await?;
    ///     }
    ///     async fn handler(task: u32) {
    ///         println!("Processing task: {task}");
    ///     }
    ///     let worker = WorkerBuilder::new("worker-1")
    ///         .backend(storage)
    ///         .build(handler);
    ///     let mut stream = worker.stream();
    ///     while let Some(evt) = stream.next().await {
    ///         println!("Event: {:?}", evt);
    ///     }
    ///     Ok(())
    /// }
    /// ```
    pub fn stream(
        mut self,
    ) -> impl Stream<Item = Result<Event, WorkerError>> + use<Args, S, M, FB> {
        let ctx = &mut self.context;
        ctx.bind_service::<M::Service>();
        let mut backend = self.backend;
        let event_handler = self.event_handler;
        ctx.add_listener(event_handler);
        let backend_middleware = backend.middleware(ctx);

        struct ServiceBuilder<L> {
            layer: L,
        }

        impl<L> ServiceBuilder<L> {
            fn layer<T>(self, layer: T) -> ServiceBuilder<Stack<T, L>> {
                ServiceBuilder {
                    layer: Stack::new(layer, self.layer),
                }
            }
            fn service<S>(&self, service: S) -> L::Service
            where
                L: Layer<S>,
            {
                self.layer.layer(service)
            }
        }

        let svc = ServiceBuilder {
            layer: Data::new(ctx.clone()),
        };
        let service = svc
            // backend middleware should be the next layer so it can observe all requests released by user middleware
            .layer(backend_middleware)
            // pass the user defined middleware
            .layer(self.middleware)
            // A lifecycle service that
            // - when all layers are ready, inform the worker its ready to accept tasks
            // - Track all tasks to allow graceful shutdowns
            // - increment the attempt count on the first poll
            // - Generate a task token
            .layer(LifecycleLayer::new(ctx.clone()))
            .service(self.service);
        let w = ctx.clone();
        StreamExt::inspect(
            WorkerStream::new(service, backend, ctx),
            move |res| match &res {
                Ok(e) => {
                    w.emit_event(e);
                }
                Err(e) => {
                    error!("WorkerError: {e}");
                }
            },
        )
    }
}

#[cfg(test)]
mod tests {
    use std::{
        future::ready,
        io::ErrorKind,
        ops::Deref,
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        time::Duration,
    };

    use futures_channel::mpsc::SendError;
    use futures_core::future::BoxFuture;

    use crate::{
        backend::{TaskSink, memory::MemoryStorage},
        task::{ExecutionContext, context::TaskContext},
        worker::{
            builder::WorkerBuilder,
            ext::{
                ack::{Acknowledge, AcknowledgementExt},
                circuit_breaker::CircuitBreaker,
                event_listener::EventListenerExt,
                long_running::LongRunningExt,
            },
        },
    };

    use super::*;

    const ITEMS: u32 = 100;

    #[tokio::test]
    async fn basic_worker_run() {
        let mut in_memory = MemoryStorage::new();
        for i in 0..ITEMS {
            in_memory.push(i).await.unwrap();
        }

        #[derive(Clone, Debug, Default)]
        struct Count(Arc<AtomicUsize>);

        impl Deref for Count {
            type Target = Arc<AtomicUsize>;
            fn deref(&self) -> &Self::Target {
                &self.0
            }
        }

        async fn task(
            task: u32,
            worker: WorkerContext,
            count: Data<Count>,
            ctx: TaskContext,
        ) -> Result<(), BoxDynError> {
            tokio::spawn(ctx.run_until_executed(async {
                tokio::time::sleep(Duration::from_secs(3)).await;
                // Because the task stops after 2 seconds
                println!("This is never called");
            }));
            tokio::time::sleep(Duration::from_secs(2)).await;
            count.fetch_add(1, Ordering::Relaxed);
            if task == ITEMS - 1 {
                worker.stop().unwrap();
                return Err("Worker stopped!")?;
            }

            println!("Elapsed: {:?}", ctx.elapsed());
            Ok(())
        }

        #[derive(Debug, Clone)]
        struct MyAcknowledger;

        impl Acknowledge<()> for MyAcknowledger {
            type Error = SendError;
            type Future = BoxFuture<'static, Result<(), SendError>>;
            fn ack(
                &mut self,
                res: &Result<(), BoxDynError>,
                ctx: &ExecutionContext,
            ) -> Self::Future {
                println!("{res:?}, {ctx:?}");
                // Call webhook with the result and ctx?
                ready(Ok(())).boxed()
            }
        }

        let worker = WorkerBuilder::new("rango-tango")
            .backend(in_memory)
            .data(Count::default())
            .break_circuit()
            .long_running()
            .ack_with(MyAcknowledger)
            .on_event(|wrk, ev| {
                println!("On Event = {ev:?} from {}", wrk.name());
            })
            .build(task);
        worker.run().await.unwrap();
    }

    #[tokio::test]
    async fn basic_worker_stream() {
        let mut in_memory = MemoryStorage::new();

        for i in 0..ITEMS {
            in_memory.push(i).await.unwrap();
        }

        #[derive(Clone, Debug, Default)]
        struct Count(Arc<AtomicUsize>);

        impl Deref for Count {
            type Target = Arc<AtomicUsize>;
            fn deref(&self) -> &Self::Target {
                &self.0
            }
        }

        async fn task(task: u32, count: Data<Count>, worker: WorkerContext) {
            tokio::time::sleep(Duration::from_secs(1)).await;
            count.fetch_add(1, Ordering::Relaxed);
            if task == ITEMS - 1 {
                worker.stop().unwrap();
            }
        }
        let worker = WorkerBuilder::new("rango-tango")
            .backend(in_memory)
            .data(Count::default())
            .break_circuit()
            .long_running()
            .on_event(|wrk, ev| {
                println!("CTX {:?}, On Event = {ev:?}", wrk.name());
            })
            .build(task);
        let mut event_stream = worker.stream();
        while let Some(Ok(ev)) = event_stream.next().await {
            println!("On Event = {ev:?}");
        }
    }

    #[tokio::test]
    async fn with_shutdown_signal() {
        let mut in_memory = MemoryStorage::new();
        for i in 0..ITEMS {
            in_memory.push(i).await.unwrap();
        }

        async fn task(_: u32) -> Result<(), BoxDynError> {
            Ok(())
        }

        let worker = WorkerBuilder::new("rango-tango")
            .backend(in_memory)
            .on_event(|wrk, ev| {
                println!("On Event = {ev:?} from {}", wrk.name());
            })
            .build(task);
        let signal = async {
            let ctrl_c = tokio::signal::ctrl_c().map_err(|e| e.into());
            let timeout = tokio::time::sleep(Duration::from_secs(5)).map(|_| {
                Err::<(), WorkerError>(WorkerError::IoError(std::io::Error::new(
                    ErrorKind::Other,
                    "Timeout",
                )))
            });
            let _ = futures_util::try_join!(ctrl_c, timeout)?;
            Ok::<(), WorkerError>(())
        };
        let res = worker.run_until(signal).await;
        match res {
            Err(WorkerError::IoError(_)) => {
                println!("Worker exited gracefully");
            }
            _ => panic!("Expected graceful exit error"),
        }
    }
}
