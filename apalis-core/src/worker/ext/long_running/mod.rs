//! # Extension traits for long running tasks
//!
//! It includes a tracker for monitoring task duration and a middleware layer to integrate with the worker's service stack.
//! The long-running task support ensures that tasks exceeding a specified duration are properly tracked and managed, allowing for graceful shutdown and resource cleanup.
//!
//! ## Features
//! - [`TaskTracker`]: Monitors the duration of tasks and provides a mechanism to wait for their completion.
//! - [`LongRunningLayer`]: A Tower middleware layer that wraps the worker's service to add long-running task tracking capabilities.
//! - [`Runner`]: A runner that can be injected into tasks to allow them to register long-running operations.
//! - [`RunnerContext`]: A context object built on top of the runner for spawning long running futures.
//! - [`Receiver`]: A receiver for results from long running futures.
//! - [`LongRunningExt`]: Provides an extension trait for easily adding long-running support to workers.
//!
//! ## Example
//!
//! ```rust
//! # use apalis_core::worker::ext::long_running::{Runner, LongRunningExt};
//! # use apalis_core::worker::context::WorkerContext;
//! # use apalis_core::backend::memory::MemoryStorage;
//! # use apalis_core::worker::builder::WorkerBuilder;
//! # use std::time::Duration;
//! # use crate::apalis_core::backend::TaskSink;
//! # use apalis_core::error::BoxDynError;
//! # use futures_util::TryStreamExt;
//!
//! #[tokio::main]
//! async fn main() {
//!     let mut in_memory = MemoryStorage::new();
//!     in_memory.push(42).await.unwrap();
//!
//!     async fn task(
//!         task: u32,
//!         runner: Runner,
//! #       worker: WorkerContext,
//!     ) -> Result<u32, BoxDynError> {
//!         let (ctx, receiver) = runner.channel();
//!         // Spawn and track the long-running task
//!         // Futures should be spawned by an executor and not awaited directly
//!         // Graceful shutdown is also ensured.
//!         tokio::spawn(ctx.execute(async move {
//!             // Perform a long running task
//!             tokio::time::sleep(Duration::from_secs(1)).await;
//!             task
//!         }));
//!         // Close the context and await for the results
//!         ctx.wait().await;
//!         let res = receiver.try_collect::<Vec<_>>().await?.iter().sum::<u32>();
//! #       tokio::spawn(async move {
//! #            tokio::time::sleep(Duration::from_secs(1)).await;
//! #            worker.stop().unwrap();
//! #       });
//!         Ok(res)
//!     }
//!
//!     let worker = WorkerBuilder::new("rango-tango")
//!         .backend(in_memory)
//!         .long_running()
//!         .build(task);
//!     worker.run().await.unwrap();
//! }
//! ```
use std::{
    fmt::Debug,
    future::Future,
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

use futures_channel::mpsc;
use futures_core::Stream;
use tower_layer::{Layer, Stack};
use tower_service::Service;

use crate::{
    backend::Backend,
    task::{Task, data::MissingDataError},
    task_fn::FromRequest,
    worker::{
        builder::WorkerBuilder,
        context::{Tracked, WorkerContext},
        ext::long_running::{
            future::LongRunningFuture,
            tracker::{LongRunningError, TaskTracker, TaskTrackerWaitFuture},
        },
    },
};
/// The future implementation of the long running task
pub mod future;
pub mod tracker;

/// Represents the long running middleware config
///
/// **max_duration** vs TimeoutLayer
///
/// Represents the maximum amount of time a single operation should last
/// and should not be confused with `TimeoutLayer` which controls the full task execution time.
///
///
/// See [module level documentation](self) for more details.
#[derive(Debug, Clone, Default)]
pub struct LongRunningConfig {
    max_duration: Option<Duration>,
}
impl LongRunningConfig {
    /// Create a new long running config
    #[must_use]
    pub fn new(max_duration: Duration) -> Self {
        Self {
            max_duration: Some(max_duration),
        }
    }
}

/// A receiver for collecting results from tracked futures.
#[derive(Debug)]
pub struct Receiver<T> {
    receiver: mpsc::UnboundedReceiver<Result<T, LongRunningError>>,
}

impl<T: Debug> Stream for Receiver<T> {
    type Item = Result<T, LongRunningError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match Pin::new(&mut self.receiver).poll_next(cx) {
            Poll::Ready(Some(result)) => Poll::Ready(Some(result)),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

/// A sender which is invoked when a future is complete
#[derive(Debug)]
pub struct Sender<T> {
    sender: mpsc::UnboundedSender<Result<T, LongRunningError>>,
}

impl<T> Clone for Sender<T> {
    fn clone(&self) -> Self {
        Self {
            sender: self.sender.clone(),
        }
    }
}

impl<T> Sender<T> {
    /// Send a result through the channel.
    /// Returns `Ok(())` if successful, `Err(result)` if the receiver has been dropped.
    pub fn send(
        &self,
        result: Result<T, LongRunningError>,
    ) -> Result<(), Result<T, LongRunningError>> {
        self.sender
            .unbounded_send(result)
            .map_err(|e| e.into_inner())
    }
}

/// The long running middleware context
#[derive(Debug, Clone)]
pub struct RunnerContext<Res> {
    tracker: TaskTracker,
    worker: WorkerContext,
    config: LongRunningConfig,
    sender: Sender<Res>,
}

impl<Res> RunnerContext<Res> {
    /// Closes the context and waits for the long running futures to complete
    #[must_use]
    pub fn wait(self) -> Tracked<TaskTrackerWaitFuture> {
        let _ = self.tracker.close();
        self.worker.track(self.tracker.wait())
    }
}

/// The long running task handler
///
/// See [module level documentation](self) for more details.
#[derive(Debug, Clone)]
pub struct Runner {
    tracker: TaskTracker,
    worker: WorkerContext,
    config: LongRunningConfig,
}

impl Runner {
    /// Create a channel for execution context
    ///
    /// This will allow execution of as many long running futures as possible
    /// See [mpsc::unbounded] for more details on the limits
    #[must_use]
    pub fn channel<Res>(self) -> (RunnerContext<Res>, Receiver<Res>) {
        let (sender, receiver) = mpsc::unbounded();

        let sender = Sender { sender };
        let ctx = RunnerContext {
            config: self.config,
            sender,
            tracker: self.tracker.clone(),
            worker: self.worker,
        };
        let receiver = Receiver { receiver };
        (ctx, receiver)
    }
}

impl<Res: Send + 'static> RunnerContext<Res> {
    /// Start a task that is tracked by the long running task's context
    #[must_use]
    pub fn execute<F: Future<Output = Res>>(&self, task: F) -> Tracked<LongRunningFuture<F>> {
        self.worker
            .track(self.tracker.track_future(task, &self.config, &self.sender))
    }
}

impl<Args: Sync, Ctx: Sync + Clone, IdType: Sync + Send> FromRequest<Task<Args, Ctx, IdType>>
    for Runner
{
    type Error = MissingDataError;
    async fn from_request(task: &Task<Args, Ctx, IdType>) -> Result<Self, Self::Error> {
        let runner: &Self = task.parts.data.get_checked()?;
        Ok(runner.clone())
    }
}

/// Decorates the underlying middleware with long running capabilities
///
/// See [module level documentation](self) for more details.
#[derive(Debug, Clone)]
#[allow(unused)]
pub struct LongRunningLayer(LongRunningConfig);

impl LongRunningLayer {
    /// Create a new long running layer
    #[must_use]
    pub fn new(config: LongRunningConfig) -> Self {
        Self(config)
    }
}

impl<S> Layer<S> for LongRunningLayer {
    type Service = LongRunningService<S>;

    fn layer(&self, service: S) -> Self::Service {
        LongRunningService {
            service,
            config: self.0.clone(),
        }
    }
}

/// Decorates the underlying service with long running capabilities
///
/// See [module level documentation](self) for more details.
#[derive(Debug, Clone)]
pub struct LongRunningService<S> {
    service: S,
    config: LongRunningConfig,
}

impl<S, Args, Ctx, IdType> Service<Task<Args, Ctx, IdType>> for LongRunningService<S>
where
    S: Service<Task<Args, Ctx, IdType>>,
    S::Future: Send + 'static,
    S::Response: Send,
    S::Error: Send,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = S::Future;

    fn poll_ready(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.service.poll_ready(cx)
    }

    fn call(&mut self, mut task: Task<Args, Ctx, IdType>) -> Self::Future {
        let tracker = TaskTracker::new();
        let worker: WorkerContext = task.parts.data.get().cloned().expect("");
        let config = self.config.clone();
        let runner = Runner {
            tracker,
            worker,
            config,
        };
        task.parts.data.insert(runner);
        self.service.call(task)
    }
}

/// Helper trait for building long running workers from [`WorkerBuilder`]
///
/// See [module level documentation](self) for more details.
pub trait LongRunningExt<Args, Ctx, Source, Middleware>: Sized {
    /// Extension for executing long running jobs
    fn long_running(self) -> WorkerBuilder<Args, Ctx, Source, Stack<LongRunningLayer, Middleware>> {
        self.long_running_with_cfg(Default::default())
    }
    /// Extension for executing long running jobs with a config
    fn long_running_with_cfg(
        self,
        cfg: LongRunningConfig,
    ) -> WorkerBuilder<Args, Ctx, Source, Stack<LongRunningLayer, Middleware>>;
}

impl<Args, B, M, Ctx> LongRunningExt<Args, Ctx, B, M> for WorkerBuilder<Args, Ctx, B, M>
where
    M: Layer<LongRunningLayer>,
    B: Backend<Args = Args, Context = Ctx>,
{
    fn long_running_with_cfg(
        self,
        cfg: LongRunningConfig,
    ) -> WorkerBuilder<Args, Ctx, B, Stack<LongRunningLayer, M>> {
        let this = self.layer(LongRunningLayer::new(cfg));
        WorkerBuilder {
            name: this.name,
            request: this.request,
            layer: this.layer,
            source: this.source,
            shutdown: this.shutdown,
            event_handler: this.event_handler,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use futures_util::TryStreamExt;

    use crate::{
        backend::{TaskSink, memory::MemoryStorage},
        error::BoxDynError,
        worker::{
            builder::WorkerBuilder,
            context::WorkerContext,
            ext::{event_listener::EventListenerExt, long_running::LongRunningExt},
        },
    };

    use super::*;

    const ITEMS: u32 = 10;

    #[tokio::test]
    async fn basic_worker() {
        let mut in_memory = MemoryStorage::new();
        for i in 0..ITEMS {
            in_memory.push(i).await.unwrap();
        }

        async fn task(
            task: u32,
            runner: Runner,
            worker: WorkerContext,
        ) -> Result<u32, BoxDynError> {
            let (ctx, receiver) = runner.channel();
            tokio::spawn(ctx.execute(async move {
                tokio::time::sleep(Duration::from_secs(1)).await;
                task * 2
            }));
            tokio::spawn(ctx.execute(async move {
                tokio::time::sleep(Duration::from_secs(1)).await;
                task * 5
            }));

            ctx.wait().await;
            let res = receiver.try_collect::<Vec<_>>().await?.iter().sum::<u32>();

            if task == ITEMS - 1 {
                tokio::spawn(async move {
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    worker.stop().unwrap();
                });
            }
            Ok(res)
        }
        // let config = LongRunningConfig::new(Duration::from_secs(1));
        let worker = WorkerBuilder::new("rango-tango")
            .backend(in_memory)
            // .long_running_with_cfg(config)
            .long_running()
            .on_event(|ctx, ev| {
                println!("On Event = {ev:?} from {}", ctx.name());
            })
            .build(task);
        worker.run().await.unwrap();
    }
}
