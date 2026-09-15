//! # Extension traits for long running tasks
//!
//! It includes a tracker for monitoring task duration and a middleware layer to integrate with the worker's service stack.
//! The long-running task support ensures that tasks exceeding a specified duration are properly tracked and managed, allowing for graceful shutdown and resource cleanup.
//!
//! ## Features
//! - [`LongRunningLayer`]: A Tower middleware layer that wraps the worker's service to add long-running task tracking capabilities.
//! - [`TaskRunner`]: A runner that can be injected into tasks to allow them to register long-running operations.
//! - [`LongRunningExt`]: Provides an extension trait for easily adding long-running support to workers.
//!
//! ## Example
//!
//! ```rust
//! # use apalis_core::worker::ext::long_running::LongRunningExt;
//! # use apalis_core::worker::ext::long_running::TaskRunner;
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
//!         mut handle: TaskRunner<u32>,
//! #       worker: WorkerContext,
//!     ) -> Result<u32, BoxDynError> {
//!         handle.execute(tokio::spawn(async move {
//!             tokio::time::sleep(Duration::from_secs(1)).await;
//!             task * 2
//!         }));
//!         handle.execute(tokio::spawn(async move {
//!             tokio::time::sleep(Duration::from_secs(1)).await;
//!             task * 5
//!         }));
//!         let res = handle.try_collect::<Vec<u32>>().await?.iter().sum::<u32>();
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

use futures_core::{Stream, future::BoxFuture};
use futures_util::{FutureExt, StreamExt, stream::FuturesUnordered};
use tower_layer::{Layer, Stack};
use tower_service::Service;

use crate::{
    backend::Backend,
    error::BoxDynError,
    task::from_request::FromRequest,
    task::{Task, context::TaskContext, data::MissingDataError},
    worker::{
        builder::WorkerBuilder,
        ext::long_running::future::{LongRunningError, LongRunningFuture},
    },
};
/// The future implementation of the long running task
pub mod future;

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
    ///
    /// Max duration is the maximum amount of time a single operation should last
    /// and should not be confused with `TimeoutLayer` which controls the full task execution time
    #[must_use]
    pub fn new(max_duration: Duration) -> Self {
        Self {
            max_duration: Some(max_duration),
        }
    }
}

/// The long running task handler
///
/// See [module level documentation](self) for more details.
#[must_use = "A runner must collect its results and use them"]
#[derive(Debug)]
pub struct TaskRunner<Res> {
    task: TaskContext,
    config: LongRunningConfig,
    results: FuturesUnordered<BoxFuture<'static, Result<Res, LongRunningError>>>,
}

impl<T: Send + 'static> TaskRunner<T> {
    /// Start a task that is tracked by the long running task's context
    pub fn execute<F, Err>(&mut self, future: F)
    where
        F: Future<Output = Result<T, Err>> + Send + Sync + 'static,
        Err: Into<BoxDynError> + Send + 'static,
    {
        let fut = LongRunningFuture {
            future,
            task: self.task.clone(),
            #[cfg(feature = "sleep")]
            timeout: self.config.max_duration.map(futures_timer::Delay::new),
            max_duration: self.config.max_duration,
        };

        self.results.push(
            fut.map(|rs| match rs {
                Ok(Ok(res)) => Ok(res),
                Ok(Err(res)) => Err(LongRunningError::Execution(res.into())),
                Err(e) => Err(e),
            })
            .boxed(),
        );
    }
}

impl<T> Stream for TaskRunner<T> {
    type Item = Result<T, LongRunningError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.get_mut().results.poll_next_unpin(cx)
    }
}

impl<Args: Sync, Res> FromRequest<Task<Args>> for TaskRunner<Res> {
    type Error = MissingDataError;
    async fn from_request(task: &Task<Args>) -> Result<Self, Self::Error> {
        let config = task
            .data()
            .get_checked::<LongRunningConfig>()
            .cloned()
            .expect("LongRunningConfig should be present in ExecutionContext");
        let task: TaskContext = TaskContext::from_request(task).await?;
        Ok(Self {
            task,
            config,
            results: FuturesUnordered::default(),
        })
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

impl<S, Args> Service<Task<Args>> for LongRunningService<S>
where
    S: Service<Task<Args>>,
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

    fn call(&mut self, mut task: Task<Args>) -> Self::Future {
        task.inject_data(self.config.clone());
        self.service.call(task)
    }
}

/// Helper trait for building long running workers from [`WorkerBuilder`]
///
/// See [module level documentation](self) for more details.
pub trait LongRunningExt<Args, Source, Middleware>: Sized {
    /// Extension for executing long running jobs
    fn long_running(self) -> WorkerBuilder<Args, Source, Stack<LongRunningLayer, Middleware>> {
        self.long_running_with_cfg(Default::default())
    }
    /// Extension for executing long running jobs with a config
    fn long_running_with_cfg(
        self,
        cfg: LongRunningConfig,
    ) -> WorkerBuilder<Args, Source, Stack<LongRunningLayer, Middleware>>;
}

impl<Args, B, M> LongRunningExt<Args, B, M> for WorkerBuilder<Args, B, M>
where
    M: Layer<LongRunningLayer>,
    B: Backend,
{
    fn long_running_with_cfg(
        self,
        cfg: LongRunningConfig,
    ) -> WorkerBuilder<Args, B, Stack<LongRunningLayer, M>> {
        let this = self.layer(LongRunningLayer::new(cfg));
        WorkerBuilder {
            context: this.context,
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
            mut handle: TaskRunner<u32>,
            worker: WorkerContext,
        ) -> Result<u32, BoxDynError> {
            handle.execute(tokio::spawn(async move {
                tokio::time::sleep(Duration::from_secs(1)).await;
                task * 2
            }));
            handle.execute(tokio::spawn(async move {
                tokio::time::sleep(Duration::from_secs(1)).await;
                task * 5
            }));

            let res = handle.try_collect::<Vec<u32>>().await?.iter().sum::<u32>();

            if task == ITEMS - 1 {
                tokio::spawn(async move {
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    worker.stop().unwrap();
                });
            }
            Ok(res)
        }

        let worker = WorkerBuilder::new("rango-tango")
            .backend(in_memory)
            .long_running()
            .on_event(|wrk, ev| {
                println!("On Event = {ev:?} from {}", wrk.name());
            })
            .build(task);
        worker.run().await.unwrap();
    }
}
