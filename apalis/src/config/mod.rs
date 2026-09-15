use std::{collections::HashSet, fmt::Debug, marker::PhantomData, time::Duration};

use apalis_core::{
    backend::{Backend, BackendConfig, TryNewBackend},
    error::BoxDynError,
    layers::{Identity, Stack},
    task::Task,
    worker::builder::WorkerBuilder,
};

use tower::{
    Layer, Service, ServiceBuilder,
    util::{BoxLayer, BoxService},
};

#[cfg(feature = "tracing")]
use crate::layers::tracing::TraceLayer;

/// A standalone config that allows quickly bootstrapping workers
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct WorkerConfig<B>
where
    B: BackendConfig,
{
    /// The name of the worker
    pub name: String,
    /// The config for the backend
    pub backend: B::Config,
    /// Middleware to be applied
    pub middleware: HashSet<Middleware>,
}

/// Configuration for worker middlewares
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[non_exhaustive]
pub enum Middleware {
    /// A simple timeout middleware that cancels jobs running longer than the specified duration
    #[cfg(feature = "timeout")]
    Timeout {
        /// The duration after which the job should be cancelled
        duration: Duration,
    },
    /// A rate limiting middleware that limits the number of tasks processed per unit of time
    #[cfg(feature = "limit")]
    RateLimit {
        /// Maximum number of tasks to process
        num: u64,
        /// Time window for the rate limit
        per: Duration,
    },
    /// A concurrency middleware that limits the number of tasks processed concurrently
    #[cfg(feature = "limit")]
    Concurrency {
        /// Maximum number of concurrent tasks
        max: usize,
    },
    /// A middleware that catches panics and prevents them from crashing the worker
    #[cfg(feature = "catch-panic")]
    CatchPanic,

    // Retries need S: Clone
    // /// A middleware that retries failed tasks up to a certain number of times
    // #[cfg(feature = "retry")]
    // Retries {
    //     /// Maximum number of retries
    //     max: usize,
    // },
    /// Enable tracing
    #[cfg(feature = "tracing")]
    Tracing,
}

impl Middleware {
    /// Convert this middleware descriptor into a boxed `tower::Layer`.
    pub fn to_layer<S, Args>(self) -> BoxLayer<S, Task<Args>, S::Response, BoxDynError>
    where
        S: Service<Task<Args>> + Send + Sync + 'static,
        S::Response: Send + 'static,
        S::Error: Into<BoxDynError> + Send + Sync + 'static,
        S::Future: Send + 'static,
        Args: Send + 'static,
        S::Response: Debug, // Specifically only `tracing`
    {
        match self {
            #[cfg(feature = "timeout")]
            Self::Timeout { duration } => {
                let layer = ServiceBuilder::new()
                    .map_err(Into::into)
                    .timeout(duration)
                    .into_inner();
                BoxLayer::new(layer)
            }
            #[cfg(feature = "limit")]
            Self::RateLimit { num, per } => {
                let layer = ServiceBuilder::new()
                    .rate_limit(num, per)
                    .map_err(Into::into)
                    .into_inner();
                BoxLayer::new(layer)
            }
            #[cfg(feature = "limit")]
            Self::Concurrency { max } => {
                let layer = ServiceBuilder::new()
                    .concurrency_limit(max)
                    .map_err(Into::into)
                    .into_inner();
                BoxLayer::new(layer)
            }

            #[cfg(feature = "catch-panic")]
            Self::CatchPanic => {
                use crate::layers::catch_panic::CatchPanicLayer;

                let layer = ServiceBuilder::new()
                    .layer(CatchPanicLayer::new())
                    .map_err(Into::into)
                    .into_inner();
                BoxLayer::new(layer)
            }

            #[cfg(feature = "tracing")]
            Self::Tracing => {
                let layer = ServiceBuilder::new()
                    .layer(TraceLayer::new())
                    .map_err(Into::into)
                    .into_inner();
                BoxLayer::new(layer)
            }
        }
    }
}
type ConfiguredWorker<B> =
    WorkerBuilder<<B as BackendConfig>::Args, B, Stack<MiddlewareStack<B>, Identity>>;

/// Builds a [`WorkerBuilder`] from a serialized/external configuration.
pub trait WorkerFromConfig<B>
where
    B::Backend: Backend + BackendConfig,
    B: TryNewBackend,
{
    /// Attempt to build a [`WorkerBuilder`] from the given [`WorkerConfig`].
    fn try_config(config: WorkerConfig<B>) -> Result<ConfiguredWorker<B::Backend>, B::Error>;
}

impl<B> WorkerFromConfig<B> for WorkerBuilder<(), (), Identity>
where
    B: TryNewBackend,
    B::Backend: BackendConfig + Backend,
{
    fn try_config(config: WorkerConfig<B>) -> Result<ConfiguredWorker<B::Backend>, B::Error> {
        let backend = B::try_new(config.backend)?;

        Ok(Self::new(config.name)
            .backend(backend)
            .layer(MiddlewareStack::new(config.middleware)))
    }
}

#[allow(unreachable_patterns)]
fn middleware_priority(m: &Middleware) -> u8 {
    match m {
        #[cfg(feature = "catch-panic")]
        Middleware::CatchPanic => 0,
        #[cfg(feature = "timeout")]
        Middleware::Timeout { .. } => 1,
        #[cfg(feature = "limit")]
        Middleware::Concurrency { .. } => 3,
        #[cfg(feature = "limit")]
        Middleware::RateLimit { .. } => 4,
        #[cfg(feature = "tracing")]
        Middleware::Tracing => 6,
        _ => unreachable!(),
    }
}

/// A stack containing all the middleware to apply
#[derive(Debug, Clone)]
pub struct MiddlewareStack<B> {
    layers: HashSet<Middleware>,
    backend: PhantomData<B>,
}

impl<B> MiddlewareStack<B> {
    /// Build a new Middleware stack
    #[must_use]
    pub fn new(layers: HashSet<Middleware>) -> Self {
        Self {
            backend: PhantomData,
            layers,
        }
    }
}

impl<S, B> Layer<S> for MiddlewareStack<B>
where
    B: Backend + BackendConfig,
    S: Service<Task<B::Args>> + Send + 'static,
    S::Response: Send + Debug + 'static,
    S::Error: Into<BoxDynError> + Send + Sync + 'static,
    S::Future: Send + 'static,
    B::Args: Send + 'static,
{
    type Service = BoxService<Task<B::Args>, S::Response, BoxDynError>;

    fn layer(&self, inner: S) -> Self::Service {
        let mut ordered: Vec<Middleware> = self.layers.iter().copied().collect();
        ordered.sort_by_key(middleware_priority);

        let mut boxed = BoxService::new(ServiceBuilder::new().map_err(Into::into).service(inner));

        for mw in ordered {
            let layer = mw.to_layer();

            boxed = layer.layer(boxed);
        }

        boxed
    }
}
