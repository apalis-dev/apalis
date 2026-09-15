//! Builder types for composing and building workers.
//!
//! The `WorkerBuilder` component is the recommended
//! way to construct [`Worker`] instances in a flexible and
//! composable manner.
//!
//! The builder pattern enables customization of various parts of a worker,
//! in the following order:
//!
//! 1. Setting a backend that implements the [`Backend`] trait
//! 2. Adding application state via [`Data`](crate::task::data)
//! 3. Decorating the service pipeline with middleware
//! 4. Handling lifecycle events with `on_event`
//! 5. Providing task processing logic using [`build`](WorkerBuilder::build) that implements [`IntoWorkerService`].
//!
//! The [`IntoWorkerService`] trait can be used to convert a function or a service into a worker service. The following implementations are provided:
//! - For async functions via [`task_fn`](crate::task::task_fn::task_fn)
//! - For any type that implements the [`Service`] trait for `T: Task`
//! - For workflows via [`apalis-workflow`](https://docs.rs/apalis-workflow)
//!
//! ## Basic usage
//!
//! ```rust,no_run
//! # use apalis_core::worker::builder::WorkerBuilder;
//! # use apalis_core::backend::memory::MemoryStorage;
//! # use apalis_core::worker::context::WorkerContext;
//! # use apalis_core::task::data::Data;
//! # use apalis_core::backend::TaskSink;
//! # use apalis_core::worker::ext::event_listener::EventListenerExt;
//!
//! # #[tokio::main]
//! # async fn main() {
//! # let mut in_memory = MemoryStorage::new();
//! # in_memory.push(24).await.unwrap();
//! async fn task(job: u32, count: Data<usize>, worker: WorkerContext) {
//!     println!("Received job: {job:?}");
//!     worker.stop().unwrap();
//! }
//!
//! let worker = WorkerBuilder::new("rango-tango")
//!     .backend(in_memory)
//!     .data(0usize)
//!     .on_event(|worker, ev| {
//!         println!("On Event = {:?}", ev);
//!     })
//!     .build(task);
//!
//! worker.run().await.unwrap();
//! # }
//! ```
//! ## Order
//!
//! The order in which you add layers affects how tasks are processed. Layers added earlier are wrapped by those added later.
//!
//! ### Why does order matter?
//! Each layer wraps the previous one, so the outermost layer is applied last. This means that middleware added later can observe or modify the effects of earlier layers. For example, tracing added before retry will see all retries as a single operation, while tracing added after retry will log each retry attempt separately.
//!
//! For example:
//! ```ignore
//! WorkerBuilder::new()
//!     .enable_tracing()
//!     .retry(RetryPolicy::retries(3))
//!     .build(task);
//! ```
//! In this case, tracing is applied before retry. The tracing span may not reflect the correct attempt count.
//!
//! Reversing the order:
//! ```ignore
//! WorkerBuilder::new()
//!     .retry(RetryPolicy::retries(3))
//!     .enable_tracing()
//!     .build(task);
//! ```
//! Now, retry is applied first, and tracing wraps around it. The tracing span will correctly capture retries.
//!
//! **Tip:** Add layers in the order you want them to wrap task processing.
use std::marker::PhantomData;
use tower_layer::{Identity, Stack};
use tower_service::Service;

use crate::{
    backend::{Backend, BackendConfig},
    monitor::shutdown::Shutdown,
    task::data::Data,
    worker::{
        Worker, context::WorkerContext, event::EventHandlerBuilder, service::IntoWorkerService,
    },
};

/// Declaratively builds a [`Worker`]
pub struct WorkerBuilder<Args, Source, Middleware> {
    pub(crate) context: WorkerContext,
    pub(crate) request: PhantomData<Args>,
    pub(crate) layer: Middleware,
    pub(crate) source: Source,
    pub(crate) event_handler: EventHandlerBuilder,
    pub(crate) shutdown: Option<Shutdown>,
}

impl<Args, Source, Middleware> std::fmt::Debug for WorkerBuilder<Args, Source, Middleware> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WorkerBuilder")
            .field("id", &self.context.name())
            .field("job", &std::any::type_name::<Args>())
            .field("layer", &std::any::type_name::<Middleware>())
            .field("source", &std::any::type_name::<Source>())
            .finish()
    }
}

impl WorkerBuilder<(), (), Identity> {
    /// Build a new [`WorkerBuilder`] instance with a name for the worker to build
    pub fn new<T: Into<WorkerContext>>(name: T) -> Self {
        Self {
            request: PhantomData,
            layer: Identity::new(),
            source: (),
            context: name.into(),
            event_handler: EventHandlerBuilder::default(),
            shutdown: None,
        }
    }
}

impl WorkerBuilder<(), (), Identity> {
    /// Set the source to a backend that implements [Backend]
    pub fn backend<NB, NJ>(self, backend: NB) -> WorkerBuilder<NJ, NB, Identity>
    where
        NB: Backend + BackendConfig<Args = NJ>,
    {
        WorkerBuilder {
            request: PhantomData,
            layer: self.layer,
            source: backend,
            context: self.context,
            shutdown: self.shutdown,
            event_handler: self.event_handler,
        }
    }
}

impl<Args, M, B> WorkerBuilder<Args, B, M>
where
    B: Backend,
{
    /// Allows of decorating the service that consumes jobs.
    /// Allows adding multiple middleware in one call
    pub fn chain<NewLayer>(
        self,
        f: impl FnOnce(M) -> NewLayer,
    ) -> WorkerBuilder<Args, B, NewLayer> {
        let middleware = f(self.layer);

        WorkerBuilder {
            request: self.request,
            layer: middleware,
            context: self.context,
            source: self.source,
            shutdown: self.shutdown,
            event_handler: self.event_handler,
        }
    }
    /// Allows adding middleware to the layer stack
    pub fn layer<U>(self, layer: U) -> WorkerBuilder<Args, B, Stack<U, M>> {
        WorkerBuilder {
            request: self.request,
            source: self.source,
            layer: Stack::new(layer, self.layer),
            context: self.context,
            shutdown: self.shutdown,
            event_handler: self.event_handler,
        }
    }

    /// Adds data to the context
    ///
    /// This will be shared by all requests
    pub fn data<D>(self, data: D) -> WorkerBuilder<Args, B, Stack<Data<D>, M>> {
        WorkerBuilder {
            request: self.request,
            source: self.source,
            layer: Stack::new(Data::new(data), self.layer),
            context: self.context,
            shutdown: self.shutdown,
            event_handler: self.event_handler,
        }
    }

    /// Map the backend decorating and composing a new backend
    ///
    /// **NOTE**
    /// This method mutates the backend but not the sink
    /// This means that you may need to confirm the sink changes in the decorations
    #[doc(hidden)]
    pub fn map_backend<F, NB>(self, map: F) -> WorkerBuilder<Args, NB, M>
    where
        NB: Backend<Task = B::Task>,
        F: FnOnce(B) -> NB,
    {
        WorkerBuilder {
            request: self.request,
            source: map(self.source),
            layer: self.layer,
            context: self.context,
            shutdown: self.shutdown,
            event_handler: self.event_handler,
        }
    }
}

/// Finalizes the builder and constructs a [`Worker`] with the provided service
impl<Args, B, M> WorkerBuilder<Args, B, M> {
    /// Consumes the builder and a service to construct the final worker
    pub fn build<W, Svc, NB>(self, service: W) -> Worker<Args, W::Backend, Svc, M>
    where
        W: IntoWorkerService<B, Svc, Backend = NB>,
        B: Backend + BackendConfig,
        NB: Backend + BackendConfig + Send + Unpin + 'static,
        Svc: Service<NB::Task>,
        Args: Send + 'static,
    {
        let svc = service.into_service(self.source);
        let mut worker = Worker::new(self.context, svc.backend, svc.service, self.layer);
        worker.event_handler = self
            .event_handler
            .write()
            .map(|mut d| d.take())
            .unwrap()
            .unwrap_or(Box::new(|_, _e| {
                debug!("[>] {_e}");
            }));
        worker.shutdown = self.shutdown;
        worker
    }
}
