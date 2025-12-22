#![cfg(feature = "sleep")]
use std::{
    collections::VecDeque,
    fmt,
    fmt::Debug,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use futures_core::stream::BoxStream;
use futures_sink::Sink;
use futures_util::{
    FutureExt, StreamExt, TryStreamExt,
    lock::Mutex,
    sink::{self},
    stream,
};
use tower_layer::Identity;

use crate::{
    backend::{
        Backend, BackendExt,
        codec::IdentityCodec,
        custom::{BackendBuilder, CustomBackend},
        poll_strategy::{
            IntervalStrategy, MultiStrategy, PollContext, PollStrategyExt, StrategyBuilder,
        },
    },
    error::BoxDynError,
    task::{Task, task_id::RandomId},
    worker::context::WorkerContext,
};

/// Wrapper type for the shared
#[derive(Debug)]
pub struct InMemoryDb<T> {
    inner: Arc<Mutex<VecDeque<Task<T, (), RandomId>>>>,
}

impl<T> Clone for InMemoryDb<T> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<T> InMemoryDb<T> {
    /// Create a new InMemoryDb instance
    #[must_use]
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(VecDeque::new())),
        }
    }

    /// Consume the InMemoryDb and return the inner Arc<Mutex<...>>
    #[must_use]
    pub fn into_inner(self) -> Arc<Mutex<VecDeque<Task<T, (), RandomId>>>> {
        self.inner
    }

    /// Get a reference to the inner Arc<Mutex<...>>
    #[must_use]
    pub fn as_arc(&self) -> &Arc<Mutex<VecDeque<Task<T, (), RandomId>>>> {
        &self.inner
    }
}

impl<T> Default for InMemoryDb<T> {
    fn default() -> Self {
        Self::new()
    }
}

/// Configuration for the in-memory VecDeque backend
#[derive(Debug, Clone)]
pub struct Config {
    strategy: MultiStrategy,
    prev_count: Arc<AtomicUsize>,
}
/// Type alias for the boxed sink type
pub type BoxSink<'a, T> = Pin<Box<dyn Sink<T, Error = VecDequeError> + Send + Sync + 'a>>;
/// Type alias for the sink type
pub type InMemorySink<T> = BoxSink<'static, Task<T, (), RandomId>>;

/// Type alias for the complete in-memory backend
pub struct VecDequeBackend<T>(
    CustomBackend<
        T,
        InMemoryDb<T>,
        BoxStream<'static, Result<Option<Task<T, (), RandomId>>, VecDequeError>>,
        InMemorySink<T>,
        RandomId,
        Config,
    >,
);

impl<T> Debug for VecDequeBackend<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("VecDequeBackend<T>").finish()
    }
}

impl<T> Clone for VecDequeBackend<T> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

/// Errors encountered while using the `VecDequeBackend`
#[derive(Debug, thiserror::Error, Clone)]
pub enum VecDequeError {
    /// Error occurred during polling
    #[error("Polling error: {0}")]
    PollError(Arc<BoxDynError>),
    /// Error occurred during sending
    #[error("Sending error: {0}")]
    SendError(Arc<BoxDynError>),
}

impl<T> Backend for VecDequeBackend<T>
where
    T: Send + 'static,
{
    type Args = T;

    type IdType = RandomId;

    type Context = ();

    type Stream = BoxStream<'static, Result<Option<Task<T, (), Self::IdType>>, VecDequeError>>;

    type Layer = Identity;

    type Beat = BoxStream<'static, Result<(), VecDequeError>>;

    type Error = VecDequeError;

    fn heartbeat(&self, worker: &WorkerContext) -> Self::Beat {
        self.0
            .heartbeat(worker)
            .map_err(|e| VecDequeError::PollError(Arc::new(e.into())))
            .boxed()
    }

    fn middleware(&self) -> Self::Layer {
        self.0.middleware()
    }
    fn poll(self, worker: &WorkerContext) -> Self::Stream {
        self.0
            .poll(worker)
            .map_err(|e| VecDequeError::PollError(Arc::new(e.into())))
            .boxed()
    }
}

impl<T: Clone> BackendExt for VecDequeBackend<T>
where
    T: Send + 'static,
{
    type Codec = IdentityCodec;
    type Compact = T;
    type CompactStream = Self::Stream;

    fn get_queue(&self) -> crate::backend::queue::Queue {
        std::any::type_name::<T>().into()
    }

    fn poll_compact(self, worker: &WorkerContext) -> Self::CompactStream {
        self.0
            .poll(worker)
            .map_err(|e| VecDequeError::PollError(Arc::new(e.into())))
            .boxed()
    }
}

impl<T> Sink<Task<T, (), RandomId>> for VecDequeBackend<T>
where
    T: Send + 'static,
{
    type Error = VecDequeError;

    fn poll_ready(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        Pin::new(&mut self.get_mut().0)
            .poll_ready(cx)
            .map_err(|e| VecDequeError::SendError(Arc::new(e.into())))
    }

    fn start_send(self: Pin<&mut Self>, item: Task<T, (), RandomId>) -> Result<(), Self::Error> {
        Pin::new(&mut self.get_mut().0)
            .start_send(item)
            .map_err(|e| VecDequeError::SendError(Arc::new(e.into())))
    }

    fn poll_flush(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        Pin::new(&mut self.get_mut().0)
            .poll_flush(cx)
            .map_err(|e| VecDequeError::SendError(Arc::new(e.into())))
    }

    fn poll_close(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        Pin::new(&mut self.get_mut().0)
            .poll_close(cx)
            .map_err(|e| VecDequeError::SendError(Arc::new(e.into())))
    }
}

/// Create an in-memory `VecDeque` backend with polling strategy for tasks of type T
#[must_use]
pub fn backend<T>(poll_interval: Duration) -> VecDequeBackend<T>
where
    T: Send + 'static,
{
    let memory = InMemoryDb::new();

    let strategy = StrategyBuilder::new()
        .apply(IntervalStrategy::new(poll_interval))
        .build();

    let config = Config {
        strategy,
        prev_count: Arc::new(AtomicUsize::new(1)),
    };

    let backend = BackendBuilder::new_with_cfg(config)
        .database(memory)
        .fetcher(
            |db: &mut InMemoryDb<T>, config: &Config, worker: &WorkerContext| {
                let poll_strategy = config.strategy.clone();
                let poll_ctx = PollContext::new(worker.clone(), config.prev_count.clone());
                let poller = poll_strategy.build_stream(&poll_ctx);
                stream::unfold(
                    (db.clone(), config.clone(), poller, worker.clone()),
                    |(p, config, mut poller, ctx)| async move {
                        poller.next().await;
                        let Some(mut db) = p.inner.try_lock() else {
                            return Some((
                                Err::<Option<Task<T, (), RandomId>>, VecDequeError>(
                                    VecDequeError::PollError(Arc::new(
                                        "Failed to acquire lock".into(),
                                    )),
                                ),
                                (p, config, poller, ctx),
                            ));
                        };
                        let item = db.pop_front();
                        drop(db);
                        if let Some(item) = item {
                            config.prev_count.store(1, Ordering::Relaxed);
                            Some((Ok::<_, VecDequeError>(Some(item)), (p, config, poller, ctx)))
                        } else {
                            config.prev_count.store(0, Ordering::Relaxed);
                            Some((
                                Ok::<Option<Task<T, (), RandomId>>, VecDequeError>(None),
                                (p, config, poller, ctx),
                            ))
                        }
                    },
                )
                .boxed()
            },
        )
        .sink(|db, _| {
            Box::pin(sink::unfold(db.clone(), move |p, item| {
                async move {
                    let Some(mut db) = p.inner.try_lock() else {
                        return Err(VecDequeError::PollError(Arc::new(
                            "Failed to acquire lock".into(),
                        )));
                    };
                    db.push_back(item);
                    drop(db);
                    Ok::<_, VecDequeError>(p)
                }
                .boxed()
                .shared()
            })) as _
        })
        .build()
        .unwrap();

    VecDequeBackend(backend)
}
