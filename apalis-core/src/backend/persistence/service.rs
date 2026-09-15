use std::task::{Context, Poll};

use futures_channel::mpsc::Sender;
use futures_core::future::BoxFuture;
use futures_util::{FutureExt, SinkExt};
use tower_service::Service;

use crate::{
    backend::{TaskResult, codec::Codec, persistence::event::TaskEvent},
    error::BoxDynError,
    task::Task,
};

/// A middleware layer that persists task lifecycle events.
///
/// `TaskPersistLayer` sends task events to a persistence channel and can be
/// configured to persist task results and lock tasks while they are being
/// processed.
///
/// # Type Parameters
///
/// * `C` - The codec used to encode task data.
/// * `Compact` - The compact representation used for persisted task events.
#[derive(Clone, Debug)]
pub struct TaskPersistLayer<C, Compact> {
    sender: Sender<TaskEvent<Compact>>,
    codec: C,
    persist_results: bool,
    lock_tasks: bool,
}

impl<Compact, C> TaskPersistLayer<C, Compact> {
    /// Creates a new task persistence layer.
    ///
    /// Task results and task locking are disabled by default.
    #[must_use]
    pub(super) fn new(sender: Sender<TaskEvent<Compact>>, codec: C) -> Self {
        Self {
            sender,
            codec,
            persist_results: false,
            lock_tasks: false,
        }
    }

    /// Configures whether task results should be persisted.
    ///
    /// By default, task results are not persisted.
    #[must_use]
    pub fn persist_results(self, persist_results: bool) -> Self {
        Self {
            sender: self.sender,
            codec: self.codec,
            persist_results,
            lock_tasks: self.lock_tasks,
        }
    }

    /// Configures whether tasks should be locked while being processed.
    ///
    /// By default, tasks are not locked.
    #[must_use]
    pub fn lock_tasks(self, lock_tasks: bool) -> Self {
        Self {
            sender: self.sender,
            codec: self.codec,
            persist_results: self.persist_results,
            lock_tasks,
        }
    }
}

impl<S, Compact, C> tower_layer::Layer<S> for TaskPersistLayer<C, Compact>
where
    C: Clone,
{
    type Service = TaskPersistService<S, Compact, C>;

    fn layer(&self, inner: S) -> Self::Service {
        TaskPersistService {
            inner,
            sender: self.sender.clone(),
            codec: self.codec.clone(),
            should_ack: self.persist_results,
            should_lock: self.lock_tasks,
        }
    }
}

#[derive(Clone, Debug)]
pub struct TaskPersistService<S, Compact, C> {
    inner: S,
    sender: Sender<TaskEvent<Compact>>,
    codec: C,
    should_ack: bool,
    should_lock: bool,
}

impl<S, Args, C> Service<Task<Args>> for TaskPersistService<S, C::Compact, C>
where
    S: Service<Task<Args>> + Send + 'static,
    S::Future: Send + 'static,
    S::Error: Into<BoxDynError>,
    Args: Send + 'static,
    C: Codec<S::Response> + Clone + Send + 'static,
    C::Error: std::error::Error,
    S::Response: Send,
    C::Compact: Send,
{
    type Response = S::Response;
    type Error = BoxDynError;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        match self.sender.poll_ready(cx) {
            Poll::Ready(Ok(_)) => self.inner.poll_ready(cx).map_err(|e| e.into()),
            Poll::Ready(Err(e)) => Poll::Ready(Err(e.into())),
            Poll::Pending => Poll::Pending,
        }
    }

    fn call(&mut self, req: Task<Args>) -> Self::Future {
        let mut sender = self.sender.clone();
        let ctx = req.ctx().clone();
        let fut = self.inner.call(req);
        let codec = self.codec.clone();
        let should_ack = self.should_ack;
        let should_lock = self.should_lock;
        async move {
            let task_id = ctx.task_id().unwrap().clone();
            if should_lock {
                sender
                    .send(TaskEvent::Lock {
                        task_id: task_id.clone(),
                    })
                    .await?;
            }

            let res = fut.await.map_err(|e| e.into());
            if should_ack {
                let status = ctx.status();
                let result = res
                    .as_ref()
                    .map_err(|e| e.to_string())
                    .and_then(|res| codec.encode(res).map_err(|e| e.to_string()));
                let attempt = ctx.attempt();
                sender
                    .send(TaskEvent::Complete(TaskResult {
                        task_id,
                        attempt,
                        result,
                        status,
                    }))
                    .await?;
            }
            res
        }
        .boxed()
    }
}
