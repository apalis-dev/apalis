//! Task lifecycle tracking.
//!
//! This module provides middleware for tracking the lifecycle of tasks as they
//! are processed by a worker. The [`LifecycleLayer`] registers tasks with the
//! worker, tracks their execution state, records attempts, and removes them
//! from the worker when processing completes.
//!
//! The lifecycle service also handles task cancellation and maps task results
//! to their corresponding [`Status`] values.
use std::{
    pin::Pin,
    sync::atomic::Ordering,
    task::{Context, Poll},
};

use futures_util::FutureExt;
use tower_layer::Layer;
use tower_service::Service;

use crate::{
    error::{AbortError, BoxDynError, DeferredError, RetryAfterError},
    task::{
        Task,
        attempt::Attempt,
        context::{TaskContext, TaskStateError},
        status::{AtomicStatus, Status},
    },
    worker::context::WorkerContext,
};

/// A layer that tracks the lifecycle of tasks processed by a worker.
///
/// The layer registers each task with the [`WorkerContext`] and wraps the
/// resulting task future so that its lifecycle state can be updated as the
/// future progresses.
#[derive(Debug, Clone)]
pub struct LifecycleLayer {
    worker: WorkerContext,
}

impl LifecycleLayer {
    pub(super) fn new(worker: WorkerContext) -> Self {
        Self { worker }
    }
}

impl<S> Layer<S> for LifecycleLayer {
    type Service = LifecycleService<S>;

    fn layer(&self, service: S) -> Self::Service {
        LifecycleService {
            worker: self.worker.clone(),
            service,
        }
    }
}

/// A service that tracks the lifecycle of a task.
///
/// `LifecycleService` registers tasks with the worker before processing and
/// wraps the underlying service future in a [`LifecycleFuture`] to track task
/// state and completion.
#[derive(Debug, Clone)]
pub struct LifecycleService<S> {
    worker: WorkerContext,
    service: S,
}

impl<S, Args> Service<Task<Args>> for LifecycleService<S>
where
    S: Service<Task<Args>>,
    S::Error: Into<BoxDynError>,
{
    type Response = S::Response;
    type Error = BoxDynError;
    type Future = LifecycleFuture<S::Future>;

    fn poll_ready(&mut self, cx: &mut std::task::Context<'_>) -> Poll<Result<(), Self::Error>> {
        let result = self.service.poll_ready(cx).map_err(Into::into);
        if self.worker.is_shutting_down() || self.worker.is_paused() || result.is_pending() {
            self.worker.is_ready.store(false, Ordering::SeqCst);
            return Poll::Pending;
        }

        match &result {
            Poll::Ready(Ok(_)) => self.worker.is_ready.store(true, Ordering::SeqCst),
            Poll::Pending | Poll::Ready(Err(_)) => {
                self.worker.is_ready.store(false, Ordering::SeqCst)
            }
        }

        result
    }

    fn call(&mut self, task: Task<Args>) -> Self::Future {
        let attempt = task.raw_attempt().clone();
        let status = task.raw_status().clone();
        let task_ctx = self
            .worker
            .register_task(task.ctx())
            .expect("Could not register task");

        LifecycleFuture {
            fut: self.service.call(task),
            worker: self.worker.clone(),
            task_ctx,
            record_attempt: Some(attempt),
            task_status: status,
        }
    }
}

/// A future that tracks the lifecycle of a task.
///
/// The future updates the task status when processing begins, handles
/// cancellation, and records the final task status when processing completes.
#[pin_project::pin_project(PinnedDrop)]
#[derive(Debug)]
pub struct LifecycleFuture<Fut> {
    #[pin]
    fut: Fut,
    task_ctx: TaskContext,
    worker: WorkerContext,
    record_attempt: Option<Attempt>,
    task_status: AtomicStatus,
}

/// Errors that can occur during the lifecycle of a task.
///
/// These errors indicate that a task could not transition through its
/// lifecycle as requested.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum TaskLifecycleError {
    /// The task does not have an identifier.
    ///
    /// A task identifier is required for lifecycle operations that need to
    /// track or associate the task with its execution state.
    #[error("task doesn't have an id")]
    MissingTaskId,

    /// The task already has a token associated with it.
    ///
    /// A task can only have one lifecycle token. Attempting to assign another
    /// token results in this error.
    #[error("task already has a token")]
    Duplicate,

    /// The task exited after it was manually canceled.
    ///
    /// Contains the [`TaskContext`] associated with the task at the time it
    /// exited.
    #[error("task exited after manually being canceled")]
    Exit(TaskContext),

    /// The task is in an invalid state for the requested lifecycle operation.
    ///
    /// Contains the underlying [`TaskStateError`] describing the invalid
    /// state transition.
    #[error("task state is invalid: {0}")]
    StateError(#[from] TaskStateError),
}

impl<Fut, T, E> Future for LifecycleFuture<Fut>
where
    Fut: Future<Output = Result<T, E>>,
    E: Into<BoxDynError>,
{
    type Output = Result<T, BoxDynError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut this = self.project();
        if let Some(attempt) = this.record_attempt.take() {
            let _ = attempt.increment();
            this.task_status.store(Status::Running);
        }
        let task_context = this.task_ctx;
        if task_context.is_cancelled() {
            this.task_status.store(Status::Killed);
            return Poll::Ready(Err(Box::new(AbortError::new(TaskLifecycleError::Exit(
                task_context.clone(),
            )))));
        }

        match this.fut.poll_unpin(cx).map(|res| res.map_err(Into::into)) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(res) => {
                let new_status = match res {
                    Ok(_) => Status::Done,
                    Err(ref e) => match e {
                        e if e.downcast_ref::<AbortError>().is_some() => Status::Killed,
                        e if (e.downcast_ref::<RetryAfterError>().is_some()
                            || e.downcast_ref::<DeferredError>().is_some()) =>
                        {
                            Status::Pending
                        }
                        _ => Status::Failed,
                    },
                };

                if let Err(e) = task_context.complete() {
                    this.task_status.store(Status::Killed);
                    return Poll::Ready(Err(e.into()));
                }
                this.task_status.store(new_status);
                Poll::Ready(res)
            }
        }
    }
}

#[pin_project::pinned_drop]
impl<Fut> PinnedDrop for LifecycleFuture<Fut> {
    fn drop(self: Pin<&mut Self>) {
        let this = self.project();
        if !this.worker.remove_task(this.task_ctx) {
            warn!("Dropped a task without removing it from the worker's context");
        }
        if this.worker.task_count() == 0 {
            this.worker.wake();
        }
    }
}
