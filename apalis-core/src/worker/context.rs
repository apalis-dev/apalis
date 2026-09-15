//! Worker context and task tracking.
//!
//! [`WorkerContext`] is responsible for managing
//! the execution lifecycle of a worker, tracking tasks, handling shutdown, and emitting
//! lifecycle events.
//!
//! ## Lifecycle
//! A `WorkerContext` goes through distinct phases of operation:
//!
//! - **Pending**: Created via [`WorkerContext::new`] and must be explicitly started.
//! - **Running**: Activated by calling [`WorkerContext::start`]. The worker becomes ready to accept and track tasks.
//! - **Paused**: Temporarily halted via [`WorkerContext::pause`]. New tasks are blocked from execution.
//! - **Resumed**: Brought back to `Running` using [`WorkerContext::resume`].
//! - **Stopped**: Finalized via [`WorkerContext::stop`]. The worker shuts down gracefully, allowing tracked tasks to complete.
//!
//! The `WorkerContext` itself implements [`Future`], and can be `.await`ed — it resolves
//! once the worker is shut down and all tasks have completed.
//!
//! ## Task Management
//! Asynchronous tasks can be tracked which ensures:
//! - Task count is incremented before execution and decremented on completion
//! - Shutdown is automatically triggered once all tasks are done
//!
//! Use [`task_count`](WorkerContext::task_count) and [`has_pending_tasks`](WorkerContext::has_pending_tasks) to inspect
//! ongoing task state.
//!
//! ## Shutdown Semantics
//! The worker is considered shutting down if:
//! - `stop()` has been called
//! - A shutdown signal (if configured) has been triggered
//!
//! Once shutdown begins, no new tasks should be accepted. Internally, a stored [`Waker`] is
//! used to drive progress toward shutdown completion.
//!
//! ## Event Handling
//! Worker lifecycle events (e.g., `Start`, `Stop`) are emitted automatically
//! and custom ones can be emitted using [`WorkerContext::emit`].
//!
//! ## Request Integration
//! `WorkerContext` implements [`FromRequest`] so it can be extracted automatically in request
//! handlers when using a compatible framework or service layer.
//!
//! ## Types
//! - [`WorkerContext`] — shared state container for a worker
use std::{
    fmt::{self},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    task::{Context, Waker},
    time::{Duration, Instant},
};

use dashmap::DashSet;

use crate::{
    error::{WorkerError, WorkerStateError},
    monitor::shutdown::Shutdown,
    task::from_request::FromRequest,
    task::{
        ExecutionContext, Task,
        context::{TaskContext, TaskStateError},
        data::MissingDataError,
    },
    worker::{
        event::{Event, EventListener, RawEventListener},
        lifecycle::TaskLifecycleError,
        state::{InnerWorkerState, WorkerState},
    },
};

/// Utility for managing a worker's context
///
/// A worker context is created for each worker thread and is responsible for managing
/// the worker's state, task tracking, and event handling.
///
///  **Tip**: All fields are wrapped inside [`Arc`] so it should be cheap to clone
#[derive(Clone)]
pub struct WorkerContext {
    pub(crate) name: Arc<String>,
    /// The waker used to wake the worker when tasks complete or shutdown is triggered.
    waker: Arc<Mutex<Option<Waker>>>,
    state: Arc<WorkerState>,
    pub(crate) shutdown: Option<Shutdown>,
    event_handler: EventListener,
    pub(super) is_ready: Arc<AtomicBool>,
    service: &'static str,
    tasks: Arc<DashSet<TaskContext>>,
    instant: Instant,
    restarts: Arc<AtomicUsize>,
}

impl fmt::Debug for WorkerContext {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WorkerContext")
            .field("shutdown", &["Shutdown handle"])
            .field("task_count", &self.task_count())
            .field("state", &self.state.load(Ordering::SeqCst))
            .field("service", &self.service)
            .field("is_ready", &self.is_ready)
            .field("tasks", &"[..]")
            .field("elapsed", &self.instant.elapsed())
            .field("restarts", &self.restarts)
            .finish()
    }
}

impl WorkerContext {
    /// Create a new worker context
    #[must_use]
    pub fn new(name: &str) -> Self {
        Self {
            name: Arc::new(name.to_owned()),
            service: "Unspecified",
            waker: Default::default(),
            state: Default::default(),
            shutdown: Default::default(),
            event_handler: Arc::new(Box::new(|_, _| {
                // noop
            })),
            is_ready: Default::default(),
            tasks: Default::default(),
            instant: Instant::now(),
            restarts: Default::default(),
        }
    }

    /// Get the worker id
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Attach a shutdown handle, registering this worker to be woken when it fires.
    ///
    /// Observing shutdown is not enough on its own: an idle worker is only re-polled when
    /// its own waker fires, and the monitor holds more workers than
    /// `futures_util::future::join_all` re-polls unconditionally. Handing the waker slot to
    /// the handle is what guarantees every worker sees the flag.
    pub(crate) fn attach_shutdown(&mut self, shutdown: Shutdown) {
        shutdown.register_waker(&self.waker);
        self.shutdown = Some(shutdown);
    }

    /// Start running the worker
    pub fn start(&mut self) -> Result<(), WorkerError> {
        let current_state = self.state.load(Ordering::SeqCst);
        if current_state != InnerWorkerState::Pending {
            return Err(WorkerError::StateError(WorkerStateError::AlreadyStarted));
        }
        self.state
            .store(InnerWorkerState::Running, Ordering::SeqCst);
        self.is_ready.store(false, Ordering::SeqCst);
        info!("Worker {} started", self.name());
        self.wake();
        Ok(())
    }

    /// Restart running the worker
    pub(crate) fn restart(&self) -> Result<(), WorkerError> {
        self.state
            .store(InnerWorkerState::Pending, Ordering::SeqCst);
        self.is_ready.store(false, Ordering::SeqCst);
        self.cleanup();
        self.restarts.fetch_add(1, Ordering::SeqCst);
        info!("Worker {} restarted", self.name());
        self.wake();
        Ok(())
    }

    /// Pauses a worker, preventing any new jobs from being polled
    pub fn pause(&self) -> Result<(), WorkerError> {
        if !self.is_running() {
            return Err(WorkerError::StateError(WorkerStateError::NotRunning));
        }
        self.state.store(InnerWorkerState::Paused, Ordering::SeqCst);
        info!("Worker {} paused", self.name());
        Ok(())
    }

    /// Resume a worker that is paused
    pub fn resume(&self) -> Result<(), WorkerError> {
        if !self.is_paused() {
            return Err(WorkerError::StateError(WorkerStateError::NotPaused));
        }
        if self.is_shutting_down() {
            return Err(WorkerError::StateError(WorkerStateError::ShuttingDown));
        }
        self.state
            .store(InnerWorkerState::Running, Ordering::SeqCst);
        self.wake();
        info!("Worker {} resumed", self.name());
        Ok(())
    }

    /// Calling this function triggers shutting down the worker while waiting for any tasks to complete
    pub fn stop(&self) -> Result<(), WorkerError> {
        let current_state = self.state.load(Ordering::SeqCst);
        if current_state == InnerWorkerState::Pending {
            return Err(WorkerError::StateError(WorkerStateError::NotStarted));
        }
        self.state
            .store(InnerWorkerState::Stopped, Ordering::SeqCst);
        self.wake();
        self.emit_ref(&Event::Stop);
        info!("Worker {} stopped", self.name());
        Ok(())
    }

    /// Checks if the worker is ready to consume new tasks
    #[must_use]
    pub fn is_ready(&self) -> bool {
        self.is_running() && !self.is_shutting_down() && self.is_ready.load(Ordering::SeqCst)
    }

    /// Get the `type_name` of the service used
    ///
    /// ## Example
    /// ```ignore
    /// async fn send_email(email: Email) {}
    ///
    /// // Might be something like:
    /// TaskFn<send_email, Email, ()>>
    /// ```
    #[must_use]
    pub fn get_service(&self) -> &str {
        self.service
    }

    pub(super) fn bind_service<T>(&mut self) {
        let service = std::any::type_name::<T>();

        const RULES: &[(&str, &str, &str)] = &[
            (
                "Retry<",
                "Trace<",
                "`retries()` must be before `enable_tracing()`; traces produced will provide invalid attempt information",
            ),
            (
                "Retry<",
                "PrometheusService<",
                "`retries()` must be before `prometheus()`; metrics inside retries will be invalid",
            ),
            (
                "Retry<",
                "Timeout<",
                "`retries()` must be before `timeout()`; timeouts will be applied to the total retry process",
            ),
            (
                "Timeout<",
                "Trace<",
                "`timeout()` should be before `enable_tracing()`; otherwise timeout failures may not be reflected correctly in traces",
            ),
            (
                "Timeout<",
                "PrometheusService<",
                "`timeout()` should be before `prometheus()`; otherwise timeout failures may not be reflected correctly in metrics",
            ),
            (
                "ConcurrencyLimit<",
                "Retry<",
                "`concurrency()` should generally be before `retries()`; otherwise each retry may consume a separate concurrency slot",
            ),
            (
                "ConcurrencyLimit<",
                "Timeout<",
                "`concurrency()` should generally be before `timeout()`; otherwise queued requests may consume timeout duration",
            ),
            (
                "RateLimit<",
                "Retry<",
                "`rate_limit()` should generally be before `retries()`; otherwise retries may consume rate-limit capacity",
            ),
            (
                "LoadShed<",
                "Retry<",
                "`load_shed()` should generally be before `retries()`; otherwise retries may repeatedly encounter load-shed failures",
            ),
            (
                "Retry<",
                "CatchPanicService<",
                "`catch_panic()` should generally be after `retries()` if panics are intended to participate in retry handling",
            ),
        ];

        for &(outer, inner, message) in RULES {
            if let (Some(a), Some(b)) = (service.find(outer), service.find(inner)) {
                if a > b {
                    warn!("{message}");
                }
            }
        }

        self.service = service;
    }

    /// Checks whether the worker is running
    #[must_use]
    pub fn is_running(&self) -> bool {
        self.state.load(Ordering::SeqCst) == InnerWorkerState::Running
    }

    /// Checks whether the worker is pending
    #[must_use]
    pub fn is_pending(&self) -> bool {
        self.state.load(Ordering::SeqCst) == InnerWorkerState::Pending
    }

    /// Checks whether the worker is paused
    #[must_use]
    pub fn is_paused(&self) -> bool {
        self.state.load(Ordering::SeqCst) == InnerWorkerState::Paused
    }

    /// Checks whether the worker has been stopped
    #[must_use]
    pub fn is_stopped(&self) -> bool {
        self.state.load(Ordering::SeqCst) == InnerWorkerState::Stopped || self.is_terminated()
    }

    /// Checks whether the worker is terminated
    ///
    #[must_use]
    pub fn is_terminated(&self) -> bool {
        self.state.load(Ordering::SeqCst) == InnerWorkerState::Terminated
    }

    /// Checks the current futures in the worker domain
    /// This include futures spawned via `worker.track`
    #[must_use]
    pub fn task_count(&self) -> usize {
        self.tasks.len()
    }

    /// Checks whether the worker has pending tasks
    #[must_use]
    pub fn has_pending_tasks(&self) -> bool {
        self.task_count() > 0
    }

    /// Is the shutdown token called
    #[must_use]
    pub fn is_shutting_down(&self) -> bool {
        self.is_stopped() || self.shutdown.as_ref().is_some_and(|s| s.is_shutting_down())
    }

    /// Get the current worker state
    #[must_use]
    pub fn state(&self) -> &str {
        self.state.as_str()
    }

    /// Emits an event to the worker's event handler
    pub(crate) fn emit_event(&self, event: &Event) {
        self.emit_ref(event);
    }

    /// Emits a [`Event::Custom`] to the worker's event handler
    pub fn emit<T: Send + Sync + 'static>(&self, data: T) {
        self.emit_ref(&Event::custom(data));
    }

    fn emit_ref(&self, event: &Event) {
        let handler = self.event_handler.as_ref();
        handler(self, event);
    }

    /// Calls a method to signify a heartbeat with the worker
    pub fn heartbeat(&self, cx: &mut Context<'_>) {
        self.register_waker(cx);
        self.emit_ref(&Event::HeartBeat);
        // Mark the worker as ready/alive.
        self.is_ready.store(true, Ordering::SeqCst);
    }

    /// Wraps the event listener with a new function
    pub(crate) fn add_listener<F: Fn(&Self, &Event) + Send + Sync + 'static>(&mut self, f: F) {
        let cur = self.event_handler.clone();
        let new: RawEventListener = Box::new(move |ctx, ev| {
            f(ctx, ev);
            cur(ctx, ev);
        });
        self.event_handler = Arc::new(new);
    }

    /// Register the current waker for the worker
    ///
    /// This is used to wake the worker when tasks complete or shutdown is triggered.
    pub(crate) fn register_waker(&self, cx: &Context<'_>) {
        if let Ok(mut guard) = self.waker.lock() {
            if guard
                .as_ref()
                .is_none_or(|stored| !stored.will_wake(cx.waker()))
            {
                *guard = Some(cx.waker().clone());
            }
        }
    }

    pub(crate) fn wake(&self) {
        if let Ok(waker) = self.waker.lock() {
            if let Some(waker) = &*waker {
                waker.wake_by_ref();
            }
        }
    }

    /// Register the [`ExecutionContext`] to get the [`TaskContext`]
    pub(super) fn register_task(
        &self,
        ctx: &Arc<ExecutionContext>,
    ) -> Result<TaskContext, TaskLifecycleError> {
        let task_id = ctx
            .task_id()
            .ok_or(TaskLifecycleError::MissingTaskId)?
            .to_string();

        let tasks = &self.tasks;

        if tasks.contains(task_id.as_str()) {
            return Err(TaskLifecycleError::Duplicate);
        }

        let token = TaskContext::new(ctx);
        tasks.insert(token.clone());
        Ok(token)
    }

    /// Cancel a specific task, if it's tracked.
    pub fn cancel_task(&self, context: &TaskContext) -> Result<(), TaskStateError> {
        if let Some(token) = self.tasks.get(context) {
            token.cancel()
        } else {
            Err(TaskStateError::TaskNotFound)
        }
    }

    /// Remove the token once the task completes, to avoid unbounded growth.
    pub(super) fn remove_task(&self, ctx: &TaskContext) -> bool {
        let task_id = ctx.task_id();
        self.tasks.remove(task_id).is_some()
    }

    /// Extracts the [`TaskContext`] from the [`WorkerContext`]
    pub(crate) fn get_task_context(
        &self,
        ctx: &Arc<ExecutionContext>,
    ) -> Result<TaskContext, MissingDataError> {
        self.get_task(ctx.task_id().unwrap().to_string().as_str())
    }

    /// Get the task context for a task attached to a worker
    pub fn get_task(&self, task_id: &str) -> Result<TaskContext, MissingDataError> {
        let tasks = &self.tasks;
        Ok(tasks
            .get(task_id)
            .ok_or(MissingDataError::NotFound("TaskContext".to_owned()))?
            .clone())
    }

    /// Remove any completed tasks
    pub fn cleanup(&self) {
        let tasks = &self.tasks;
        tasks.retain(|token| !(token.is_completed() && token.is_empty()));
    }

    /// Get the context of each running task
    #[must_use]
    pub fn tasks(&self) -> Vec<TaskContext> {
        self.tasks.iter().map(|s| s.clone()).collect()
    }

    /// Returns the amount of time elapsed since this worker started.
    #[must_use]
    pub fn elapsed(&self) -> Duration {
        self.instant.elapsed()
    }

    /// Returns the number of times the worker has been restated:
    ///
    /// See also [`Monitor::should_restart`]
    ///
    /// [`Monitor::should_restart`]: crate::monitor::Monitor::should_restart
    #[must_use]
    pub fn restarts(&self) -> usize {
        self.restarts.load(Ordering::SeqCst)
    }

    /// This forces a shutting down worker to exit.
    pub fn kill(&mut self) -> Result<(), WorkerError> {
        if !self.is_shutting_down() {
            return Err(WorkerError::StateError(WorkerStateError::InvalidState(
                "Worker is not shutting down".to_owned(),
            )));
        }
        if self.task_count() != 0 {
            self.tasks()
                .into_iter()
                .map(|a| a.cancel())
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| {
                    WorkerError::StateError(WorkerStateError::InvalidState(e.to_string()))
                })?;
        }
        self.state
            .store(InnerWorkerState::Terminated, Ordering::SeqCst);
        self.wake();
        Ok(())
    }
}

impl From<&str> for WorkerContext {
    fn from(name: &str) -> Self {
        Self::new(name)
    }
}

impl From<String> for WorkerContext {
    fn from(name: String) -> Self {
        Self::new(&name)
    }
}

impl From<&Self> for WorkerContext {
    fn from(context: &Self) -> Self {
        context.clone()
    }
}

impl<Args: Sync> FromRequest<Task<Args>> for WorkerContext {
    type Error = MissingDataError;
    async fn from_request(task: &Task<Args>) -> Result<Self, Self::Error> {
        task.data().get_checked().cloned()
    }
}

impl Drop for WorkerContext {
    fn drop(&mut self) {
        if Arc::strong_count(&self.state) > 1 {
            // There are still other references to this context, so we shouldn't log a warning.
            return;
        }
        if self.is_running() && self.has_pending_tasks() {
            error!(
                "Worker '{}' is being dropped while running with `{}` tasks. Consider calling stop() before dropping.",
                self.name(),
                self.task_count()
            );
        }
    }
}

#[cfg(test)]
mod tests {

    use futures_util::FutureExt;

    use crate::{
        backend::memory::MemoryStorage, error::BoxDynError, worker::builder::WorkerBuilder,
    };
    use std::time::Duration;

    use super::*;

    #[tokio::test]
    async fn test_worker_state_transitions() {
        let backend = MemoryStorage::<u32>::new();

        let ctx = WorkerContext::new("test-worker");

        let worker = WorkerBuilder::new(&ctx)
            .backend(backend)
            .build(|_task: u32| async { Ok::<_, BoxDynError>(()) });

        let worker_handle = tokio::spawn(async move { worker.run().boxed().await });
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Initial state: worker should be running
        assert!(ctx.is_running());
        assert!(!ctx.is_shutting_down());
        assert!(!ctx.is_stopped());

        // Pause the worker
        ctx.pause().unwrap();
        assert!(ctx.is_paused());
        assert!(
            !ctx.is_shutting_down(),
            "Paused worker should NOT be considered shutting down"
        );

        // Resume the worker
        ctx.resume().unwrap();
        assert!(ctx.is_running());
        assert!(!ctx.is_paused());

        // Stop the worker
        ctx.stop().unwrap();
        assert!(ctx.is_stopped());
        assert!(ctx.is_shutting_down());

        // Try to resume a stopped worker (should fail with NotPaused error since state is Stopped)
        assert!(
            matches!(
                ctx.resume(),
                Err(WorkerError::StateError(WorkerStateError::NotPaused))
            ),
            "Resuming a stopped worker should fail with NotPaused error"
        );

        worker_handle.await.unwrap().unwrap();
    }
}
