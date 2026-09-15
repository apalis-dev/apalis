//! Task execution context and lifecycle tracking.
//!
//! This module provides [`TaskContext`], a lightweight handle for interacting
//! with a task while it is being executed. A task context is associated with a
//! single task and can be cloned and shared with sub-tasks.
//!
//! Unlike [`WorkerContext`], which represents state shared by all tasks running
//! on a worker, [`TaskContext`] represents the lifecycle of one task. It can be
//! used to request cancellation, observe completion, wait for execution to
//! finish, access the task's [`ExecutionContext`], and track sub-tasks.
//!
//! A task context is considered executed once the task has either completed or
//! been cancelled. Sub-tasks spawned through [`TaskContext::run_until_executed`]
//! are tied to their parent context and are tracked until they complete.
//!
//! # Lifecycle
//!
//! A task context progresses through the following states:
//!
//! ```text
//! Running
//!    ├── cancel() ──> Cancelled
//!    └── complete() ─> Completed
//! ```
//!
//! Once a task reaches a terminal state, subsequent attempts to cancel or
//! complete it return a [`TaskStateError`].
//!
//! # Waiting for execution
//!
//! [`TaskContext::executed`] returns a future that resolves when the task is
//! either completed or cancelled. The future registers a waker and therefore
//! does not require polling in a loop.
//!
//! # Sub-tasks
//!
//! [`TaskContext::run_until_executed`] can be used to associate additional
//! asynchronous work with a task. A sub-task runs until it completes or its
//! parent context is executed. Sub-tasks are counted by their parent context,
//! allowing task execution to account for work spawned from the task.
//!
//! # Task state errors
//!
//! [`TaskStateError`] describes failures when transitioning a task between
//! lifecycle states. [`TerminalState`] identifies the terminal state that
//! prevented the requested transition.
use futures_util::task::AtomicWaker;
use std::{
    borrow::Borrow,
    fmt,
    hash::{Hash, Hasher},
    pin::Pin,
    sync::{
        Arc, Weak,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Poll},
    time::{Duration, Instant},
};

use crate::{
    task::from_request::FromRequest,
    task::{ExecutionContext, Task, data::MissingDataError},
    worker::context::WorkerContext,
};

const RUNNING: usize = 0;
const CANCELLED: usize = 1; // Task was cancelled during polling
const COMPLETED: usize = 2; // Task was completed via polling

/// The context for a task, which can be used to cancel the task or spawn sub-tasks.
///
/// Unlike [`WorkerContext`], which is shared across all tasks,
/// a [`TaskContext`] is unique to a single task and can be cloned to share with sub-tasks.
/// A task context is considered "executed" when the task has successfully completed or has been cancelled, and all sub-tasks have also completed.
#[derive(Clone, Debug)]
pub struct TaskContext {
    task_id: Arc<str>,
    state: Arc<AtomicUsize>,
    /// Wakes any pending [`WaitForExecutionFuture`] future when `complete() or cancel()` is called.
    waker: Arc<AtomicWaker>,
    instant: Instant,
    inner: Weak<ExecutionContext>,
    /// The number of sub-tasks currently spawned by this task context.
    sub_tasks_count: Arc<AtomicUsize>,
}

impl TaskContext {
    /// Builds a new task context from the given [`ExecutionContext`].
    ///
    /// # Panics
    ///
    /// Panics if `ctx.task_id` is `None`; every execution context driving
    /// a task is expected to carry one.
    #[must_use]
    pub(crate) fn new(ctx: &Arc<ExecutionContext>) -> Self {
        Self {
            task_id: ctx
                .task_id
                .as_ref()
                .expect("A task id must be included")
                .to_string()
                .into(),
            state: Arc::new(AtomicUsize::new(RUNNING)),
            waker: Arc::new(AtomicWaker::new()),
            instant: Instant::now(),
            inner: Arc::downgrade(ctx),
            sub_tasks_count: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// Requests cancellation of the task.
    ///
    /// Wakes any pending [WaitForExecutionFuture] futures waiting for execution to complete
    ///
    /// # Errors
    ///
    /// Returns an error if the task had already reached a terminal state
    /// (already cancelled or already completed).
    pub fn cancel(&self) -> Result<(), TaskStateError> {
        self.state
            .compare_exchange(RUNNING, CANCELLED, Ordering::AcqRel, Ordering::Acquire)
            .map_err(|prev| {
                TaskStateError::AlreadyExecuted(match prev {
                    CANCELLED => TerminalState::Cancelled,
                    COMPLETED => TerminalState::Completed,
                    other => unreachable!("unexpected task state: {other}"),
                })
            })?;
        self.waker.wake();
        Ok(())
    }

    /// Marks the task as completed.
    ///
    /// Wakes any pending [WaitForExecutionFuture] futures.
    ///
    /// # Errors
    ///
    /// Returns an error if the task had already reached a terminal state
    /// (already cancelled or already completed).
    pub(crate) fn complete(&self) -> Result<(), TaskStateError> {
        self.state
            .compare_exchange(RUNNING, COMPLETED, Ordering::AcqRel, Ordering::Acquire)
            .map_err(|prev| {
                TaskStateError::AlreadyTerminated(match prev {
                    CANCELLED => TerminalState::Cancelled,
                    COMPLETED => TerminalState::Completed,
                    other => unreachable!("unexpected task state: {other}"),
                })
            })?;
        self.waker.wake();
        Ok(())
    }

    /// Returns whether the task has been completed.
    #[must_use]
    pub fn is_completed(&self) -> bool {
        self.state.load(Ordering::Acquire) == COMPLETED
    }

    /// Returns whether cancellation has been requested.
    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        self.state.load(Ordering::Acquire) == CANCELLED
    }

    /// Returns whether there is nothing left to wait on:
    ///
    /// If true the task has either completed or been cancelled.
    #[must_use]
    pub fn is_executed(&self) -> bool {
        self.state.load(Ordering::Acquire) != RUNNING
    }

    /// Returns a future that resolves once the task is executed — either
    /// completed or cancelled.
    ///
    /// Can be awaited standalone or raced against other futures, e.g.:
    ///
    /// ```ignore
    /// futures_util::select! {
    ///     _ = ctx.executed().fuse() => { /* task done */ }
    ///     res = some_work.fuse() => { /* completed with `res` */ }
    /// }
    /// ```
    pub fn executed(&self) -> WaitForExecutionFuture {
        WaitForExecutionFuture {
            inner: self.clone(),
        }
    }

    /// Returns how long the task has been running.
    #[must_use]
    pub fn elapsed(&self) -> Duration {
        self.instant.elapsed()
    }

    /// Returns the task id for this token.
    #[must_use]
    pub fn task_id(&self) -> &str {
        &self.task_id
    }

    /// Recovers the execution context for the task, if it's still available.
    ///
    /// Returns `None` once the owning [`ExecutionContext`] has been dropped.
    #[must_use]
    pub fn execution_context(&self) -> Option<Arc<ExecutionContext>> {
        self.inner.upgrade()
    }

    fn start_task(&self) {
        self.sub_tasks_count.fetch_add(1, Ordering::Relaxed);
    }

    fn end_task(&self) {
        self.sub_tasks_count.fetch_sub(1, Ordering::Relaxed);
    }

    /// Returns the number of sub-tasks currently spawned by this task context.
    #[must_use]
    pub fn len(&self) -> usize {
        self.sub_tasks_count.load(Ordering::Relaxed)
    }

    /// Returns whether there are any subtasks running.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Spawns a (sub-task) future that is tied to this task context.
    ///
    /// Runs a future to completion, returning its result unless this
    /// [`TaskContext`] is completed first.
    ///
    /// Biased towards completion: if the future resolves and the task
    /// is completed in the same poll, the future's result wins.
    pub fn run_until_executed<F>(&self, fut: F) -> SubTaskFuture<F>
    where
        F: Future,
    {
        self.start_task();
        SubTaskFuture {
            parent: self.clone(),
            future: fut,
        }
    }
}

/// Errors that can occur when transitioning a [`TaskContext`]'s lifecycle state.
#[derive(Debug, thiserror::Error, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum TaskStateError {
    /// The task could not be cancelled because it had already reached a
    /// terminal state.
    #[error("task cannot be cancelled: already {0}")]
    AlreadyExecuted(TerminalState),

    /// The task could not be marked completed because it had already
    /// reached a terminal state.
    #[error("task cannot be completed: already {0}")]
    AlreadyTerminated(TerminalState),

    /// Task could not be found
    #[error("task cannot be found")]
    TaskNotFound,
}

/// The terminal state a task had already reached, when a state transition fails.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum TerminalState {
    /// Task already canceled
    Cancelled,
    /// Task already completed
    Completed,
}

impl fmt::Display for TerminalState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Cancelled => write!(f, "cancelled"),
            Self::Completed => write!(f, "completed"),
        }
    }
}

/// A future that resolves once its parent [`TaskContext`] is executed —
/// either completed or cancelled.
///
/// Unlike polling [`TaskContext::is_executed`] in a loop, this future
/// registers a waker so it can be awaited on its own — e.g. raced against
/// other work with `select!` — without needing another future to drive it.
#[must_use = "futures do nothing unless you `.await` or poll them"]
#[derive(Debug)]
pub struct WaitForExecutionFuture {
    inner: TaskContext,
}

impl Future for WaitForExecutionFuture {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if self.inner.is_executed() {
            Poll::Ready(())
        } else {
            self.inner.waker.register(cx.waker());
            Poll::Pending
        }
    }
}

/// A future that runs a sub-task tied to a [`TaskContext`].
///
/// The sub-task is cancelled if the parent task context is completed before
/// the future resolves. The sub-task is also tracked by the parent context,
/// and the parent context will not be considered complete until all sub-tasks
/// have completed.
#[must_use = "futures do nothing unless polled"]
#[pin_project::pin_project(PinnedDrop)]
#[derive(Debug)]
pub struct SubTaskFuture<F: Future> {
    parent: TaskContext,
    #[pin]
    future: F,
}

#[pin_project::pinned_drop]
impl<F: Future> PinnedDrop for SubTaskFuture<F> {
    fn drop(self: Pin<&mut Self>) {
        self.parent.end_task();
    }
}

impl<F: Future> Future for SubTaskFuture<F> {
    type Output = Option<F::Output>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        if let Poll::Ready(res) = this.future.poll(cx) {
            Poll::Ready(Some(res))
        } else if this.parent.is_executed() {
            Poll::Ready(None)
        } else {
            Poll::Pending
        }
    }
}

impl PartialEq for TaskContext {
    fn eq(&self, other: &Self) -> bool {
        self.task_id() == other.task_id()
    }
}
impl Eq for TaskContext {}

impl Hash for TaskContext {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.task_id().hash(state);
    }
}

impl Borrow<str> for TaskContext {
    fn borrow(&self) -> &str {
        &self.task_id
    }
}

impl<Args: Sync> FromRequest<Task<Args>> for TaskContext {
    type Error = MissingDataError;
    async fn from_request(task: &Task<Args>) -> Result<Self, Self::Error> {
        let worker: &WorkerContext = task.data().get_checked()?;
        let token = worker.get_task_context(&task.ctx)?;
        Ok(token)
    }
}
