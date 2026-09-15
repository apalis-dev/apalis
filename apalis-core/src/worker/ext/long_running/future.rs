use thiserror::Error;

use core::fmt;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use crate::error::BoxDynError;
use crate::task::context::TaskContext;

/// A future that is tracked as a task by a [`TaskRunner`].
///
/// The associated [`TaskRunner`] cannot complete until this future is dropped.
///
/// [`TaskRunner`]: crate::worker::ext::long_running::TaskRunner
#[must_use = "futures do nothing unless polled"]
#[pin_project::pin_project]
pub struct LongRunningFuture<F>
where
    F: Future,
{
    #[pin]
    pub(super) future: F,
    #[pin]
    #[cfg(feature = "sleep")]
    pub(super) timeout: Option<futures_timer::Delay>,
    pub(super) max_duration: Option<Duration>,
    pub(super) task: TaskContext,
}

/// An error encountered during a long running task
#[derive(Error, Debug)]
#[non_exhaustive]
pub enum LongRunningError {
    /// Operation exceeded maximum duration
    #[error("Operation exceeded maximum duration of {max_duration:?}")]
    Timeout {
        /// The max duration that a future should run
        max_duration: Duration,
    },

    /// The parent task was canceled
    #[error("Parent task was cancelled ")]
    Cancelled,

    /// The executor failed
    #[error("The executor failed: {0} ")]
    Execution(BoxDynError),
}

impl<F> Future for LongRunningFuture<F>
where
    F: Future,
    F::Output: Send + 'static,
{
    type Output = Result<F::Output, LongRunningError>;

    #[inline]
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        // Check timeout if configured
        #[cfg(feature = "sleep")]
        if let Some(timeout) = this.timeout.as_pin_mut() {
            if timeout.poll(cx).is_ready() {
                let error = LongRunningError::Timeout {
                    max_duration: this.max_duration.unwrap(),
                };

                return Poll::Ready(Err(error));
            }
        }
        if this.task.is_cancelled() {
            return Poll::Ready(Err(LongRunningError::Cancelled));
        }

        // Poll the inner future
        match this.future.poll(cx) {
            Poll::Ready(output) => Poll::Ready(Ok(output)),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl<F: Future> fmt::Debug for LongRunningFuture<F> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LongRunningFuture")
            .field("future", &"<future>")
            .field("max_duration", &self.max_duration)
            .finish()
    }
}
