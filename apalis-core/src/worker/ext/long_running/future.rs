use crate::worker::ext::long_running::Sender;
use crate::worker::ext::long_running::tracker::{LongRunningError, TaskTrackerToken};
use core::fmt;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

/// A future that is tracked as a task by a [`TaskTracker`].
///
/// The associated [`TaskTracker`] cannot complete until this future is dropped.
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
    pub(super) token: TaskTrackerToken,
    pub(super) sender: Sender<F::Output>,
}

impl<F> Future for LongRunningFuture<F>
where
    F: Future,
    F::Output: Send + 'static,
{
    type Output = Result<(), LongRunningError>;

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

                let _ = this.sender.send(Err(error.clone()));

                return Poll::Ready(Err(error));
            }
        }

        // Poll the inner future
        match this.future.poll(cx) {
            Poll::Ready(output) => {
                let _ = this.sender.send(Ok(output));
                Poll::Ready(Ok(()))
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl<F: Future> fmt::Debug for LongRunningFuture<F> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LongRunningFuture")
            .field("future", &"<future>")
            .field("task_tracker", &self.token)
            .finish()
    }
}
