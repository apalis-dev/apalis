use std::{
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

use crate::backend::poll_strategy::{BackoffConfig, BackoffStrategy, PollSnapshot, PollStrategy};

/// Interval-based polling strategy with optional backoff
#[derive(Debug)]
pub struct IntervalStrategy {
    poll_interval: Duration,
    delay: futures_timer::Delay,
}

impl IntervalStrategy {
    /// Create a new IntervalStrategy with the specified interval
    #[must_use]
    pub fn new(poll_interval: Duration) -> Self {
        Self {
            poll_interval,
            delay: futures_timer::Delay::new(poll_interval),
        }
    }

    /// Get the current polling interval
    #[must_use]
    pub fn poll_interval(&self) -> Duration {
        self.poll_interval
    }

    /// Wrap the IntervalStrategy with a BackoffStrategy
    /// This will apply exponential backoff to the polling interval
    /// based on the provided [`BackoffConfig`].`
    #[must_use]
    pub fn with_backoff(self, config: BackoffConfig) -> BackoffStrategy {
        BackoffStrategy::new(self.poll_interval(), config)
    }
}

impl PollStrategy for IntervalStrategy {
    fn poll_gate(&mut self, cx: &mut Context<'_>, _: &PollSnapshot) -> Poll<()> {
        match Pin::new(&mut self.delay).poll(cx) {
            Poll::Ready(()) => {
                self.delay = futures_timer::Delay::new(self.poll_interval);
                // Wake immediately to register the new delay's waker so it makes progress.
                cx.waker().wake_by_ref();
                Poll::Ready(())
            }
            Poll::Pending => Poll::Pending,
        }
    }
}
