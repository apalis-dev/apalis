use std::{
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

use futures_timer::Delay;

use crate::backend::ext::poll_strategy::{
    BackoffConfig, BackoffStrategy, PollSnapshot, PollStrategy,
};

/// Interval-based polling strategy with optional backoff
#[derive(Debug)]
pub struct IntervalStrategy {
    poll_interval: Duration,
    delay: Option<futures_timer::Delay>,
}

impl Clone for IntervalStrategy {
    fn clone(&self) -> Self {
        Self {
            poll_interval: self.poll_interval,
            delay: None,
        }
    }
}

impl IntervalStrategy {
    /// Create a new IntervalStrategy with the specified interval
    #[must_use]
    pub fn new(poll_interval: Duration) -> Self {
        Self {
            poll_interval,
            delay: None,
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
    fn poll_drive(&mut self, cx: &mut Context<'_>, _ps: &PollSnapshot) -> Poll<Option<()>> {
        let interval = self.poll_interval();

        let delay = self.delay.get_or_insert_with(|| Delay::new(interval));

        match Pin::new(delay).poll(cx) {
            Poll::Ready(()) => {
                self.delay = Some(futures_timer::Delay::new(interval));
                Poll::Ready(Some(()))
            }
            Poll::Pending => Poll::Pending,
        }
    }
}
