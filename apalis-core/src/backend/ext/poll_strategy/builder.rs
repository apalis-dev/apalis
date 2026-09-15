use std::{
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};

use futures_core::Stream;

#[cfg(feature = "sleep")]
use crate::backend::ext::poll_strategy::{BackoffConfig, IntervalStrategy};
use crate::backend::ext::poll_strategy::{
    BoxedPollStrategy, PollSnapshot, PollStrategy, StreamStrategy,
};

/// A polling strategy that combines multiple strategies
/// The strategies are polled in the order they were added to the builder
/// In case of multiple strategies being ready at the same time, the first one added will be chosen
#[derive(Clone, Default)]
pub struct Strategy {
    strategies: Arc<std::sync::Mutex<Vec<BoxedPollStrategy>>>,
}

impl Strategy {
    /// Generate a builder for combining strategies
    #[must_use]
    pub fn new() -> Self {
        Self {
            strategies: Default::default(),
        }
    }

    /// Apply a polling strategy to the builder
    /// Strategies are executed in the order they are added, with the first strategy having the highest priority
    /// In case of multiple strategies being ready at the same time, the first one added will be chosen
    #[must_use]
    pub fn apply<S>(self, strategy: S) -> Self
    where
        S: PollStrategy + Send + Sync + 'static,
    {
        self.strategies.lock().unwrap().push(Box::new(strategy));
        self
    }

    /// Apply a stream strategy
    #[must_use]
    pub fn stream<S>(self, stream: S) -> Self
    where
        S: Stream + Unpin + Send + Sync + 'static,
    {
        self.apply(StreamStrategy::new(stream))
    }

    /// Apply an interval strategy
    #[must_use]
    #[cfg(feature = "sleep")]
    pub fn interval(self, period: Duration) -> Self {
        self.apply(IntervalStrategy::new(period))
    }

    /// Apply an interval with backoff
    #[must_use]
    #[cfg(feature = "sleep")]
    pub fn interval_with_backoff(self, period: Duration, backoff: BackoffConfig) -> Self {
        self.apply(IntervalStrategy::new(period).with_backoff(backoff))
    }
}

impl std::fmt::Debug for Strategy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MultiStrategy")
            .field("strategies", &self.strategies.lock().unwrap().len())
            .finish()
    }
}

impl PollStrategy for Strategy {
    fn poll_drive(&mut self, cx: &mut Context<'_>, worker: &PollSnapshot) -> Poll<Option<()>> {
        // Priority order: first-added strategy wins if multiple are ready.
        for strategy in &mut self.strategies.lock().unwrap().iter_mut() {
            if strategy.poll_drive(cx, worker).is_ready() {
                return Poll::Ready(Some(()));
            }
        }
        Poll::Pending
    }
}
