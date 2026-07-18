use std::{
    sync::Arc,
    task::{Context, Poll},
};

use crate::backend::poll_strategy::{BoxedPollStrategy, PollSnapshot, PollStrategy};

/// Builder for composing multiple polling strategies
pub struct StrategyBuilder {
    strategies: Vec<BoxedPollStrategy>,
}

impl std::fmt::Debug for StrategyBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StrategyBuilder")
            .field("strategies", &self.strategies.len())
            .finish()
    }
}

impl Default for StrategyBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl StrategyBuilder {
    /// Create a new StrategyBuilder
    #[must_use]
    pub fn new() -> Self {
        Self {
            strategies: Vec::new(),
        }
    }

    /// Apply a polling strategy to the builder
    /// Strategies are executed in the order they are added, with the first strategy having the highest priority
    /// In case of multiple strategies being ready at the same time, the first one added will be chosen
    #[must_use]
    pub fn apply<S>(mut self, strategy: S) -> Self
    where
        S: PollStrategy + Send + Sync + 'static,
    {
        self.strategies.push(Box::new(strategy));
        self
    }

    /// Build the MultiStrategy from the builder
    /// Consumes the builder and returns a MultiStrategy
    #[must_use]
    pub fn build(self) -> MultiStrategy {
        MultiStrategy {
            strategies: Arc::new(std::sync::Mutex::new(self.strategies)),
        }
    }
}

/// A polling strategy that combines multiple strategies
/// The strategies are polled in the order they were added to the builder
/// In case of multiple strategies being ready at the same time, the first one added will be chosen
#[derive(Clone)]
pub struct MultiStrategy {
    strategies: Arc<std::sync::Mutex<Vec<BoxedPollStrategy>>>,
}

impl std::fmt::Debug for MultiStrategy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MultiStrategy")
            .field("strategies", &self.strategies.lock().unwrap().len())
            .finish()
    }
}

impl PollStrategy for MultiStrategy {
    fn poll_gate(&mut self, cx: &mut Context<'_>, worker: &PollSnapshot) -> Poll<()> {
        // Priority order: first-added strategy wins if multiple are ready.
        for strategy in &mut self.strategies.lock().unwrap().iter_mut() {
            if strategy.poll_gate(cx, worker).is_ready() {
                return Poll::Ready(());
            }
        }
        cx.waker().wake_by_ref();
        Poll::Pending
    }
}
