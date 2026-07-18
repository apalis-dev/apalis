use std::task::{Context, Poll};

use crate::backend::poll_strategy::{PollSnapshot, PollStrategy};

mod stream;
pub use stream::*;
#[cfg(feature = "sleep")]
mod interval;
#[cfg(feature = "sleep")]
pub use interval::*;
mod future;
pub use future::*;
#[cfg(feature = "sleep")]
mod backoff;
#[cfg(feature = "sleep")]
pub use backoff::*;

/// A polling strategy that wraps another strategy
/// This is useful for coercing strategies
#[derive(Debug, Clone)]
pub struct WrapperStrategy<S>
where
    S: PollStrategy + Send + 'static,
{
    strategy: S,
}

impl<S> WrapperStrategy<S>
where
    S: PollStrategy + Send + 'static,
{
    /// Create a new WrapperStrategy from a strategy
    pub fn new(strategy: S) -> Self {
        Self { strategy }
    }
}

impl<S> PollStrategy for WrapperStrategy<S>
where
    S: PollStrategy + Send + 'static,
{
    fn poll_gate(&mut self, cx: &mut Context<'_>, worker: &PollSnapshot) -> Poll<()> {
        self.strategy.poll_gate(cx, worker)
    }
}
