use std::task::{Context, Poll};

use futures_core::Stream;
use futures_util::StreamExt;

use crate::backend::poll_strategy::{PollSnapshot, PollStrategy};

/// A polling strategy that uses a provided stream
#[derive(Debug, Clone)]
pub struct StreamStrategy<S> {
    stm: S,
}

impl<S> StreamStrategy<S>
where
    S: Stream + Unpin + Send + 'static,
{
    /// Create a new StreamStrategy from a stream
    pub fn new(stm: S) -> Self {
        Self { stm }
    }
}

impl<S: Stream + Unpin> PollStrategy for StreamStrategy<S> {
    fn poll_gate(&mut self, cx: &mut Context<'_>, _: &PollSnapshot) -> Poll<()> {
        self.stm.poll_next_unpin(cx).map(|_| ())
    }
}
