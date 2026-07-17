use core::fmt;
use std::{
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
};

use crate::backend::poll_strategy::{PollSnapshot, PollStrategy};
/// A polling strategy that uses a future factory to create futures for polling
#[derive(Clone)]
pub struct FutureStrategy<F> {
    future_factory: Arc<Mutex<dyn Fn(&PollSnapshot) -> F + Send>>,
    current: Option<Pin<Box<F>>>,
}

impl<F> fmt::Debug for FutureStrategy<F> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FutureStrategy")
            .field("future_factory", &"FnMut(WorkerContext, usize) -> F")
            .finish()
    }
}

impl<F> FutureStrategy<F>
where
    F: Future<Output = ()> + Send + 'static,
{
    /// Create a new FutureStrategy from a future factory
    pub fn new<Factory>(factory: Factory) -> Self
    where
        Factory: Fn(&PollSnapshot) -> F + Send + 'static,
    {
        Self {
            future_factory: Arc::new(Mutex::new(factory)),
            current: None,
        }
    }
}

impl<F> PollStrategy for FutureStrategy<F>
where
    F: Future<Output = ()> + Send + Sync + 'static,
{
    fn poll_gate(&mut self, cx: &mut Context<'_>, snapshot: &PollSnapshot) -> Poll<()> {
        if self.current.is_none() {
            let Ok(factory) = (self.future_factory).try_lock() else {
                return Poll::Pending;
            };
            let fut = (factory)(snapshot);
            self.current = Some(Box::pin(fut));
        }

        let fut = self.current.as_mut().unwrap();
        match fut.as_mut().poll(cx) {
            Poll::Ready(()) => {
                self.current = None;
                Poll::Ready(())
            }
            Poll::Pending => Poll::Pending,
        }
    }
}
