use std::{
    collections::VecDeque,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll, Waker},
};

use futures_sink::Sink;
use tower_layer::Identity;

use crate::{
    backend::{Backend, codec::IdentityCodec},
    error::BoxDynError,
    task::{Task, task_id::RandomId},
    worker::context::WorkerContext,
};

/// A simple in-memory backend that uses a `VecDeque` to store tasks.
///
/// This backend is primarily intended for testing and demonstration purposes. It does not persist tasks and is not suitable for production use.
#[derive(Debug, Clone)]
pub struct VecDequeBackend<T> {
    queue: Arc<Mutex<VecDeque<Task<T, RandomId>>>>,
    waker: Arc<Mutex<Option<Waker>>>,
}

impl<T> Default for VecDequeBackend<T> {
    fn default() -> Self {
        Self {
            queue: Arc::new(Mutex::new(VecDeque::new())),
            waker: Arc::new(Mutex::new(None)),
        }
    }
}

impl<T> VecDequeBackend<T> {
    /// Create a new `VecDequeBackend` with default capacity.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a new `VecDequeBackend` with a specified capacity for the internal queue.
    #[must_use]
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            queue: Arc::new(Mutex::new(VecDeque::with_capacity(capacity))),
            waker: Arc::new(Mutex::new(None)),
        }
    }
}

/// Error type for VecDequeBackend
#[derive(Debug, thiserror::Error, Clone)]
pub enum VecDequeError {
    /// Error occurred during polling
    #[error("Polling error: {0}")]
    PollError(Arc<BoxDynError>),
    /// Error occurred during sending
    #[error("Sending error: {0}")]
    SendError(Arc<BoxDynError>),
}

impl<T> Backend for VecDequeBackend<T>
where
    T: Send + Clone + 'static,
{
    type Args = T;
    type Id = RandomId;
    type Config = ();
    type Layer = Identity;
    type Error = VecDequeError;
    type Codec = IdentityCodec;
    type Compact = T;

    fn codec(&self) -> &Self::Codec {
        &IdentityCodec
    }

    fn config(&self) -> &Self::Config {
        &()
    }

    fn poll_ready(
        &mut self,
        cx: &mut Context<'_>,
        _worker: &WorkerContext,
    ) -> Poll<Result<(), Self::Error>> {
        if self
            .queue
            .lock()
            .map_err(|e| VecDequeError::PollError(Arc::new(e.to_string().into())))?
            .is_empty()
        {
            *self.waker.lock().unwrap() = Some(cx.waker().clone());
            Poll::Pending
        } else {
            Poll::Ready(Ok(()))
        }
    }

    fn middleware(&self) -> Self::Layer {
        Identity::new()
    }

    fn poll_next(
        &mut self,
        _cx: &mut Context<'_>,
        _worker: &WorkerContext,
    ) -> Poll<Option<Result<Task<Self::Compact, Self::Id>, Self::Error>>> {
        match self
            .queue
            .lock()
            .map_err(|e| VecDequeError::PollError(Arc::new(e.to_string().into())))?
            .pop_front()
        {
            Some(task) => Poll::Ready(Some(Ok(task))),
            None => Poll::Ready(None),
        }
    }

    fn poll_close(
        &mut self,
        cx: &mut Context<'_>,
        worker: &WorkerContext,
    ) -> Poll<Result<(), Self::Error>> {
        if self
            .queue
            .lock()
            .map_err(|e| VecDequeError::PollError(Arc::new(e.to_string().into())))?
            .is_empty()
        {
            Poll::Ready(Ok(()))
        } else {
            self.poll_ready(cx, worker)
        }
    }
}

impl<T> Sink<Task<T, RandomId>> for VecDequeBackend<T>
where
    T: Send + Unpin + 'static,
{
    type Error = VecDequeError;

    fn poll_ready(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn start_send(self: Pin<&mut Self>, item: Task<T, RandomId>) -> Result<(), Self::Error> {
        let this = self.get_mut();

        let mut tasks = this
            .queue
            .lock()
            .map_err(|e| VecDequeError::SendError(Arc::new(e.to_string().into())))?;

        if let Some(ref key) = item.ctx.idempotency_key {
            let exists = tasks.iter().any(|task| {
                task.ctx
                    .idempotency_key
                    .as_ref()
                    .map(|existing| existing == key)
                    .unwrap_or(false)
            });

            if exists {
                return Ok(());
            }
        }

        tasks.push_back(item);

        if let Some(waker) = this.waker.lock().unwrap().take() {
            waker.wake();
        }

        Ok(())
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
}
