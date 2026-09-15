use std::{
    collections::VecDeque,
    fmt::Debug,
    ops::{Deref, DerefMut},
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

use futures_channel::mpsc::{self, Receiver};
use futures_timer::Delay;
use futures_util::FutureExt;

pub use crate::backend::persistence::{event::TaskEvent, service::TaskPersistLayer, state::State};
use crate::{backend::future::BoxSyncFuture, task::Task, worker::context::WorkerContext};

mod event;
mod service;
mod sink;
mod state;

/// Persistence layer for registering workers, maintaining their liveness,
/// fetching work, recovering abandoned tasks, and handling task events.
pub trait Persistence: Clone + Send + 'static {
    /// The compact type the upstream backend will use
    type Compact;

    /// The response type to be stored in the db
    type Response;

    /// Error returned by persistence operations.
    type Error: std::error::Error;

    /// Registers the worker with the persistence backend.
    fn register(
        &mut self,
        worker: &WorkerContext,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// Records a heartbeat for the worker.
    fn heartbeat(
        &mut self,
        worker: &WorkerContext,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// Fetches the next batch of tasks available for processing.
    fn fetch_next(
        &mut self,
        worker: &WorkerContext,
    ) -> impl Future<Output = Result<Vec<Task<Self::Compact>>, Self::Error>> + Send;

    /// Re-enqueues tasks that were abandoned by a worker.
    fn reenqueue_abandoned(
        &mut self,
        tasks: Vec<Task<Self::Compact>>,
        worker: &WorkerContext,
    ) -> impl Future<Output = Result<u64, Self::Error>> + Send;

    /// Handles task lifecycle events produced by the worker.
    fn handle_events(
        &mut self,
        events: Vec<TaskEvent<Self::Response>>,
        worker: &WorkerContext,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// Handle tasks being pushed
    fn push_tasks(
        &mut self,
        tasks: Vec<Task<Self::Compact>>,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;
}

/// A backend that drives a [`Persistence`] instance to emit tasks
///
/// This provides a lightweight way to compose a backend without worrying about polling
#[derive(Debug)]
pub struct Persisted<P>
where
    P: Persistence,
{
    inner: P,
    state: State<Task<P::Compact>, P::Error>,
    heartbeat_timer: Option<Delay>,
    receiver: Option<Receiver<TaskEvent<P::Response>>>,
    sink_buffer: Vec<Task<P::Compact>>,
    sink_future: Option<BoxSyncFuture<Result<(), P::Error>>>,
}

impl<P: Clone> Clone for Persisted<P>
where
    P: Persistence,
{
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            state: State::NotBootstrapped,
            heartbeat_timer: None,
            receiver: None,
            sink_buffer: Vec::new(),
            sink_future: None,
        }
    }
}

impl<P> Deref for Persisted<P>
where
    P: Persistence,
{
    type Target = P;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<P> DerefMut for Persisted<P>
where
    P: Persistence,
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

macro_rules! poll_fut_or_return {
    ($fut:expr, $cx:expr) => {
        match $fut.poll_unpin($cx) {
            Poll::Ready(Ok(v)) => v,
            Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
            Poll::Pending => return Poll::Pending,
        }
    };
}

impl<P> Persisted<P>
where
    P: Persistence + Clone,
    P::Compact: Send + 'static,
    P::Response: Send,
{
    /// Creates a new `Persisted` in the `NotBootstrapped` state.
    ///
    /// The worker will register itself with the backend on the first call
    /// to [`poll_ready`](Self::poll_ready).
    pub fn new(inner: P) -> Self {
        Self {
            inner,
            heartbeat_timer: None,
            receiver: None,
            state: State::NotBootstrapped,
            sink_buffer: Vec::new(),
            sink_future: None,
        }
    }

    /// Get the current state
    pub fn state(&self) -> &State<Task<P::Compact>, P::Error> {
        &self.state
    }

    /// Get the a mutable state
    pub fn state_mut(&mut self) -> &mut State<Task<P::Compact>, P::Error> {
        &mut self.state
    }

    /// Drains any pending events from the receiver.
    ///
    /// Returns `None` if there's nothing to process.
    fn try_drain_events(&mut self, _: &mut Context<'_>) -> Option<Vec<TaskEvent<P::Response>>> {
        let receiver = self.receiver.as_mut()?;
        let mut events = Vec::new();
        let mut polls = 0u32;

        while let Ok(event) = receiver.try_recv() {
            polls += 1;
            events.push(event);
        }

        trace!(
            drained = events.len(),
            polls, "try_drain_events: receiver drain complete"
        );

        if events.is_empty() {
            None
        } else {
            debug!(
                count = events.len(),
                "try_drain_events: buffered events ready"
            );
            Some(events)
        }
    }

    /// Build a layer that wraps a TaskPersistLayer
    pub fn layer<C>(&mut self, codec: C, batch_size: usize) -> TaskPersistLayer<C, P::Response> {
        // Currently we only emit Lock and Ack
        let buffer = batch_size * 4;
        let (tx, rx) = mpsc::channel(buffer);
        self.receiver = Some(rx);
        TaskPersistLayer::new(tx, codec)
    }

    /// Drives the provider's internal state machine (bootstrap, heartbeat,
    /// event processing) until it is genuinely ready for [`poll_next`](Self::poll_next)
    /// to be called, or until it must return `Pending`.
    pub fn poll_ready(
        &mut self,
        cx: &mut Context<'_>,
        worker: &WorkerContext,
        heartbeat_interval: Duration,
    ) -> Poll<Result<(), P::Error>> {
        trace!("poll_ready: {:?}", self.state);
        loop {
            match &mut self.state {
                State::NotBootstrapped => {
                    debug!("poll_ready: NotBootstrapped -> registering worker");
                    let mut provider = self.inner.clone();
                    let worker = worker.clone();
                    let fut = async move {
                        provider.register(&worker).await?;
                        Ok(())
                    };
                    self.state = State::RegisterWorker(fut.boxed().into());
                }

                State::RegisterWorker(fut) | State::HeartBeat(fut) => match fut.poll_unpin(cx) {
                    Poll::Pending => {
                        return Poll::Pending;
                    }
                    Poll::Ready(Err(e)) => {
                        warn!(error = ?e, "poll_ready: HeartBeat failed -> Dead");
                        self.state = State::Dead;
                        return Poll::Ready(Err(e));
                    }
                    Poll::Ready(Ok(())) => {
                        debug!(
                            ?heartbeat_interval,
                            "poll_ready: HeartBeat complete, arming heartbeat timer"
                        );
                        worker.heartbeat(cx);
                        self.heartbeat_timer = Some(Delay::new(heartbeat_interval));
                        self.state = State::Ready;
                    }
                },

                State::ProcessEvents(fut) => match fut.poll_unpin(cx) {
                    Poll::Pending => {
                        return Poll::Pending;
                    }
                    Poll::Ready(Err(e)) => {
                        warn!(error = ?e, "poll_ready: ProcessEvents failed -> Dead");
                        self.state = State::Dead;
                        return Poll::Ready(Err(e));
                    }
                    Poll::Ready(Ok(())) => {
                        trace!("poll_ready: ProcessEvents complete -> Ready");
                        self.state = State::Ready;
                    }
                },

                State::Ready => {
                    let heartbeat_due = Pin::new(self.heartbeat_timer.as_mut().unwrap())
                        .poll(cx)
                        .is_ready();

                    trace!(
                        heartbeat_due,
                        ?heartbeat_interval,
                        "poll_ready: State::Ready evaluating heartbeat timer"
                    );

                    if heartbeat_due {
                        debug!("poll_ready: heartbeat due -> dispatching HeartBeat future");
                        let mut provider = self.inner.clone();
                        let worker = worker.clone();
                        let fut = async move {
                            provider.heartbeat(&worker).await?;
                            Ok(())
                        };
                        self.state = State::HeartBeat(fut.boxed().into());
                        continue;
                    }

                    if let Some(events) = self.try_drain_events(cx) {
                        debug!(
                            count = events.len(),
                            "poll_ready: draining events -> dispatching ProcessEvents future"
                        );
                        let mut provider = self.inner.clone();
                        let worker = worker.clone();
                        let fut = async move { provider.handle_events(events, &worker).await };
                        self.state = State::ProcessEvents(fut.boxed().into());
                        continue;
                    }

                    trace!("poll_ready: no heartbeat/events pending -> Ready(Ok)");
                    return Poll::Ready(Ok(()));
                }

                _ => {
                    trace!(state = ?self.state, "poll_ready: state handled elsewhere -> Ready(Ok)");
                    return Poll::Ready(Ok(()));
                }
            }
        }
    }

    /// Pulls the next task from the backend, fetching a fresh batch when the
    /// internal buffer is exhausted.
    ///
    /// Assumes [`poll_ready`](Self::poll_ready) has already driven the state
    /// machine into `State::Ready`; other states fall through to `Pending`
    /// since they're the responsibility of `poll_ready`/`poll_close`.
    #[allow(clippy::type_complexity)]
    pub fn poll_next(
        &mut self,
        cx: &mut Context<'_>,
        worker: &WorkerContext,
    ) -> Poll<Option<Result<Task<P::Compact>, P::Error>>> {
        trace!("poll_next: poll: {state:?}", state = self.state);
        loop {
            match &mut self.state {
                State::Ready => {
                    trace!("poll_next: State::Ready -> dispatching Fetch future");
                    let mut provider = self.inner.clone();
                    let worker = worker.clone();
                    let fut = async move { provider.fetch_next(&worker).await };
                    self.state = State::Fetch(fut.boxed().into());
                }

                State::Fetch(fut) => match fut.poll_unpin(cx) {
                    Poll::Pending => {
                        return Poll::Pending;
                    }
                    Poll::Ready(Ok(tasks)) if tasks.is_empty() => {
                        trace!(
                            "poll_next: fetch_next returned empty batch -> Ready, parking waker"
                        );
                        self.state = State::Ready;
                        worker.register_waker(cx);
                        return Poll::Pending;
                    }
                    Poll::Ready(Ok(tasks)) => {
                        debug!(count = tasks.len(), "poll_next: fetch_next returned batch");
                        self.state = State::Buffered(VecDeque::from(tasks));
                    }
                    Poll::Ready(Err(e)) => {
                        warn!(error = ?e, "poll_next: fetch_next failed -> Dead");
                        self.state = State::Dead;
                        return Poll::Ready(Some(Err(e)));
                    }
                },

                State::Buffered(buffer) => {
                    if let Some(task) = buffer.pop_front() {
                        if buffer.is_empty() {
                            trace!("poll_next: buffer exhausted after pop -> Ready");
                            self.state = State::Ready;
                        }
                        trace!(task_id = ?task.task_id(), "poll_next: yielding task from buffer");
                        return Poll::Ready(Some(Ok(task)));
                    }
                    trace!("poll_next: buffer already empty -> Ready");
                    self.state = State::Ready;
                }

                State::Dead => {
                    debug!("poll_next: State::Dead -> Ready(None)");
                    return Poll::Ready(None);
                }

                // RegisterWorker/HeartBeat/ProcessEvents/NotBootstrapped are
                // poll_ready's responsibility — if we land here, the caller
                // skipped poll_ready or called poll_next while it was still Pending.
                _ => {
                    error!(state = ?self.state, "poll_next: state not owned by poll_next -> Panic");
                    unreachable!("poll_ready should have been called first");
                }
            }
        }
    }

    /// Gracefully winds down the provider: finishes in-flight bootstrap/heartbeat
    /// event futures, re-enqueues any abandoned buffered tasks, then flushes
    /// remaining events before signalling completion.
    pub fn poll_close(
        &mut self,
        cx: &mut Context<'_>,
        worker: &WorkerContext,
    ) -> Poll<Result<(), P::Error>> {
        trace!("poll_close: Start: {:?}", self.state);
        loop {
            match &mut self.state {
                // Let any in-flight future finish before we start forcing drains
                State::RegisterWorker(fut) | State::HeartBeat(fut) => {
                    poll_fut_or_return!(fut, cx);
                    debug!("poll_close: in-flight RegisterWorker/HeartBeat completed");
                    worker.heartbeat(cx);
                    self.state = State::Ready;
                }

                State::ProcessEvents(fut) => {
                    poll_fut_or_return!(fut, cx);
                    trace!("poll_close: ProcessEvents complete -> Ready");
                    self.state = State::Ready;
                }

                State::ReenqueueOrphaned(fut) => {
                    poll_fut_or_return!(fut, cx);
                    debug!("poll_close: ReenqueueOrphaned complete -> Ready");
                    self.state = State::Ready;
                }

                State::Buffered(remaining) => {
                    let mut provider = self.inner.clone();
                    let worker = worker.clone();

                    if !remaining.is_empty() {
                        debug!(
                            count = remaining.len(),
                            "poll_close: re-enqueuing abandoned buffered tasks"
                        );
                        let tasks = Vec::from(std::mem::take(remaining));
                        let fut = async move { provider.reenqueue_abandoned(tasks, &worker).await };
                        self.state = State::ReenqueueOrphaned(fut.boxed().into());
                    } else {
                        trace!("poll_close: buffer already empty -> Ready");
                        self.state = State::Ready;
                    }
                }

                State::Ready => {
                    let mut provider = self.inner.clone();
                    let worker = worker.clone();

                    if let Some(events) = self.try_drain_events(cx) {
                        debug!(
                            count = events.len(),
                            "poll_close: flushing remaining events before shutdown"
                        );
                        let fut = async move { provider.handle_events(events, &worker).await };
                        self.state = State::ProcessEvents(fut.boxed().into());
                        continue;
                    }
                    debug!("poll_close: no remaining work -> Ready(Ok)");
                    return Poll::Ready(Ok(()));
                }

                _ => {
                    trace!("poll_close: unexpected state -> forcing Ready");
                    self.state = State::Ready;
                }
            }
        }
    }
}
