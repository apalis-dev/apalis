//! Polling strategies for backends.
//!
//! This module provides abstractions and implementations for different polling strategies
//! used by backends to determine when to poll for new tasks. Strategies can be
//! combined, customized, and extended to suit various workload requirements.
//!
//! # Features
//!
//! - Trait [`PollStrategy`] for defining custom polling strategies.
//! - Extension trait [`PollStrategyExt`] for ergonomic usage.
//! - [`PollContext`] struct for passing contextual information to strategies.
//! - Boxed trait object type [`BoxedPollStrategy`] for dynamic dispatch.
//! - Built-in strategies and combinators.
//!
//! # Usage
//!
//! Implement the [`PollStrategy`] trait for your custom strategy, or use the provided
//! strategies and combinators. Use [`PollContext`] to access worker state and previous
//! task counts.
//!
//! See submodules for available strategies and builder utilities.
use std::{
    task::{Context, Poll},
    time::{Duration, Instant},
};

mod strategies;
pub use strategies::*;
mod builder;
pub use builder::*;
mod race_next;
pub use race_next::*;

/// A boxed poll strategy
pub type BoxedPollStrategy = Box<dyn PollStrategy + Send + Sync + 'static>;

/// A trait for different polling strategies
/// All strategies can be combined in a race condition
pub trait PollStrategy {
    /// Poll to determine whether the next poll cycle should occur now.
    ///
    /// Returns `Poll::Ready(())` when it's time to poll, registering the
    /// waker via `cx` if not ready.
    ///
    /// # Contract
    /// If this returns `Poll::Pending`, the implementation **must** arrange
    /// for `cx.waker()` to be woken when it becomes ready to poll again.
    fn poll_gate(&mut self, cx: &mut Context<'_>, snapshot: &PollSnapshot) -> Poll<()>;

    /// Signifies a poll was made
    #[allow(unused_variables)]
    fn on_poll(&mut self, snapshot: &PollSnapshot) {}
}

/// Snapshot of the current polling state.
///
/// This is intended to be passed to adaptive polling strategies.
#[derive(Debug, Clone, Copy, Default)]
pub struct PollSnapshot {
    /// Number of consecutive successful polls since the last `Pending`.
    pub consecutive_ready: usize,

    /// Total number of successful polls.
    pub total_ready: u64,

    /// Total number of pending polls.
    pub total_pending: u64,

    /// Duration since the last successful poll.
    pub idle_for: Duration,
}

/// Keeps a record of the current poll status
#[derive(Debug, Default, Clone)]
pub struct PollMetrics {
    consecutive_ready: usize,
    total_ready: u64,
    total_pending: u64,
    last_ready: Option<Instant>,
}

/// The worker's activity classification based on consecutive successful polls.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ActivityState {
    /// No work has been produced since the last `Pending` (or ever).
    #[default]
    Idle,
    /// Exactly one successful poll since the last idle period — just started working.
    WakingUp,
    /// More than one consecutive successful poll — sustained activity.
    Saturated,
}

impl ActivityState {
    #[must_use]
    const fn from_consecutive_ready(consecutive_ready: usize) -> Self {
        match consecutive_ready {
            0 => Self::Idle,
            1 => Self::WakingUp,
            _ => Self::Saturated,
        }
    }
}

impl PollSnapshot {
    /// Create a new polling context.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            consecutive_ready: 0,
            total_ready: 0,
            total_pending: 0,
            idle_for: Duration::from_secs(0),
        }
    }

    /// Total number of polls performed.
    #[must_use]
    pub const fn total_polls(&self) -> u64 {
        self.total_ready + self.total_pending
    }

    /// Returns true if no polls have been performed yet.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.total_polls() == 0
    }

    /// Fraction of polls that returned work.
    ///
    /// Returns `1.0` if no polls have occurred yet.
    #[must_use]
    pub fn hit_rate(&self) -> f64 {
        let total = self.total_polls();

        if total == 0 {
            1.0
        } else {
            self.total_ready as f64 / total as f64
        }
    }

    /// Fraction of polls that returned `Pending`.
    #[must_use]
    pub fn miss_rate(&self) -> f64 {
        1.0 - self.hit_rate()
    }

    /// Returns whether the worker has been idle for at least `duration`.
    #[must_use]
    pub fn idle_for_at_least(&self, duration: Duration) -> bool {
        self.idle_for >= duration
    }

    /// Returns whether the worker has been idle for less than `duration`.
    #[must_use]
    pub fn idle_for_less_than(&self, duration: Duration) -> bool {
        self.idle_for < duration
    }

    /// Returns true if enough consecutive jobs have been received to
    /// consider switching to an aggressive polling strategy.
    #[must_use]
    pub const fn should_poll_aggressively(&self, threshold: usize) -> bool {
        self.consecutive_ready >= threshold
    }

    /// Returns true if the worker should consider backing off after
    /// remaining idle for the given duration.
    #[must_use]
    pub fn should_backoff(&self, max_idle: Duration) -> bool {
        self.consecutive_ready == 0 && self.idle_for >= max_idle
    }

    /// Classify the worker's current activity state.
    #[must_use]
    pub const fn activity(&self) -> ActivityState {
        ActivityState::from_consecutive_ready(self.consecutive_ready)
    }

    /// Returns true if the previous poll produced work (not idle).
    #[must_use]
    pub const fn is_busy(&self) -> bool {
        !matches!(self.activity(), ActivityState::Idle)
    }

    /// Returns true if the worker is currently idle.
    #[must_use]
    pub const fn is_idle(&self) -> bool {
        matches!(self.activity(), ActivityState::Idle)
    }

    /// Returns whether this is the first successful poll after an idle period.
    #[must_use]
    pub const fn is_waking_up(&self) -> bool {
        matches!(self.activity(), ActivityState::WakingUp)
    }

    /// Returns whether the worker has sustained activity.
    #[must_use]
    pub const fn is_saturated(&self) -> bool {
        matches!(self.activity(), ActivityState::Saturated)
    }
}

impl PollMetrics {
    /// Register a ready call response
    pub fn on_ready(&mut self) {
        self.consecutive_ready = self.consecutive_ready.saturating_add(1);
        self.total_ready = self.total_ready.saturating_add(1);
        self.last_ready = Some(Instant::now());
    }

    /// Register a pending poll response
    pub fn on_pending(&mut self) {
        self.consecutive_ready = 0;
        self.total_pending = self.total_pending.saturating_add(1);
    }

    /// Get the snapshot of the current poll status
    #[must_use]
    pub fn snapshot(&self) -> PollSnapshot {
        PollSnapshot {
            consecutive_ready: self.consecutive_ready,
            total_ready: self.total_ready,
            total_pending: self.total_pending,
            idle_for: self
                .last_ready
                .map(|instant| instant.elapsed())
                .unwrap_or_default(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::VecDeque, sync::Arc, time::Duration};

    use futures_channel::mpsc;

    use futures_util::{
        FutureExt, SinkExt, StreamExt,
        lock::Mutex,
        sink,
        stream::{self},
    };

    use crate::{
        error::BoxDynError,
        task::{Task, task_id::RandomId},
        worker::{
            builder::WorkerBuilder, context::WorkerContext, ext::event_listener::EventListenerExt,
        },
    };

    use super::*;

    const ITEMS: u32 = 10;

    #[tokio::test]
    #[cfg(feature = "sleep")]
    async fn basic_strategy_backend() {
        use crate::backend::{dequeue::VecDequeBackend, ext::BackendExt};
        let backoff = BackoffConfig::new(Duration::from_secs(5))
            .with_multiplier(1.5)
            .with_jitter(0.2);
        let interval = IntervalStrategy::new(Duration::from_millis(200)).with_backoff(backoff);

        let mut backend = VecDequeBackend::new().with_poll_strategy(interval);

        for i in 0..ITEMS {
            use crate::task::builder::TaskBuilder;

            backend.send(TaskBuilder::new(i).build()).await.unwrap();
        }

        async fn task(task: u32, ctx: WorkerContext) -> Result<(), BoxDynError> {
            tokio::time::sleep(Duration::from_secs(1)).await;
            if task == ITEMS - 1 {
                tokio::time::sleep(Duration::from_secs(5)).await;
                ctx.stop().unwrap();
                return Err("Worker stopped!")?;
            }
            Ok(())
        }

        let worker = WorkerBuilder::new("rango-tango")
            .backend(backend)
            .on_event(|ctx, ev| {
                println!("On Event = {ev:?} from {}", ctx.name());
            })
            .build(task);
        worker.run().await.unwrap();
    }

    #[tokio::test]
    #[cfg(feature = "sleep")]
    async fn custom_strategy_backend() {
        use crate::backend::{custom::BackendBuilder, ext::BackendExt};

        let memory = Arc::new(Mutex::new(VecDeque::new()));
        let backoff = BackoffConfig::new(Duration::from_secs(5))
            .with_multiplier(1.5)
            .with_jitter(0.2);
        let interval = IntervalStrategy::new(Duration::from_millis(200)).with_backoff(backoff);

        let when_i_am_ready = FutureStrategy::new(move |_| {
            // println!("Waiting to be ready...");
            crate::timer::sleep(Duration::from_millis(1500))
        });

        let (mut tx, rx) = mpsc::channel(1);

        tokio::spawn(async move {
            for i in 0..ITEMS {
                tokio::time::sleep(Duration::from_secs((i) as u64)).await;
                if tx.send(()).await.is_err() {
                    break;
                }
            }
        });

        let strategy = StrategyBuilder::new()
            .apply(when_i_am_ready)
            .apply(interval)
            .apply(StreamStrategy::new(rx))
            .build();

        let mut backend = BackendBuilder::new_with_cfg(())
            .database(memory)
            .fetcher(|db, _, worker| {
                stream::unfold((db.clone(), worker.clone()), |(p, ctx)| async move {
                    let mut db = p.lock().await;
                    let item = db.pop_front();
                    drop(db);
                    if let Some(item) = item {
                        Some((Ok::<_, BoxDynError>(Some(item)), (p, ctx)))
                    } else {
                        Some((
                            Ok::<Option<Task<u32, (), RandomId>>, BoxDynError>(None),
                            (p, ctx),
                        ))
                    }
                })
                .boxed()
            })
            .sink(|db, _| {
                sink::unfold(db.clone(), move |p, item| {
                    async move {
                        let mut db = p.lock().await;
                        db.push_back(item);
                        drop(db);
                        Ok::<_, BoxDynError>(p)
                    }
                    .boxed()
                })
            })
            .build()
            .unwrap()
            .with_poll_strategy(strategy);

        for i in 0..ITEMS {
            use crate::task::builder::TaskBuilder;

            backend.send(TaskBuilder::new(i).build()).await.unwrap();
        }

        async fn task(task: u32, ctx: WorkerContext) -> Result<(), BoxDynError> {
            tokio::time::sleep(Duration::from_secs(1)).await;
            if task == ITEMS - 1 {
                tokio::time::sleep(Duration::from_secs(10)).await;
                ctx.stop().unwrap();
                return Err("Worker stopped!")?;
            }
            Ok(())
        }

        let worker = WorkerBuilder::new("rango-tango")
            .backend(backend)
            .on_event(|ctx, ev| {
                println!("On Event = {ev:?} from {}", ctx.name());
            })
            .build(task);
        worker.run().await.unwrap();
    }
}
