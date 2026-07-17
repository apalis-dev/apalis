use std::{
    sync::atomic::{AtomicU64, Ordering},
    task::{Context, Poll},
    time::Duration,
};

use futures_util::FutureExt;

use crate::backend::poll_strategy::{PollSnapshot, PollStrategy};

// Simple PRNG state for jitter (thread-safe)
static JITTER_STATE: AtomicU64 = AtomicU64::new(1);

/// A polling strategy that applies exponential backoff to an inner interval strategy
#[derive(Debug)]
pub struct BackoffStrategy {
    default_delay: Duration,
    current_delay: Duration,
    backoff_config: BackoffConfig,
    delay: futures_timer::Delay,
}

impl BackoffStrategy {
    /// Create a new BackoffStrategy with the given BackoffConfig
    #[must_use]
    pub fn new(default_delay: Duration, config: BackoffConfig) -> Self {
        Self {
            default_delay,
            current_delay: default_delay,
            backoff_config: config,
            delay: futures_timer::Delay::new(default_delay),
        }
    }
}

impl PollStrategy for BackoffStrategy {
    fn poll_gate(&mut self, cx: &mut Context<'_>, _: &PollSnapshot) -> Poll<()> {
        self.delay.poll_unpin(cx)
    }

    fn on_poll(&mut self, ctx: &PollSnapshot) {
        self.current_delay = if ctx.is_idle() {
            self.backoff_config
                .next_delay(self.default_delay, self.current_delay, true)
        } else {
            self.default_delay
        };

        self.delay = futures_timer::Delay::new(self.current_delay);
    }
}

/// Backoff configuration for strategies
#[derive(Debug, Clone)]
pub struct BackoffConfig {
    max_delay: Duration,
    multiplier: f64,
    jitter_factor: f64, // 0.0 to 1.0
}

impl Default for BackoffConfig {
    fn default() -> Self {
        Self {
            max_delay: Duration::from_secs(60),
            multiplier: 2.0,
            jitter_factor: 0.1,
        }
    }
}

impl BackoffConfig {
    /// Create a new BackoffConfig with the specified maximum delay
    #[must_use]
    pub fn new(max: Duration) -> Self {
        Self {
            max_delay: max,
            ..Default::default()
        }
    }

    /// Set the multiplier for exponential backoff
    #[must_use]
    pub fn with_multiplier(mut self, multiplier: f64) -> Self {
        self.multiplier = multiplier;
        self
    }

    /// Set the jitter factor (0.0 to 1.0) for randomizing delays
    #[must_use]
    pub fn with_jitter(mut self, jitter_factor: f64) -> Self {
        self.jitter_factor = jitter_factor.clamp(0.0, 1.0);
        self
    }

    /// Calculate the next delay with backoff and jitter
    fn next_delay(&self, default_delay: Duration, current_delay: Duration, idle: bool) -> Duration {
        let base_delay = if idle {
            let next = Duration::from_secs_f64(current_delay.as_secs_f64() * self.multiplier);
            next.min(self.max_delay)
        } else {
            default_delay
        };

        if self.jitter_factor > 0.0 {
            let mut state = JITTER_STATE.load(Ordering::Relaxed);
            state = state.wrapping_mul(1103515245).wrapping_add(12345);
            JITTER_STATE.store(state, Ordering::Relaxed);

            let normalized = (state as f64) / (u64::MAX as f64);
            let jitter_range = base_delay.as_secs_f64() * self.jitter_factor;
            let jitter = (normalized - 0.5) * 2.0 * jitter_range;
            let jittered = base_delay.as_secs_f64() + jitter;
            Duration::from_secs_f64(jittered.max(0.0))
        } else {
            base_delay
        }
    }
}
