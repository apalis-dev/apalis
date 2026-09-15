//! A shutdown monitor that can be used to signal shutdown to tasks and workers.
//!
//! ## Overview
//! It provides a future that resolves when a shutdown is initiated, allowing tasks to await shutdown
//! signals and perform cleanup operations.

use std::{
    pin::Pin,
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicBool, Ordering},
    },
    task::{Context, Poll, Waker},
};

use futures_core::Future;

/// A shutdown token that stops execution
#[derive(Clone, Debug)]
pub struct Shutdown {
    inner: Arc<ShutdownCtx>,
}

impl Shutdown {
    /// Create a new shutdown handle
    #[must_use]
    pub fn new() -> Self {
        Self {
            inner: Arc::new(ShutdownCtx::new()),
        }
    }

    /// Set the future to await before shutting down
    pub fn shutdown_after<F: Future>(&self, f: F) -> impl Future<Output = F::Output> + use<F> {
        let handle = self.clone();
        async move {
            let result = f.await;
            handle.start_shutdown();
            result
        }
    }

    /// Register a waker slot to be woken the moment shutdown starts.
    ///
    /// Awaiting [`Shutdown`] itself is not enough for a worker: a worker parked on an idle
    /// backend is only re-polled when its own waker fires, and setting the shutdown flag
    /// does not touch it. Each worker therefore hands its waker slot over here, once, when
    /// the monitor attaches this handle to it.
    ///
    /// The slot is held weakly, so a dropped worker does not keep it alive; dead slots are
    /// pruned when shutdown starts. Registering the same slot twice is a no-op.
    pub(crate) fn register_waker(&self, waker: &Arc<Mutex<Option<Waker>>>) {
        let Ok(mut listeners) = self.inner.listeners.lock() else {
            return;
        };
        let slot = Arc::downgrade(waker);
        if listeners.iter().any(|listener| listener.ptr_eq(&slot)) {
            return;
        }
        listeners.push(slot);
    }
}

impl Default for Shutdown {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug)]
pub(crate) struct ShutdownCtx {
    state: AtomicBool,
    waker: Mutex<Option<Waker>>,
    /// Waker slots of the workers attached to this handle. See [`Shutdown::register_waker`].
    listeners: Mutex<Vec<Weak<Mutex<Option<Waker>>>>>,
}
impl ShutdownCtx {
    fn new() -> Self {
        Self {
            state: AtomicBool::default(),
            waker: Mutex::default(),
            listeners: Mutex::default(),
        }
    }
    fn shutdown(&self) {
        self.state.store(true, Ordering::Relaxed);
        self.wake();
        self.wake_listeners();
    }

    /// Wake every registered worker so it observes the flag, dropping slots whose worker is gone.
    fn wake_listeners(&self) {
        let Ok(mut listeners) = self.listeners.lock() else {
            return;
        };
        listeners.retain(|listener| {
            let Some(slot) = listener.upgrade() else {
                return false;
            };
            if let Ok(waker) = slot.lock() {
                if let Some(waker) = waker.as_ref() {
                    waker.wake_by_ref();
                }
            }
            true
        });
    }

    fn is_shutting_down(&self) -> bool {
        self.state.load(Ordering::Relaxed)
    }

    pub(crate) fn wake(&self) {
        if let Some(waker) = self.waker.lock().unwrap().take() {
            waker.wake();
        }
    }
}

impl Shutdown {
    /// Check if the system is shutting down
    #[must_use]
    pub fn is_shutting_down(&self) -> bool {
        self.inner.is_shutting_down()
    }

    /// Start the shutdown process
    pub fn start_shutdown(&self) {
        self.inner.shutdown()
    }
}

impl Future for Shutdown {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let ctx = &self.inner;
        if ctx.state.load(Ordering::Relaxed) {
            Poll::Ready(())
        } else {
            *ctx.waker.lock().unwrap() = Some(cx.waker().clone());
            Poll::Pending
        }
    }
}
