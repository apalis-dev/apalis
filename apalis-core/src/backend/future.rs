use std::{
    fmt::Debug,
    sync::Mutex,
    task::{Context, Poll},
};

use futures_core::future::BoxFuture;

/// Wraps a `!Sync` boxed future so it can live inside a `Sync` container.
/// Safe because we only ever poll it through `&mut self`, which never
/// contends the lock.
pub struct BoxSyncFuture<T>(Mutex<BoxFuture<'static, T>>);

impl<T> Debug for BoxSyncFuture<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BoxSyncFuture { .. }").finish()
    }
}

impl<T> BoxSyncFuture<T> {
    /// Compose a Boxed Future that is Sync
    #[must_use]
    pub fn new(fut: BoxFuture<'static, T>) -> Self {
        Self(Mutex::new(fut))
    }

    /// Call poll directly to avoid pinning twice
    pub fn poll_unpin(&mut self, cx: &mut Context<'_>) -> Poll<T> {
        // get_mut() never actually locks — we already have &mut self.
        self.0.get_mut().unwrap().as_mut().poll(cx)
    }
}

impl<T> From<BoxFuture<'static, T>> for BoxSyncFuture<T> {
    fn from(fut: BoxFuture<'static, T>) -> Self {
        Self::new(fut)
    }
}
