use std::collections::VecDeque;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::task::{Context, Poll, Waker};

use crate::backend::*;
use crate::task::Task;
use crate::worker::context::WorkerContext;
use dashmap::DashMap;
use futures_core::ready;
use futures_sink::Sink;
use futures_util::lock::Mutex;
use futures_util::{FutureExt, SinkExt, StreamExt};

#[derive(Debug)]
struct Inner<B> {
    backend: Mutex<B>,
    wakers: DashMap<usize, Waker>,
    next_key: std::sync::atomic::AtomicUsize,
}

/// Create a cloneable handle to the inner backend where all handles are clone.
#[derive(Debug)]
pub struct Shared<B: WireFormatBackend> {
    inner: Arc<Inner<B>>,
    waker_key: usize,
    codec: B::Codec,
    sink: VecDeque<Task<B::Compact>>,
}

impl<B> Clone for Shared<B>
where
    B::Codec: Clone,
    B: WireFormatBackend,
{
    fn clone(&self) -> Self {
        let inner = self.inner.clone();
        let waker_key = inner
            .next_key
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Self {
            inner,
            waker_key,
            codec: self.codec.clone(),
            sink: Default::default(),
        }
    }
}

impl<B: WireFormatBackend> Shared<B>
where
    B::Codec: Clone,
{
    /// Build a new Sharable backend
    pub fn new(backend: B) -> Self {
        let codec = backend.codec().clone();
        Self {
            inner: Inner {
                backend: Mutex::new(backend),
                next_key: AtomicUsize::new(0),
                wakers: Default::default(),
            }
            .into(),
            waker_key: 0,
            codec,
            sink: VecDeque::new(),
        }
    }
}

impl<B: WireFormatBackend> Shared<B> {
    fn register_waker(inner: &Inner<B>, key: usize, cx: &Context<'_>) {
        let res = &inner.wakers;
        res.insert(key, cx.waker().clone());
    }

    fn wake_others(inner: &Inner<B>, except: usize) {
        let wakers = &inner.wakers;
        wakers.retain(|key, waker| {
            if *key != except {
                waker.wake_by_ref();
            }
            true
        });
    }
}

impl<B> Backend for Shared<B>
where
    B: Backend + WireFormatBackend,
{
    type Task = B::Task;
    type Error = B::Error;

    fn poll_ready(
        &mut self,
        cx: &mut Context<'_>,
        worker: &WorkerContext,
    ) -> Poll<Result<(), Self::Error>> {
        if let Some(mut guard) = self.inner.backend.try_lock() {
            let result = guard.poll_ready(cx, worker);
            drop(guard);

            if result.is_ready() {
                Self::wake_others(&self.inner, self.waker_key);
            }
            result
        } else {
            Self::register_waker(&self.inner, self.waker_key, cx);
            Poll::Pending
        }
    }

    fn poll_next(
        &mut self,
        cx: &mut Context<'_>,
        worker: &WorkerContext,
    ) -> Poll<Option<Result<Self::Task, Self::Error>>> {
        let inner = &self.inner;

        if let Some(mut guard) = inner.backend.try_lock() {
            let result = guard.poll_next(cx, worker);
            drop(guard);
            Self::wake_others(inner, self.waker_key);
            result
        } else {
            Self::register_waker(inner, self.waker_key, cx);
            Poll::Pending
        }
    }

    fn poll_close(
        &mut self,
        cx: &mut Context<'_>,
        worker: &WorkerContext,
    ) -> Poll<Result<(), Self::Error>> {
        let inner = self.inner.as_ref();

        if let Some(mut guard) = inner.backend.try_lock() {
            let result = guard.poll_close(cx, worker);
            drop(guard);
            if result.is_ready() {
                inner.wakers.remove(&self.waker_key);
                Self::wake_others(inner, self.waker_key);
            } else {
                Self::wake_others(inner, self.waker_key);
            }
            result
        } else {
            Self::register_waker(inner, self.waker_key, cx);
            Poll::Pending
        }
    }
}

impl<B: WireFormatBackend> WireFormatBackend for Shared<B> {
    type Codec = B::Codec;

    type Compact = B::Compact;

    fn codec(&self) -> &Self::Codec {
        &self.codec
    }
}

impl<B, Err> Sink<Task<B::Compact>> for Shared<B>
where
    B: Backend<Error = Err> + Sink<Task<B::Compact>, Error = Err> + Unpin,
    B: WireFormatBackend,
    B::Codec: Unpin,
    B::Compact: Unpin,
{
    type Error = Err;
    fn start_send(self: Pin<&mut Self>, item: Task<B::Compact>) -> Result<(), Self::Error> {
        let this = self.get_mut();

        if let Some(mut guard) = this.inner.backend.try_lock() {
            let result = guard.start_send_unpin(item);
            drop(guard);
            if result.is_ok() {
                Self::wake_others(&this.inner, this.waker_key);
            }
            result
        } else {
            this.sink.push_back(item);
            Ok(())
        }
    }
    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let this = self.get_mut();
        let mut guard = ready!(this.inner.backend.lock().poll_unpin(cx));
        while !this.sink.is_empty() {
            ready!(guard.poll_ready_unpin(cx))?;
            let item = this.sink.pop_front().unwrap();
            guard.start_send_unpin(item)?;
        }

        guard.poll_ready_unpin(cx)
    }
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let mut guard = ready!(self.inner.backend.lock().poll_unpin(cx));
        guard.poll_flush_unpin(cx)
    }
    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let mut guard = ready!(self.inner.backend.lock().poll_unpin(cx));
        guard.poll_close_unpin(cx)
    }
}

// delegate_deref!(Shared<B>, inner);

impl<B> BackendConfig for Shared<B>
where
    B: BackendConfig + WireFormatBackend,
{
    type Id = B::Id;
    type Args = B::Args;
    type Kind = B::Kind;
    type Config = B::Config;
    type Layer = B::Layer;
    fn config(&self) -> &Self::Config {
        unreachable!("Dont call config on shared")
    }
    fn middleware(&mut self, worker: &mut WorkerContext) -> Self::Layer {
        self.inner.backend.try_lock().unwrap().middleware(worker)
    }
}

impl<B> FetchById for Shared<B>
where
    B: FetchById,
    B::Task: Send,
    B: Backend + WireFormatBackend + Send,
    B::Codec: Send,
    B::Compact: Send,
{
    async fn fetch_by_id(
        &mut self,
        task_id: &crate::task::task_id::TaskId,
    ) -> Result<Option<B::Task>, Self::Error> {
        self.inner.backend.lock().await.fetch_by_id(task_id).await
    }
}
impl<B> Update for Shared<B>
where
    B: Update + Send,
    B::Task: Send,
    B: Backend + WireFormatBackend + Send,
    B::Codec: Send,
    B::Compact: Send,
{
    async fn update(&mut self, task: Self::Task) -> Result<(), Self::Error> {
        self.inner.backend.lock().await.update(task).await
    }
}
impl<B> Reschedule for Shared<B>
where
    B: Reschedule,
    B::Task: Send,
    B: Backend + WireFormatBackend + Send,
    B::Codec: Send,
    B::Compact: Send,
{
    async fn reschedule(
        &mut self,
        task: Self::Task,
        wait: std::time::Duration,
    ) -> Result<(), Self::Error> {
        self.inner.backend.lock().await.reschedule(task, wait).await
    }
}
impl<B> Vacuum for Shared<B>
where
    B: Vacuum,
    B: Backend + WireFormatBackend + Send,
    B::Codec: Send,
    B::Compact: Send,
{
    async fn vacuum(&mut self) -> Result<usize, Self::Error> {
        self.inner.backend.lock().await.vacuum().await
    }
}
impl<B> ResumeById for Shared<B>
where
    B: ResumeById,
    B: Backend + WireFormatBackend + Send,
    B::Codec: Send,
    B::Compact: Send,
{
    async fn resume_by_id(&mut self, id: TaskId) -> Result<bool, Self::Error> {
        self.inner.backend.lock().await.resume_by_id(id).await
    }
}
impl<B> ResumeAbandoned for Shared<B>
where
    B: ResumeAbandoned,
    B: Backend + WireFormatBackend + Send,
    B::Codec: Send,
    B::Compact: Send,
{
    async fn resume_abandoned(&mut self) -> Result<usize, Self::Error> {
        self.inner.backend.lock().await.resume_abandoned().await
    }
}
impl<B> RegisterWorker for Shared<B>
where
    B: RegisterWorker,
    B: Backend + WireFormatBackend + Send,
    B::Codec: Send,
    B::Compact: Send,
{
    async fn register_worker(&mut self, worker_id: String) -> Result<(), Self::Error> {
        self.inner
            .backend
            .lock()
            .await
            .register_worker(worker_id)
            .await
    }
}
impl<Output, B> WaitForCompletion<Output> for Shared<B>
where
    B: WaitForCompletion<Output> + Sync + 'static,
    Output: Send + 'static,
    B: Backend + WireFormatBackend + Send,
    B::Codec: Sync,
    B::Codec: Send,
    B::Compact: Send + Sync,
{
    type ResultStream =
        futures_core::stream::BoxStream<'static, Result<TaskResult<Output>, Self::Error>>;
    fn wait_for(&self, task_ids: impl IntoIterator<Item = TaskId>) -> Self::ResultStream {
        let inner = self.inner.clone();
        let task_ids: Vec<_> = task_ids.into_iter().collect();
        futures_util::stream::once(async move {
            let backend = inner.backend.lock().await;
            backend.wait_for(task_ids)
        })
        .flatten()
        .boxed()
    }
    async fn check_status(
        &self,
        task_ids: impl IntoIterator<Item = TaskId> + Send,
    ) -> Result<Vec<TaskResult<Output>>, Self::Error> {
        self.inner.backend.lock().await.check_status(task_ids).await
    }
}

impl<B: WireFormatBackend> Drop for Shared<B> {
    fn drop(&mut self) {
        if let Some((_key, waker)) = self.inner.wakers.remove(&self.waker_key) {
            waker.wake();
        }
    }
}
