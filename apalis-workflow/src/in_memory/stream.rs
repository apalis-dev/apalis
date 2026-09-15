use std::{
    collections::HashSet,
    marker::PhantomData,
    pin::Pin,
    sync::{Arc, atomic::Ordering},
    task::{Context, Poll},
};

use apalis_core::{
    backend::{TaskResult, memory::MemoryStorageError},
    task::task_id::TaskId,
};
use futures_util::Stream;
use serde::de::DeserializeOwned;

use crate::in_memory::result_store::ResultStore;

pub(crate) type WaitId = u64;

#[derive(Debug, Clone)]
pub struct WaitForStream<O> {
    task_ids: Vec<TaskId>,
    completed: HashSet<TaskId>,
    store: Arc<ResultStore>,
    id: WaitId,
    _marker: PhantomData<O>,
}

impl<O> WaitForStream<O> {
    pub(super) fn new(task_ids: impl IntoIterator<Item = TaskId>, store: Arc<ResultStore>) -> Self {
        let id = store.next_id.fetch_add(1, Ordering::Relaxed);
        Self {
            task_ids: task_ids.into_iter().collect(),
            completed: HashSet::new(),
            store,
            id,
            _marker: PhantomData,
        }
    }
}

impl<O> Stream for WaitForStream<O>
where
    O: DeserializeOwned + Unpin,
{
    type Item = Result<TaskResult<O>, MemoryStorageError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();

        #[cfg(feature = "tracing")]
        tracing::trace!(
            completed = ?this.completed.len(),
            results = ?&this.store.results.len(),
            wakers = ?&this.store.wakers.len(),
            "Checking the results",
        );
        for task_id in &this.task_ids {
            if this.completed.contains(task_id) {
                continue;
            }

            if let Some(value) = this.store.get(task_id) {
                this.completed.insert(task_id.clone());
                let decoded = value
                    .result
                    .map(|a| O::deserialize(a).map_err(|e| e.to_string()))
                    .map_err(|e| MemoryStorageError::Other(e.into()))?;

                return Poll::Ready(Some(Ok(TaskResult {
                    task_id: value.task_id,
                    attempt: value.attempt,
                    status: value.status,
                    result: decoded,
                })));
            }
        }

        if this.completed.len() == this.task_ids.len() {
            return Poll::Ready(None);
        }
        this.store.register_waker(this.id, cx.waker());
        Poll::Pending
    }
}
