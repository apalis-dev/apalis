use std::{
    sync::{Arc, atomic::AtomicU64},
    task::Waker,
};

use apalis_core::{backend::TaskResult, task::task_id::TaskId};
use dashmap::DashMap;

use crate::in_memory::stream::WaitId;

#[derive(Debug, Clone)]
pub struct ResultStore {
    pub(super) results: Arc<DashMap<TaskId, TaskResult<serde_json::Value>>>,
    pub(super) next_id: Arc<AtomicU64>,
    pub(super) wakers: Arc<DashMap<WaitId, Waker>>,
}

impl Default for ResultStore {
    fn default() -> Self {
        Self {
            results: Arc::new(DashMap::new()),
            wakers: Arc::new(DashMap::new()),
            next_id: Default::default(),
        }
    }
}

impl ResultStore {
    pub fn insert(&self, task_id: TaskId, result: TaskResult<serde_json::Value>) {
        self.results.insert(task_id, result);
        self.wake_all();
    }

    pub fn get(&self, task_id: &TaskId) -> Option<TaskResult<serde_json::Value>> {
        self.results.get(task_id).map(|a| a.clone())
    }

    pub fn register_waker(&self, id: WaitId, waker: &Waker) {
        self.wakers.insert(id, waker.clone());
    }

    fn wake_all(&self) {
        let ids: Vec<WaitId> = self.wakers.iter().map(|e| *e.key()).collect();

        for id in ids {
            if let Some((_, waker)) = self.wakers.remove(&id) {
                waker.wake_by_ref();
            }
        }
    }
}
