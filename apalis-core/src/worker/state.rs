use std::sync::atomic::{
    AtomicUsize,
    Ordering::{self, Relaxed},
};

use crate::error::{WorkerError, WorkerStateError};

#[derive(Clone, Debug, Default, PartialEq, Eq)]
#[repr(usize)]
pub(super) enum InnerWorkerState {
    #[default]
    Pending,
    Running,
    Paused,
    Stopped,
    Terminated,
}

impl TryFrom<usize> for InnerWorkerState {
    type Error = WorkerError;

    fn try_from(value: usize) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::Pending),
            1 => Ok(Self::Running),
            2 => Ok(Self::Paused),
            3 => Ok(Self::Stopped),
            4 => Ok(Self::Terminated),
            v => Err(WorkerError::StateError(WorkerStateError::InvalidState(
                format!("{v} not a valid state"),
            ))),
        }
    }
}

/// Represents the state of a worker
#[derive(Debug, Default)]
pub(super) struct WorkerState {
    inner: AtomicUsize,
}

impl WorkerState {
    pub(crate) fn load(&self, order: Ordering) -> InnerWorkerState {
        InnerWorkerState::try_from(self.inner.load(order)).expect("Invalid enum value")
    }

    pub(crate) fn store(&self, state: InnerWorkerState, order: Ordering) {
        self.inner.store(state as usize, order);
    }

    pub(crate) fn as_str(&self) -> &str {
        match self.load(Relaxed) {
            InnerWorkerState::Pending => "pending",
            InnerWorkerState::Running => "running",
            InnerWorkerState::Paused => "paused",
            InnerWorkerState::Stopped => "stopped",
            InnerWorkerState::Terminated => "dead",
        }
    }
}
