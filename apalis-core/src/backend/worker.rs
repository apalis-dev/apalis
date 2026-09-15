use crate::backend::Backend;

/// Allows resuming all abandoned tasks in the backend
pub trait ResumeAbandoned: Backend {
    /// Resume all abandoned tasks
    fn resume_abandoned(&mut self) -> impl Future<Output = Result<usize, Self::Error>> + Send;
}

/// Allows registering a worker with the backend
pub trait RegisterWorker: Backend {
    /// Registers a worker
    fn register_worker(
        &mut self,
        worker_id: String,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;
}

/// A generic filter to choose which workers to include in an operation
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum WorkerFilter {
    /// All workers except the provided one
    AllExcept(String),
    /// Only the provided one
    Only(String),
    /// No filter
    None,
}
