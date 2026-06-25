use crate::Adapter;

/// Error type for [`FileStorage`].
#[derive(Debug, thiserror::Error)]
pub enum FileStorageError<A: Adapter> {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("job at line_id: {line_id} not found")]
    JobNotFound { line_id: usize },

    #[error("parse error: {0}")]
    Parse(String),

    #[error("Adapter error: {0}")]
    AdapterError(A::Error),

    #[error("Lock would block")]
    WouldBlockLock,
}
