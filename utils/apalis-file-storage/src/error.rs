use crate::Adapter;
use std::fmt;

/// Error type for [`FileStorage`].
#[derive(thiserror::Error)]
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

impl<A> fmt::Debug for FileStorageError<A>
where
    A: Adapter,
    A::Error: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(err) => f.debug_tuple("Io").field(err).finish(),
            Self::Json(err) => f.debug_tuple("Json").field(err).finish(),
            Self::JobNotFound { line_id } => f
                .debug_struct("JobNotFound")
                .field("line_id", line_id)
                .finish(),
            Self::Parse(msg) => f.debug_tuple("Parse").field(msg).finish(),
            Self::AdapterError(err) => f.debug_tuple("AdapterError").field(err).finish(),
            Self::WouldBlockLock => f.write_str("WouldBlockLock"),
        }
    }
}
