//! Represents a queue in the backend
//!
//! This module provides the `Queue` struct and related functionality for managing
//! queues in the backend. A queue is identified by its name and is used to group
//! tasks for processing by workers.
//!
//! The `Queue` struct is designed to be lightweight and easily clonable, allowing
//! it to be passed around in various contexts. It uses an `Arc<String>` internally
//! to store the queue name, ensuring efficient memory usage and thread safety.
//!
//! The module also includes an implementation of the `FromRequest` trait, allowing
//! extraction of the queue information from a task context. This is useful for
//! workers that need to know which queue they are processing tasks from.
use std::{str::FromStr, sync::Arc};

use crate::task::{
    Task,
    from_request::FromRequest,
    metadata::{Metadata, MetadataStore},
};

/// Represents a queue in the backend
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Queue(Arc<str>);

impl From<String> for Queue {
    fn from(value: String) -> Self {
        Self(Arc::from(value))
    }
}
impl AsRef<str> for Queue {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl From<&str> for Queue {
    fn from(value: &str) -> Self {
        Self(Arc::from(value))
    }
}

impl FromStr for Queue {
    type Err = std::convert::Infallible;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self(Arc::from(s)))
    }
}

impl std::fmt::Display for Queue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[cfg(feature = "serde")]
impl serde::Serialize for Queue {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&self.0)
    }
}

#[cfg(feature = "serde")]
impl<'de> serde::Deserialize<'de> for Queue {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        Ok(Self(Arc::from(s)))
    }
}

impl<Args> FromRequest<Task<Args>> for Queue
where
    Args: Sync,
{
    type Error = QueueError;

    async fn from_request(req: &Task<Args>) -> Result<Self, Self::Error> {
        let queue = req.queue().cloned().ok_or(QueueError::NotFound)?;
        Ok(queue)
    }
}

/// Errors that can occur when extracting queue information from a task context
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum QueueError {
    /// Queue data not found in task context
    #[error("Queue data not found in task context. This is likely a bug. Please report it.")]
    NotFound,
}

impl Metadata for Queue {
    type Error = QueueError;

    fn extract(store: &MetadataStore) -> Result<Self, Self::Error> {
        store
            .get("queue")
            .map(|s| Self::from(s.as_str()))
            .ok_or(QueueError::NotFound)
    }

    fn inject(&self, map: &mut MetadataStore) -> Result<(), Self::Error> {
        map.insert("queue", self.0.to_string())
            .map_err(|_| QueueError::NotFound)
    }
}
