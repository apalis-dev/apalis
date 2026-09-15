//! Event definitions and utility types for worker events
//!
//! The `Event` enum defines various events that can occur during the lifecycle of a worker, such as starting, stopping, idling, and encountering errors.

use std::{
    any::Any,
    fmt,
    sync::{Arc, RwLock},
};

use crate::{error::BoxDynError, worker::context::WorkerContext};

/// An event handler for a worker
pub type EventHandlerBuilder =
    Arc<RwLock<Option<Box<dyn Fn(&WorkerContext, &Event) + Send + Sync>>>>;

/// Type alias for an event listener function wrapped in an `Arc`
pub type EventListener = Arc<RawEventListener>;

/// Event listening type
pub(crate) type RawEventListener = Box<dyn Fn(&WorkerContext, &Event) + Send + Sync>;

/// Events emitted by a worker
#[non_exhaustive]
pub enum Event {
    /// Worker started
    Start,
    /// Worker did a heartbeat
    HeartBeat,
    /// A custom event
    Custom(Box<dyn Any + 'static + Send + Sync>),
    /// A result of processing
    Success(Box<dyn Any + 'static + Send + Sync>),
    /// Error encountered during processing, would not stop the worker, but would be logged and sent to the event handler
    Error(Arc<BoxDynError>),
    /// Worker stopped
    Stop,
    /// Worker exited, no further processing
    Exit,
}

impl std::fmt::Debug for Event {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Start => f.write_str("Start"),
            Self::HeartBeat => f.write_str("HeartBeat"),
            Self::Custom(_) => f.write_str("Custom(..)"),
            Self::Success(_) => f.write_str("Success(..)"),
            Self::Error(error) => f.debug_tuple("Error").field(error).finish(),
            Self::Stop => f.write_str("Stop"),
            Self::Exit => f.write_str("Exit"),
        }
    }
}

impl fmt::Display for Event {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let event_description = match &self {
            Self::Start => "worker started".to_owned(),
            Self::Custom(_) => "custom event".to_owned(),
            Self::Error(err) => format!("worker encountered an error: {err}"),
            Self::Stop => "worker stopped".to_owned(),
            Self::HeartBeat => "worker heartbeat".to_owned(),
            Self::Success(_) => "worker completed task successfully".to_owned(),
            Self::Exit => "worker exited".to_owned(),
        };

        write!(f, "{event_description}")
    }
}

impl Event {
    /// If the event is an error, return the error
    #[must_use]
    pub fn as_error(&self) -> Option<Arc<BoxDynError>> {
        match self {
            Self::Error(err) => Some(err.clone()),
            _ => None,
        }
    }

    /// Create a custom event
    #[must_use]
    pub fn custom<T: 'static + Send + Sync>(data: T) -> Self {
        Self::Custom(Box::new(data))
    }
}
