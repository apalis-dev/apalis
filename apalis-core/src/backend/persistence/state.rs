use std::{collections::VecDeque, fmt::Debug};

use crate::backend::future::BoxSyncFuture;

/// Current lifecycle state of a `Persistence` loop.
#[non_exhaustive]
pub enum State<Task, Error> {
    /// The persistence layer has not yet been initialized.
    NotBootstrapped,

    /// The persistence layer is initialized and ready to perform work.
    Ready,

    /// Registering the worker with the persistence backend.
    RegisterWorker(BoxSyncFuture<Result<(), Error>>),

    /// Sending a heartbeat to keep the worker alive.
    HeartBeat(BoxSyncFuture<Result<(), Error>>),

    /// Processing task lifecycle events.
    ProcessEvents(BoxSyncFuture<Result<(), Error>>),

    /// Fetching the next batch of tasks from the persistence backend.
    Fetch(BoxSyncFuture<Result<Vec<Task>, Error>>),

    /// Re-enqueuing tasks that were abandoned by another worker.
    ReenqueueOrphaned(BoxSyncFuture<Result<u64, Error>>),

    /// Tasks fetched from persistence and buffered for processing.
    Buffered(VecDeque<Task>),

    /// The persistence loop has stopped and will perform no further work.
    Dead,
}

impl<T, E> Debug for State<T, E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotBootstrapped => write!(f, "NotBootstrapped"),
            Self::Ready => write!(f, "Ready"),
            Self::RegisterWorker(_) => write!(f, "RegisterWorker"),
            Self::HeartBeat(_) => write!(f, "HeartBeat"),
            Self::ProcessEvents(_) => write!(f, "ProcessEvents"),
            Self::Fetch(_) => write!(f, "Fetch"),
            Self::ReenqueueOrphaned(_) => write!(f, "ReenqueueOrphaned"),
            Self::Buffered(buffer) => write!(f, "Buffered({} items)", buffer.len()),
            Self::Dead => write!(f, "Dead"),
        }
    }
}
