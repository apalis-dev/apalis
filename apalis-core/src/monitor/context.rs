//! Monitor context and worker control.
//!
//! This module provides [`MonitorContext`], a shared handle for inspecting
//! and controlling the workers supervised by a monitor.
//!
//! A monitor context provides access to the workers managed by the monitor and
//! exposes operations for controlling their execution, including pausing,
//! resuming, stopping, waking, and cancelling tasks.
//!
//! It also provides a non-blocking [`MonitorContext::shutdown`] operation for
//! initiating shutdown of the monitor.
//!
//! # Worker control
//!
//! Workers are identified by name and can be controlled through the following
//! methods:
//!
//! * [`MonitorContext::pause_worker`] pauses a worker.
//! * [`MonitorContext::resume_worker`] resumes a paused worker.
//! * [`MonitorContext::stop_worker`] stops a worker.
//! * [`MonitorContext::wake_worker`] wakes a worker so it can continue polling.
//!
//! # Task cancellation
//!
//! [`MonitorContext::cancel_task`] cancels a task currently registered with a
//! worker. The task is identified by both its worker name and task ID.
//!
//! # Shutdown
//!
//! [`MonitorContext::shutdown`] initiates monitor shutdown by signalling the
//! monitor's shutdown handle. It does not wait for the supervised workers to
//! finish stopping.
//!
//! # Errors
//!
//! Worker operations return [`WorkerError`] when the requested worker cannot
//! be found or when the requested operation fails. Task cancellation may
//! return a boxed error containing the underlying worker or task error.
use crate::{
    error::{BoxDynError, WorkerError, WorkerStateError},
    monitor::{MonitorError, shutdown::Shutdown},
    worker::context::WorkerContext,
};

/// A handle to the monitor which allows probing
/// Shared context for the monitor, providing access to the shutdown
/// handle and the set of workers it supervises.
#[derive(Debug, Clone)]
pub struct MonitorContext {
    /// Handle used to signal and coordinate shutdown of the monitor.
    pub(super) shutdown: Shutdown,
    /// The workers currently managed by this monitor.
    pub(super) workers: Vec<WorkerContext>,
}

impl MonitorContext {
    /// Returns a reference to all workers managed by this monitor.
    #[must_use]
    pub fn workers(&self) -> &Vec<WorkerContext> {
        &self.workers
    }

    /// Initiates a shutdown of the monitor.
    ///
    /// This signals the shutdown handle but does not block waiting
    /// for workers to finish.
    pub fn shutdown(&self) -> Result<(), MonitorError> {
        self.shutdown.start_shutdown();
        Ok(())
    }

    /// Resumes the worker with the given `name`.
    ///
    /// # Errors
    /// Returns [`WorkerError`] if no worker with `name` exists, or if
    /// the resume itself fails.
    pub fn resume_worker(&self, name: &str) -> Result<(), WorkerError> {
        self.get_worker(name)?.resume()
    }

    /// Pauses the worker with the given `name`.
    ///
    /// # Errors
    /// Returns [`WorkerError`] if no worker with `name` exists, or if
    /// pausing fails.
    pub fn pause_worker(&self, name: &str) -> Result<(), WorkerError> {
        self.get_worker(name)?.pause()
    }

    /// Stops the worker with the given `name`.
    ///
    /// # Errors
    /// Returns [`WorkerError`] if no worker with `name` exists, or if
    /// stopping fails.
    pub fn stop_worker(&self, name: &str) -> Result<(), WorkerError> {
        self.get_worker(name)?.stop()
    }

    /// Wakes the worker with the given `name`.
    ///
    /// # Errors
    /// Returns [`WorkerError`] if no worker with `name` exists.
    pub fn wake_worker(&self, name: &str) -> Result<(), WorkerError> {
        self.get_worker(name)?.wake();
        Ok(())
    }

    /// Looks up a worker by name.
    ///
    /// # Errors
    /// Returns [`WorkerError::StateError`] if no worker with `name`
    /// exists among the managed workers.
    pub fn get_worker(&self, name: &str) -> Result<&WorkerContext, WorkerError> {
        self.workers
            .iter()
            .find(|w| w.name() == name)
            .ok_or_else(|| {
                WorkerError::StateError(WorkerStateError::InvalidState("NOT_FOUND".to_owned()))
            })
    }

    /// Attempt to cancel a task
    pub fn cancel_task(&self, worker: &str, task_id: &str) -> Result<(), BoxDynError> {
        let worker = self.get_worker(worker)?;
        let context = worker.get_task(task_id)?;
        worker.cancel_task(&context)?;
        Ok(())
    }
}
