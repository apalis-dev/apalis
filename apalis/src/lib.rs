#![cfg_attr(docsrs, feature(doc_cfg))]
#![doc = include_str!("../README.md")]
//! ## Feature flags
#![cfg_attr(
    feature = "docsrs",
    cfg_attr(doc, doc = ::document_features::document_features!())
)]
//!
//! [`Service`]: https://docs.rs/tower/latest/tower/trait.Service.html
//! [`tower`]: https://crates.io/crates/tower
//! [`tower-http`]: https://crates.io/crates/tower-http
//! [`Layer`]: https://docs.rs/tower/latest/tower/trait.Layer.html
//! [`Stream`]: https://docs.rs/futures/latest/futures/stream/trait.Stream.html
#![warn(
    missing_debug_implementations,
    missing_docs,
    rust_2018_idioms,
    unreachable_pub
)]
/// apalis fully supports middleware via [`Layer`](https://docs.rs/tower/latest/tower/trait.Layer.html)
pub mod layers;

/// Common imports
pub mod prelude {
    pub use crate::layers::WorkerBuilderExt;
    #[cfg(feature = "retry")]
    pub use crate::layers::retry::{
        BackoffRetryPolicy, FromTaskConfigPolicy, RetryIfPolicy, RetryPolicy,
    };
    pub use apalis_core::{
        backend::{
            Backend, BackendExt, Expose, FetchById, Filter, ListAllTasks, ListQueues, ListTasks,
            ListWorkers, Metrics, QueueInfo, RegisterWorker, Reschedule, ResumeAbandoned,
            ResumeById, RunningWorker, StatType, Statistic, TaskResult, TaskSink, TaskSinkError,
            TaskStream, Update, WaitForCompletion,
        },
        backend::{codec::*, custom::*, memory::*, pipe::*, poll_strategy::*, shared::*},
        error::*,
        layers::*,
        monitor::{ExitError, Monitor, MonitorError, MonitoredWorkerError, shutdown::Shutdown},
        task::ExecutionContext,
        task::Task,
        task::attempt::Attempt,
        task::builder::TaskBuilder,
        task::data::{AddExtension, Data, MissingDataError},
        task::extensions::Extensions,
        task::metadata::{Meta, Metadata, MetadataError, MetadataStore},
        task::status::Status,
        task::task_id::RandomId,
        task::task_id::TaskId,
        task::task_id::TaskIdError,
        task_fn::{FromRequest, IntoResponse, TaskFn, task_fn},
        worker::builder::*,
        worker::ext::{
            ack::*, circuit_breaker::*, event_listener::*, long_running::*, parallelize::*,
        },
        worker::{Worker, context::WorkerContext, event::Event},
    };
}
