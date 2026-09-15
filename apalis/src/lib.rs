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
/// Inbuilt middleware build on top of tower's [`Layer`](https://docs.rs/tower/latest/tower/trait.Layer.html)
pub mod layers;

/// Worker configuration utilities allowing easier worker decoration
#[cfg(feature = "config")]
pub mod config;

/// A "prelude" of common imports
pub mod prelude {
    pub use crate::layers::WorkerBuilderExt;
    #[cfg(feature = "retry")]
    pub use crate::layers::retry::{
        BackoffRetryPolicy, FromTaskConfigPolicy, RetryIfPolicy, RetryPolicy,
    };
    pub use apalis_core::{
        backend::{
            Backend, Expose, FetchById, Filter, ListAllTasks, ListQueues, ListTasks, ListWorkers,
            Metrics, QueueInfo, RegisterWorker, Reschedule, ResumeAbandoned, ResumeById,
            RunningWorker, StatType, Statistic, TaskResult, TaskSink, TaskSinkError, Update,
            WaitForCompletion, ext::BackendExt, ext::PollNextArgsError,
        },
        backend::{
            codec::*, custom::*, ext::pipe::*, ext::poll_strategy::*, ext::shared::Shared,
            factory::*, memory::*,
        },
        error::*,
        layers::*,
        monitor::{
            ExitError, Monitor, MonitorError, MonitoredWorkerError, context::MonitorContext,
            shutdown::Shutdown,
        },
        task::{
            ExecutionContext, Task,
            attempt::Attempt,
            builder::TaskBuilder,
            context::{SubTaskFuture, TaskContext, WaitForExecutionFuture},
            data::{AddExtension, Data, MissingDataError},
            extensions::Extensions,
            from_request::FromRequest,
            into_response::IntoResponse,
            metadata::{Meta, Metadata, MetadataError, MetadataStore},
            status::Status,
            task_fn::TaskFn,
            task_fn::task_fn,
            task_id::{RandomId, TaskId, TaskIdError},
        },
        worker::builder::*,
        worker::ext::{
            ack::*, circuit_breaker::*, event_listener::*, long_running::*, parallelize::*,
        },
        worker::{
            Worker, context::WorkerContext, event::Event, lifecycle::*, service::IntoWorkerService,
            service::WorkerService,
        },
    };
}
