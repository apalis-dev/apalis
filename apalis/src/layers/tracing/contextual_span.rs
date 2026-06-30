use std::fmt::Display;

use apalis_core::task::Task;
use tracing::{Level, Span};

#[cfg(feature = "opentelemetry")]
use crate::layers::tracing::OtelTraceContext;
use crate::layers::tracing::{DEFAULT_MESSAGE_LEVEL, MakeSpan};

/// A [`Span`]s whose context that was created in a previous operation now used in the current [`Trace`] context.
/// This assumes that [`TracingContext`] was injected into the task metadata during pushing
///
///
/// [`Span`]: tracing::Span
/// [`Trace`]: super::Trace
#[derive(Debug, Clone)]
pub struct ContextualTaskSpan {
    level: Level,
}

impl ContextualTaskSpan {
    /// Create a new `ContextualTaskSpan`.
    pub fn new() -> Self {
        Self {
            level: DEFAULT_MESSAGE_LEVEL,
        }
    }

    /// Set the [`Level`] used for the [tracing span].
    ///
    /// Defaults to [`Level::DEBUG`].
    ///
    /// [tracing span]: https://docs.rs/tracing/latest/tracing/#spans
    pub fn level(mut self, level: Level) -> Self {
        self.level = level;
        self
    }
}

impl Default for ContextualTaskSpan {
    fn default() -> Self {
        Self::new()
    }
}

impl<Args, Conn, IdType> MakeSpan<Args, Conn, IdType> for ContextualTaskSpan
where
    IdType: Display,
{
    fn make_span(&mut self, req: &Task<Args, Conn, IdType>) -> Span {
        let task_id = req
            .ctx
            .task_id
            .as_ref()
            .expect("A task must have an ID")
            .to_string();
        println!("Fetching");
        #[cfg(feature = "opentelemetry")]
        let tracing_ctx: apalis_core::task::metadata::TracingContext =
            apalis_core::task::metadata::Metadata::extract(&req.ctx.metadata).unwrap_or_default();
        let attempt = &req.ctx.attempt;
        let span = Span::current();

        macro_rules! make_span {
            ($level:expr) => {
                tracing::span!(
                    parent: span,
                    $level,
                    "task",
                    task_id = task_id,
                    attempt = attempt.current(),
                )
            };
        }

        let span = match self.level {
            Level::ERROR => make_span!(Level::ERROR),
            Level::WARN => make_span!(Level::WARN),
            Level::INFO => make_span!(Level::INFO),
            Level::DEBUG => make_span!(Level::DEBUG),
            Level::TRACE => make_span!(Level::TRACE),
        };

        #[cfg(feature = "opentelemetry")]
        OtelTraceContext::from(tracing_ctx).restore(&span);

        span
    }
}
