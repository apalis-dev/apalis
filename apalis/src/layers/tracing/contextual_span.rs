use std::fmt::Display;

use apalis_core::task::{
    Task,
    metadata::{MetadataExt, TracingContext},
};
use tracing::{Level, Span};

use crate::layers::tracing::{DEFAULT_MESSAGE_LEVEL, MakeSpan};

/// A [`Span`]s whose context that was created in a previous operation now used in the current [`Trace`] context.
/// This assumes that [`TracingContext`] was injected into the task during pushing using [`MetadataExt`]
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

impl<Args, Ctx, IdType> MakeSpan<Args, Ctx, IdType> for ContextualTaskSpan
where
    Ctx: MetadataExt<TracingContext>,
    IdType: Display,
{
    fn make_span(&mut self, req: &Task<Args, Ctx, IdType>) -> Span {
        let task_id = req
            .parts
            .task_id
            .as_ref()
            .expect("A task must have an ID")
            .to_string();
        let tracing_ctx = req.parts.ctx.extract().unwrap_or_default();
        let attempt = &req.parts.attempt;
        let span = Span::current();

        macro_rules! make_span {
            ($level:expr) => {
                tracing::span!(
                    parent: span,
                    $level,
                    "task",
                    task_id = task_id,
                    attempt = attempt.current(),
                    trace_id = tracing_ctx.trace_id(),
                    span_id = tracing_ctx.span_id(),
                    trace_flags = tracing_ctx.trace_flags(),
                    trace_state = tracing_ctx.trace_state(),
                )
            };
        }

        match self.level {
            Level::ERROR => make_span!(Level::ERROR),
            Level::WARN => make_span!(Level::WARN),
            Level::INFO => make_span!(Level::INFO),
            Level::DEBUG => make_span!(Level::DEBUG),
            Level::TRACE => make_span!(Level::TRACE),
        }
    }
}
