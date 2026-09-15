use std::{
    pin::Pin,
    task::{Context, Poll},
};

use crate::{
    backend::{
        Backend, BackendConfig, WireFormatBackend,
        codec::Codec,
        finalize::{Durable, Ephemeral},
    },
    error::BoxDynError,
    task::{Task, builder::TaskBuilder},
};
use futures_channel::mpsc::SendError;
use futures_core::Stream;
use futures_sink::Sink;
use futures_util::SinkExt;
use futures_util::StreamExt;
use futures_util::stream;

/// Error type for TaskSink operations
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum TaskSinkError<PushError> {
    /// Error occurred while pushing the task
    #[error("Failed to push task: {0}")]
    PushError(#[from] PushError),
    /// Error occurred during encoding/decoding of the task
    #[error("Failed to encode/decode task: {0}")]
    CodecError(BoxDynError),

    /// Error occurred while sending new task
    #[error("Failed to send new task: {0}")]
    SendError(SendError),
}

/// A sink for submitting tasks to a backend.
///
/// `TaskSink` provides two levels of task submission:
///
/// - The [`Sink`] methods ([`start_send`], [`poll_ready`], [`poll_flush`], and
///   [`poll_close`]) provide low-level, poll-based control over submission.
/// - The convenience methods ([`push`], [`push_bulk`], [`push_stream`], [`push_task`],
///   and [`push_all`]) provide asynchronous ways to submit tasks without
///   manually driving the sink.
///
/// # Task types
///
/// `Args` is the type accepted by the backend's high-level submission methods,
/// while `Task<Args>` represents a fully constructed task with its associated
/// task metadata.
///
/// `Kind` identifies the kind of backend being targeted.
///
/// [`start_send`]: Sink::start_send
/// [`poll_ready`]: Sink::poll_ready
/// [`poll_flush`]: Sink::poll_flush
/// [`poll_close`]: Sink::poll_close
/// [`push`]: TaskSink::push
/// [`push_bulk`]: TaskSink::push_bulk
/// [`push_stream`]: TaskSink::push_stream
/// [`push_task`]: TaskSink::push_task
/// [`push_all`]: TaskSink::push_all
pub trait TaskSink<Args, Kind>: Backend {
    /// Begins sending a task to the sink.
    ///
    /// This method is the counterpart to [`Sink::start_send`].
    /// The caller must ensure that the sink is ready to accept the task by
    /// successfully polling [`Sink::poll_ready`] first.
    ///
    /// The task may be buffered internally and may not be persisted until
    /// [`Sink::poll_flush`] is driven to completion.
    fn start_send(self: Pin<&mut Self>, item: Task<Args>)
    -> Result<(), TaskSinkError<Self::Error>>;

    /// Polls the sink until it is ready to accept another task.
    ///
    /// Returns [`Poll::Ready`] when a subsequent call to [`start_send`] may
    /// be made.
    ///
    /// When [`Poll::Pending`] is returned, the sink is not currently ready and
    /// the caller must wait for the provided waker to be notified before
    /// polling again.
    ///
    /// [`start_send`]: Sink::start_send
    fn poll_ready(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), TaskSinkError<Self::Error>>>;

    /// Polls the sink until all previously submitted tasks have been flushed.
    ///
    /// A successful [`Poll::Ready`] indicates that all tasks accepted by the
    /// sink have been flushed to the backend.
    ///
    /// This does not close the sink; additional tasks may be submitted after
    /// a successful flush.
    fn poll_flush(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), TaskSinkError<Self::Error>>>;

    /// Polls the sink until it has been closed.
    ///
    /// Once closed, the sink must no longer accept new tasks.
    ///
    /// Implementations should flush any pending tasks before completing the
    /// close operation.
    fn poll_close(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), TaskSinkError<Self::Error>>>;

    /// Pushes a single task into the backend.
    ///
    /// The returned future completes when the task has been accepted by the
    /// backend.
    fn push(
        &mut self,
        task: Args,
    ) -> impl Future<Output = Result<(), TaskSinkError<Self::Error>>> + Send;

    /// Pushes multiple tasks into the backend.
    ///
    /// Implementations may use a backend-specific bulk operation to submit
    /// the tasks more efficiently than calling [`TaskSink::push`] for each task.
    fn push_bulk(
        &mut self,
        tasks: Vec<Args>,
    ) -> impl Future<Output = Result<(), TaskSinkError<Self::Error>>> + Send;

    /// Pushes tasks from a stream into the backend.
    ///
    /// The stream is consumed until it is exhausted or an error occurs.
    /// Implementations may process the stream incrementally rather than
    /// collecting all tasks before submission.
    fn push_stream(
        &mut self,
        tasks: impl Stream<Item = Args> + Unpin + Send,
    ) -> impl Future<Output = Result<(), TaskSinkError<Self::Error>>> + Send;

    /// Pushes a fully constructed task into the backend.
    ///
    /// Use this method when the task has already been constructed and its
    /// metadata should be preserved rather than generated by the backend.
    fn push_task(
        &mut self,
        task: Task<Args>,
    ) -> impl Future<Output = Result<(), TaskSinkError<Self::Error>>> + Send;

    /// Pushes fully constructed tasks from a stream into the backend.
    ///
    /// The stream is consumed until it is exhausted or an error occurs.
    /// Implementations may process the stream incrementally rather than
    /// collecting all tasks before submission.
    fn push_all(
        &mut self,
        tasks: impl Stream<Item = Task<Args>> + Unpin + Send,
    ) -> impl Future<Output = Result<(), TaskSinkError<Self::Error>>> + Send;
}
impl<Args, S, E, C> TaskSink<Args, Durable> for S
where
    S: Sink<Task<C::Compact>, Error = E>
        + Unpin
        + Backend<Error = E>
        + WireFormatBackend<Codec = C>
        + BackendConfig<Args = Args, Kind = Durable>
        + Send,
    Args: Send,
    C::Compact: Send,
    C: Codec<Args> + Clone + Send + Sync,
    E: Send,
    C::Error: std::error::Error + Send + Sync + 'static,
{
    fn start_send(
        self: Pin<&mut Self>,
        item: Task<Args>,
    ) -> Result<(), TaskSinkError<Self::Error>> {
        let codec = self.codec();
        let task = item.try_map_args(|t| {
            codec
                .encode(&t)
                .map_err(|e| TaskSinkError::CodecError(e.into()))
        })?;
        Sink::start_send(self, task).map_err(|e| TaskSinkError::PushError(e))
    }

    fn poll_ready(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), TaskSinkError<Self::Error>>> {
        Sink::poll_ready(self, cx).map_err(|e| TaskSinkError::PushError(e))
    }

    fn poll_flush(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), TaskSinkError<Self::Error>>> {
        Sink::poll_flush(self, cx).map_err(|e| TaskSinkError::PushError(e))
    }

    fn poll_close(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), TaskSinkError<Self::Error>>> {
        Sink::poll_close(self, cx).map_err(|e| TaskSinkError::PushError(e))
    }
    async fn push(&mut self, task: Args) -> Result<(), TaskSinkError<Self::Error>> {
        use futures_util::SinkExt;
        let encoded = self
            .codec()
            .encode(&task)
            .map_err(|e| TaskSinkError::CodecError(e.into()))?;
        self.send(TaskBuilder::new(encoded).build()).await?;
        Ok(())
    }

    async fn push_bulk(&mut self, tasks: Vec<Args>) -> Result<(), TaskSinkError<Self::Error>> {
        use futures_util::SinkExt;
        let tasks = tasks
            .into_iter()
            .map(TaskBuilder::new)
            .map(|task| {
                task.try_map_args(|t| {
                    self.codec()
                        .encode(&t)
                        .map_err(|e| TaskSinkError::CodecError(e.into()))
                })
                .map(|t| t.build())
            })
            .collect::<Result<Vec<_>, _>>()?;
        self.send_all(&mut stream::iter(tasks.into_iter().map(Ok)))
            .await?;
        Ok(())
    }

    async fn push_stream(
        &mut self,
        tasks: impl Stream<Item = Args> + Unpin + Send,
    ) -> Result<(), TaskSinkError<Self::Error>> {
        let codec = self.codec().clone();
        self.sink_map_err(|e| TaskSinkError::PushError(e))
            .send_all(&mut tasks.map(TaskBuilder::new).map(|task| {
                task.try_map_args(|t| {
                    codec
                        .encode(&t)
                        .map_err(|e| TaskSinkError::CodecError(e.into()))
                })
                .map(|t| t.build())
            }))
            .await
    }

    async fn push_task(&mut self, task: Task<Args>) -> Result<(), TaskSinkError<Self::Error>> {
        use futures_util::SinkExt;
        let codec = self.codec();
        let task = task.try_map_args(|t| {
            codec
                .encode(&t)
                .map_err(|e| TaskSinkError::CodecError(e.into()))
        })?;
        self.sink_map_err(|e| TaskSinkError::PushError(e))
            .send(task)
            .await
    }

    async fn push_all(
        &mut self,
        tasks: impl Stream<Item = Task<Args>> + Unpin + Send,
    ) -> Result<(), TaskSinkError<Self::Error>> {
        use futures_util::SinkExt;
        let codec = self.codec().clone();
        let mut encoded = tasks.map(|task| {
            task.try_map_args(|t| {
                codec
                    .encode(&t)
                    .map_err(|e| TaskSinkError::CodecError(e.into()))
            })
        });
        self.sink_map_err(|e| TaskSinkError::PushError(e))
            .send_all(&mut encoded)
            .await
    }
}

impl<Args, S, E> TaskSink<Args, Ephemeral> for S
where
    S: Sink<Task<Args>, Error = E>
        + Unpin
        + Backend<Error = E>
        + BackendConfig<Args = Args, Kind = Ephemeral>
        + Send,
    Args: Send,
    E: Send,
{
    fn start_send(
        self: Pin<&mut Self>,
        item: Task<Args>,
    ) -> Result<(), TaskSinkError<Self::Error>> {
        Sink::start_send(self, item).map_err(|e| TaskSinkError::PushError(e))
    }
    fn poll_ready(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), TaskSinkError<Self::Error>>> {
        Sink::poll_ready(self, cx).map_err(|e| TaskSinkError::PushError(e))
    }

    fn poll_flush(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), TaskSinkError<Self::Error>>> {
        Sink::poll_flush(self, cx).map_err(|e| TaskSinkError::PushError(e))
    }

    fn poll_close(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), TaskSinkError<Self::Error>>> {
        Sink::poll_close(self, cx).map_err(|e| TaskSinkError::PushError(e))
    }

    async fn push(&mut self, args: Args) -> Result<(), TaskSinkError<Self::Error>> {
        let task = TaskBuilder::new(args).build();
        self.send(task)
            .await
            .map_err(|e| TaskSinkError::PushError(e))
    }

    async fn push_bulk(&mut self, tasks: Vec<Args>) -> Result<(), TaskSinkError<Self::Error>> {
        let tasks = tasks
            .into_iter()
            .map(TaskBuilder::new)
            .map(|t| Ok::<_, E>(t.build()))
            .collect::<Result<Vec<_>, _>>()?;
        self.send_all(&mut stream::iter(tasks.into_iter().map(Ok)))
            .await
            .map_err(|e| TaskSinkError::PushError(e))?;
        Ok(())
    }

    async fn push_stream(
        &mut self,
        tasks: impl Stream<Item = Args> + Unpin + Send,
    ) -> Result<(), TaskSinkError<Self::Error>> {
        self.sink_map_err(|e| TaskSinkError::PushError(e))
            .send_all(&mut tasks.map(TaskBuilder::new).map(|task| Ok(task.build())))
            .await
    }

    async fn push_task(&mut self, task: Task<Args>) -> Result<(), TaskSinkError<Self::Error>> {
        use futures_util::SinkExt;
        self.sink_map_err(|e| TaskSinkError::PushError(e))
            .send(task)
            .await
    }

    async fn push_all(
        &mut self,
        tasks: impl Stream<Item = Task<Args>> + Unpin + Send,
    ) -> Result<(), TaskSinkError<Self::Error>> {
        use futures_util::SinkExt;

        self.sink_map_err(|e| TaskSinkError::PushError(e))
            .send_all(&mut tasks.map(Ok))
            .await
    }
}
