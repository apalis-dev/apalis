use std::{
    task::{Context, Poll},
    time::Duration,
};

use futures_core::stream::BoxStream;
use futures_util::StreamExt;
use tower_layer::Identity;

use crate::{
    backend::{
        Backend, BackendConfig, FetchById, Filter, ListAllTasks, ListQueues, ListTasks,
        ListWorkers, Metrics, QueueInfo, RegisterWorker, Reschedule, ResumeAbandoned, ResumeById,
        RunningWorker, Statistic, TaskResult, Update, Vacuum, WaitForCompletion, WireFormatBackend,
        finalize::Ephemeral,
    },
    task::{
        Task,
        builder::TaskBuilder,
        task_id::{RandomId, TaskId},
    },
    worker::context::WorkerContext,
};

/// Backend implementation based on VecDeque
pub(crate) mod dequeue;
/// In-memory backend implementation
pub(crate) mod memory;

// A boxed stream of tasks that implements the `Backend` trait
impl<T, E> Backend for BoxStream<'_, Result<T, E>>
where
    E: std::error::Error + Send + Sync + 'static,
{
    type Task = Task<T>;

    type Error = E;

    fn poll_ready(
        &mut self,
        _cx: &mut Context<'_>,
        _worker: &WorkerContext,
    ) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn poll_next(
        &mut self,
        cx: &mut Context<'_>,
        _worker: &WorkerContext,
    ) -> Poll<Option<Result<Self::Task, Self::Error>>> {
        self.poll_next_unpin(cx).map_ok(|t| {
            TaskBuilder::new(t)
                .task_id(TaskId::from_string(RandomId::default()))
                .build()
        })
    }

    fn poll_close(
        &mut self,
        _cx: &mut Context<'_>,
        _worker: &WorkerContext,
    ) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
}

impl<T, E> BackendConfig for BoxStream<'_, Result<T, E>>
where
    E: std::error::Error + Send + Sync + 'static,
{
    type Args = T;
    type Kind = Ephemeral;

    type Id = RandomId;

    type Config = ();

    type Layer = Identity;

    fn config(&self) -> &Self::Config {
        &()
    }

    fn middleware(&mut self, _worker: &mut WorkerContext) -> Self::Layer {
        Identity::new()
    }
}

impl<B: Backend> Backend for &mut B {
    type Task = B::Task;

    type Error = B::Error;

    fn poll_ready(
        &mut self,
        cx: &mut Context<'_>,
        worker: &WorkerContext,
    ) -> Poll<Result<(), Self::Error>> {
        (**self).poll_ready(cx, worker)
    }

    fn poll_next(
        &mut self,
        cx: &mut Context<'_>,
        worker: &WorkerContext,
    ) -> Poll<Option<Result<Self::Task, Self::Error>>> {
        (**self).poll_next(cx, worker)
    }

    fn poll_close(
        &mut self,
        cx: &mut Context<'_>,
        worker: &WorkerContext,
    ) -> Poll<Result<(), Self::Error>> {
        (**self).poll_close(cx, worker)
    }
}

impl<B: BackendConfig> BackendConfig for &mut B {
    type Args = B::Args;
    type Kind = B::Kind;

    type Id = B::Id;

    type Config = B::Config;

    type Layer = B::Layer;

    fn config(&self) -> &Self::Config {
        BackendConfig::config(*self)
    }

    fn middleware(&mut self, worker: &mut WorkerContext) -> Self::Layer {
        BackendConfig::middleware(*self, worker)
    }
}

impl<B: WireFormatBackend> WireFormatBackend for &mut B {
    type Codec = B::Codec;

    type Compact = B::Compact;

    fn codec(&self) -> &Self::Codec {
        WireFormatBackend::codec(*self)
    }
}

impl<B> FetchById for &mut B
where
    B: FetchById,
    B::Task: Send,
    B: Backend + Send,
{
    async fn fetch_by_id(
        &mut self,
        task_id: &crate::task::task_id::TaskId,
    ) -> Result<Option<B::Task>, Self::Error> {
        <B as FetchById>::fetch_by_id(*self, task_id).await
    }
}
impl<B> Update for &mut B
where
    B: Update + Send,
    B::Task: Send,
    B: Backend + Send,
{
    async fn update(&mut self, task: Self::Task) -> Result<(), Self::Error> {
        <B as Update>::update(*self, task).await
    }
}
impl<B> Reschedule for &mut B
where
    B: Reschedule,
    B::Task: Send,
    B: Backend + Send,
{
    async fn reschedule(
        &mut self,
        task: Self::Task,
        wait: std::time::Duration,
    ) -> Result<(), Self::Error> {
        <B as Reschedule>::reschedule(*self, task, wait).await
    }
}
impl<B> Vacuum for &mut B
where
    B: Vacuum,
    B: Backend + Send,
{
    async fn vacuum(&mut self) -> Result<usize, Self::Error> {
        <B as Vacuum>::vacuum(*self).await
    }
    async fn vacuum_before(&mut self, duration: Duration) -> Result<usize, Self::Error> {
        <B as Vacuum>::vacuum_before(*self, duration).await
    }
}
impl<B> ResumeById for &mut B
where
    B: ResumeById,
    B: Backend + Send,
{
    async fn resume_by_id(&mut self, id: TaskId) -> Result<bool, Self::Error> {
        <B as ResumeById>::resume_by_id(*self, id).await
    }
}
impl<B> ResumeAbandoned for &mut B
where
    B: ResumeAbandoned,
    B: Backend + Send,
{
    async fn resume_abandoned(&mut self) -> Result<usize, Self::Error> {
        <B as ResumeAbandoned>::resume_abandoned(*self).await
    }
}
impl<B> RegisterWorker for &mut B
where
    B: RegisterWorker,
    B: Backend + Send,
{
    async fn register_worker(&mut self, worker_id: String) -> Result<(), Self::Error> {
        <B as RegisterWorker>::register_worker(*self, worker_id).await
    }
}
impl<Output, B> WaitForCompletion<Output> for &mut B
where
    B: WaitForCompletion<Output> + Sync,
    Output: 'static,
    B: Backend + Send,
{
    type ResultStream = BoxStream<'static, Result<TaskResult<Output>, Self::Error>>;
    fn wait_for(&mut self, task_ids: impl IntoIterator<Item = TaskId>) -> Self::ResultStream {
        use futures_util::StreamExt;
        let result = <B as WaitForCompletion<Output>>::wait_for(*self, task_ids);
        result.boxed()
    }
    async fn check_status(
        &mut self,
        task_ids: impl IntoIterator<Item = TaskId> + Send,
    ) -> Result<Vec<TaskResult<Output>>, Self::Error> {
        <B as WaitForCompletion<Output>>::check_status(*self, task_ids).await
    }
}
impl<B> ListQueues for &mut B
where
    B: ListQueues + Send + Sync,
    B: Backend + Send,
{
    async fn list_queues(&self) -> Result<Vec<QueueInfo>, Self::Error> {
        <B as ListQueues>::list_queues(*self).await
    }
}
impl<B> ListWorkers for &mut B
where
    B: ListWorkers + Send + Sync,
    B: Backend + Send,
{
    async fn list_workers(&self) -> Result<Vec<RunningWorker>, Self::Error> {
        <B as ListWorkers>::list_workers(*self).await
    }
    async fn list_all_workers(&self) -> Result<Vec<RunningWorker>, Self::Error> {
        <B as ListWorkers>::list_all_workers(*self).await
    }
}
impl<B> ListTasks for &mut B
where
    B: ListTasks + Send + Sync,
    B: Backend + Send,
{
    async fn list_tasks(&self, filter: &Filter) -> Result<Vec<Task<Self::Compact>>, Self::Error> {
        <B as ListTasks>::list_tasks(*self, filter).await
    }
}
impl<B> ListAllTasks for &mut B
where
    B: ListAllTasks + Send + Sync,
    B: Backend + Send,
{
    async fn list_all_tasks(
        &self,
        filter: &Filter,
    ) -> Result<Vec<Task<Self::Compact>>, Self::Error> {
        <B as ListAllTasks>::list_all_tasks(*self, filter).await
    }
}
impl<B> Metrics for &mut B
where
    B: Metrics + Send + Sync,
    B: Backend + Send,
{
    async fn global(&self) -> Result<Vec<Statistic>, Self::Error> {
        <B as Metrics>::global(*self).await
    }
    async fn fetch_by_queue(&self) -> Result<Vec<Statistic>, Self::Error> {
        <B as Metrics>::fetch_by_queue(*self).await
    }
}
