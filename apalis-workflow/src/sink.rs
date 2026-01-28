use apalis_core::{
    backend::{BackendExt, TaskSinkError, codec::Codec},
    error::BoxDynError,
    task::{Task, builder::TaskBuilder, metadata::MetadataExt, task_id::TaskId},
};
use futures::Sink;
use petgraph::graph::NodeIndex;

use crate::{
    dag::{DagFlowContext, decode::DagCodec},
    id_generator::GenerateId,
    sequential::WorkflowContext,
};

/// Extension trait for pushing tasks into a workflow
pub trait WorkflowSink<Args>: BackendExt + Sized
where
    Self::Codec: Codec<Args, Compact = Self::Compact>,
{
    /// Push a single task into the workflow sink at the start
    fn push_start(
        &mut self,
        args: Args,
    ) -> impl Future<Output = Result<(), TaskSinkError<Self::Error>>> + Send;

    /// Push a single task into the workflow sink at the start
    fn start_fan_out(
        &mut self,
        args: Args,
    ) -> impl Future<Output = Result<(), TaskSinkError<Self::Error>>> + Send
    where
        Args: DagCodec<Self>,
        Args::Error: std::error::Error + Send + Sync + 'static;

    /// Push a step into the workflow sink at the specified index
    ///
    /// This is a helper method for pushing tasks into the workflow sink
    /// with the appropriate workflow context metadata.
    /// Ideally, this should be used internally by the workflow executor
    /// rather than being called directly.
    fn push_step(
        &mut self,
        step: Args,
        index: usize,
    ) -> impl Future<Output = Result<(), TaskSinkError<Self::Error>>> + Send;

    /// Push a node into the workflow sink at the specified index
    ///
    /// This is a helper method for pushing tasks into the workflow sink
    /// with the appropriate DAG flow context metadata.
    /// Ideally, this should be used internally by the DAG executor
    /// rather than being called directly.
    fn push_node(
        &mut self,
        node: Args,
        index: NodeIndex,
    ) -> impl Future<Output = Result<(), TaskSinkError<Self::Error>>> + Send;
}

impl<S: Send, Args: Send, Compact, Err, MetaErr> WorkflowSink<Args> for S
where
    S: Sink<Task<Compact, S::Context, S::IdType>, Error = Err>
        + BackendExt<Error = Err, Compact = Compact>
        + Unpin,
    S::IdType: GenerateId + Send,
    S::Codec: Codec<Args, Compact = Compact>,
    S::Context: MetadataExt<WorkflowContext, Error = MetaErr>
        + MetadataExt<DagFlowContext<S::IdType>, Error = MetaErr>
        + Send,
    Err: std::error::Error + Send + Sync + 'static,
    <S::Codec as Codec<Args>>::Error: Into<BoxDynError> + Send + Sync + 'static,
    MetaErr: Into<BoxDynError> + Send + Sync + 'static,
    Compact: Send + 'static,
{
    async fn push_start(&mut self, args: Args) -> Result<(), TaskSinkError<Self::Error>> {
        use futures::SinkExt;
        let task_id = TaskId::new(S::IdType::generate());
        let compact = S::Codec::encode(&args).map_err(|e| TaskSinkError::CodecError(e.into()))?;
        let task = TaskBuilder::new(compact)
            .with_task_id(task_id.clone())
            .build();
        self.send(task)
            .await
            .map_err(|e| TaskSinkError::PushError(e))
    }

    async fn start_fan_out(&mut self, args: Args) -> Result<(), TaskSinkError<Self::Error>>
    where
        Args: DagCodec<Self>,
        Args::Error: std::error::Error + Send + Sync + 'static,
    {
        use futures::SinkExt;
        let task_id = TaskId::new(S::IdType::generate());

        let compact = Args::encode(args).map_err(|e| TaskSinkError::CodecError(e.into()))?;
        let task = TaskBuilder::new(compact)
            .with_task_id(task_id.clone())
            .build();
        self.send(task)
            .await
            .map_err(|e| TaskSinkError::PushError(e))
    }

    async fn push_step(
        &mut self,
        step: Args,
        index: usize,
    ) -> Result<(), TaskSinkError<Self::Error>> {
        use futures::SinkExt;
        let task_id = TaskId::new(S::IdType::generate());
        let compact = S::Codec::encode(&step).map_err(|e| TaskSinkError::CodecError(e.into()))?;
        let task = TaskBuilder::new(compact)
            .meta(WorkflowContext { step_index: index })
            .with_task_id(task_id.clone())
            .build();
        self.send(task)
            .await
            .map_err(|e| TaskSinkError::PushError(e))
    }

    async fn push_node(
        &mut self,
        node: Args,
        index: NodeIndex,
    ) -> Result<(), TaskSinkError<Self::Error>> {
        use futures::SinkExt;
        let task_id = TaskId::new(S::IdType::generate());
        let compact = S::Codec::encode(&node).map_err(|e| TaskSinkError::CodecError(e.into()))?;
        let task = TaskBuilder::new(compact)
            .meta(DagFlowContext {
                current_node: index,
                completed_nodes: Default::default(),
                current_position: index.index(),
                is_initial: true,
                node_task_ids: Default::default(),
                prev_node: None,
                root_task_id: Some(task_id.clone()),
            })
            .with_task_id(task_id.clone())
            .build();
        self.send(task)
            .await
            .map_err(|e| TaskSinkError::PushError(e))
    }
}
