use std::{fmt::Display, str::FromStr};

use apalis_core::{
    backend::{Backend, BackendConfig, TaskSinkError, WireFormatBackend, codec::Codec},
    error::BoxDynError,
    task::{Task, builder::TaskBuilder, task_id::GenerateId},
};
use futures_sink::Sink;
use petgraph::graph::NodeIndex;

use crate::{
    graph::{GraphFlowContext, decode::GraphCodec},
    sequential::WorkflowContext,
};

/// Extension trait for pushing tasks into a workflow
pub trait WorkflowSink<Args>: WireFormatBackend + Backend + Sized
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
        Args: GraphCodec<Self>,
        Args::Error: std::error::Error + Send + Sync + 'static;

    /// Push a step into the workflow sink at the specified index
    ///
    /// This is a helper method for pushing tasks into the workflow sink
    /// with the appropriate workflow context metadata.
    /// Ideally, this should be used internally by the workflow executor
    /// rather than being called directly.
    fn push_step(
        &mut self,
        args: Args,
        index: usize,
    ) -> impl Future<Output = Result<(), TaskSinkError<Self::Error>>> + Send;

    /// Push a node into the workflow sink at the specified index
    ///
    /// This is a helper method for pushing tasks into the workflow sink
    /// with the appropriate Graph flow context metadata.
    /// Ideally, this should be used internally by the Graph executor
    /// rather than being called directly.
    fn push_node(
        &mut self,
        node: Args,
        index: NodeIndex,
    ) -> impl Future<Output = Result<(), TaskSinkError<Self::Error>>> + Send;
}

impl<S: Send, Args: Send, Compact, Err> WorkflowSink<Args> for S
where
    S: Sink<Task<Compact>, Error = Err>
        + Backend<Error = Err>
        + WireFormatBackend<Compact = Compact>
        + BackendConfig
        + Unpin,
    S::Id: GenerateId + Send + Sync + FromStr + Display,
    S::Codec: Codec<Args, Compact = Compact>,
    Err: std::error::Error + Send + Sync + 'static,
    <S::Codec as Codec<Args>>::Error: Into<BoxDynError> + Send + Sync + 'static,
    Compact: Send + 'static,
    <S::Id as FromStr>::Err: std::error::Error + Send + Sync + 'static,
{
    async fn push_start(&mut self, args: Args) -> Result<(), TaskSinkError<Self::Error>> {
        use futures_util::SinkExt;

        let codec = self.codec();
        let task_id = S::Id::generate();
        let compact =
            S::Codec::encode(codec, &args).map_err(|e| TaskSinkError::CodecError(e.into()))?;
        let task = TaskBuilder::new(compact).task_id(task_id.clone()).build();
        self.send(task)
            .await
            .map_err(|e| TaskSinkError::PushError(e))
    }

    async fn start_fan_out(&mut self, args: Args) -> Result<(), TaskSinkError<Self::Error>>
    where
        Args: GraphCodec<Self>,
        Args::Error: std::error::Error + Send + Sync + 'static,
    {
        use futures_util::SinkExt;
        let task_id = S::Id::generate();
        let codec = self.codec();
        let compact = Args::encode(args, codec).map_err(|e| TaskSinkError::CodecError(e.into()))?;
        let task = TaskBuilder::new(compact).task_id(task_id.clone()).build();
        self.send(task)
            .await
            .map_err(|e| TaskSinkError::PushError(e))
    }

    async fn push_step(
        &mut self,
        step: Args,
        index: usize,
    ) -> Result<(), TaskSinkError<Self::Error>> {
        use futures_util::SinkExt;
        let task_id = S::Id::generate();
        let codec = self.codec();
        let compact =
            S::Codec::encode(codec, &step).map_err(|e| TaskSinkError::CodecError(e.into()))?;
        let task = TaskBuilder::new(compact)
            .metadata(&WorkflowContext { step_index: index })
            .task_id(task_id.clone())
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
        use futures_util::SinkExt;
        let task_id = S::Id::generate();
        let codec = self.codec();
        let compact =
            S::Codec::encode(codec, &node).map_err(|e| TaskSinkError::CodecError(e.into()))?;
        let task = TaskBuilder::new(compact)
            .metadata(&GraphFlowContext {
                current_node: index,
                completed_nodes: Default::default(),
                current_position: index.index(),
                is_initial: true,
                node_task_ids: Default::default(),
                prev_node: None,
                root_task_id: Some(task_id.clone()),
            })
            .task_id(task_id.clone())
            .build();
        self.send(task)
            .await
            .map_err(|e| TaskSinkError::PushError(e))
    }
}
