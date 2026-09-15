use std::marker::PhantomData;

use apalis_core::{
    backend::{Backend, BackendConfig, WireFormatBackend},
    error::BoxDynError,
    task::{Task, task_id::GenerateId},
    worker::service::{IntoWorkerService, WorkerService},
};
use futures_sink::Sink;

use crate::{
    sequential::backend::WorkflowBackend,
    sequential::{
        router::WorkflowRouter,
        service::WorkflowService,
        step::{Identity, Layer, Stack, Step},
    },
};

/// A workflow represents a sequence of steps to be executed in order.
#[derive(Debug)]
pub struct SteppedFlow<Start, Current, Backend, T = Identity> {
    pub(crate) inner: T,
    pub(crate) name: String,
    _marker: PhantomData<(Start, Current, Backend)>,
}

impl<Start, Backend> SteppedFlow<Start, Start, Backend> {
    #[allow(missing_docs)]
    #[must_use]
    pub fn new(name: &str) -> Self {
        Self {
            inner: Identity,
            name: name.to_owned(),
            _marker: PhantomData,
        }
    }
}

impl<Start, Cur, B, L> SteppedFlow<Start, Cur, B, L> {
    /// Adds a new step to the workflow pipeline.
    ///
    /// This method should be used with caution, as it allows adding arbitrary steps
    /// and manipulating types. It is recommended to use higher-level abstractions for
    /// common workflow patterns.
    #[must_use]
    pub fn add_step<S, Output>(self, step: S) -> SteppedFlow<Start, Output, B, Stack<S, L>> {
        SteppedFlow {
            inner: Stack::new(step, self.inner),
            name: self.name,
            _marker: PhantomData,
        }
    }

    /// Finalizes the workflow by attaching a root step.
    pub fn finalize<S>(self, root: S) -> SteppedFlow<Start, Cur, B, L::Step>
    where
        S: Step<Cur, B>,
        L: Layer<S>,
        B: Backend + WireFormatBackend,
    {
        SteppedFlow {
            inner: self.inner.layer(root),
            name: self.name,
            _marker: PhantomData,
        }
    }
}

impl<Start, Cur, B, L> SteppedFlow<Start, Cur, B, L>
where
    B: Backend,
{
    /// Builds the workflow by layering the root step.
    pub fn build<N>(self) -> L::Step
    where
        L: Layer<RootStep<N>>,
    {
        let root = RootStep(std::marker::PhantomData);
        self.inner.layer(root)
    }
}

/// The root step of a workflow.
#[derive(Clone, Debug)]
pub struct RootStep<Res>(std::marker::PhantomData<Res>);

impl<Res> Default for RootStep<Res> {
    fn default() -> Self {
        Self(std::marker::PhantomData)
    }
}

impl<Input, Current, B: Backend + WireFormatBackend> Step<Input, B> for RootStep<Current> {
    type Response = Current;
    type Error = BoxDynError;
    fn register(&mut self, _ctx: &mut WorkflowRouter<B>) -> Result<(), BoxDynError> {
        Ok(())
    }
}

impl<Input, Output, Current, B, Compact, L, Err>
    IntoWorkerService<B, WorkflowService<B, Input, Output>> for SteppedFlow<Input, Current, B, L>
where
    B: Backend<Task = Task<Compact>, Error = Err>
        + WireFormatBackend<Compact = Compact>
        + BackendConfig<Args = Input>
        + Send
        + Sync
        + 'static
        + Sink<Task<Compact>, Error = Err>
        + Clone
        + Unpin,
    B::Id: Send + 'static + Default + GenerateId,
    L: Layer<RootStep<Current>>,
    L::Step: Step<Output, B>,
    B::Codec: Clone,
    Err: std::error::Error + Send + Sync + 'static,
    Compact: Send + 'static,
{
    type Task = Task<Compact>;
    type Backend = WorkflowBackend<B>;
    fn into_service(
        self,
        backend: B,
    ) -> WorkerService<Self::Backend, WorkflowService<B, Input, Output>> {
        let mut ctx = WorkflowRouter::<B>::new();

        let mut root = self.finalize(RootStep(std::marker::PhantomData));

        root.inner
            .register(&mut ctx)
            .expect("Failed to register workflow steps");

        WorkerService {
            service: WorkflowService::new(ctx.steps, backend.clone()),
            backend: WorkflowBackend::new(backend),
        }
    }
}
