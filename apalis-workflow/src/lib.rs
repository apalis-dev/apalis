#![cfg_attr(docsrs, feature(doc_cfg))]
#![doc = include_str!("../README.md")]
#![warn(
    missing_debug_implementations,
    missing_docs,
    rust_2018_idioms,
    unreachable_pub
)]

use apalis_core::{error::BoxDynError, task::Task};

use crate::sequential::router::{GoTo, StepResult};

type BoxedService<Input, Output> = tower::util::BoxCloneSyncService<Input, Output, BoxDynError>;
type SteppedService<Compact, Ctx, IdType> =
    BoxedService<Task<Compact, Ctx, IdType>, GoTo<StepResult<Compact, IdType>>>;

type DagService<Compact, Ctx, IdType> = BoxedService<Task<Compact, Ctx, IdType>, Compact>;

/// combinator for chaining multiple workflows.
pub mod composite;
/// utilities for directed acyclic graph workflows.
pub mod dag;
mod id_generator;
/// utilities for workflow steps.
pub mod sequential;
/// utilities for workflow sinks.
pub mod sink;

pub use {
    dag::DagFlow, dag::executor::DagExecutor, sequential::workflow::Workflow, sink::WorkflowSink,
};

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, time::Duration};

    use apalis_core::{
        task::{
            builder::TaskBuilder,
            task_id::{RandomId, TaskId},
        },
        task_fn::task_fn,
        worker::{
            builder::WorkerBuilder, context::WorkerContext, event::Event,
            ext::event_listener::EventListenerExt,
        },
    };
    use apalis_file_storage::JsonStorage;
    use futures::SinkExt;
    use serde_json::Value;

    use crate::sequential::{AndThen, repeat_until::RepeaterState, workflow::Workflow};

    use super::*;

    #[tokio::test]
    async fn basic_workflow() {
        let workflow = Workflow::new("and-then-workflow")
            .and_then(async |input: u32| (input) as usize)
            .delay_for(Duration::from_secs(1))
            .and_then(async |input: usize| (input) as usize)
            .delay_for(Duration::from_secs(1))
            .delay_with(|_| Duration::from_secs(1))
            .repeat_until(|res: usize, state: RepeaterState<RandomId>| async move {
                println!("Iteration {}: got result {}", state.iterations(), res);
                // Repeat until we have iterated 3 times
                // Of course, in a real-world scenario, the condition would be based on `res`
                if state.iterations() < 3 {
                    None
                } else {
                    Some(res)
                }
            })
            .add_step(AndThen::new(task_fn(async |input: usize| {
                Ok::<_, BoxDynError>(input.to_string())
            })))
            .and_then(async |input: String, _task_id: TaskId<_>| input.parse::<usize>())
            .and_then(async |res: usize| {
                Ok::<_, BoxDynError>((0..res).enumerate().collect::<HashMap<_, _>>())
            })
            .filter_map(async |(index, input): (usize, usize)| {
                if input % 2 == 0 {
                    Some(index.to_string())
                } else {
                    None
                }
            })
            .fold(
                async move |(acc, item): (usize, String), _wrk: WorkerContext| {
                    println!("Folding item {item} with acc {acc}");
                    let item = item.parse::<usize>().unwrap();
                    let acc = acc + item;
                    acc
                },
            )
            .and_then(async |res: usize, wrk: WorkerContext| {
                wrk.stop().unwrap();
                println!("Completed with {res:?}");
            });

        let mut backend: JsonStorage<Value> = JsonStorage::new_temp().unwrap();

        backend
            .send(TaskBuilder::new(Value::from(17)).build())
            .await
            .unwrap();

        let worker = WorkerBuilder::new("rango-tango")
            .backend(backend)
            .on_event(|ctx, ev| {
                println!("On Event = {ev:?}");
                if matches!(ev, Event::Error(_)) {
                    ctx.stop().unwrap();
                }
            })
            .build(workflow);
        worker.run().await.unwrap();
    }
}
