#![cfg_attr(docsrs, feature(doc_cfg))]
#![doc = include_str!("../README.md")]
#![warn(
    missing_debug_implementations,
    missing_docs,
    rust_2018_idioms,
    unreachable_pub
)]

use apalis_core::{error::BoxDynError, task::Task};

use crate::{
    graph::NodeInput,
    sequential::router::{GoTo, StepResponse},
};

type BoxedService<Input, Output> = tower::util::BoxCloneSyncService<Input, Output, BoxDynError>;
type SteppedService<Compact> = BoxedService<Task<Compact>, GoTo<StepResponse>>;

type NodeService<Compact> = BoxedService<Task<NodeInput<Compact>>, (Compact, serde_json::Value)>;

/// combinator for chaining multiple workflows.
pub mod composite;
/// utilities for directed acyclic graph workflows.
pub mod graph;

/// utilities for workflow steps.
pub mod sequential;
/// utilities for workflow sinks.
pub mod sink;

/// In memory backend for running workflows without persistence
pub mod in_memory;

pub use {graph::GraphFlow, sequential::workflow::SteppedFlow, sink::WorkflowSink};

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, num::ParseIntError, time::Duration};

    use apalis_core::{
        backend::TaskSink,
        task::{metadata::Meta, task_id::TaskId},
        worker::{
            builder::WorkerBuilder, context::WorkerContext, event::Event,
            ext::event_listener::EventListenerExt,
        },
    };

    use crate::{
        in_memory::InMemoryWorkflow,
        sequential::{repeat_until::RepeaterState, workflow::SteppedFlow},
    };

    use super::*;

    #[tokio::test]
    async fn basic_workflow() {
        type RepeatUntilState = Meta<RepeaterState>;
        let workflow = SteppedFlow::new("and-then-workflow")
            .and_then(async |input: i32| (input) as usize)
            .delay_for(Duration::from_secs(1))
            .and_then(async |input: usize| (input) as isize)
            .delay_for(Duration::from_secs(1))
            .delay_with(|_| Duration::from_secs(1))
            .repeat_until(|res, state: RepeatUntilState| async move {
                println!("Iteration {}: got result {}", state.iterations(), res);
                if state.iterations() < 3 {
                    Ok::<_, BoxDynError>(None)
                } else {
                    Ok(Some(res))
                }
            })
            .and_then(async |input: isize| Ok::<_, BoxDynError>(input.to_string()))
            .and_then(async |input: String, _task_id: TaskId| input.parse::<usize>())
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
                async move |(acc, item): (u32, String), _wrk: WorkerContext| {
                    println!("Folding item {item} with acc {acc}");
                    let item = item.parse::<u32>()?;
                    let acc = acc + item;
                    Ok::<_, ParseIntError>(acc)
                },
            )
            .and_then(async |res: u32, wrk: WorkerContext| {
                wrk.stop().unwrap();
                println!("Completed with {res:?}");
            });

        let mut backend = InMemoryWorkflow::create();

        backend.push(200).await.unwrap();

        let worker = WorkerBuilder::new("rango-tango")
            .backend(backend)
            .on_event(|_, ev| {
                if matches!(ev, Event::Error(_)) {
                    panic!("{ev}");
                }
            })
            .build(workflow);
        worker.run().await.unwrap();
    }
}
