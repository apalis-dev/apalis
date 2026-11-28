//! A task that already carries graph metadata must not be treated as a new
//! entry task when that metadata cannot be read. Only a missing current node
//! means "entry task"; every other `GraphFlowContextError` should reach the
//! worker as-is instead of being replaced by a `DuplicateEntry` error from
//! re-injecting a fresh context.

use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex},
};

use apalis_codec::json::JsonCodec;
use apalis_core::{
    backend::codec::Codec,
    task::{
        builder::TaskBuilder,
        metadata::{Metadata, MetadataStore},
        task_fn::task_fn,
    },
    worker::{
        builder::WorkerBuilder, context::WorkerContext, event::Event,
        ext::event_listener::EventListenerExt,
    },
};
use apalis_workflow::{
    GraphFlow, WorkflowSink, graph::GraphFlowContext, in_memory::InMemoryWorkflow,
};
use futures_util::SinkExt;
use petgraph::graph::NodeIndex;

const CURRENT_NODE_KEY: &str = "apalis_workflow.graph.current_node";
const CURRENT_POSITION_KEY: &str = "apalis_workflow.graph.current_position";
const NODE_TASK_IDS_KEY: &str = "apalis_workflow.graph.node_task_ids";
const IS_INITIAL_KEY: &str = "apalis_workflow.graph.is_initial";

/// What a worker did with a single task.
#[derive(Debug)]
struct Outcome {
    ran: Vec<&'static str>,
    errors: Vec<String>,
}

impl Outcome {
    /// The task must fail with `expected` and must not run any node.
    fn assert_failed_with(&self, expected: &str) {
        assert!(
            self.ran.is_empty(),
            "no node should run, but these did: {:?}",
            self.ran
        );
        assert_eq!(
            self.errors.len(),
            1,
            "expected one error, got {:?}",
            self.errors
        );
        assert!(
            self.errors[0].contains(expected),
            "expected an error containing {expected:?}, got {:?}",
            self.errors[0]
        );
        assert!(
            !self.errors[0].contains("Duplicate entry"),
            "the real error was replaced by a duplicate entry error: {:?}",
            self.errors[0]
        );
    }
}

/// Valid metadata for a task that is already at the middle node of
/// `start -> middle -> end`, written by `GraphFlowContext` itself.
fn mid_workflow_metadata() -> MetadataStore {
    let context = GraphFlowContext {
        prev_node: Some(NodeIndex::new(0)),
        current_node: NodeIndex::new(1),
        completed_nodes: HashSet::from([NodeIndex::new(0)]),
        node_task_ids: HashMap::new(),
        current_position: 1,
        is_initial: false,
        root_task_id: None,
    };
    let mut store = MetadataStore::new();
    context.inject(&mut store).unwrap();
    store
}

/// Runs `start -> middle -> end` on one task. `metadata` of `None` pushes a
/// normal entry task with `push_start`; otherwise the task is sent to the
/// middle node with that metadata. The worker stops on the first error or
/// when `end` finishes.
async fn run(metadata: Option<MetadataStore>) -> Outcome {
    let ran = Arc::new(Mutex::new(Vec::new()));
    let errors = Arc::new(Mutex::new(Vec::new()));

    let graph = GraphFlow::new("malformed-context");
    let r = ran.clone();
    let start = graph.add_node(
        "start",
        task_fn(move |n: u32| {
            let r = r.clone();
            async move {
                r.lock().unwrap().push("start");
                n as usize
            }
        }),
    );
    let r = ran.clone();
    let middle = graph
        .add_node(
            "middle",
            task_fn(move |n: usize| {
                let r = r.clone();
                async move {
                    r.lock().unwrap().push("middle");
                    n + 1
                }
            }),
        )
        .depends_on(&start);
    let r = ran.clone();
    let _end = graph
        .add_node(
            "end",
            task_fn(move |n: usize, worker: WorkerContext| {
                let r = r.clone();
                async move {
                    r.lock().unwrap().push("end");
                    worker.stop().unwrap();
                    n
                }
            }),
        )
        .depends_on(&middle);

    let mut backend = InMemoryWorkflow::create();
    match metadata {
        None => backend.push_start(5u32).await.unwrap(),
        Some(store) => {
            let payload = JsonCodec::<Vec<u8>>::encode(&JsonCodec::default(), &5usize).unwrap();
            backend
                .send(TaskBuilder::new(payload).with_metadata(store).build())
                .await
                .unwrap();
        }
    }

    let e = errors.clone();
    let worker = WorkerBuilder::new("malformed-context")
        .backend(backend)
        .on_event(move |worker, event| {
            if let Event::Error(error) = event {
                e.lock().unwrap().push(error.to_string());
                let _ = worker.stop();
            }
        })
        .build(graph);
    worker.run().await.unwrap();

    let ran = ran.lock().unwrap().clone();
    let errors = errors.lock().unwrap().clone();
    Outcome { ran, errors }
}

#[tokio::test]
async fn task_without_metadata_still_starts_as_entry() {
    let outcome = run(None).await;
    assert_eq!(outcome.ran, ["start", "middle", "end"]);
    assert!(outcome.errors.is_empty(), "{:?}", outcome.errors);
}

#[tokio::test]
async fn valid_mid_workflow_metadata_continues_at_its_node() {
    let outcome = run(Some(mid_workflow_metadata())).await;
    assert_eq!(outcome.ran, ["middle", "end"]);
    assert!(outcome.errors.is_empty(), "{:?}", outcome.errors);
}

#[tokio::test]
async fn missing_current_position_is_reported() {
    let mut store = mid_workflow_metadata();
    store.remove(CURRENT_POSITION_KEY).unwrap();
    run(Some(store))
        .await
        .assert_failed_with(&format!("missing key {CURRENT_POSITION_KEY}"));
}

#[tokio::test]
async fn malformed_node_index_is_reported() {
    let mut store = mid_workflow_metadata();
    // `current_position` is parsed as a `usize`, and its `ParseIntError`
    // converts into `ParseNodeIndex`, the same as the node keys.
    store.replace(CURRENT_POSITION_KEY, "not-a-number").unwrap();
    run(Some(store))
        .await
        .assert_failed_with("could not parse node index");
}

#[tokio::test]
async fn malformed_task_id_is_reported() {
    let mut store = mid_workflow_metadata();
    // An entry without the `node=task_id` delimiter.
    store.replace(NODE_TASK_IDS_KEY, "0").unwrap();
    run(Some(store))
        .await
        .assert_failed_with("could not parse task id");
}

#[tokio::test]
async fn malformed_is_initial_is_reported() {
    let mut store = mid_workflow_metadata();
    store.replace(IS_INITIAL_KEY, "trueish").unwrap();
    run(Some(store))
        .await
        .assert_failed_with(&format!("could not parse {IS_INITIAL_KEY}"));
}

#[test]
fn metadata_keys_match_graph_flow_context() {
    // Guards the key names above against drifting from `GraphFlowContext`.
    let store = mid_workflow_metadata();
    for key in [
        CURRENT_NODE_KEY,
        CURRENT_POSITION_KEY,
        NODE_TASK_IDS_KEY,
        IS_INITIAL_KEY,
    ] {
        assert!(
            store.contains_key(key),
            "GraphFlowContext no longer writes {key}"
        );
    }
}
