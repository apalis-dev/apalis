use std::{
    convert::Infallible,
    error::Error,
    fmt::Debug,
    future::ready,
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use apalis_codec::json::JsonCodec;
use apalis_core::{
    backend::{codec::Codec, ext::with_codec::WithCodec, memory::MemoryStorage},
    error::BoxDynError,
    task::{Task, builder::TaskBuilder},
};
use apalis_workflow::graph::{
    GraphFlow, NodeInput, decode::GraphCodec, error::GraphFlowError, node::GraphNodeService,
};
use futures_util::{FutureExt, future::poll_fn};
use petgraph::graph::NodeIndex;
use serde::{Serialize, de::DeserializeOwned};
use tower::{Service, service_fn};

type TestBackend = WithCodec<MemoryStorage<Vec<u8>>, JsonCodec<Vec<u8>>>;
type Pair = (u32, u32);
type Twelve = (u32, u32, u32, u32, u32, u32, u32, u32, u32, u32, u32, u32);

fn nested_payload(length: usize) -> Vec<u8> {
    let codec = JsonCodec::<Vec<u8>>::default();
    let elements: Vec<Vec<u8>> = (1..=length)
        .map(|value| codec.encode(&(value as u32)).unwrap())
        .collect();
    codec.encode(&elements).unwrap()
}

async fn call_node<Input>(
    input: NodeInput<Vec<u8>>,
    expected_input: Option<Input>,
) -> Result<(Vec<u8>, serde_json::Value), BoxDynError>
where
    Input: GraphCodec<TestBackend, Error = GraphFlowError>
        + DeserializeOwned
        + Serialize
        + Debug
        + PartialEq
        + 'static,
{
    let calls = Arc::new(AtomicUsize::new(0));
    let handler_calls = calls.clone();
    let expected_calls = usize::from(expected_input.is_some());
    let handler = service_fn(move |request: Task<Input>| {
        handler_calls.fetch_add(1, Ordering::SeqCst);
        if let Some(expected) = &expected_input {
            assert_eq!(&request.args, expected);
        }
        ready(Ok::<u32, Infallible>(99))
    });
    let mut node = GraphNodeService::<_, TestBackend, Input>::new(handler);
    let request = TaskBuilder::new(input)
        .data(JsonCodec::<Vec<u8>>::default())
        .build();

    poll_fn(|cx| node.poll_ready(cx)).await.unwrap();
    let called = catch_unwind(AssertUnwindSafe(|| node.call(request)));
    assert_eq!(calls.load(Ordering::SeqCst), expected_calls);
    assert!(
        called.is_ok(),
        "GraphNodeService::call panicked synchronously"
    );

    let awaited = AssertUnwindSafe(called.unwrap()).catch_unwind().await;
    assert_eq!(calls.load(Ordering::SeqCst), expected_calls);
    assert!(
        awaited.is_ok(),
        "GraphNodeService future panicked when polled"
    );
    awaited.unwrap()
}

macro_rules! wrong_length_test {
    ($name:ident, $input:ty, $expected:expr, $length:expr) => {
        #[tokio::test]
        async fn $name() {
            let result =
                call_node::<$input>(NodeInput::Single(nested_payload($length)), None).await;
            let error = result.unwrap_err();
            assert!(matches!(
                error.downcast_ref::<GraphFlowError>(),
                Some(GraphFlowError::InputCountMismatch { expected, actual })
                    if *expected == $expected && *actual == $length
            ));
        }
    };
}

wrong_length_test!(pair_rejects_zero_elements_without_panicking, Pair, 2, 0);
wrong_length_test!(pair_rejects_one_element_without_panicking, Pair, 2, 1);
wrong_length_test!(pair_rejects_three_elements_without_panicking, Pair, 2, 3);
wrong_length_test!(
    twelve_rejects_zero_elements_without_panicking,
    Twelve,
    12,
    0
);
wrong_length_test!(
    twelve_rejects_eleven_elements_without_panicking,
    Twelve,
    12,
    11
);
wrong_length_test!(
    twelve_rejects_thirteen_elements_without_panicking,
    Twelve,
    12,
    13
);

#[tokio::test]
async fn pair_with_two_encoded_elements_calls_handler_once() {
    let result = call_node(NodeInput::Single(nested_payload(2)), Some((1_u32, 2_u32)))
        .await
        .unwrap();
    assert_eq!(result.0, b"99");
    assert_eq!(result.1, serde_json::json!(99));
}

#[tokio::test]
async fn twelve_with_twelve_encoded_elements_calls_handler_once() {
    let expected: Twelve = (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12);
    let result = call_node(NodeInput::Single(nested_payload(12)), Some(expected))
        .await
        .unwrap();
    assert_eq!(result.0, b"99");
    assert_eq!(result.1, serde_json::json!(99));
}

fn assert_original_json_error(actual: BoxDynError, expected: serde_json::Error) {
    let mut source: &(dyn Error + 'static) = actual.as_ref();
    let json_error = loop {
        if let Some(error) = source.downcast_ref::<serde_json::Error>() {
            break error;
        }
        source = source.source().expect("original serde_json error missing");
    };
    assert_eq!(json_error.to_string(), expected.to_string());
    assert_eq!(json_error.classify(), expected.classify());
    assert_eq!(json_error.line(), expected.line());
    assert_eq!(json_error.column(), expected.column());
}

#[tokio::test]
async fn corrupt_outer_payload_returns_original_json_error() {
    let codec = JsonCodec::<Vec<u8>>::default();
    let payload = b"{".to_vec();
    let expected = <JsonCodec as Codec<Vec<Vec<u8>>>>::decode(&codec, &payload).unwrap_err();
    let actual = call_node::<Pair>(NodeInput::Single(payload), None)
        .await
        .unwrap_err();
    assert!(matches!(
        actual.downcast_ref::<GraphFlowError>(),
        Some(GraphFlowError::Codec(_))
    ));
    assert_original_json_error(actual, expected);
}

#[tokio::test]
async fn wrong_element_type_returns_original_json_error() {
    let codec = JsonCodec::<Vec<u8>>::default();
    let bad_element = codec.encode(&"not a number".to_owned()).unwrap();
    let expected = <JsonCodec as Codec<u32>>::decode(&codec, &bad_element).unwrap_err();
    let payload = codec
        .encode(&vec![codec.encode(&1_u32).unwrap(), bad_element])
        .unwrap();
    let actual = call_node::<Pair>(NodeInput::Single(payload), None)
        .await
        .unwrap_err();
    assert!(matches!(
        actual.downcast_ref::<GraphFlowError>(),
        Some(GraphFlowError::Codec(_))
    ));
    assert_original_json_error(actual, expected);
}

#[test]
fn tuple_inputs_register_through_all_graph_entry_points() {
    async fn task((left, right): Pair) -> u32 {
        left + right
    }

    async fn route((left, right): Pair) -> Result<NodeIndex, Infallible> {
        Ok(NodeIndex::new((left + right) as usize))
    }

    let graph = GraphFlow::<TestBackend>::new("tuple-inputs");
    let _node = graph.add_node(
        "tuple-node",
        service_fn(|request: Task<Pair>| ready(Ok::<u32, Infallible>(request.args.0))),
    );
    let _task = graph.add_task(task);
    let _route = graph.route(route);
    graph.validate().unwrap();
}

#[tokio::test]
async fn fan_in_wrong_length_returns_json_error_without_panicking() {
    let values = vec![serde_json::json!(1)];
    let expected =
        serde_json::from_value::<Pair>(serde_json::Value::Array(values.clone())).unwrap_err();
    let actual = call_node::<Pair>(NodeInput::FanIn(values), None)
        .await
        .unwrap_err();
    assert_original_json_error(actual, expected);
}
