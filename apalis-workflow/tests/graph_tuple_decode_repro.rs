use std::{collections::BTreeMap, error::Error, panic::catch_unwind};

use apalis_codec::json::JsonCodec;
use apalis_core::backend::{codec::Codec, ext::with_codec::WithCodec, memory::MemoryStorage};
use apalis_workflow::graph::{decode::GraphCodec, error::GraphFlowError};

type Backend = WithCodec<MemoryStorage<Vec<u8>>, JsonCodec<Vec<u8>>>;
type Pair = (u32, u32);
type Dozen = (u32, u32, u32, u32, u32, u32, u32, u32, u32, u32, u32, u32);

fn encoded_list(len: usize) -> Vec<u8> {
    let codec = JsonCodec::<Vec<u8>>::default();
    let elements: Vec<Vec<u8>> = (1..=len)
        .map(|n| codec.encode(&(n as u32)).unwrap())
        .collect();
    let wire = codec.encode(&elements).unwrap();
    let outer: Vec<Vec<u8>> = codec.decode(&wire).unwrap();
    assert_eq!(
        outer, elements,
        "outer codec must succeed before checking arity"
    );
    wire
}

macro_rules! invalid_length {
    ($name:ident, $tuple:ty, $expected:expr, $actual:expr) => {
        #[test]
        fn $name() {
            let codec = JsonCodec::<Vec<u8>>::default();
            let wire = encoded_list($actual);
            let outcome = catch_unwind(|| <$tuple as GraphCodec<Backend>>::decode(&wire, &codec));
            let result = outcome.unwrap_or_else(|panic| {
                let message = panic.downcast_ref::<String>().map(String::as_str)
                    .or_else(|| panic.downcast_ref::<&str>().copied()).unwrap_or("unknown panic");
                panic!("expected Err without panic for tuple arity {} and length {}; caught: {message}", $expected, $actual);
            });
            assert!(matches!(
                result,
                Err(GraphFlowError::InputCountMismatch { expected, actual })
                    if expected == $expected && actual == $actual
            ));
        }
    };
}

invalid_length!(pair_empty_returns_error_without_panicking, Pair, 2, 0);
invalid_length!(pair_short_returns_error_without_panicking, Pair, 2, 1);
invalid_length!(pair_long_returns_error_without_panicking, Pair, 2, 3);
invalid_length!(dozen_empty_returns_error_without_panicking, Dozen, 12, 0);
invalid_length!(dozen_short_returns_error_without_panicking, Dozen, 12, 11);
invalid_length!(dozen_long_returns_error_without_panicking, Dozen, 12, 13);

#[test]
fn valid_pair_round_trip_preserves_wire_bytes_and_order() {
    let codec = JsonCodec::<Vec<u8>>::default();
    let wire = <Pair as GraphCodec<Backend>>::encode((1, 2), &codec).unwrap();
    assert_eq!(wire, b"[[49],[50]]");
    assert_eq!(wire, encoded_list(2));
    assert_eq!(
        <Pair as GraphCodec<Backend>>::decode(&wire, &codec).unwrap(),
        (1, 2)
    );
}

#[test]
fn valid_dozen_round_trip_preserves_wire_bytes_and_order() {
    let codec = JsonCodec::<Vec<u8>>::default();
    let input = (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12);
    let wire = <Dozen as GraphCodec<Backend>>::encode(input, &codec).unwrap();
    assert_eq!(
        wire,
        b"[[49],[50],[51],[52],[53],[54],[55],[56],[57],[49,48],[49,49],[49,50]]"
    );
    assert_eq!(wire, encoded_list(12));
    assert_eq!(
        <Dozen as GraphCodec<Backend>>::decode(&wire, &codec).unwrap(),
        input
    );
}

#[test]
fn damaged_outer_data_retains_codec_error() {
    let codec = JsonCodec::<Vec<u8>>::default();
    let wire = b"{".to_vec();
    let expected = <JsonCodec<Vec<u8>> as Codec<Vec<Vec<u8>>>>::decode(&codec, &wire).unwrap_err();
    let actual = catch_unwind(|| <Pair as GraphCodec<Backend>>::decode(&wire, &codec))
        .expect("damaged outer input must not panic")
        .unwrap_err();
    assert_codec_source(actual, expected);
}

#[test]
fn correct_length_wrong_element_type_retains_codec_error() {
    let codec = JsonCodec::<Vec<u8>>::default();
    let invalid = codec.encode(&"not a number".to_owned()).unwrap();
    let expected = <JsonCodec<Vec<u8>> as Codec<u32>>::decode(&codec, &invalid).unwrap_err();
    let wire = codec
        .encode(&vec![codec.encode(&1u32).unwrap(), invalid])
        .unwrap();
    let outer: Vec<Vec<u8>> = codec.decode(&wire).unwrap();
    assert_eq!(outer.len(), 2);
    let actual = catch_unwind(|| <Pair as GraphCodec<Backend>>::decode(&wire, &codec))
        .expect("invalid element input must not panic")
        .unwrap_err();
    assert_codec_source(actual, expected);
}

#[test]
fn plain_json_array_is_not_the_nested_byte_wire_format() {
    let codec = JsonCodec::<Vec<u8>>::default();
    let wire = b"[1,2]".to_vec();
    let expected = <JsonCodec<Vec<u8>> as Codec<Vec<Vec<u8>>>>::decode(&codec, &wire).unwrap_err();
    let result = catch_unwind(|| <Pair as GraphCodec<Backend>>::decode(&wire, &codec))
        .expect("this input should fail before the tuple length check");
    assert_codec_source(result.unwrap_err(), expected);
}

fn assert_codec_source(actual: GraphFlowError, expected: serde_json::Error) {
    assert!(matches!(&actual, GraphFlowError::Codec(_)));
    let source = actual.source().expect("codec error must retain its source");
    let original = source
        .downcast_ref::<serde_json::Error>()
        .expect("source must be the original serde_json error");
    assert_eq!(original.classify(), expected.classify());
    assert_eq!(original.line(), expected.line());
    assert_eq!(original.column(), expected.column());
    assert_eq!(original.to_string(), expected.to_string());
}

#[test]
fn tuple_encode_preserves_first_and_later_field_codec_errors() {
    let codec = JsonCodec::<Vec<u8>>::default();
    let invalid = BTreeMap::from([(vec![1_u8], 2_u32)]);

    let expected = codec.encode(&invalid).unwrap_err();
    let actual = <(BTreeMap<Vec<u8>, u32>, u32) as GraphCodec<Backend>>::encode(
        (invalid.clone(), 3),
        &codec,
    )
    .unwrap_err();
    assert_codec_source(actual, expected);

    let expected = codec.encode(&invalid).unwrap_err();
    let actual =
        <(u32, BTreeMap<Vec<u8>, u32>) as GraphCodec<Backend>>::encode((3, invalid), &codec)
            .unwrap_err();
    assert_codec_source(actual, expected);
}
