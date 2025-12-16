use std::marker::PhantomData;

use apalis_core::backend::codec::Codec;
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Json encoding and decoding
#[derive(Debug, Clone, Default)]
pub struct JsonCodec<Output> {
    _o: PhantomData<Output>,
}

impl<T: Serialize + for<'de> Deserialize<'de>> Codec<T> for JsonCodec<Vec<u8>> {
    type Compact = Vec<u8>;
    type Error = serde_json::Error;
    fn encode(input: &T) -> Result<Vec<u8>, Self::Error> {
        serde_json::to_vec(input)
    }

    fn decode(compact: &Vec<u8>) -> Result<T, Self::Error> {
        serde_json::from_slice(compact)
    }
}

impl<T: Serialize + for<'de> Deserialize<'de>> Codec<T> for JsonCodec<String> {
    type Compact = String;
    type Error = serde_json::Error;
    fn encode(input: &T) -> Result<String, Self::Error> {
        serde_json::to_string(input)
    }
    fn decode(compact: &String) -> Result<T, Self::Error> {
        serde_json::from_str(compact)
    }
}

impl<T: Serialize + for<'de> Deserialize<'de>> Codec<T> for JsonCodec<Value> {
    type Compact = Value;
    type Error = serde_json::Error;
    fn encode(input: &T) -> Result<Value, Self::Error> {
        serde_json::to_value(input)
    }

    fn decode(compact: &Value) -> Result<T, Self::Error> {
        T::deserialize(compact)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Serialize, Deserialize, Debug, PartialEq)]
    struct TestStruct {
        id: u32,
        name: String,
    }

    #[test]
    fn test_json_codec_vec_u8_roundtrip() {
        let original = TestStruct {
            id: 1,
            name: "Test".to_string(),
        };
        let encoded = JsonCodec::<Vec<u8>>::encode(&original).unwrap();
        let decoded: TestStruct = JsonCodec::<Vec<u8>>::decode(&encoded).unwrap();
        assert_eq!(original, decoded);
    }

    #[test]
    fn test_json_codec_string_roundtrip() {
        let original = TestStruct {
            id: 2,
            name: "Example".to_string(),
        };
        let encoded = JsonCodec::<String>::encode(&original).unwrap();
        let decoded: TestStruct = JsonCodec::<String>::decode(&encoded).unwrap();
        assert_eq!(original, decoded);
    }

    #[test]
    fn test_json_codec_value_roundtrip() {
        let original = TestStruct {
            id: 3,
            name: "Sample".to_string(),
        };
        let encoded = JsonCodec::<Value>::encode(&original).unwrap();
        let decoded: TestStruct = JsonCodec::<Value>::decode(&encoded).unwrap();
        assert_eq!(original, decoded);
    }
}
