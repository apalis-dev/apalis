use apalis_core::backend::codec::Codec;
use serde::{Deserialize, Serialize};

/// MsgPack encoding and decoding
#[derive(Debug, Clone, Default)]
pub struct MsgPackCodec;

/// Errors that can occur during MsgPack encoding/decoding
#[derive(thiserror::Error, Debug)]
pub enum MsgPackCodecError {
    /// Error during encoding
    #[error("Encoding error: {0}")]
    EncodeError(#[from] rmp_serde::encode::Error),
    /// Error during decoding
    #[error("Decoding error: {0}")]
    DecodeError(#[from] rmp_serde::decode::Error),
}

impl<T: Serialize + for<'de> Deserialize<'de>> Codec<T> for MsgPackCodec {
    type Compact = Vec<u8>;
    type Error = MsgPackCodecError;
    fn encode(input: &T) -> Result<Vec<u8>, Self::Error> {
        Ok(rmp_serde::to_vec(input)?)
    }

    fn decode(compact: &Vec<u8>) -> Result<T, Self::Error> {
        Ok(rmp_serde::from_slice(compact)?)
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
    fn test_encode_decode_roundtrip() {
        let original = TestStruct {
            id: 1,
            name: "Test".to_string(),
        };

        // Encode the original struct
        let encoded = MsgPackCodec::encode(&original).expect("Encoding failed");

        // Decode back to struct
        let decoded: TestStruct = MsgPackCodec::decode(&encoded).expect("Decoding failed");

        // Assert that the original and decoded structs are equal
        assert_eq!(original, decoded);
    }
}
