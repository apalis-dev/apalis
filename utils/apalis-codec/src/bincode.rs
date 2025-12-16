use apalis_core::backend::codec::Codec;
use bincode::{Decode, Encode};

/// Bincode encoding and decoding
#[derive(Debug, Clone, Default)]
pub struct BincodeCodec;

/// Errors that can occur during Bincode encoding/decoding
#[derive(thiserror::Error, Debug)]
pub enum BincodeCodecError {
    /// Error during encoding
    #[error("Encoding error: {0}")]
    EncodeError(#[from] bincode::error::EncodeError),
    /// Error during decoding
    #[error("Decoding error: {0}")]
    DecodeError(#[from] bincode::error::DecodeError),
}

impl<T: Encode + Decode<()>> Codec<T> for BincodeCodec {
    type Compact = Vec<u8>;
    type Error = BincodeCodecError;
    fn encode(input: &T) -> Result<Vec<u8>, Self::Error> {
        let config = bincode::config::standard();
        Ok(bincode::encode_to_vec(input, config)?)
    }

    fn decode(compact: &Vec<u8>) -> Result<T, Self::Error> {
        let config = bincode::config::standard();
        Ok(bincode::decode_from_slice(compact, config)?.0)
    }
}
#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Encode, Decode, Debug, PartialEq)]
    struct TestData {
        id: u32,
        name: String,
    }

    #[test]
    fn test_encode_decode_roundtrip() {
        let original = TestData {
            id: 42,
            name: "test".to_string(),
        };

        let encoded = BincodeCodec::encode(&original).expect("encoding failed");
        let decoded: TestData = BincodeCodec::decode(&encoded).expect("decoding failed");

        assert_eq!(original, decoded);
    }
}
