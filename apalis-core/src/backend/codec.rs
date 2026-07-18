//! Utilities for encoding and decoding task arguments and results
//!
//! # Overview
//!
//! The `Codec` trait allows for converting values
//! between a type `T` and a more compact or transport-friendly representation.
//! This is particularly useful for serializing/deserializing, compressing/expanding,
//! or otherwise encoding/decoding values in a custom format.
//!
//! The module includes several implementations of the `Codec` trait, such as `IdentityCodec`
//! and `NoopCodec`, as well as a JSON codec when the `json` feature is enabled.

/// A trait for converting values between a type `T` and a more compact or
/// transport-friendly representation for a `Backend`. Examples include json
/// and bytes.
///
/// This is useful when you need to serialize/deserialize, compress/expand,
/// or otherwise encode/decode values in a custom format.
///
/// By default, a backend doesn't care about the specific type implementing [`Codec`]
/// but rather the [`Codec::Compact`] type. This means if it can accept bytes, you
/// can use familiar crates such as bincode and rkyv
///
/// # Type Parameters
/// - `T`: The type of value being encoded/decoded.
pub trait Codec<T> {
    /// The error type returned if encoding or decoding fails.
    type Error;

    /// The compact or encoded representation of `T`.
    ///
    /// This could be a primitive type, a byte buffer, or any other
    /// representation that is more efficient to store or transmit.
    type Compact;

    /// Encode a value of type `T` into its compact representation.
    ///
    /// # Errors
    /// Returns [`Self::Error`] if the value cannot be encoded.
    fn encode(&self, val: &T) -> Result<Self::Compact, Self::Error>;

    /// Decode a compact representation back into a value of type `T`.
    ///
    /// # Errors
    /// Returns [`Self::Error`] if the compact representation cannot
    /// be decoded into a valid `T`.
    fn decode(&self, val: &Self::Compact) -> Result<T, Self::Error>;
}

/// A codec that performs no transformation, returning the input value as-is.
#[derive(Debug, Clone, Default)]
pub struct IdentityCodec;

impl<T> Codec<T> for IdentityCodec
where
    T: Clone,
{
    type Compact = T;
    type Error = std::convert::Infallible;

    fn encode(&self, val: &T) -> Result<Self::Compact, Self::Error> {
        Ok(val.clone())
    }

    fn decode(&self, val: &Self::Compact) -> Result<T, Self::Error> {
        Ok(val.clone())
    }
}
