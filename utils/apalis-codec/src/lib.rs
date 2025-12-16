//! Utility codecs for apalis
//!
//! Supports different encoding and decoding strategies for task arguments and results.

/// Encoding for tasks using json
#[cfg(feature = "json")]
pub mod json;

/// Encoding for tasks using MessagePack
#[cfg(feature = "msgpack")]
pub mod msgpack;

/// Encoding for tasks using bincode
#[cfg(feature = "bincode")]
pub mod bincode;
