#![cfg_attr(docsrs, feature(doc_cfg))]
#![doc = include_str!("../README.md")]

/// Encoding for tasks using json
#[cfg(feature = "json")]
pub mod json;

/// Encoding for tasks using MessagePack
#[cfg(feature = "msgpack")]
pub mod msgpack;

/// Encoding for tasks using bincode
#[cfg(feature = "bincode")]
pub mod bincode;
