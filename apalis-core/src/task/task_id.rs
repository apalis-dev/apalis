//! Defines the `TaskId` type and related functionality.
//!
//! `TaskId` is an identifier for a task, supporting a fixed set of common
//! id representations (integer, string, and optionally UUID/ULID when the
//! corresponding features are enabled).
use std::{
    fmt::{Debug, Display},
    str::FromStr,
};

use crate::{
    task::from_request::FromRequest,
    task::{Task, data::MissingDataError},
};

pub use random_id::RandomId;

/// A wrapper type that defines a task id.
///
/// Supports a fixed set of common identifier representations. `Uuid` and
/// `Ulid` variants are only available when the corresponding crate features
/// are enabled.
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Debug, Clone, Eq, Hash, PartialEq, PartialOrd, Ord)]
#[cfg_attr(feature = "serde", serde(untagged))]
#[non_exhaustive]
pub enum TaskId {
    /// An integer-based id
    Int(u64),
    /// A string-based id
    String(String),
    /// A UUID-based id
    #[cfg(feature = "uuid")]
    Uuid(uuid::Uuid),
    /// A ULID-based id
    #[cfg(feature = "ulid")]
    Ulid(ulid::Ulid),
}

impl TaskId {
    /// Construct a `TaskId` from an integer.
    #[must_use]
    pub fn from_int(id: u64) -> Self {
        Self::Int(id)
    }

    /// Construct a `TaskId` from a string.
    #[must_use]
    pub fn from_string(id: impl Into<String>) -> Self {
        Self::String(id.into())
    }

    /// Construct a `TaskId` from a UUID.
    #[cfg(feature = "uuid")]
    #[must_use]
    pub fn from_uuid(id: uuid::Uuid) -> Self {
        Self::Uuid(id)
    }

    /// Construct a `TaskId` from a ULID.
    #[cfg(feature = "ulid")]
    #[must_use]
    pub fn from_ulid(id: ulid::Ulid) -> Self {
        Self::Ulid(id)
    }

    /// Returns the inner value as an integer, if this is an `Int` variant.
    #[must_use]
    #[allow(clippy::match_wildcard_for_single_variants)]
    pub fn as_int(&self) -> Option<u64> {
        match self {
            Self::Int(id) => Some(*id),
            _ => None,
        }
    }

    /// Returns the inner value as a string slice, if this is a `String` variant.
    #[must_use]
    #[allow(clippy::match_wildcard_for_single_variants)]
    pub fn as_string(&self) -> Option<&str> {
        match self {
            Self::String(id) => Some(id.as_str()),
            _ => None,
        }
    }

    /// Returns the inner value as a `Uuid`, if this is a `Uuid` variant.
    #[cfg(feature = "uuid")]
    #[must_use]
    pub fn as_uuid(&self) -> Option<uuid::Uuid> {
        match self {
            Self::Uuid(id) => Some(*id),
            _ => None,
        }
    }

    /// Returns the inner value as a `Ulid`, if this is a `Ulid` variant.
    #[cfg(feature = "ulid")]
    #[must_use]
    pub fn as_ulid(&self) -> Option<ulid::Ulid> {
        match self {
            Self::Ulid(id) => Some(*id),
            _ => None,
        }
    }
}

impl TaskId {
    /// Generates a deterministic uuid based on the task id.
    #[cfg(feature = "uuid")]
    #[must_use]
    pub fn to_uuid(&self) -> uuid::Uuid {
        const NAMESPACE: uuid::Uuid = uuid::Uuid::from_u128(0x6ba7b8109dad11d180b400c04fd430c8);

        match self {
            Self::Int(id) => uuid::Uuid::new_v5(&NAMESPACE, &id.to_be_bytes()),
            Self::String(id) => uuid::Uuid::new_v5(&NAMESPACE, id.as_bytes()),
            #[cfg(feature = "uuid")]
            Self::Uuid(id) => *id,
            #[cfg(feature = "ulid")]
            Self::Ulid(id) => (*id).into(),
        }
    }
}

/// Errors that can occur when parsing a `TaskId` from a string
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum TaskIdError {
    /// The string did not match any known `TaskId` representation
    #[error("could not decode task_id: `{0}`")]
    Decode(String),
}

impl FromStr for TaskId {
    type Err = TaskIdError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        #[cfg(feature = "uuid")]
        if let Ok(id) = uuid::Uuid::from_str(s) {
            return Ok(Self::Uuid(id));
        }

        #[cfg(feature = "ulid")]
        if let Ok(id) = ulid::Ulid::from_str(s) {
            return Ok(Self::Ulid(id));
        }

        if let Ok(id) = u64::from_str(s) {
            return Ok(Self::Int(id));
        }

        if !s.is_empty() {
            return Ok(Self::String(s.to_owned()));
        }

        Err(TaskIdError::Decode(s.to_owned()))
    }
}

impl TryFrom<&'_ str> for TaskId {
    type Error = TaskIdError;

    fn try_from(value: &'_ str) -> Result<Self, Self::Error> {
        Self::from_str(value)
    }
}

impl Display for TaskId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Int(id) => Display::fmt(id, f),
            Self::String(id) => Display::fmt(id, f),
            #[cfg(feature = "uuid")]
            Self::Uuid(id) => Display::fmt(id, f),
            #[cfg(feature = "ulid")]
            Self::Ulid(id) => Display::fmt(id, f),
        }
    }
}

impl<Args: Sync> FromRequest<Task<Args>> for TaskId {
    type Error = MissingDataError;
    async fn from_request(req: &Task<Args>) -> Result<Self, Self::Error> {
        req.task_id().cloned().ok_or(MissingDataError::NotFound(
            std::any::type_name::<Self>().to_owned(),
        ))
    }
}

impl From<u64> for TaskId {
    fn from(id: u64) -> Self {
        Self::Int(id)
    }
}

impl From<String> for TaskId {
    fn from(id: String) -> Self {
        Self::String(id)
    }
}

#[cfg(feature = "uuid")]
impl From<uuid::Uuid> for TaskId {
    fn from(id: uuid::Uuid) -> Self {
        Self::Uuid(id)
    }
}

#[cfg(feature = "ulid")]
impl From<ulid::Ulid> for TaskId {
    fn from(id: ulid::Ulid) -> Self {
        Self::Ulid(id)
    }
}

/// Helper function to combine a list of ids into a single string
///
/// The output should be json compatible
pub fn coalesce_ids<T: Display>(task_ids: impl IntoIterator<Item = T> + Send) -> String {
    use std::fmt::Write;
    let mut ids = String::from("[");

    for (i, id) in task_ids.into_iter().enumerate() {
        if i != 0 {
            ids.push(',');
        }
        write!(&mut ids, "\"{id}\"").unwrap();
    }

    ids.push(']');
    ids
}
/// Trait for generating unique IDs
pub trait GenerateId {
    /// Generate a new unique ID
    fn generate() -> TaskId;
}

#[cfg(feature = "uuid")]
impl GenerateId for uuid::Uuid {
    fn generate() -> TaskId {
        TaskId::Uuid(Self::new_v4())
    }
}

#[cfg(feature = "ulid")]
impl GenerateId for ulid::Ulid {
    fn generate() -> TaskId {
        TaskId::Ulid(Self::generate())
    }
}

impl GenerateId for RandomId {
    fn generate() -> TaskId {
        TaskId::from_string(Self::default())
    }
}

impl GenerateId for u64 {
    fn generate() -> TaskId {
        TaskId::Int(rand::random::<Self>())
    }
}

mod random_id {
    use super::*;
    use std::convert::Infallible;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};

    const ALPHABET: &[u8] = b"abcdefghijkmnopqrstuvwxyz23456789-";
    const BASE: u64 = 34;
    const TIME_LEN: usize = 6;
    const RANDOM_LEN: usize = 5;

    /// A simple, unique, time-ordered ID (zero-deps).
    ///
    /// Consider using a ulid/uuid/nanoid in backend implementation
    /// This is a placeholder and does not guarantee/tested as the other implementations
    #[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
    #[derive(Debug, Clone, Eq, Hash, PartialEq, PartialOrd, Ord)]
    pub struct RandomId(String);

    impl From<RandomId> for String {
        fn from(value: RandomId) -> Self {
            value.0
        }
    }

    impl FromStr for RandomId {
        type Err = Infallible;
        fn from_str(s: &str) -> Result<Self, Self::Err> {
            Ok(Self(s.to_owned()))
        }
    }

    #[allow(clippy::infallible_try_from)]
    impl TryFrom<&'_ str> for RandomId {
        type Error = Infallible;

        fn try_from(value: &'_ str) -> Result<Self, Self::Error> {
            Self::from_str(value)
        }
    }

    impl Display for RandomId {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            Display::fmt(&self.0, f)
        }
    }

    impl Default for RandomId {
        fn default() -> Self {
            Self(unique_id())
        }
    }

    // Atomic counter to ensure uniqueness within same millisecond
    static COUNTER: AtomicU64 = AtomicU64::new(0);

    /// Converts a number to base-64 using the NanoID alphabet.
    fn encode_base64(mut value: u64, length: usize) -> String {
        let mut buf = vec![b'A'; length];
        for i in (0..length).rev() {
            buf[i] = ALPHABET[(value % BASE) as usize];
            value /= BASE;
        }
        String::from_utf8(buf).unwrap()
    }

    /// Generates a unique, time-ordered NanoID-style string.
    pub(super) fn unique_id() -> String {
        let timestamp = current_time_millis();
        let time_str = encode_base64(timestamp, TIME_LEN);

        // Counter ensures uniqueness across fast calls
        let count = COUNTER.fetch_add(1, Ordering::Relaxed);
        let rand_part = encode_base64(rand::random::<u64>(), RANDOM_LEN);

        format!("{time_str}{rand_part}{count}")
    }

    /// Returns current time in milliseconds since UNIX epoch.
    fn current_time_millis() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64
    }
}
