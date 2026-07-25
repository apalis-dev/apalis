//! Defines the `TaskId` type and related functionality.
//!
//! `TaskId` is a wrapper around a generic identifier type, providing type safety and utility methods for task identification.
//!
use std::{
    fmt::{Debug, Display},
    hash::Hash,
    str::FromStr,
};

use crate::{
    task::{Task, data::MissingDataError},
    task_fn::FromRequest,
};

pub use random_id::RandomId;

/// A wrapper type that defines a task id.
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Debug, Clone, Copy, Eq, Hash, PartialEq, PartialOrd, Ord, Default)]
pub struct TaskId<Id>(Id);

impl<Id> TaskId<Id> {
    /// Generate a new [`TaskId`]
    pub fn new(id: Id) -> Self {
        Self(id)
    }
    /// Get the inner value
    pub fn inner(&self) -> &Id {
        &self.0
    }
}

/// Errors that can occur when parsing a `TaskId` from a string
#[derive(Debug, thiserror::Error)]
pub enum TaskIdError<E> {
    /// Decoding error
    #[error("could not decode task_id: `{0}`")]
    Decode(E),
}

impl<Id: FromStr> FromStr for TaskId<Id> {
    type Err = TaskIdError<Id::Err>;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self::new(Id::from_str(s).map_err(TaskIdError::Decode)?))
    }
}

impl<Id: FromStr> TryFrom<&'_ str> for TaskId<Id> {
    type Error = TaskIdError<Id::Err>;

    fn try_from(value: &'_ str) -> Result<Self, Self::Error> {
        Self::from_str(value)
    }
}

impl<Id: Display> Display for TaskId<Id> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Display::fmt(&self.0, f)
    }
}

impl<Args: Sync, Id: Sync + Send + Clone> FromRequest<Task<Args, Id>> for TaskId<Id> {
    type Error = MissingDataError;
    async fn from_request(task: &Task<Args, Id>) -> Result<Self, Self::Error> {
        task.ctx.task_id.clone().ok_or(MissingDataError::NotFound(
            std::any::type_name::<Self>().to_owned(),
        ))
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

    /// Generates a unique, time-ordered NanoID-style string (zero-deps).
    pub(super) fn unique_id() -> String {
        let timestamp = current_time_millis();
        let time_str = encode_base64(timestamp, TIME_LEN);

        // Counter ensures uniqueness across fast calls
        let count = COUNTER.fetch_add(1, Ordering::Relaxed);
        let rand_part = encode_base64(xorshift64(timestamp ^ count), RANDOM_LEN);

        format!("{time_str}{rand_part}{count}")
    }

    /// Returns current time in milliseconds since UNIX epoch.
    fn current_time_millis() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64
    }

    /// Simple xorshift PRNG
    fn xorshift64(mut x: u64) -> u64 {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        x
    }
}
