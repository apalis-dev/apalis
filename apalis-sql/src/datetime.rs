//! DateTime abstraction for unified time handling.
//!
//! This module provides a unified API for datetime operations in the SQL backends,
//! abstracting over the differences between the `chrono` and `time` crates.
//!
//! # Feature Flags
//!
//! The datetime implementation is selected based on enabled features:
//!
//! - **`time` feature enabled**: Uses `time::OffsetDateTime` as the underlying type.
//!   This takes precedence even if `chrono` is also enabled.
//! - **`chrono` feature enabled (without `time`)**: Uses `chrono::DateTime<Utc>`.
//!
//! # Why This Abstraction?
//!
//! Different SQL database drivers have varying levels of support for datetime crates.
//! Some work better with `chrono`, others with `time`. This module allows users to
//! choose the datetime crate that best fits their database driver and application
//! needs, while the rest of the codebase uses a consistent API through the
//! [`DateTimeExt`] trait.
//!
//! # Usage
//!
//! ```rust
//! use apalis_sql::{DateTime, DateTimeExt};
//!
//! // Get current time (works with either feature)
//! let now = DateTime::now();
//!
//! // Convert to Unix timestamp
//! let timestamp = now.to_unix_timestamp();
//!
//! // Create from Unix timestamp
//! let dt = DateTime::from_unix_timestamp(timestamp);
//! ```

/// DateTime type alias that uses either chrono or time depending on enabled features.
///
/// When the `time` feature is enabled, this is `time::OffsetDateTime`.
/// When the `chrono` feature is enabled (and `time` is not), this is `chrono::DateTime<Utc>`.
#[cfg(all(feature = "chrono", not(feature = "time")))]
pub type DateTime = chrono::DateTime<chrono::Utc>;

/// DateTime type alias that uses either chrono or time depending on enabled features.
///
/// When the `time` feature is enabled, this is `time::OffsetDateTime`.
/// When the `chrono` feature is enabled (and `time` is not), this is `chrono::DateTime<Utc>`.
#[cfg(feature = "time")]
pub type DateTime = time::OffsetDateTime;

/// Extension trait for SQL datetime operations.
///
/// This trait provides a unified API for datetime operations regardless of
/// whether `chrono` or `time` feature is enabled.
pub trait DateTimeExt {
    /// Returns the current UTC datetime.
    fn now() -> Self;

    /// Returns the Unix timestamp (seconds since epoch).
    fn to_unix_timestamp(&self) -> i64;

    /// Creates a datetime from Unix timestamp (seconds since epoch).
    fn from_unix_timestamp(secs: i64) -> Self;
}

#[cfg(all(feature = "chrono", not(feature = "time")))]
impl DateTimeExt for DateTime {
    fn now() -> Self {
        chrono::Utc::now()
    }

    fn to_unix_timestamp(&self) -> i64 {
        self.timestamp()
    }

    fn from_unix_timestamp(secs: i64) -> Self {
        DateTime::from_timestamp(secs, 0).unwrap_or_default()
    }
}

#[cfg(feature = "time")]
impl DateTimeExt for DateTime {
    fn now() -> Self {
        Self::now_utc()
    }

    fn to_unix_timestamp(&self) -> i64 {
        self.unix_timestamp()
    }

    fn from_unix_timestamp(secs: i64) -> Self {
        Self::from_unix_timestamp(secs).unwrap_or(Self::UNIX_EPOCH)
    }
}
