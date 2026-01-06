#![doc=include_str!("../README.md")]
use apalis_core::backend::StatType;

/// SQL backend for Apalis
pub mod config;
/// SQL context for jobs stored in a SQL database
pub mod context;
/// DateTime abstraction for unified time handling
pub mod datetime;
/// SQL task row representation and conversion
pub mod from_row;

/// Extension traits for `TaskBuilder`
pub mod ext;

pub use datetime::{DateTime, DateTimeExt};
pub use from_row::TaskRow;

/// Convert a string to a StatType
#[must_use]
pub fn stat_type_from_string(s: &str) -> StatType {
    match s {
        // "Number" => StatType::Number,
        "Decimal" => StatType::Decimal,
        "Percentage" => StatType::Percentage,
        "Timestamp" => StatType::Timestamp,
        _ => StatType::Number,
    }
}
