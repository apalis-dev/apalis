use crate::util::RawTask;

/// Pluggable serialization strategy for [`FileStorage`].
pub trait Adapter: Send + 'static {
    /// The format-native representation of one record.
    type Line: Send + Clone;

    /// The error that will be returned by the adapter.
    type Error;

    /// Serialize `line` into bytes including the record terminator (`\n`).
    fn serialize(&self, line: &Self::Line) -> Result<Vec<u8>, Self::Error>;

    /// Deserialize one raw record (terminator already stripped) into a `Line`.
    fn deserialize(&self, raw: &[u8]) -> Result<Self::Line, Self::Error>;

    /// Convert a format-native `Line` into the common [`TaskWithMeta`].
    fn to_entry(&self, line: Self::Line) -> Result<RawTask, Self::Error>;

    /// Convert a [`TaskWithMeta`] into a format-native `Line` ready for
    /// serialization.
    fn from_entry(entry: &RawTask) -> Result<Self::Line, Self::Error>;

    /// Return `true` if `raw` is a structural line (e.g. a CSV header)
    /// that should be skipped rather than deserialized as a task entry.
    /// The default implementation always returns `false`, so `JsonAdapter`
    /// and any other self-describing adapter needs no change.
    fn is_header(&self, _raw: &[u8]) -> bool {
        false
    }

    /// Return a header line to be written at the start of the file.
    fn header(&self, _entries: &Vec<RawTask>) -> Option<Vec<u8>> {
        None
    }
}
