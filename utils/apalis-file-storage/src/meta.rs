use apalis_core::task::metadata::MetadataExt;
use serde::{Deserialize, Serialize};

/// A simple wrapper around a JSON map to represent task metadata
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct JsonMapMetadata(serde_json::Map<String, serde_json::Value>);

impl<T: Serialize + for<'de> Deserialize<'de>> MetadataExt<T> for JsonMapMetadata {
    type Error = serde_json::Error;
    fn extract(&self) -> Result<T, serde_json::Error> {
        use serde::de::Error as _;
        let key = std::any::type_name::<T>();
        match self.0.get(key) {
            Some(value) => T::deserialize(value),
            None => Err(serde_json::Error::custom(format!(
                "No entry for type `{key}` in metadata"
            ))),
        }
    }

    fn inject(&mut self, value: T) -> Result<(), serde_json::Error> {
        let key = std::any::type_name::<T>();
        let json_value = serde_json::to_value(value)?;
        self.0.insert(key.to_owned(), json_value);
        Ok(())
    }
}
