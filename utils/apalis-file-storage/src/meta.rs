use apalis_core::task::metadata::{MetadataExt, MetadataStore};
use serde::{Deserialize, Serialize};

/// A simple wrapper around a JSON map to represent task metadata
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct JsonMapMetadata(MetadataStore);

impl MetadataExt for JsonMapMetadata {
    fn metadata(&self) -> &MetadataStore {
        &self.0
    }

    fn metadata_mut(&mut self) -> &mut MetadataStore {
        &mut self.0
    }
}
