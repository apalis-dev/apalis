use apalis_core::task::task_id::RandomId;

/// Trait for generating unique IDs
pub trait GenerateId {
    /// Generate a new unique ID
    fn generate() -> Self;
}

#[cfg(feature = "uuid")]
impl GenerateId for uuid::Uuid {
    fn generate() -> Self {
        Self::new_v4()
    }
}

#[cfg(feature = "ulid")]
impl GenerateId for ulid::Ulid {
    fn generate() -> Self {
        Self::new()
    }
}

impl GenerateId for RandomId {
    fn generate() -> Self {
        Self::default()
    }
}

#[cfg(feature = "rand")]
impl GenerateId for u64 {
    fn generate() -> Self {
        rand::random::<Self>()
    }
}
#[cfg(feature = "rand")]
impl GenerateId for i64 {
    fn generate() -> Self {
        rand::random::<Self>()
    }
}

#[cfg(feature = "rand")]
impl GenerateId for u128 {
    fn generate() -> Self {
        rand::random::<Self>()
    }
}

#[cfg(feature = "rand")]
impl GenerateId for i128 {
    fn generate() -> Self {
        rand::random::<Self>()
    }
}
