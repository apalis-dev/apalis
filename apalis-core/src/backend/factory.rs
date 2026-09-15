//! # Shared connection support for backends
//!
//! The [`BackendFactory`] trait defines how to create backend using the same connection instances, potentially with configuration options.
//! This allows for flexible and reusable backend implementations that can be easily integrated into different parts of an application.
//!
//! ## Features:
//! - `BackendFactory` trait: Defines methods for creating backend instances from one connection, with or without configuration.
//! - Support for various backend types.
//! - Performance optimizations by allowing backends to reuse connections and resources.

use crate::backend::{Backend, BackendConfig};

/// Trait for creating backend instances, generic over job-type arguments.
pub trait BackendFactory<Args> {
    /// The backend type produced by this factory.
    type Backend: Backend + BackendConfig<Args = Args>;
    /// The error returned if backend creation fails.
    type Error;

    /// Create a backend using `Config::default()`.
    fn create(&mut self) -> Result<Self::Backend, Self::Error>
    where
        <Self::Backend as BackendConfig>::Config: Default,
    {
        self.create_with_config(Default::default())
    }

    /// Create a backend using the given configuration.
    fn create_with_config(
        &mut self,
        config: <Self::Backend as BackendConfig>::Config,
    ) -> Result<Self::Backend, Self::Error>;
}
