//! Task metadata extension trait and implementations
//!
//! The [`MetadataExt`] trait allows injecting and extracting metadata associated with tasks.
//! It includes implementations for common metadata types.
//!
//! ## Overview
//! - `Metadata`: A trait for extracting and injecting metadata.
//!
//! # Usage
//! Implement the `MetadataExt` trait for your context types to enable easy extraction and injection
//! from task contexts. This allows middleware and services to access and modify task metadata in a
//! type-safe manner.
use crate::task::Task;
use crate::task_fn::FromRequest;
use std::collections::HashMap;
use std::convert::Infallible;
use std::fmt;
use std::ops::Deref;
#[cfg(feature = "tracing")]
use std::str::FromStr;

/// Metadata wrapper for task contexts.
#[derive(Debug, Clone)]
pub struct Meta<T>(pub T);

impl<T> Deref for Meta<T> {
    type Target = T;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/// Extension trait for types that expose task metadata.
///
/// `MetadataExt` provides a uniform interface for accessing, mutating,
/// injecting, and extracting strongly typed metadata values backed by a
/// [`MetadataStore`].
///
/// This trait is commonly implemented by task contexts, job payloads,
/// execution environments, or workflow state containers that need to
/// carry metadata across system boundaries.
///
/// # Provided Methods
///
/// - [`metadata`](Self::metadata): Returns an immutable reference to the
///   underlying [`MetadataStore`].
/// - [`metadata_mut`](Self::metadata_mut): Returns a mutable reference to
///   the underlying [`MetadataStore`].
/// - [`extract`](Self::extract): Extracts a typed metadata value from the store.
/// - [`inject`](Self::inject): Injects a typed metadata value into the store.
///
/// # Examples
///
/// ```rust
/// # use std::collections::HashMap;
/// # use apalis_core::task::metadata::Metadata;
/// # use apalis_core::task::metadata::MetadataStore;
/// # use apalis_core::task::metadata::MetadataExt;
/// #
/// struct TaskContext {
///     metadata: MetadataStore,
/// }
///
/// impl MetadataExt for TaskContext {
///     fn metadata(&self) -> &MetadataStore {
///         &self.metadata
///     }
///
///     fn metadata_mut(&mut self) -> &mut MetadataStore {
///         &mut self.metadata
///     }
/// }
/// ```
///
/// Injecting and extracting typed metadata:
///
/// ```rust
/// # use std::collections::HashMap;
/// # use apalis_core::task::metadata::Metadata;
/// # use apalis_core::task::metadata::MetadataStore;
/// # use apalis_core::task::metadata::MetadataExt;
/// #
/// #[derive(Debug, PartialEq)]
/// struct RequestId(String);
///
/// impl Metadata for RequestId {
///     type Error = std::convert::Infallible;
///
///     fn inject(&self, metadata: &mut MetadataStore) -> Result<(), Self::Error> {
///         let _ = metadata.insert("request_id", self.0.clone());
///         Ok(())
///     }
///
///     fn extract(metadata: &MetadataStore) -> Result<Self, Self::Error> {
///         Ok(Self(
///             metadata
///                 .get("request_id")
///                 .cloned()
///                 .unwrap_or_default(),
///         ))
///     }
/// }
///
/// struct Context {
///     metadata: MetadataStore,
/// }
///
/// impl MetadataExt for Context {
///     fn metadata(&self) -> &MetadataStore {
///         &self.metadata
///     }
///
///     fn metadata_mut(&mut self) -> &mut MetadataStore {
///         &mut self.metadata
///     }
/// }
///
/// let mut ctx = Context {
///     metadata: MetadataStore::new(),
/// };
///
/// ctx.inject(&RequestId("req-123".into()));
///
/// let request_id = ctx.extract::<RequestId>()?;
///
/// assert_eq!(request_id, RequestId("req-123".into()));
///
/// # Ok::<(), std::convert::Infallible>(())
/// ```
pub trait MetadataExt {
    /// Returns an immutable reference to the underlying metadata store.
    fn metadata(&self) -> &MetadataStore;

    /// Returns a mutable reference to the underlying metadata store.
    fn metadata_mut(&mut self) -> &mut MetadataStore;

    /// Extracts a strongly typed metadata value from the underlying store.
    ///
    /// This method delegates extraction logic to the [`Metadata`] implementation
    /// for `T`.
    ///
    /// # Errors
    ///
    /// Returns `T::Error` if extraction fails.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use std::convert::Infallible;
    /// # use apalis_core::task::metadata::Metadata;
    /// # use apalis_core::task::metadata::MetadataStore;
    /// #
    /// struct UserId(String);
    ///
    /// impl Metadata for UserId {
    ///     type Error = Infallible;
    ///
    ///     fn inject(&self, metadata: &mut MetadataStore) -> Result<(), Self::Error> {
    ///         let _ = metadata.insert("user_id", self.0.clone());
    ///         Ok(())
    ///     }
    ///
    ///     fn extract(metadata: &MetadataStore) -> Result<Self, Self::Error> {
    ///         Ok(Self(
    ///             metadata
    ///                 .get("user_id")
    ///                 .cloned()
    ///                 .unwrap_or_default(),
    ///         ))
    ///     }
    /// }
    /// ```
    fn extract<T: Metadata>(&self) -> Result<T, T::Error> {
        T::extract(self.metadata())
    }

    /// Injects a strongly typed metadata value into the underlying store.
    ///
    /// This method delegates serialization/injection logic to the [`Metadata`]
    /// implementation for `T`.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use apalis_core::task::metadata::Metadata;
    /// # use apalis_core::task::metadata::MetadataStore;
    /// struct CorrelationId(String);
    ///
    /// impl Metadata for CorrelationId {
    ///     type Error = std::convert::Infallible;
    ///
    ///     fn inject(&self, metadata: &mut MetadataStore) -> Result<(), Self::Error> {
    ///         let _ = metadata.insert("correlation_id", self.0.clone());
    ///         Ok(())
    ///     }
    ///
    ///     fn extract(_: &MetadataStore) -> Result<Self, Self::Error> {
    ///         unreachable!()
    ///     }
    /// }
    /// ```
    fn inject<T: Metadata>(&mut self, value: &T) -> Result<(), T::Error> {
        value.inject(self.metadata_mut())
    }
}

/// A lightweight key-value metadata store backed by a `HashMap`.
///
/// `MetadataStore` is designed for storing arbitrary string metadata such as
/// task attributes, labels, annotations, headers, or contextual information.
///
/// Keys are unique within the store. Attempting to insert a duplicate key
/// returns a [`MetadataError::DuplicateKey`] error.
///
/// # Examples
///
/// ```rust
/// # use std::collections::HashMap;
/// # use apalis_core::task::metadata::Metadata;
/// # use apalis_core::task::metadata::MetadataStore;
/// # use apalis_core::task::metadata::MetadataError;
/// #
/// let mut metadata = MetadataStore::new();
///
/// metadata.insert("request_id", "abc-123")?;
/// metadata.insert("environment", "production")?;
///
/// assert_eq!(
///     metadata.get("request_id"),
///     Some(&"abc-123".to_string())
/// );
///
/// assert!(metadata.contains_key("environment"));
///
/// # Ok::<(), MetadataError>(())
/// ```
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MetadataStore(HashMap<String, String>);

/// Errors returned by [`MetadataStore`].
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum MetadataError {
    /// Returned when attempting to insert a key that already exists.
    #[error("The key already exists in the store")]
    DuplicateKey(String),
}

impl MetadataStore {
    /// Creates an empty [`MetadataStore`].
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use apalis_core::task::metadata::Metadata;
    /// # use apalis_core::task::metadata::MetadataStore;
    /// let metadata = MetadataStore::new();
    ///
    /// assert_eq!(metadata.iter().count(), 0);
    /// ```
    #[must_use]
    pub fn new() -> Self {
        Self(HashMap::new())
    }

    /// Inserts a key-value pair into the store.
    ///
    /// Returns an error if the key already exists.
    ///
    /// # Errors
    ///
    /// Returns [`MetadataError::DuplicateKey`] if the provided key is already
    /// present in the store.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use apalis_core::task::metadata::Metadata;
    /// # use apalis_core::task::metadata::MetadataStore;
    /// # use apalis_core::task::metadata::MetadataError;
    /// let mut metadata = MetadataStore::new();
    ///
    /// metadata.insert("region", "us-east-1")?;
    ///
    /// assert_eq!(
    ///     metadata.get("region"),
    ///     Some(&"us-east-1".to_string())
    /// );
    ///
    /// # Ok::<(), MetadataError>(())
    /// ```
    ///
    /// Duplicate keys are rejected:
    ///
    /// ```rust
    /// # use apalis_core::task::metadata::Metadata;
    /// # use apalis_core::task::metadata::MetadataStore;
    /// # use apalis_core::task::metadata::MetadataError;
    /// let mut metadata = MetadataStore::new();
    ///
    /// metadata.insert("service", "api")?;
    ///
    /// let err = metadata.insert("service", "worker").unwrap_err();
    ///
    /// assert_eq!(
    ///     err,
    ///     MetadataError::DuplicateKey("service".to_string())
    /// );
    ///
    /// # Ok::<(), MetadataError>(())
    /// ```
    pub fn insert<K, V>(&mut self, key: K, value: V) -> Result<(), MetadataError>
    where
        K: Into<String>,
        V: Into<String>,
    {
        let key = key.into();

        if self.0.contains_key(&key) {
            return Err(MetadataError::DuplicateKey(key));
        }

        self.0.insert(key, value.into());

        Ok(())
    }

    /// Returns a reference to the value corresponding to the given key.
    ///
    /// Returns `None` if the key does not exist.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use apalis_core::task::metadata::Metadata;
    /// # use apalis_core::task::metadata::MetadataStore;
    /// # use apalis_core::task::metadata::MetadataError;
    /// let mut metadata = MetadataStore::new();
    ///
    /// metadata.insert("version", "1.0")?;
    ///
    /// assert_eq!(
    ///     metadata.get("version"),
    ///     Some(&"1.0".to_string())
    /// );
    ///
    /// assert_eq!(metadata.get("missing"), None);
    ///
    /// # Ok::<(), MetadataError>(())
    /// ```
    #[must_use]
    pub fn get(&self, key: &str) -> Option<&String> {
        self.0.get(key)
    }

    /// Removes a key from the store, returning the stored value if it existed.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use apalis_core::task::metadata::Metadata;
    /// # use apalis_core::task::metadata::MetadataStore;
    /// # use apalis_core::task::metadata::MetadataError;
    /// let mut metadata = MetadataStore::new();
    ///
    /// metadata.insert("token", "secret")?;
    ///
    /// assert_eq!(
    ///     metadata.remove("token"),
    ///     Some("secret".to_string())
    /// );
    ///
    /// assert!(!metadata.contains_key("token"));
    ///
    /// # Ok::<(), MetadataError>(())
    /// ```
    pub fn remove(&mut self, key: &str) -> Option<String> {
        self.0.remove(key)
    }

    /// Returns `true` if the store contains the specified key.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use apalis_core::task::metadata::Metadata;
    /// # use apalis_core::task::metadata::MetadataStore;
    /// # use apalis_core::task::metadata::MetadataError;
    /// let mut metadata = MetadataStore::new();
    ///
    /// metadata.insert("owner", "alice")?;
    ///
    /// assert!(metadata.contains_key("owner"));
    /// assert!(!metadata.contains_key("missing"));
    ///
    /// # Ok::<(), MetadataError>(())
    /// ```
    #[must_use]
    pub fn contains_key(&self, key: &str) -> bool {
        self.0.contains_key(key)
    }

    /// Returns an iterator over all key-value pairs in the store.
    ///
    /// The iterator yields `(&String, &String)` pairs.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use apalis_core::task::metadata::Metadata;
    /// # use apalis_core::task::metadata::MetadataStore;
    /// # use apalis_core::task::metadata::MetadataError;
    /// let mut metadata = MetadataStore::new();
    ///
    /// metadata.insert("a", "1")?;
    /// metadata.insert("b", "2")?;
    ///
    /// let items: Vec<_> = metadata.iter().collect();
    ///
    /// assert_eq!(items.len(), 2);
    ///
    /// # Ok::<(), MetadataError>(())
    /// ```
    pub fn iter(&self) -> impl Iterator<Item = (&String, &String)> {
        self.0.iter()
    }

    /// Consumes the store and returns the underlying `HashMap`.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use apalis_core::task::metadata::Metadata;
    /// # use apalis_core::task::metadata::MetadataStore;
    /// # use apalis_core::task::metadata::MetadataError;
    /// let mut metadata = MetadataStore::new();
    ///
    /// metadata.insert("key", "value")?;
    ///
    /// let inner = metadata.into_inner();
    ///
    /// assert_eq!(
    ///     inner.get("key"),
    ///     Some(&"value".to_string())
    /// );
    ///
    /// # Ok::<(), MetadataError>(())
    /// ```
    #[must_use]
    pub fn into_inner(self) -> HashMap<String, String> {
        self.0
    }

    /// Get a typed metadata entry.
    pub fn extract_as<M: Metadata>(&self) -> Result<M, M::Error> {
        M::extract(self)
    }
}

/// Implemented by types that can be stored as metadata.
/// Provides a stable key and string-based serialization.
pub trait Metadata: Sized {
    /// The error produced when extracting the Metadata
    type Error;

    /// Extract `Metadata` from the store
    fn extract(store: &MetadataStore) -> Result<Self, Self::Error>;

    /// Inject [`Self`] into the store
    fn inject(&self, map: &mut MetadataStore) -> Result<(), Self::Error>;
}

impl<T: Metadata, Args: Send + Sync, Ctx: MetadataExt + Send + Sync, IdType: Send + Sync>
    FromRequest<Task<Args, Ctx, IdType>> for Meta<T>
{
    type Error = T::Error;

    async fn from_request(task: &Task<Args, Ctx, IdType>) -> Result<Self, Self::Error> {
        task.parts.ctx.extract().map(Meta)
    }
}

impl<Args: Send + Sync, Ctx: MetadataExt + Send + Sync, IdType: Send + Sync>
    FromRequest<Task<Args, Ctx, IdType>> for MetadataStore
{
    type Error = Infallible;

    async fn from_request(task: &Task<Args, Ctx, IdType>) -> Result<Self, Self::Error> {
        Ok(task.parts.ctx.metadata().clone())
    }
}

/// Metadata used specifically for storing tracing context.
#[cfg(feature = "tracing")]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Debug, Default, Clone)]
pub struct TracingContext {
    trace_id: Option<String>,
    span_id: Option<String>,
    trace_flags: Option<u8>,
    trace_state: Option<String>,
}

#[cfg(feature = "tracing")]
impl TracingContext {
    /// Create a new empty `TracingContext`.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the trace ID.
    #[must_use]
    pub fn with_trace_id(mut self, trace_id: impl Into<String>) -> Self {
        self.trace_id = Some(trace_id.into());
        self
    }

    /// Set the span ID.
    #[must_use]
    pub fn with_span_id(mut self, span_id: impl Into<String>) -> Self {
        self.span_id = Some(span_id.into());
        self
    }

    /// Set the trace flags.
    #[must_use]
    pub fn with_trace_flags(mut self, trace_flags: u8) -> Self {
        self.trace_flags = Some(trace_flags);
        self
    }

    /// Set the trace state.
    #[must_use]
    pub fn with_trace_state(mut self, trace_state: impl Into<String>) -> Self {
        self.trace_state = Some(trace_state.into());
        self
    }

    /// Get the trace ID.
    #[must_use]
    pub fn trace_id(&self) -> &Option<String> {
        &self.trace_id
    }

    /// Get the span ID.
    #[must_use]
    pub fn span_id(&self) -> &Option<String> {
        &self.span_id
    }

    /// Get the trace flags.
    #[must_use]
    pub fn trace_flags(&self) -> &Option<u8> {
        &self.trace_flags
    }

    /// Get the trace state.
    #[must_use]
    pub fn trace_state(&self) -> &Option<String> {
        &self.trace_state
    }
}

#[cfg(feature = "tracing")]
/// Error provided by parsing TracingContext
#[derive(Debug, thiserror::Error)]
pub enum TracingContextParseError {
    /// Missing Field
    #[error("Missing Field: {0}")]
    MissingField(&'static str),
    /// Invalid flags
    #[error("Invalid flags: {0}")]
    InvalidTraceFlags(std::num::ParseIntError),
    /// Invalid Format
    #[error("Invalid Format")]
    InvalidFormat,
    /// Key {apalis_core.tracing.context} not found in Metadata
    #[error("Key {{apalis_core.tracing.context}} not found in Metadata")]
    MissingKey,
    /// Duplicate entry
    #[error("Duplicate entry: {0}")]
    DuplicateEntry(#[from] MetadataError),
}

// Serialization format: a single W3C traceparent-style string.
//
//   <trace_id>;<span_id>;<trace_flags>;<trace_state>
//
// Each field is either its value or `-` if None.
//
// Example:
//   "4bf92f3577b34da6a3ce929d0e0e4736;00f067aa0ba902b7;01;congo=t61rcWkgMzE"
//   "4bf92f3577b34da6a3ce929d0e0e4736;00f067aa0ba902b7;-;-"
#[cfg(feature = "tracing")]
impl fmt::Display for TracingContext {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{};{};{};{}",
            self.trace_id.as_deref().unwrap_or("-"),
            self.span_id.as_deref().unwrap_or("-"),
            self.trace_flags
                .map(|v| v.to_string())
                .as_deref()
                .unwrap_or("-"),
            self.trace_state.as_deref().unwrap_or("-"),
        )
    }
}

#[cfg(feature = "tracing")]
impl FromStr for TracingContext {
    type Err = TracingContextParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut parts = s.splitn(4, ';');

        let mut next = |field| {
            parts
                .next()
                .ok_or(TracingContextParseError::MissingField(field))
        };

        let trace_id = match next("trace_id")? {
            "-" => None,
            v => Some(v.to_owned()),
        };

        let span_id = match next("span_id")? {
            "-" => None,
            v => Some(v.to_owned()),
        };

        let trace_flags = match next("trace_flags")? {
            "-" => None,
            v => Some(
                v.parse::<u8>()
                    .map_err(TracingContextParseError::InvalidTraceFlags)?,
            ),
        };

        let trace_state = match next("trace_state")? {
            "-" => None,
            v => Some(v.to_owned()),
        };

        Ok(Self {
            trace_id,
            span_id,
            trace_flags,
            trace_state,
        })
    }
}

#[cfg(feature = "tracing")]
const TRACING_CONTENT_KEY: &str = "apalis_core.tracing.context";

#[cfg(feature = "tracing")]
impl Metadata for TracingContext {
    type Error = TracingContextParseError;
    fn extract(store: &MetadataStore) -> Result<Self, Self::Error> {
        store
            .get(TRACING_CONTENT_KEY)
            .ok_or(TracingContextParseError::InvalidFormat)?
            .parse()
    }

    fn inject(&self, map: &mut MetadataStore) -> Result<(), Self::Error> {
        Ok(map.insert(TRACING_CONTENT_KEY, self.to_string())?)
    }
}

#[cfg(test)]
#[allow(unused)]
mod tests {
    use std::{convert::Infallible, fmt::Debug, num::ParseIntError, task::Poll, time::Duration};

    use crate::{
        error::BoxDynError,
        task::{
            Task,
            metadata::{Meta, Metadata, MetadataExt, MetadataStore},
        },
        task_fn::FromRequest,
    };
    use futures_core::future::BoxFuture;
    use tower::Service;

    #[derive(Debug, Clone)]
    struct ExampleService<S> {
        service: S,
    }
    #[derive(Debug, Clone, Default)]
    struct ExampleConfig {
        timeout: Duration,
    }

    const EXAMPLE_CONFIG: &str = "apalis_core.example.config";

    impl Metadata for ExampleConfig {
        type Error = ParseIntError;
        fn extract(store: &MetadataStore) -> Result<Self, Self::Error> {
            let timeout = store
                .get(EXAMPLE_CONFIG)
                .unwrap()
                .parse::<u64>()
                .map(Duration::from_secs)?;
            Ok(ExampleConfig { timeout })
        }

        fn inject(&self, map: &mut MetadataStore) -> Result<(), ParseIntError> {
            let value = self.timeout.as_secs().to_string();
            map.insert(EXAMPLE_CONFIG, value).unwrap();
            Ok(())
        }
    }

    struct SampleStore(MetadataStore);

    impl MetadataExt for SampleStore {
        fn metadata(&self) -> &MetadataStore {
            &self.0
        }

        fn metadata_mut(&mut self) -> &mut MetadataStore {
            &mut self.0
        }
    }

    impl<S, Args: Send + Sync + 'static, Ctx: Send + Sync + 'static, IdType: Send + Sync + 'static>
        Service<Task<Args, Ctx, IdType>> for ExampleService<S>
    where
        S: Service<Task<Args, Ctx, IdType>> + Clone + Send + 'static,
        Ctx: MetadataExt + Send,
        S::Future: Send + 'static,
    {
        type Response = S::Response;
        type Error = S::Error;
        type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

        fn poll_ready(&mut self, cx: &mut std::task::Context<'_>) -> Poll<Result<(), Self::Error>> {
            self.service.poll_ready(cx)
        }

        fn call(&mut self, request: Task<Args, Ctx, IdType>) -> Self::Future {
            let mut svc = self.service.clone();

            // Do something with config
            Box::pin(async move {
                let _config: Meta<ExampleConfig> = request.extract().await.unwrap();
                svc.call(request).await
            })
        }
    }

    #[cfg(feature = "tracing")]
    #[test]
    fn tracing_context_keeps_explicit_fields() {
        let context = crate::task::metadata::TracingContext::new()
            .with_trace_id("4bf92f3577b34da6a3ce929d0e0e4736")
            .with_span_id("00f067aa0ba902b7")
            .with_trace_flags(1)
            .with_trace_state("vendor=acme");

        assert_eq!(
            context.trace_id(),
            &Some("4bf92f3577b34da6a3ce929d0e0e4736".to_string())
        );
        assert_eq!(context.span_id(), &Some("00f067aa0ba902b7".to_string()));
        assert_eq!(context.trace_flags(), &Some(1));
        assert_eq!(context.trace_state(), &Some("vendor=acme".to_string()));
    }
}
