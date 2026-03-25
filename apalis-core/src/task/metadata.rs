//! Task metadata extension trait and implementations
//!
//! The [`MetadataExt`] trait allows injecting and extracting metadata associated with tasks.
//! It includes implementations for common metadata types.
//!
//! ## Overview
//! - `MetadataExt<T>`: A trait for extracting and injecting metadata of type `T`.
//!
//! # Usage
//! Implement the `MetadataExt` trait for your metadata types to enable easy extraction and injection
//! from task contexts. This allows middleware and services to access and modify task metadata in a
//! type-safe manner.
use crate::task::Task;
use crate::task_fn::FromRequest;
use std::ops::Deref;

/// Metadata wrapper for task contexts.
#[derive(Debug, Clone)]
pub struct Meta<T>(pub T);

impl<T> Deref for Meta<T> {
    type Target = T;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/// Task metadata extension trait and implementations.
/// This trait allows for injecting and extracting metadata associated with tasks.
pub trait MetadataExt<T> {
    /// The error type that can occur during extraction or injection.
    type Error;
    /// Extract metadata of type `T`.
    fn extract(&self) -> Result<T, Self::Error>;
    /// Inject metadata of type `T`.
    fn inject(&mut self, value: T) -> Result<(), Self::Error>;
}

impl<T, Args: Send + Sync, Ctx: MetadataExt<T> + Send + Sync, IdType: Send + Sync>
    FromRequest<Task<Args, Ctx, IdType>> for Meta<T>
{
    type Error = Ctx::Error;

    async fn from_request(task: &Task<Args, Ctx, IdType>) -> Result<Self, Self::Error> {
        task.parts.ctx.extract().map(Meta)
    }
}

/// Metadata used specifically for storing the tracing context
#[cfg(feature = "tracing")]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Debug, Default, Clone)]
pub struct TracingContext {
    traceparent: Option<String>,
    tracestate: Option<String>,
}

#[cfg(feature = "tracing")]
impl TracingContext {
    /// Create a new empty `TracingContext`.
    #[must_use]
    fn new() -> Self {
        Self::default()
    }

    /// Get the `traceparent` header value.
    #[must_use]
    pub fn traceparent(&self) -> Option<&str> {
        self.traceparent.as_deref()
    }

    /// Get the `tracestate` header value.
    #[must_use]
    pub fn tracestate(&self) -> Option<&str> {
        self.tracestate.as_deref()
    }

    /// Capture tracing context from the currently active span.
    ///
    /// Returns `None` if no `traceparent` could be extracted from the current context.
    #[must_use]
    pub fn current() -> Self {
        use tracing_opentelemetry::OpenTelemetrySpanExt as _;

        let mut context = Self::new();
        let current = tracing::Span::current().context();
        opentelemetry::global::get_text_map_propagator(|propagator| {
            propagator.inject_context(&current, &mut context);
        });

        context
    }

    /// Restore this context as the OpenTelemetry parent of the provided span.
    pub fn restore(&self, span: &tracing::Span) {
        use opentelemetry::trace::TraceContextExt as _;
        use tracing_opentelemetry::OpenTelemetrySpanExt as _;

        let parent_context =
            opentelemetry::global::get_text_map_propagator(|propagator| propagator.extract(self));

        if parent_context.span().span_context().is_valid() {
            let _ = span.set_parent(parent_context);
        }
    }
}

#[cfg(feature = "tracing")]
impl opentelemetry::propagation::Extractor for TracingContext {
    fn get(&self, key: &str) -> Option<&str> {
        if key.eq_ignore_ascii_case("traceparent") {
            return self.traceparent();
        }
        if key.eq_ignore_ascii_case("tracestate") {
            return self.tracestate();
        }
        None
    }

    fn keys(&self) -> Vec<&str> {
        let mut keys = Vec::with_capacity(2);
        if self.traceparent().is_some() {
            keys.push("traceparent");
        }
        if self.tracestate.is_some() {
            keys.push("tracestate");
        }
        keys
    }
}

#[cfg(feature = "tracing")]
impl opentelemetry::propagation::Injector for TracingContext {
    fn set(&mut self, key: &str, value: String) {
        if key.eq_ignore_ascii_case("traceparent") {
            self.traceparent = Some(value);
        } else if key.eq_ignore_ascii_case("tracestate") {
            self.tracestate = Some(value);
        }
    }
}

#[cfg(test)]
#[allow(unused)]
mod tests {
    use std::{convert::Infallible, fmt::Debug, task::Poll, time::Duration};

    use crate::{
        error::BoxDynError,
        task::{
            Task,
            metadata::{Meta, MetadataExt},
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

    struct SampleStore;

    impl MetadataExt<ExampleConfig> for SampleStore {
        type Error = Infallible;
        fn extract(&self) -> Result<ExampleConfig, Self::Error> {
            Ok(ExampleConfig {
                timeout: Duration::from_secs(1),
            })
        }
        fn inject(&mut self, _: ExampleConfig) -> Result<(), Self::Error> {
            unreachable!()
        }
    }

    impl<S, Args: Send + Sync + 'static, Ctx: Send + Sync + 'static, IdType: Send + Sync + 'static>
        Service<Task<Args, Ctx, IdType>> for ExampleService<S>
    where
        S: Service<Task<Args, Ctx, IdType>> + Clone + Send + 'static,
        Ctx: MetadataExt<ExampleConfig> + Send,
        Ctx::Error: Debug,
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
    fn tracing_context_stores_otel_headers_via_injector() {
        use opentelemetry::propagation::{Extractor as _, Injector as _};

        let mut context = crate::task::metadata::TracingContext::new();
        context.set(
            "traceparent",
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01".to_owned(),
        );
        context.set("tracestate", "vendor=acme".to_string());

        assert_eq!(
            context.get("traceparent"),
            Some("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
        );
        assert_eq!(context.get("tracestate"), Some("vendor=acme"));
    }
}
