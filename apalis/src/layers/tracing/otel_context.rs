use apalis_core::task::metadata::TracingContext;
use opentelemetry::{global, trace::TraceContextExt as _};
use tracing::Span;
use tracing_opentelemetry::OpenTelemetrySpanExt as _;

/// OpenTelemetry-focused wrapper around [`TracingContext`].
///
/// This type provides a clearer API surface for users that explicitly work with
/// OpenTelemetry context propagation, while preserving `apalis-core` compatibility.
#[derive(Debug, Default, Clone)]
pub struct OtelTraceContext(pub(crate) TracingContext);

impl OtelTraceContext {
    /// Capture context from the currently active tracing span.
    #[must_use]
    pub fn current() -> Self {
        let mut carrier = HeaderCarrier::default();
        let current = Span::current().context();
        global::get_text_map_propagator(|propagator| {
            propagator.inject_context(&current, &mut carrier);
        });

        Self::from(carrier.into_tracing_context())
    }

    /// Restore this context as the OpenTelemetry parent of the provided span.
    pub(crate) fn restore(&self, span: &tracing::Span) {
        let Some(carrier) = HeaderCarrier::from_tracing_context(&self.0) else {
            return;
        };
        let parent_context =
            global::get_text_map_propagator(|propagator| propagator.extract(&carrier));

        if parent_context.span().span_context().is_valid() {
            let _ = span.set_parent(parent_context);
        }
    }
}

impl From<TracingContext> for OtelTraceContext {
    fn from(value: TracingContext) -> Self {
        Self(value)
    }
}

impl From<OtelTraceContext> for TracingContext {
    fn from(value: OtelTraceContext) -> Self {
        value.0
    }
}

#[derive(Debug, Default, Clone)]
struct HeaderCarrier {
    traceparent: String,
    tracestate: Option<String>,
}

impl HeaderCarrier {
    fn from_tracing_context(context: &TracingContext) -> Option<Self> {
        let trace_id = context.trace_id().as_deref()?;
        let span_id = context.span_id().as_deref()?;
        let trace_flags = context.trace_flags().unwrap_or_default();

        Some(Self {
            traceparent: format!("00-{trace_id}-{span_id}-{trace_flags:02x}"),
            tracestate: context.trace_state().clone(),
        })
    }

    fn into_tracing_context(self) -> TracingContext {
        let mut context = TracingContext::new();
        if let Some((trace_id, span_id, trace_flags)) = parse_traceparent(&self.traceparent) {
            context = context
                .with_trace_id(trace_id)
                .with_span_id(span_id)
                .with_trace_flags(trace_flags);
        }
        if let Some(tracestate) = self.tracestate {
            context = context.with_trace_state(tracestate);
        }
        context
    }
}

impl opentelemetry::propagation::Extractor for HeaderCarrier {
    fn get(&self, key: &str) -> Option<&str> {
        if key.eq_ignore_ascii_case("traceparent") {
            return Some(self.traceparent.as_str());
        }
        if key.eq_ignore_ascii_case("tracestate") {
            return self.tracestate.as_deref();
        }
        None
    }

    fn keys(&self) -> Vec<&str> {
        let mut keys = vec!["traceparent"];
        if self.tracestate.is_some() {
            keys.push("tracestate");
        }
        keys
    }
}

impl opentelemetry::propagation::Injector for HeaderCarrier {
    fn set(&mut self, key: &str, value: String) {
        if key.eq_ignore_ascii_case("traceparent") {
            self.traceparent = value;
        } else if key.eq_ignore_ascii_case("tracestate") {
            self.tracestate = Some(value);
        }
    }
}

fn parse_traceparent(value: &str) -> Option<(String, String, u8)> {
    let mut parts = value.split('-');
    let version = parts.next()?;
    let trace_id = parts.next()?;
    let span_id = parts.next()?;
    let trace_flags = parts.next()?;
    if parts.next().is_some() {
        return None;
    }

    if !is_hex_with_len(version, 2)
        || !is_hex_with_len(trace_id, 32)
        || !is_hex_with_len(span_id, 16)
        || !is_hex_with_len(trace_flags, 2)
    {
        return None;
    }

    let trace_flags = u8::from_str_radix(trace_flags, 16).ok()?;
    Some((trace_id.to_owned(), span_id.to_owned(), trace_flags))
}

fn is_hex_with_len(value: &str, expected_len: usize) -> bool {
    value.len() == expected_len && value.as_bytes().iter().all(u8::is_ascii_hexdigit)
}
