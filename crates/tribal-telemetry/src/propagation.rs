//! W3C traceparent serialisation and deserialisation utilities.
//!
//! Encapsulates the OpenTelemetry propagation API so that consumer
//! crates never import `opentelemetry` or `tracing-opentelemetry`
//! types directly.

use std::collections::HashMap;

use opentelemetry::{propagation::TextMapPropagator, trace::TraceContextExt};
use opentelemetry_sdk::propagation::TraceContextPropagator;
use tracing_opentelemetry::OpenTelemetrySpanExt;

/// Extracts the current span's trace context as a W3C `traceparent` string.
///
/// Returns `None` when no valid OpenTelemetry context is attached to the
/// current span (e.g. OTLP export is disabled or there is no active span).
///
/// Format: `00-{32 hex trace ID}-{16 hex span ID}-{2 hex flags}`
#[must_use]
pub fn current_trace_context() -> Option<String> {
    let span = tracing::Span::current();
    let otel_context = span.context();
    let span_ref = otel_context.span();
    let span_context = span_ref.span_context();

    if !span_context.is_valid() {
        return None;
    }

    Some(format!(
        "00-{}-{}-{:02x}",
        span_context.trace_id(),
        span_context.span_id(),
        span_context.trace_flags(),
    ))
}

/// Outcome of attempting to link a span to a serialised trace context.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TraceLink {
    /// Parent context was successfully linked to the span.
    Linked,
    /// Traceparent was absent, empty, or malformed; span remains a root.
    Invalid,
}

impl TraceLink {
    /// Returns `true` when the trace context was absent or invalid.
    #[must_use]
    pub fn is_invalid(self) -> bool {
        matches!(self, Self::Invalid)
    }
}

/// Sets the parent of `span` from a W3C `traceparent` string.
///
/// Returns [`TraceLink::Linked`] when the traceparent was valid and the
/// parent was set successfully. Returns [`TraceLink::Invalid`] when the
/// traceparent is `None`, empty, or malformed — the span remains a root
/// span and the caller should record `tribal.trace_context.invalid = true`.
#[must_use]
pub fn parent_span_from_traceparent(span: &tracing::Span, traceparent: Option<&str>) -> TraceLink {
    let Some(value) = traceparent.filter(|s| !s.is_empty()) else {
        return TraceLink::Invalid;
    };

    let mut carrier = HashMap::with_capacity(1);
    carrier.insert("traceparent".to_owned(), value.to_owned());

    let propagator = TraceContextPropagator::new();
    let extracted = propagator.extract(&carrier);

    let extracted_span_context = extracted.span().span_context().clone();
    if !extracted_span_context.is_valid() {
        return TraceLink::Invalid;
    }

    // set_parent may fail if the OTel layer is absent; treat as invalid.
    if span.set_parent(extracted).is_err() {
        return TraceLink::Invalid;
    }

    TraceLink::Linked
}

#[cfg(test)]
mod tests {
    use opentelemetry::trace::TracerProvider;
    use opentelemetry_sdk::trace::SdkTracerProvider;
    use tracing_subscriber::layer::SubscriberExt;

    use super::*;

    /// Builds a tracing subscriber with an `OTel` layer backed by an
    /// in-memory tracer provider (no exporter).
    fn otel_subscriber() -> (impl tracing::Subscriber, SdkTracerProvider) {
        let provider = SdkTracerProvider::builder().build();
        let otel_layer = tracing_opentelemetry::layer().with_tracer(provider.tracer("test"));
        let subscriber = tracing_subscriber::registry().with(otel_layer);
        (subscriber, provider)
    }

    #[test]
    fn test_round_trip_serialisation() {
        let (subscriber, _provider) = otel_subscriber();
        tracing::subscriber::with_default(subscriber, || {
            let span = tracing::info_span!("test_parent");
            let _guard = span.enter();

            let traceparent =
                current_trace_context().expect("should produce traceparent with OTel layer");

            // Verify format: 00-{32 hex}-{16 hex}-{2 hex}
            let parts: Vec<&str> = traceparent.split('-').collect();
            assert_eq!(parts.len(), 4, "traceparent should have 4 parts");
            assert_eq!(parts[0], "00", "version should be 00");
            assert_eq!(parts[1].len(), 32, "trace ID should be 32 hex chars");
            assert_eq!(parts[2].len(), 16, "span ID should be 16 hex chars");

            // Deserialise into a child span and verify trace ID matches.
            let child = tracing::info_span!("test_child");
            let result = parent_span_from_traceparent(&child, Some(&traceparent));
            assert_eq!(result, TraceLink::Linked, "valid traceparent should link");

            // The child span's OTel context should share the same trace ID.
            let child_context = child.context();
            let child_span_context = child_context.span().span_context().clone();
            let original_trace_id = parts[1];
            assert_eq!(
                child_span_context.trace_id().to_string(),
                original_trace_id,
                "child should inherit the parent's trace ID",
            );
        });
    }

    #[test]
    fn test_malformed_traceparent_returns_true() {
        let (subscriber, _provider) = otel_subscriber();
        tracing::subscriber::with_default(subscriber, || {
            let span = tracing::info_span!("test_malformed");
            assert!(
                parent_span_from_traceparent(&span, Some("garbage")).is_invalid(),
                "malformed traceparent should be flagged invalid",
            );
        });
    }

    #[test]
    fn test_none_traceparent_returns_true() {
        let (subscriber, _provider) = otel_subscriber();
        tracing::subscriber::with_default(subscriber, || {
            let span = tracing::info_span!("test_none");
            assert!(
                parent_span_from_traceparent(&span, None).is_invalid(),
                "None traceparent should be flagged invalid",
            );
        });
    }

    #[test]
    fn test_empty_traceparent_returns_true() {
        let (subscriber, _provider) = otel_subscriber();
        tracing::subscriber::with_default(subscriber, || {
            let span = tracing::info_span!("test_empty");
            assert!(
                parent_span_from_traceparent(&span, Some("")).is_invalid(),
                "empty traceparent should be flagged invalid",
            );
        });
    }

    #[test]
    fn test_no_otel_layer_returns_none() {
        // Plain subscriber without OTel layer.
        let subscriber = tracing_subscriber::registry();
        tracing::subscriber::with_default(subscriber, || {
            let span = tracing::info_span!("test_no_otel");
            let _guard = span.enter();

            let result = current_trace_context();
            assert!(result.is_none(), "should return None without an OTel layer",);
        });
    }
}
