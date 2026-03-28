//! W3C traceparent serialisation and deserialisation utilities.
//!
//! Encapsulates the OpenTelemetry propagation API so that consumer
//! crates never import `opentelemetry` or `tracing-opentelemetry`
//! types directly.

use std::collections::HashMap;

use opentelemetry::{
    propagation::TextMapPropagator,
    trace::{SpanContext, TraceContextExt, TraceId},
};
use opentelemetry_sdk::propagation::TraceContextPropagator;
use tracing_opentelemetry::OpenTelemetrySpanExt;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Extracts the `SpanContext` from the current tracing span's OpenTelemetry
/// context, returning `None` when no valid context is attached.
fn current_span_context() -> Option<SpanContext> {
    let span = tracing::Span::current();
    let otel_context = span.context();
    let span_ref = otel_context.span();
    let sc = span_ref.span_context().clone();

    if sc.is_valid() { Some(sc) } else { None }
}

// ---------------------------------------------------------------------------
// Serialisation
// ---------------------------------------------------------------------------

/// Extracts the current span's trace context as a W3C `traceparent` string.
///
/// Returns `None` when no valid OpenTelemetry context is attached to the
/// current span (e.g. OTLP export is disabled or there is no active span).
///
/// Format: `00-{32 hex trace ID}-{16 hex span ID}-{2 hex flags}`
#[must_use]
pub fn current_trace_context() -> Option<String> {
    let sc = current_span_context()?;
    Some(format!(
        "00-{}-{}-{:02x}",
        sc.trace_id(),
        sc.span_id(),
        sc.trace_flags(),
    ))
}

/// Extracts the current span's trace ID as a 32-character lowercase hex
/// string.
///
/// Returns `None` when no valid OpenTelemetry context is attached to the
/// current span (e.g. OTLP export is disabled or there is no active span).
#[must_use]
pub fn current_trace_id() -> Option<String> {
    current_span_context().map(|sc| sc.trace_id().to_string())
}

// ---------------------------------------------------------------------------
// Validation
// ---------------------------------------------------------------------------

/// Returns `true` when `s` is a syntactically valid OpenTelemetry trace ID:
/// exactly 32 hex characters, not all zeros.
///
/// Delegates to [`TraceId::from_hex`] for hex parsing and rejects the
/// all-zero invalid ID.
#[must_use]
pub fn is_valid_trace_id(s: &str) -> bool {
    s.len() == 32 && TraceId::from_hex(s).is_ok_and(|id| id != TraceId::INVALID)
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

            assert!(
                current_trace_context().is_none(),
                "should return None without an OTel layer",
            );
            assert!(
                current_trace_id().is_none(),
                "should return None without an OTel layer",
            );
        });
    }

    // -- current_trace_id ---------------------------------------------------

    #[test]
    fn test_current_trace_id_matches_traceparent() {
        let (subscriber, _provider) = otel_subscriber();
        tracing::subscriber::with_default(subscriber, || {
            let span = tracing::info_span!("test_trace_id");
            let _guard = span.enter();

            let traceparent = current_trace_context().expect("OTel layer present");
            let trace_id = current_trace_id().expect("OTel layer present");

            let traceparent_trace_id = traceparent.split('-').nth(1).unwrap();
            assert_eq!(trace_id, traceparent_trace_id);
            assert!(is_valid_trace_id(&trace_id));
        });
    }

    // -- is_valid_trace_id --------------------------------------------------

    #[test]
    fn test_valid_trace_id() {
        assert!(is_valid_trace_id("4bf92f3577b34da6a3ce929d0e0e4736"));
    }

    #[test]
    fn test_all_zero_trace_id_is_invalid() {
        assert!(!is_valid_trace_id("00000000000000000000000000000000"));
    }

    #[test]
    fn test_short_hex_is_invalid() {
        assert!(!is_valid_trace_id("4bf92f35"));
    }

    #[test]
    fn test_non_hex_is_invalid() {
        assert!(!is_valid_trace_id("my-trace-42-not-a-valid-trace-id"));
    }

    #[test]
    fn test_uppercase_hex_is_valid() {
        // TraceId::from_hex accepts uppercase; we delegate to OTel.
        assert!(is_valid_trace_id("4BF92F3577B34DA6A3CE929D0E0E4736"));
    }
}
