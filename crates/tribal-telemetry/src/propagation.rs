//! W3C traceparent serialisation and deserialisation utilities.
//!
//! Encapsulates the OpenTelemetry propagation API so that consumer
//! crates never import `opentelemetry` or `tracing-opentelemetry`
//! types directly.

use std::collections::HashMap;

use opentelemetry::{
    Context,
    propagation::TextMapPropagator,
    trace::{SpanContext, SpanId, TraceContextExt, TraceFlags, TraceId, TraceState},
};
use opentelemetry_sdk::propagation::TraceContextPropagator;
use tracing_opentelemetry::OpenTelemetrySpanExt;

// ---------------------------------------------------------------------------
// Current span context
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
// Trace ID validation and parsing
// ---------------------------------------------------------------------------

/// Error message for an invalid `trace_id` field.
pub const INVALID_TRACE_ID: &str =
    "trace_id must be a valid OpenTelemetry trace ID (32 hex characters)";

/// Error message for an invalid `session_trace_id` field.
pub const INVALID_SESSION_TRACE_ID: &str =
    "session_trace_id must be a valid OpenTelemetry trace ID (32 hex characters)";

/// Returns `true` when `s` is a syntactically valid OpenTelemetry trace ID:
/// exactly 32 hex characters, not all zeros.
///
/// Delegates to [`TraceId::from_hex`] for hex parsing and rejects the
/// all-zero invalid ID.
#[must_use]
pub fn is_valid_trace_id(s: &str) -> bool {
    s.len() == 32 && TraceId::from_hex(s).is_ok_and(|id| id != TraceId::INVALID)
}

/// Extracts the trace ID portion from a W3C `traceparent` string.
///
/// Given `00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01`,
/// returns `Some("4bf92f3577b34da6a3ce929d0e0e4736")`. Returns `None`
/// when the input is not a fully valid W3C traceparent.
///
/// Uses the same `TraceContextPropagator` as [`parent_span_from_traceparent`]
/// so both functions accept and reject the same inputs.
#[must_use]
pub fn trace_id_from_traceparent(traceparent: &str) -> Option<String> {
    let sc = extract_span_context(traceparent)?;
    Some(sc.trace_id().to_string())
}

/// Runs the W3C `TraceContextPropagator` against a traceparent string,
/// returning the extracted `SpanContext` only when fully valid.
///
/// Uses an empty base context so parse failures never fall back to the
/// ambient span.
fn extract_span_context(traceparent: &str) -> Option<SpanContext> {
    let mut carrier = HashMap::with_capacity(1);
    carrier.insert("traceparent".to_owned(), traceparent.to_owned());

    let propagator = TraceContextPropagator::new();
    let extracted = propagator.extract_with_context(&Context::new(), &carrier);
    let sc = extracted.span().span_context().clone();

    if sc.is_valid() { Some(sc) } else { None }
}

// ---------------------------------------------------------------------------
// Span parenting
// ---------------------------------------------------------------------------

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

    let Some(sc) = extract_span_context(value) else {
        return TraceLink::Invalid;
    };

    let parent = Context::new().with_remote_span_context(sc);

    // set_parent may fail if the OTel layer is absent; treat as invalid.
    if span.set_parent(parent).is_err() {
        return TraceLink::Invalid;
    }

    TraceLink::Linked
}

/// Sets the parent of `span` from a bare OpenTelemetry trace ID.
///
/// Constructs a synthetic remote `SpanContext` so the span joins the
/// given trace. Use this when only the trace ID is known (e.g. a
/// `session_trace_id` from a prior discover call) rather than a full
/// W3C traceparent.
///
/// Returns [`TraceLink::Invalid`] when the trace ID is not valid or
/// the parent cannot be set.
#[must_use]
pub fn parent_span_from_trace_id(span: &tracing::Span, trace_id: &str) -> TraceLink {
    let Ok(tid) = TraceId::from_hex(trace_id) else {
        return TraceLink::Invalid;
    };
    if tid == TraceId::INVALID || trace_id.len() != 32 {
        return TraceLink::Invalid;
    }

    let sc = SpanContext::new(
        tid,
        SpanId::from(1u64),
        TraceFlags::SAMPLED,
        true,
        TraceState::default(),
    );
    let parent = Context::new().with_remote_span_context(sc);

    if span.set_parent(parent).is_err() {
        return TraceLink::Invalid;
    }

    TraceLink::Linked
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

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

    // -- current span context -----------------------------------------------

    #[test]
    fn test_round_trip_serialisation() {
        let (subscriber, _provider) = otel_subscriber();
        tracing::subscriber::with_default(subscriber, || {
            let span = tracing::info_span!("test_parent");
            let _guard = span.enter();

            let traceparent =
                current_trace_context().expect("should produce traceparent with OTel layer");

            let parts: Vec<&str> = traceparent.split('-').collect();
            assert_eq!(parts.len(), 4, "traceparent should have 4 parts");
            assert_eq!(parts[0], "00", "version should be 00");
            assert_eq!(parts[1].len(), 32, "trace ID should be 32 hex chars");
            assert_eq!(parts[2].len(), 16, "span ID should be 16 hex chars");

            let child = tracing::info_span!("test_child");
            let result = parent_span_from_traceparent(&child, Some(&traceparent));
            assert_eq!(result, TraceLink::Linked, "valid traceparent should link");

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

    #[test]
    fn test_no_otel_layer_returns_none() {
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

    // -- trace ID validation and parsing ------------------------------------

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

    #[test]
    fn test_trace_id_from_valid_traceparent() {
        let traceparent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
        assert_eq!(
            trace_id_from_traceparent(traceparent).as_deref(),
            Some("4bf92f3577b34da6a3ce929d0e0e4736"),
        );
    }

    #[test]
    fn test_trace_id_from_malformed_traceparent() {
        assert!(trace_id_from_traceparent("garbage").is_none());
    }

    #[test]
    fn test_trace_id_from_partially_valid_traceparent() {
        // Valid trace ID segment but invalid overall structure — must reject.
        assert!(
            trace_id_from_traceparent("garbage-4bf92f3577b34da6a3ce929d0e0e4736-junk").is_none(),
        );
    }

    #[test]
    fn test_trace_id_from_all_zero_traceparent() {
        let traceparent = "00-00000000000000000000000000000000-0000000000000000-00";
        assert!(trace_id_from_traceparent(traceparent).is_none());
    }

    // -- span parenting -----------------------------------------------------

    #[test]
    fn test_malformed_traceparent_is_invalid() {
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
    fn test_none_traceparent_is_invalid() {
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
    fn test_empty_traceparent_is_invalid() {
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
    fn test_parent_span_from_trace_id_joins_trace() {
        let (subscriber, _provider) = otel_subscriber();
        tracing::subscriber::with_default(subscriber, || {
            let trace_id = "4bf92f3577b34da6a3ce929d0e0e4736";
            let span = tracing::info_span!("test_join");
            let result = parent_span_from_trace_id(&span, trace_id);
            assert_eq!(result, TraceLink::Linked);

            let child_context = span.context();
            assert_eq!(
                child_context.span().span_context().trace_id().to_string(),
                trace_id,
                "span should join the provided trace",
            );
        });
    }

    #[test]
    fn test_parent_span_from_invalid_trace_id() {
        let (subscriber, _provider) = otel_subscriber();
        tracing::subscriber::with_default(subscriber, || {
            let span = tracing::info_span!("test_invalid_tid");
            assert!(parent_span_from_trace_id(&span, "not-a-trace-id").is_invalid());
        });
    }
}
