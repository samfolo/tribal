#![warn(clippy::pedantic)]
#![deny(warnings)]
//! Observability infrastructure for Tribal: tracing subscriber setup,
//! OTLP export configuration, metric instruments, and structured logging.

mod error;
mod exporter;
mod guard;
mod metrics;
mod otlp;
mod propagation;
mod recorder;
mod subscriber;

pub use error::TelemetryError;
pub use guard::TelemetryGuard;
pub use propagation::{
    INVALID_SESSION_TRACE_ID, INVALID_TRACE_ID, TraceLink, current_span_id, current_trace_context,
    current_trace_id, is_valid_trace_id, link_span_to_ids, parent_span_from_trace_id,
    parent_span_from_traceparent, trace_id_from_traceparent,
};
pub use recorder::{
    InferenceOperationRecord, MetricsRecorder, NoopMetricsRecorder, OtelMetricsRecorder,
    noop_recorder,
};
pub use subscriber::init_subscriber;
