#![warn(clippy::pedantic)]
#![deny(warnings)]
//! Observability infrastructure for Tribal: tracing subscriber setup,
//! OTLP export configuration, metric instruments, and structured logging.

mod error;
mod guard;
mod metrics;
mod otlp;
mod recorder;
mod subscriber;

pub use error::TelemetryError;
pub use guard::TelemetryGuard;
pub use recorder::{MetricsRecorder, NoopMetricsRecorder, OtelMetricsRecorder, noop_recorder};
pub use subscriber::init_subscriber;
