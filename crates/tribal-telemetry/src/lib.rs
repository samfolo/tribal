#![warn(clippy::pedantic)]
#![deny(warnings)]
//! Observability infrastructure for Tribal: tracing subscriber setup,
//! OTLP export configuration, metric instruments, and structured logging.

mod error;
mod guard;
mod metrics;
mod otlp;
mod subscriber;

pub use error::TelemetryError;
pub use guard::TelemetryGuard;
pub use metrics::Metrics;
pub use subscriber::init_subscriber;
