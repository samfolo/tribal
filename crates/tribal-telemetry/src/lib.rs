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
pub use metrics::{
    LABEL_MODEL, LABEL_OUTCOME, LABEL_POOL, LABEL_PROVIDER, LABEL_PROVIDER_KEY, LABEL_STAGE,
    LABEL_TASK_TYPE, Metrics,
};
pub use subscriber::init_subscriber;
