#![warn(clippy::pedantic)]
#![deny(warnings)]
//! Observability infrastructure for Tribal: tracing subscriber setup,
//! OTLP export configuration, span conventions, and structured logging.

mod config;
mod error;
mod guard;
mod subscriber;

pub use config::{LogFormat, LogOutput, LoggingConfig};
pub use error::TelemetryError;
pub use guard::TelemetryGuard;
pub use subscriber::init_subscriber;
