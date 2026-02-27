#![warn(clippy::pedantic)]
#![deny(warnings)]
//! Write-path pipeline for Tribal: task claiming, extraction, triage,
//! and relation execution, heartbeat management, and dead-lettering.

mod common;
mod config;
mod error;
mod parsing;
mod prompt;
mod stages;
mod worker;

pub use config::{ConfigError, WorkerConfig};
pub use error::WorkerError;
pub use worker::Worker;
