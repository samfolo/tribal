#![warn(clippy::pedantic)]
#![deny(warnings)]
//! Write-path pipeline for Tribal: task claiming, extraction, triage,
//! and relation execution, heartbeat management, and dead-lettering.

mod common;
mod error;
mod parsing;
mod prompt;
mod stages;
mod tag_resolution;
mod worker;

pub use error::WorkerError;
pub use worker::Worker;
