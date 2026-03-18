//! Subcommand implementations for the Tribal CLI.
//!
//! Each subcommand lives in its own module. The [`App`](crate::app::App)
//! dispatcher delegates to the corresponding `run` function.

pub(crate) mod common;
pub(crate) mod project;
pub(crate) mod serve;
pub(crate) mod setup;
