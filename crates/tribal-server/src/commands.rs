//! Subcommand implementations for the Tribal CLI.
//!
//! Each subcommand lives in its own module. The [`App`](crate::app::App)
//! dispatcher delegates to the corresponding entry-point function,
//! re-exported here as `commands::setup(...)`, `commands::serve(...)`, etc.

pub(crate) mod common;
pub(crate) mod project;
mod serve;
mod setup;
pub(crate) mod token;

pub(crate) use self::{serve::run as serve, setup::run as setup};

