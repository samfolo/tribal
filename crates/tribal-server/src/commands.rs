//! Subcommand implementations for the Tribal CLI.
//!
//! Each subcommand lives in its own module. The [`App`](crate::app::App)
//! dispatcher delegates to the corresponding entry-point function,
//! re-exported here as `commands::setup(...)`, `commands::serve(...)`, etc.
//! Modules consumed by `bootstrap` (which composes `setup` and
//! `project::register`) are `pub(crate)` so their internal `run_async`
//! entry points are reachable.

mod bootstrap;
pub(crate) mod common;
pub(crate) mod config;
pub(crate) mod project;
mod serve;
pub(crate) mod setup;
pub(crate) mod token;

pub(crate) use self::{bootstrap::run as bootstrap, serve::run as serve, setup::run as setup};
