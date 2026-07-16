//! Subcommand implementations for the Tribal CLI.
//!
//! Each subcommand lives in its own module. The [`App`](crate::app::App)
//! dispatcher delegates to thin manager projections or the two process-mode
//! owners for manager and direct server execution.

mod bootstrap;
mod check;
pub(crate) mod config;
pub(crate) mod database;
pub(crate) mod discovery;
pub(crate) mod integration;
pub(crate) mod manage;
mod presentation;
pub(crate) mod processing;
pub(crate) mod project;
pub(crate) mod providers;
pub(crate) mod reindex;
mod runtime;
pub(crate) mod serve;
pub(crate) mod threads;
pub(crate) mod token;

pub(crate) use self::{
    bootstrap::run as bootstrap,
    check::run as check,
    manage::{run as manage, shutdown as manage_shutdown},
    runtime::run as runtime,
    serve::run as serve,
};
