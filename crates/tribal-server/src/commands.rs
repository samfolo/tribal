//! Subcommand implementations for the Tribal CLI.
//!
//! Each subcommand lives in its own module. The [`App`](crate::app::App)
//! dispatcher delegates to the corresponding entry-point function,
//! re-exported here as `commands::setup(...)`, `commands::serve(...)`,
//! etc. The async cores (`*_async`) are surfaced separately so
//! integration tests in downstream crates can drive each flow with
//! explicit writers.

mod bootstrap;
mod check;
pub(crate) mod common;
pub(crate) mod config;
pub(crate) mod database;
pub(crate) mod discovery;
pub(crate) mod manage;
mod mcp_config;
mod presentation;
pub(crate) mod project;
pub(crate) mod reindex;
mod runtime;
pub(crate) mod serve;
pub(crate) mod setup;
pub(crate) mod threads;
pub(crate) mod token;

pub(crate) use self::{
    bootstrap::run as bootstrap,
    check::{CheckConfigSource, CheckReportOptions, run as check, run_report_async},
    manage::{run as manage, shutdown as manage_shutdown},
    mcp_config::run as mcp_config,
    runtime::run as runtime,
    serve::run as serve,
    setup::run as setup,
};
#[cfg(feature = "test-helpers")]
pub use self::{
    bootstrap::{BootstrapOptions, run_async as bootstrap_async},
    check::{CheckOptions, CheckOutput, run_async as check_async},
    mcp_config::{McpConfigOptions, TokenStrategy, run_async as mcp_config_async},
    setup::{SetupOutcome, run_async as setup_async},
    token::create_async as token_create_async,
};
