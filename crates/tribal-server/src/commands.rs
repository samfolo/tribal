//! Subcommand implementations for the Tribal CLI.
//!
//! Each subcommand lives in its own module. The [`App`](crate::app::App)
//! dispatcher delegates to the corresponding entry-point function,
//! re-exported here as `commands::setup(...)`, `commands::serve(...)`,
//! etc. The async cores (`*_async`) are surfaced separately so
//! integration tests in downstream crates can drive each flow with
//! explicit writers.

mod bootstrap;
pub(crate) mod common;
pub(crate) mod config;
mod mcp_config;
pub(crate) mod project;
mod serve;
pub(crate) mod setup;
pub(crate) mod token;

pub(crate) use self::{
    bootstrap::run as bootstrap, mcp_config::run as mcp_config, serve::run as serve,
    setup::run as setup,
};
#[cfg(feature = "test-helpers")]
pub use self::{
    bootstrap::{BootstrapOptions, run_async as bootstrap_async},
    mcp_config::{McpConfigOptions, run_async as mcp_config_async},
    setup::{SetupOutcome, run_async as setup_async},
    token::create_async as token_create_async,
};
