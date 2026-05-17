#![warn(clippy::pedantic)]
#![deny(warnings)]
//! Tribal server library — programmatic API for the server lifecycle.

mod app;
mod cli;
mod commands;
mod error;
mod git;
mod orchestration;
mod output;
mod startup;
mod transport;

pub use app::App;
#[cfg(feature = "test-helpers")]
pub use commands::{
    BootstrapOptions, McpConfigOptions, bootstrap_async, common::CredentialsPersistOutcome,
    mcp_config_async, token_create_async,
};
pub use error::AppError;
pub use orchestration::{ServerHandle, start_server};
#[cfg(feature = "test-helpers")]
pub use transport::{run_http_transport, run_sse_transport};
