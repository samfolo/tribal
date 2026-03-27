#![warn(clippy::pedantic)]
#![deny(warnings)]
//! Tribal server library — programmatic API for the server lifecycle.

mod app;
mod cli;
mod commands;
mod error;
mod git;
mod orchestration;
mod startup;
mod transport;

pub use app::App;
pub use error::AppError;
pub use orchestration::{ServerHandle, start_server};
#[cfg(feature = "test-helpers")]
pub use startup::{
    PromptTemplateLocation, ensure_prompt_files, load_prompts,
    watcher::reload_single_prompt,
};
#[cfg(feature = "test-helpers")]
pub use transport::run_http_transport;
#[cfg(feature = "test-helpers")]
pub use transport::run_sse_transport;
