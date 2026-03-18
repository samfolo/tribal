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

pub use app::App;
pub use error::AppError;
pub use orchestration::{ServerHandle, start_server};
