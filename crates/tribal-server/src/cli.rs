//! Command-line interface definition for the Tribal server.
//!
//! Re-exports all CLI types used by [`App`](crate::app::App) for argument
//! parsing and subcommand dispatch.

mod command;
mod default_values;
mod styles;

pub use command::{Cli, Command, ProjectCommand, ServeArgs, SetupArgs, TokenCommand};
