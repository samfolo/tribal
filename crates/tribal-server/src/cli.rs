//! Command-line interface definition for the Tribal server.
//!
//! Re-exports all CLI types used by [`App`](crate::app::App) for argument
//! parsing and subcommand dispatch.

mod command;
mod styles;

pub use command::{
    Cli, Command, ConfigCommand, ConfigShowArgs, ProjectCommand, ProjectListArgs,
    ProjectRegisterArgs, ServeArgs, SetupArgs, TokenCommand, TokenCreateArgs, TokenListArgs,
    TokenRevokeAllArgs, TokenRevokeArgs,
};
