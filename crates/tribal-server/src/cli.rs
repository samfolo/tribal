//! Command-line interface definition for the Tribal server.
//!
//! Re-exports all CLI types used by [`App`](crate::app::App) for argument
//! parsing and subcommand dispatch.

mod command;
mod flags;
mod styles;

pub use command::{
    BootstrapArgs, Cli, Command, ConfigCommand, ConfigShowArgs, McpConfigArgs, ProjectCommand,
    ProjectListArgs, ProjectRegisterArgs, ServeArgs, SetupArgs, TokenCommand, TokenCreateArgs,
    TokenListArgs, TokenRevokeAllArgs, TokenRevokeArgs,
};
pub(crate) use flags::PersistableFlag;
