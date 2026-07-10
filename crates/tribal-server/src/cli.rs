//! Command-line interface definition for the Tribal server.
//!
//! Re-exports all CLI types used by [`App`](crate::app::App) for argument
//! parsing and subcommand dispatch.

mod command;
mod flags;
mod styles;

pub use command::{
    BootstrapArgs, CheckArgs, Cli, Command, ConfigCommand, ConfigGetArgs, ConfigSetArgs,
    ConfigShowArgs, ConfigValidateArgs, DatabaseArgs, ManageArgs, McpConfigArgs, ProjectCommand,
    ProjectListArgs, ProjectRegisterArgs, ReindexCommand, ReindexRunArgs, RuntimeCommand,
    ServeArgs, SetupArgs, ThreadsCommand, ThreadsPruneArgs, TokenCommand, TokenCreateArgs,
    TokenListArgs, TokenRevokeAllArgs, TokenRevokeArgs,
};
pub(crate) use flags::PersistableFlag;
