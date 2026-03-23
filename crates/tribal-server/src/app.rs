//! Application entry point and subcommand dispatch.

use std::ffi::OsString;

use clap::{CommandFactory, Parser};

use crate::{
    cli::{Cli, Command, ProjectCommand, TokenCommand},
    commands,
    error::AppError,
};

// ---------------------------------------------------------------------------
// App
// ---------------------------------------------------------------------------

/// Top-level application container for the Tribal server.
///
/// Owns the parsed CLI arguments and dispatches to the appropriate subcommand
/// handler.
pub struct App {
    cli: Cli,
}

impl App {
    /// Parses command-line arguments from `std::env::args()` and constructs
    /// the application.
    #[must_use]
    pub fn parse() -> Self {
        Self { cli: Cli::parse() }
    }

    /// Constructs the application from explicit arguments.
    ///
    /// Useful in contexts where reading `std::env::args()` is undesirable,
    /// such as integration tests or embedded usage.
    ///
    /// # Errors
    ///
    /// Returns an [`AppError`] if argument parsing fails.
    pub fn try_from_args<I, T>(args: I) -> Result<Self, AppError>
    where
        I: IntoIterator<Item = T>,
        T: Into<OsString> + Clone,
    {
        Ok(Self {
            cli: Cli::try_parse_from(args)?,
        })
    }

    /// Dispatches to the requested subcommand.
    ///
    /// # Errors
    ///
    /// Returns an [`AppError`] if a subcommand's validation or execution
    /// fails.
    pub fn run(self) -> Result<(), AppError> {
        self.cli.global.validate()?;

        let Some(command) = self.cli.command else {
            Cli::command()
                .print_help()
                .map_err(|source| AppError::HelpOutput { source })?;
            return Ok(());
        };

        match command {
            Command::Setup { args } => {
                commands::setup(&self.cli.global.config, args)?;
            }
            Command::Serve { args } => {
                commands::serve(&self.cli.global.config, args)?;
            }
            Command::Project(command) => match command {
                ProjectCommand::Register { args } => {
                    commands::project::register(&self.cli.global.config, args)?;
                }
                ProjectCommand::List { args } => {
                    commands::project::list(&self.cli.global.config, args)?;
                }
            },
            Command::Token(command) => match command {
                TokenCommand::Create { .. } => {
                    println!("tribal token create: not yet implemented");
                }
                TokenCommand::List { .. } => {
                    println!("tribal token list: not yet implemented");
                }
                TokenCommand::Revoke { .. } => {
                    println!("tribal token revoke: not yet implemented");
                }
                TokenCommand::RevokeAll { .. } => {
                    println!("tribal token revoke-all: not yet implemented");
                }
            },
        }

        Ok(())
    }
}
