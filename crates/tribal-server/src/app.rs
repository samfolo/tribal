//! Application entry point and subcommand dispatch.

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
    /// Parses command-line arguments and constructs the application.
    #[must_use]
    pub fn new() -> Self {
        Self { cli: Cli::parse() }
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
            Command::Setup => {
                println!("tribal setup: not yet implemented");
            }
            Command::Serve { args } => {
                commands::serve::run(&self.cli.global.config, args)?;
            }
            Command::Project(command) => match command {
                ProjectCommand::Register { .. } => {
                    println!("tribal project register: not yet implemented");
                }
                ProjectCommand::List => {
                    println!("tribal project list: not yet implemented");
                }
            },
            Command::Token(command) => match command {
                TokenCommand::Create { .. } => {
                    println!("tribal token create: not yet implemented");
                }
                TokenCommand::List => {
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
