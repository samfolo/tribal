//! Application entry point and subcommand dispatch.

use std::ffi::OsString;

use clap::{CommandFactory, Parser};

use crate::{
    cli::{
        Cli, Command, ConfigCommand, DatabaseCommand, ManagerCommand, ProjectCommand,
        ReindexCommand, ThreadsCommand, TokenCommand,
    },
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
            Command::Bootstrap { args } => {
                commands::bootstrap(&self.cli.global.config, args)?;
            }
            Command::Setup { args } => {
                commands::setup(&self.cli.global.config, args)?;
            }
            Command::Serve { args } => run_async(commands::serve(&self.cli.global.config, args))?,
            Command::Manager(command) => match command {
                ManagerCommand::Run { args } => {
                    run_async(commands::manage(&self.cli.global.config, &args))?;
                }
                ManagerCommand::Shutdown => {
                    run_async(commands::manage_shutdown(&self.cli.global.config))?;
                }
            },
            Command::Runtime(command) => commands::runtime(&self.cli.global.config, &command)?,
            Command::Database(command) => match command {
                DatabaseCommand::Initialise => {
                    commands::database::initialise(&self.cli.global.config)?;
                }
            },
            Command::Project(command) => match command {
                ProjectCommand::Register { args } => {
                    commands::project::register(&self.cli.global.config, args)?;
                }
                ProjectCommand::List { args } => {
                    commands::project::list(&self.cli.global.config, args)?;
                }
            },
            Command::Config(command) => match command {
                ConfigCommand::Show { args } => {
                    commands::config::show(&self.cli.global.config, args)?;
                }
                ConfigCommand::Get { args } => {
                    commands::config::get(&self.cli.global.config, &args)?;
                }
                ConfigCommand::Set { args } => {
                    commands::config::set(&self.cli.global.config, &args)?;
                }
                ConfigCommand::Validate { args } => {
                    commands::config::validate(&self.cli.global.config, &args)?;
                }
                ConfigCommand::Path => {
                    commands::config::path(&self.cli.global.config)?;
                }
            },
            Command::Token(command) => match command {
                TokenCommand::Create { args } => {
                    commands::token::create(&self.cli.global.config, args)?;
                }
                TokenCommand::List { args } => {
                    commands::token::list(&self.cli.global.config, args)?;
                }
                TokenCommand::Revoke { args } => {
                    commands::token::revoke(&self.cli.global.config, args)?;
                }
                TokenCommand::RevokeAll { args } => {
                    commands::token::revoke_all(&self.cli.global.config, args)?;
                }
            },
            Command::McpConfig { args } => {
                commands::mcp_config(&self.cli.global.config, args)?;
            }
            Command::Check { args } => {
                commands::check(&self.cli.global.config, args)?;
            }
            Command::Reindex(command) => match command {
                ReindexCommand::Run { args } => {
                    commands::reindex::run(&self.cli.global.config, args)?;
                }
                ReindexCommand::Cancel { args } => {
                    commands::reindex::cancel(&self.cli.global.config, args)?;
                }
                ReindexCommand::Prune { args } => {
                    commands::reindex::prune(&self.cli.global.config, args)?;
                }
            },
            Command::Threads(command) => match command {
                ThreadsCommand::Prune { args } => {
                    commands::threads::prune(&self.cli.global.config, args)?;
                }
            },
        }

        Ok(())
    }
}

fn run_async(future: impl Future<Output = Result<(), AppError>>) -> Result<(), AppError> {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(|source| AppError::Runtime { source })?
        .block_on(future)
}
