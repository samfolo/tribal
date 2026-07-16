//! Application entry point and subcommand dispatch.

use std::ffi::OsString;

use clap::{CommandFactory, Parser};

use crate::{
    cli::{
        Cli, Command, ConfigCommand, DatabaseCommand, GraphCommand, IntegrationCommand,
        ManagerCommand, ModelsCommand, ProcessingCommand, ProjectCommand, ProvidersCommand,
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
                run_async(commands::bootstrap(&self.cli.global.config, *args))?;
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
            Command::Runtime(command) => {
                run_async(commands::runtime(&self.cli.global.config, &command))?;
            }
            Command::Models(ModelsCommand::List { output }) => {
                run_async(commands::discovery::models(&self.cli.global.config, output))?;
            }
            Command::Providers(command) => match command {
                ProvidersCommand::List { output } => {
                    run_async(commands::providers::list(&self.cli.global.config, output))?;
                }
                ProvidersCommand::Upsert { args } => {
                    run_async(commands::providers::upsert(&self.cli.global.config, args))?;
                }
                ProvidersCommand::Remove { name, output } => {
                    run_async(commands::providers::remove(
                        &self.cli.global.config,
                        name,
                        output,
                    ))?;
                }
                ProvidersCommand::Probe { name, output } => {
                    run_async(commands::providers::probe(
                        &self.cli.global.config,
                        name,
                        output,
                    ))?;
                }
            },
            Command::Processing(command) => match command {
                ProcessingCommand::Show { output } => {
                    run_async(commands::processing::show(&self.cli.global.config, output))?;
                }
                ProcessingCommand::Set {
                    profile,
                    connection,
                    model,
                    output,
                } => run_async(commands::processing::set(
                    &self.cli.global.config,
                    profile,
                    connection,
                    model,
                    output,
                ))?,
            },
            Command::Graph(GraphCommand::GenesisOptions { output }) => {
                run_async(commands::discovery::genesis_options(
                    &self.cli.global.config,
                    output,
                ))?;
            }
            Command::Database(command) => match command {
                DatabaseCommand::Initialise { output } => {
                    run_async(commands::database::initialise(
                        &self.cli.global.config,
                        output,
                    ))?;
                }
            },
            Command::Project(command) => match command {
                ProjectCommand::Register { args } => {
                    run_async(commands::project::register(&self.cli.global.config, args))?;
                }
                ProjectCommand::List { args } => {
                    run_async(commands::project::list(&self.cli.global.config, args))?;
                }
            },
            Command::Config(command) => match command {
                ConfigCommand::Show { args } => {
                    run_async(commands::config::show(&self.cli.global.config, args))?;
                }
                ConfigCommand::Get { args } => {
                    run_async(commands::config::get(&self.cli.global.config, &args))?;
                }
                ConfigCommand::Set { args } => {
                    run_async(commands::config::set(&self.cli.global.config, &args))?;
                }
                ConfigCommand::Validate { args } => {
                    run_async(commands::config::validate(&self.cli.global.config, &args))?;
                }
                ConfigCommand::Path { output } => {
                    run_async(commands::config::path(&self.cli.global.config, output))?;
                }
            },
            Command::Token(command) => match command {
                TokenCommand::Create { args } => {
                    run_async(commands::token::create(&self.cli.global.config, args))?;
                }
                TokenCommand::List { args } => {
                    run_async(commands::token::list(&self.cli.global.config, args))?;
                }
                TokenCommand::Revoke { args } => {
                    run_async(commands::token::revoke(&self.cli.global.config, args))?;
                }
                TokenCommand::RevokeAll { args } => {
                    run_async(commands::token::revoke_all(&self.cli.global.config, args))?;
                }
            },
            Command::Integration(IntegrationCommand::McpConfig { args }) => run_async(
                commands::integration::mcp_config(&self.cli.global.config, args),
            )?,
            Command::Check { args } => {
                run_async(commands::check(&self.cli.global.config, args))?;
            }
            Command::Reindex(command) => match command {
                ReindexCommand::Run { args } => {
                    run_async(commands::reindex::run(&self.cli.global.config, args))?;
                }
                ReindexCommand::Cancel { output } => {
                    run_async(commands::reindex::cancel(&self.cli.global.config, output))?;
                }
                ReindexCommand::Prune { args } => {
                    run_async(commands::reindex::prune(&self.cli.global.config, args))?;
                }
            },
            Command::Threads(command) => match command {
                ThreadsCommand::Prune { args } => {
                    run_async(commands::threads::prune(&self.cli.global.config, args))?;
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
