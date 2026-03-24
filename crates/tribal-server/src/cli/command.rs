//! Clap command and argument definitions for the Tribal CLI.

use clap::{ArgAction, Args, CommandFactory, Parser, Subcommand, error::ErrorKind};
use tribal_config::{CliOverrides, DatabaseCliOverrides, ServerCliOverrides, TransportKind};

use super::{default_values::DEFAULT_CONFIG_PATH, styles::STYLES};

// ---------------------------------------------------------------------------
// Long version
// ---------------------------------------------------------------------------

const LONG_VERSION: &str = concat!(
    env!("CARGO_PKG_VERSION"),
    " (",
    env!("TRIBAL_GIT_DESCRIBE"),
    ")",
);

// ---------------------------------------------------------------------------
// Root
// ---------------------------------------------------------------------------

/// A personal knowledge context engine for software development.
#[derive(Debug, Parser)]
#[command(
    name = "tribal",
    version,
    long_version = LONG_VERSION,
    about,
    styles = STYLES,
    infer_subcommands = true,
)]
pub struct Cli {
    /// Global options shared across all subcommands.
    #[command(flatten)]
    pub global: GlobalArgs,

    /// The subcommand to execute.
    #[command(subcommand)]
    pub command: Option<Command>,
}

// ---------------------------------------------------------------------------
// Global arguments
// ---------------------------------------------------------------------------

/// Options available to every subcommand.
#[derive(Debug, Args)]
pub struct GlobalArgs {
    /// Path to the configuration file.
    #[arg(
        long,
        global = true,
        default_value = DEFAULT_CONFIG_PATH,
        env = "TRIBAL_CONFIG_PATH",
        value_name = "PATH",
    )]
    pub config: String,

    /// Increase log verbosity (repeat for more: -v, -vv, -vvv).
    #[arg(short, long, global = true, action = ArgAction::Count)]
    pub verbose: u8,

    /// Suppress non-error output.
    #[arg(short, long, global = true)]
    pub quiet: bool,
}

impl GlobalArgs {
    /// Validates global argument constraints.
    ///
    /// # Errors
    ///
    /// Returns a [`clap::Error`] if both `--verbose` and `--quiet` are
    /// supplied. The two flags are mutually exclusive.
    pub fn validate(&self) -> Result<(), clap::Error> {
        if self.verbose > 0 && self.quiet {
            return Err(Cli::command().error(
                ErrorKind::ArgumentConflict,
                "--verbose and --quiet cannot be used together",
            ));
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Top-level subcommands
// ---------------------------------------------------------------------------

/// Available subcommands.
#[derive(Debug, Subcommand)]
pub enum Command {
    /// Start the MCP server.
    #[command(display_order = 0)]
    Serve {
        /// Arguments for the serve subcommand.
        #[command(flatten)]
        args: ServeArgs,
    },

    /// Run first-time database setup and migrations.
    #[command(display_order = 1)]
    Setup {
        /// Arguments for the setup subcommand.
        #[command(flatten)]
        args: SetupArgs,
    },

    /// Manage projects.
    #[command(subcommand, display_order = 2)]
    Project(ProjectCommand),

    /// Manage authentication tokens.
    #[command(subcommand, display_order = 3)]
    Token(TokenCommand),
}

// ---------------------------------------------------------------------------
// Serve
// ---------------------------------------------------------------------------

/// Arguments for the `serve` subcommand.
///
/// Transport and bind-address environment variables (`TRIBAL_TRANSPORT`,
/// `TRIBAL_BIND_ADDRESS`) are handled by the configuration loading layer,
/// not by clap. Only `--project` retains its `env` attribute because it is
/// session-scoped rather than a configuration value.
#[derive(Debug, Args)]
pub struct ServeArgs {
    /// Transport protocol for the MCP server.
    #[arg(long, help_heading = "Transport")]
    pub transport: Option<TransportKind>,

    /// Socket address to bind the HTTP/SSE listener to.
    #[arg(long, help_heading = "Transport")]
    pub bind: Option<String>,

    /// Project ID (`proj_`-prefixed) to scope the session to.
    #[arg(long, env = "TRIBAL_PROJECT_ID", help_heading = "Session")]
    pub project: Option<String>,
}

impl ServeArgs {
    /// Builds [`CliOverrides`] from explicitly-passed CLI flags.
    ///
    /// Only flags the user actually supplied on the command line are included;
    /// absent flags remain `None` so that lower-precedence layers are not
    /// masked.
    pub fn into_cli_overrides(self) -> (CliOverrides, Option<String>) {
        let server = match (&self.transport, &self.bind) {
            (None, None) => None,
            // Safe to wrap partial `Some` — `skip_serializing_if` on
            // `ServerCliOverrides` fields prevents `None` values from
            // being serialised, so they cannot mask lower-precedence layers.
            _ => Some(ServerCliOverrides {
                transport: self.transport,
                bind_address: self.bind,
            }),
        };

        let overrides = CliOverrides {
            server,
            database: None,
        };
        (overrides, self.project)
    }
}

// ---------------------------------------------------------------------------
// Shared database arguments
// ---------------------------------------------------------------------------

/// Database connection arguments shared across CLI commands.
///
/// Flattened into command-specific args structs via `#[command(flatten)]`.
/// The `into_cli_overrides` method projects the database URL into the
/// figment overlay layer.
#[derive(Debug, Args)]
pub struct DatabaseArgs {
    /// `PostgreSQL` connection URL for the Tribal database.
    #[arg(long = "database-url", short = 'd', help_heading = "Database")]
    pub database_url: Option<String>,
}

impl DatabaseArgs {
    /// Builds [`CliOverrides`] from the database URL flag.
    ///
    /// Only flags the user actually supplied on the command line are included;
    /// absent flags remain `None` so that lower-precedence layers are not
    /// masked.
    pub fn into_cli_overrides(self) -> CliOverrides {
        let database = self
            .database_url
            .map(|url| DatabaseCliOverrides { url: Some(url) });

        CliOverrides {
            server: None,
            database,
        }
    }
}

// ---------------------------------------------------------------------------
// Setup
// ---------------------------------------------------------------------------

/// Arguments for the `setup` subcommand.
#[derive(Debug, Args)]
pub struct SetupArgs {
    /// Database connection options.
    #[command(flatten)]
    pub database: DatabaseArgs,
}

impl SetupArgs {
    /// Builds [`CliOverrides`] from explicitly-passed CLI flags.
    ///
    /// Delegates to [`DatabaseArgs::into_cli_overrides`].
    pub fn into_cli_overrides(self) -> CliOverrides {
        self.database.into_cli_overrides()
    }
}

// ---------------------------------------------------------------------------
// Project
// ---------------------------------------------------------------------------

/// Project management subcommands.
#[derive(Debug, Subcommand)]
pub enum ProjectCommand {
    /// Register a new project.
    Register {
        /// Arguments for project registration.
        #[command(flatten)]
        args: ProjectRegisterArgs,
    },

    /// List all registered projects.
    List {
        /// Arguments for project listing.
        #[command(flatten)]
        args: ProjectListArgs,
    },
}

/// Arguments for `project register`.
#[derive(Debug, Args)]
pub struct ProjectRegisterArgs {
    /// Git remote URL to register. Detected from the current repository
    /// if omitted.
    #[arg(long, help_heading = "Project")]
    pub remote: Option<String>,

    /// Human-friendly project name. Derived from the git remote path
    /// if omitted.
    #[arg(long, help_heading = "Project")]
    pub name: Option<String>,

    /// Default branch name.
    #[arg(long, help_heading = "Project")]
    pub branch: Option<String>,

    /// Database connection options.
    #[command(flatten)]
    pub database: DatabaseArgs,
}

/// Arguments for `project list`.
#[derive(Debug, Args)]
pub struct ProjectListArgs {
    /// Database connection options.
    #[command(flatten)]
    pub database: DatabaseArgs,
}

impl ProjectListArgs {
    /// Builds [`CliOverrides`] from explicitly-passed CLI flags.
    ///
    /// Delegates to [`DatabaseArgs::into_cli_overrides`].
    pub fn into_cli_overrides(self) -> CliOverrides {
        self.database.into_cli_overrides()
    }
}

// ---------------------------------------------------------------------------
// Token
// ---------------------------------------------------------------------------

/// Token management subcommands.
#[derive(Debug, Subcommand)]
pub enum TokenCommand {
    /// Create a new authentication token.
    Create {
        /// Arguments for token creation.
        #[command(flatten)]
        args: TokenCreateArgs,
    },

    /// List all tokens.
    List {
        /// Arguments for token listing.
        #[command(flatten)]
        args: TokenListArgs,
    },

    /// Revoke a specific token by hash prefix.
    Revoke {
        /// Arguments for token revocation.
        #[command(flatten)]
        args: TokenRevokeArgs,
    },

    /// Revoke all tokens.
    RevokeAll {
        /// Arguments for bulk token revocation.
        #[command(flatten)]
        args: TokenRevokeAllArgs,
    },
}

/// Arguments for `token create`.
#[derive(Debug, Args)]
pub struct TokenCreateArgs {
    /// Principal key to associate with the token (e.g. `user:sam`).
    /// Defaults to `principal:local` if omitted.
    #[arg(long, help_heading = "Token")]
    pub principal: Option<String>,

    /// Token lifetime in hours. Overrides the config default for this
    /// token only.
    #[arg(long, help_heading = "Token")]
    pub ttl: Option<u64>,

    /// Database connection options.
    #[command(flatten)]
    pub database: DatabaseArgs,
}

impl TokenCreateArgs {
    /// Builds [`CliOverrides`] from explicitly-passed CLI flags.
    ///
    /// Delegates to [`DatabaseArgs::into_cli_overrides`].
    pub fn into_cli_overrides(self) -> CliOverrides {
        self.database.into_cli_overrides()
    }
}

/// Arguments for `token list`.
#[derive(Debug, Args)]
pub struct TokenListArgs {
    /// Database connection options.
    #[command(flatten)]
    pub database: DatabaseArgs,
}

impl TokenListArgs {
    /// Builds [`CliOverrides`] from explicitly-passed CLI flags.
    ///
    /// Delegates to [`DatabaseArgs::into_cli_overrides`].
    pub fn into_cli_overrides(self) -> CliOverrides {
        self.database.into_cli_overrides()
    }
}

/// Arguments for `token revoke`.
#[derive(Debug, Args)]
pub struct TokenRevokeArgs {
    /// Hash prefix identifying the token to revoke.
    #[arg(value_name = "PREFIX")]
    pub prefix: String,

    /// Database connection options.
    #[command(flatten)]
    pub database: DatabaseArgs,
}

impl TokenRevokeArgs {
    /// Builds [`CliOverrides`] from explicitly-passed CLI flags.
    ///
    /// Delegates to [`DatabaseArgs::into_cli_overrides`].
    pub fn into_cli_overrides(self) -> CliOverrides {
        self.database.into_cli_overrides()
    }
}

/// Arguments for `token revoke-all`.
#[derive(Debug, Args)]
pub struct TokenRevokeAllArgs {
    /// Revoke only tokens belonging to this principal.
    #[arg(long, help_heading = "Token")]
    pub principal: Option<String>,

    /// Database connection options.
    #[command(flatten)]
    pub database: DatabaseArgs,
}

impl TokenRevokeAllArgs {
    /// Builds [`CliOverrides`] from explicitly-passed CLI flags.
    ///
    /// Delegates to [`DatabaseArgs::into_cli_overrides`].
    pub fn into_cli_overrides(self) -> CliOverrides {
        self.database.into_cli_overrides()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use clap::{CommandFactory, Parser};
    use tribal_config::{DEFAULT_BIND_ADDRESS, ENV_CONFIG_PATH, ENV_PROJECT_ID, TransportKind};

    use super::*;

    // -- Structural validation -----------------------------------------------

    #[test]
    fn test_verify_cli() {
        Cli::command().debug_assert();
    }

    // -- Global defaults -----------------------------------------------------

    #[test]
    fn test_global_defaults() {
        let cli = Cli::try_parse_from(["tribal", "serve"]).unwrap();
        assert_eq!(cli.global.config, DEFAULT_CONFIG_PATH);
        assert_eq!(cli.global.verbose, 0);
        assert!(!cli.global.quiet);
    }

    // -- Global validation ---------------------------------------------------

    #[test]
    fn test_verbose_and_quiet_rejected() {
        let cli = Cli::try_parse_from(["tribal", "-v", "-q", "serve"]).unwrap();
        assert!(cli.global.validate().is_err());
    }

    #[test]
    fn test_verbose_without_quiet_accepted() {
        let cli = Cli::try_parse_from(["tribal", "-vv", "serve"]).unwrap();
        assert!(cli.global.validate().is_ok());
        assert_eq!(cli.global.verbose, 2);
    }

    #[test]
    fn test_quiet_without_verbose_accepted() {
        let cli = Cli::try_parse_from(["tribal", "-q", "serve"]).unwrap();
        assert!(cli.global.validate().is_ok());
        assert!(cli.global.quiet);
    }

    // -- Serve defaults ------------------------------------------------------

    #[test]
    fn test_serve_defaults() {
        let cli = Cli::try_parse_from(["tribal", "serve"]).unwrap();
        assert!(matches!(cli.command, Some(Command::Serve { ref args })
            if args.transport.is_none()
            && args.project.is_none()
            && args.bind.is_none()
        ));
    }

    // -- Serve transport/bind parsing ---------------------------------------

    #[test]
    fn test_serve_transport_parsed() {
        let cli = Cli::try_parse_from(["tribal", "serve", "--transport", "http"]).unwrap();
        assert!(matches!(
            cli.command,
            Some(Command::Serve { ref args })
            if args.transport == Some(TransportKind::Http)
        ));
    }

    #[test]
    fn test_serve_bind_parsed_as_string() {
        let cli = Cli::try_parse_from(["tribal", "serve", "--bind", DEFAULT_BIND_ADDRESS]).unwrap();
        assert!(matches!(
            cli.command,
            Some(Command::Serve { ref args })
            if args.bind.as_deref() == Some(DEFAULT_BIND_ADDRESS)
        ));
    }

    // -- into_cli_overrides -------------------------------------------------

    #[test]
    fn test_into_cli_overrides_no_flags() {
        let args = ServeArgs {
            transport: None,
            bind: None,
            project: None,
        };
        let (overrides, project) = args.into_cli_overrides();
        assert!(overrides.server.is_none());
        assert!(project.is_none());
    }

    #[test]
    fn test_into_cli_overrides_transport_only() {
        let args = ServeArgs {
            transport: Some(TransportKind::Sse),
            bind: None,
            project: Some("proj_abc".into()),
        };
        let (overrides, project) = args.into_cli_overrides();
        let server = overrides.server.unwrap();
        assert_eq!(server.transport, Some(TransportKind::Sse));
        assert!(server.bind_address.is_none());
        assert_eq!(project.as_deref(), Some("proj_abc"));
    }

    #[test]
    fn test_into_cli_overrides_bind_only() {
        let args = ServeArgs {
            transport: None,
            bind: Some(DEFAULT_BIND_ADDRESS.into()),
            project: None,
        };
        let (overrides, _) = args.into_cli_overrides();
        let server = overrides.server.unwrap();
        assert!(server.transport.is_none());
        assert_eq!(server.bind_address.as_deref(), Some(DEFAULT_BIND_ADDRESS));
    }

    #[test]
    fn test_into_cli_overrides_both_flags() {
        let args = ServeArgs {
            transport: Some(TransportKind::Http),
            bind: Some(DEFAULT_BIND_ADDRESS.into()),
            project: None,
        };
        let (overrides, _) = args.into_cli_overrides();
        let server = overrides.server.unwrap();
        assert_eq!(server.transport, Some(TransportKind::Http));
        assert_eq!(server.bind_address.as_deref(), Some(DEFAULT_BIND_ADDRESS));
    }

    // -- Invalid input ------------------------------------------------------

    #[test]
    fn test_serve_invalid_transport_rejected() {
        let result = Cli::try_parse_from(["tribal", "serve", "--transport", "grpc"]);
        assert!(result.is_err());
    }

    // -- DatabaseArgs into_cli_overrides ------------------------------------

    #[test]
    fn test_database_args_into_cli_overrides_no_flags() {
        let args = DatabaseArgs { database_url: None };
        let overrides = args.into_cli_overrides();
        assert!(overrides.server.is_none());
        assert!(overrides.database.is_none());
    }

    #[test]
    fn test_database_args_into_cli_overrides_with_url() {
        let args = DatabaseArgs {
            database_url: Some("postgres://h/db".into()),
        };
        let overrides = args.into_cli_overrides();
        let database = overrides.database.unwrap();
        assert_eq!(database.url.as_deref(), Some("postgres://h/db"));
    }

    // -- Setup defaults ------------------------------------------------------

    #[test]
    fn test_setup_parses_without_args() {
        let cli = Cli::try_parse_from(["tribal", "setup"]).unwrap();
        assert!(matches!(cli.command, Some(Command::Setup { ref args })
            if args.database.database_url.is_none()
        ));
    }

    #[test]
    fn test_setup_parses_database_url_long() {
        let cli =
            Cli::try_parse_from(["tribal", "setup", "--database-url", "postgres://h/db"]).unwrap();
        assert!(matches!(
            cli.command,
            Some(Command::Setup { ref args })
            if args.database.database_url.as_deref() == Some("postgres://h/db")
        ));
    }

    #[test]
    fn test_setup_parses_database_url_short() {
        let cli = Cli::try_parse_from(["tribal", "setup", "-d", "postgres://h/db"]).unwrap();
        assert!(matches!(
            cli.command,
            Some(Command::Setup { ref args })
            if args.database.database_url.as_deref() == Some("postgres://h/db")
        ));
    }

    // -- setup into_cli_overrides -------------------------------------------

    #[test]
    fn test_setup_into_cli_overrides_delegates_to_database_args() {
        let args = SetupArgs {
            database: DatabaseArgs {
                database_url: Some("postgres://h/db".into()),
            },
        };
        let overrides = args.into_cli_overrides();
        let database = overrides.database.unwrap();
        assert_eq!(database.url.as_deref(), Some("postgres://h/db"));
    }

    // -- Project register ---------------------------------------------------

    #[test]
    fn test_project_register_parses_without_args() {
        let cli = Cli::try_parse_from(["tribal", "project", "register"]).unwrap();
        assert!(matches!(
            cli.command,
            Some(Command::Project(ProjectCommand::Register { ref args }))
            if args.remote.is_none()
                && args.name.is_none()
                && args.branch.is_none()
                && args.database.database_url.is_none()
        ));
    }

    #[test]
    fn test_project_register_parses_all_flags() {
        let cli = Cli::try_parse_from([
            "tribal",
            "project",
            "register",
            "--remote",
            "git@github.com:user/repo.git",
            "--name",
            "my-project",
            "--branch",
            "develop",
            "-d",
            "postgres://h/db",
        ])
        .unwrap();
        assert!(matches!(
            cli.command,
            Some(Command::Project(ProjectCommand::Register { ref args }))
            if args.remote.as_deref() == Some("git@github.com:user/repo.git")
                && args.name.as_deref() == Some("my-project")
                && args.branch.as_deref() == Some("develop")
                && args.database.database_url.as_deref() == Some("postgres://h/db")
        ));
    }

    // -- Project list -------------------------------------------------------

    #[test]
    fn test_project_list_parses_without_args() {
        let cli = Cli::try_parse_from(["tribal", "project", "list"]).unwrap();
        assert!(matches!(
            cli.command,
            Some(Command::Project(ProjectCommand::List { ref args }))
            if args.database.database_url.is_none()
        ));
    }

    #[test]
    fn test_project_list_parses_database_url() {
        let cli =
            Cli::try_parse_from(["tribal", "project", "list", "-d", "postgres://h/db"]).unwrap();
        assert!(matches!(
            cli.command,
            Some(Command::Project(ProjectCommand::List { ref args }))
            if args.database.database_url.as_deref() == Some("postgres://h/db")
        ));
    }

    // -- project list into_cli_overrides ------------------------------------

    #[test]
    fn test_project_list_into_cli_overrides_delegates_to_database_args() {
        let args = ProjectListArgs {
            database: DatabaseArgs {
                database_url: Some("postgres://h/db".into()),
            },
        };
        let overrides = args.into_cli_overrides();
        let database = overrides.database.unwrap();
        assert_eq!(database.url.as_deref(), Some("postgres://h/db"));
    }

    // -- Token create -------------------------------------------------------

    #[test]
    fn test_token_create_parses_without_flags() {
        let cli = Cli::try_parse_from(["tribal", "token", "create"]).unwrap();
        assert!(matches!(
            cli.command,
            Some(Command::Token(TokenCommand::Create { ref args }))
            if args.principal.is_none()
                && args.ttl.is_none()
                && args.database.database_url.is_none()
        ));
    }

    #[test]
    fn test_token_create_parses_all_flags() {
        let cli = Cli::try_parse_from([
            "tribal",
            "token",
            "create",
            "--principal",
            "user:sam",
            "--ttl",
            "720",
            "-d",
            "postgres://h/db",
        ])
        .unwrap();
        assert!(matches!(
            cli.command,
            Some(Command::Token(TokenCommand::Create { ref args }))
            if args.principal.as_deref() == Some("user:sam")
                && args.ttl == Some(720)
                && args.database.database_url.as_deref() == Some("postgres://h/db")
        ));
    }

    #[test]
    fn test_token_create_into_cli_overrides_maps_database_url() {
        let args = TokenCreateArgs {
            principal: Some("user:sam".into()),
            ttl: Some(24),
            database: DatabaseArgs {
                database_url: Some("postgres://h/db".into()),
            },
        };
        let overrides = args.into_cli_overrides();
        let database = overrides.database.unwrap();
        assert_eq!(database.url.as_deref(), Some("postgres://h/db"));
    }

    #[test]
    fn test_token_create_into_cli_overrides_omits_absent_fields() {
        let args = TokenCreateArgs {
            principal: None,
            ttl: None,
            database: DatabaseArgs { database_url: None },
        };
        let overrides = args.into_cli_overrides();
        assert!(overrides.database.is_none());
    }

    // -- Token list ---------------------------------------------------------

    #[test]
    fn test_token_list_parses_without_flags() {
        let cli = Cli::try_parse_from(["tribal", "token", "list"]).unwrap();
        assert!(matches!(
            cli.command,
            Some(Command::Token(TokenCommand::List { ref args }))
            if args.database.database_url.is_none()
        ));
    }

    #[test]
    fn test_token_list_parses_database_url() {
        let cli =
            Cli::try_parse_from(["tribal", "token", "list", "-d", "postgres://h/db"]).unwrap();
        assert!(matches!(
            cli.command,
            Some(Command::Token(TokenCommand::List { ref args }))
            if args.database.database_url.as_deref() == Some("postgres://h/db")
        ));
    }

    #[test]
    fn test_token_list_into_cli_overrides_maps_database_url() {
        let args = TokenListArgs {
            database: DatabaseArgs {
                database_url: Some("postgres://h/db".into()),
            },
        };
        let overrides = args.into_cli_overrides();
        let database = overrides.database.unwrap();
        assert_eq!(database.url.as_deref(), Some("postgres://h/db"));
    }

    // -- Token revoke -------------------------------------------------------

    #[test]
    fn test_token_revoke_parses_positional_prefix() {
        let cli = Cli::try_parse_from(["tribal", "token", "revoke", "abc12345"]).unwrap();
        assert!(matches!(
            cli.command,
            Some(Command::Token(TokenCommand::Revoke { ref args }))
            if args.prefix == "abc12345"
        ));
    }

    #[test]
    fn test_token_revoke_requires_prefix() {
        let result = Cli::try_parse_from(["tribal", "token", "revoke"]);
        assert!(result.is_err());
    }

    #[test]
    fn test_token_revoke_into_cli_overrides_maps_database_url() {
        let args = TokenRevokeArgs {
            prefix: "abc12345".into(),
            database: DatabaseArgs {
                database_url: Some("postgres://h/db".into()),
            },
        };
        let overrides = args.into_cli_overrides();
        let database = overrides.database.unwrap();
        assert_eq!(database.url.as_deref(), Some("postgres://h/db"));
    }

    // -- Token revoke-all ---------------------------------------------------

    #[test]
    fn test_token_revoke_all_parses_without_flags() {
        let cli = Cli::try_parse_from(["tribal", "token", "revoke-all"]).unwrap();
        assert!(matches!(
            cli.command,
            Some(Command::Token(TokenCommand::RevokeAll { ref args }))
            if args.principal.is_none()
                && args.database.database_url.is_none()
        ));
    }

    #[test]
    fn test_token_revoke_all_parses_principal_flag() {
        let cli = Cli::try_parse_from(["tribal", "token", "revoke-all", "--principal", "user:sam"])
            .unwrap();
        assert!(matches!(
            cli.command,
            Some(Command::Token(TokenCommand::RevokeAll { ref args }))
            if args.principal.as_deref() == Some("user:sam")
        ));
    }

    #[test]
    fn test_token_revoke_all_into_cli_overrides_maps_database_url() {
        let args = TokenRevokeAllArgs {
            principal: Some("user:sam".into()),
            database: DatabaseArgs {
                database_url: Some("postgres://h/db".into()),
            },
        };
        let overrides = args.into_cli_overrides();
        let database = overrides.database.unwrap();
        assert_eq!(database.url.as_deref(), Some("postgres://h/db"));
    }

    // -- No subcommand ------------------------------------------------------

    #[test]
    fn test_no_subcommand_is_none() {
        let cli = Cli::try_parse_from(["tribal"]).unwrap();
        assert!(cli.command.is_none());
    }

    // -- Env var / constant alignment ----------------------------------------

    /// Verifies that the clap `env` attribute on `--config` matches
    /// [`ENV_CONFIG_PATH`].
    #[test]
    fn test_config_env_matches_constant() {
        let cmd = Cli::command();
        let arg = cmd
            .get_arguments()
            .find(|a| a.get_id() == "config")
            .expect("--config arg must exist");
        assert_eq!(
            arg.get_env().expect("--config must have env").to_str(),
            Some(ENV_CONFIG_PATH),
        );
    }

    /// Verifies that the clap `env` attribute on `serve --project` matches
    /// [`ENV_PROJECT_ID`].
    #[test]
    fn test_project_env_matches_constant() {
        let cmd = Cli::command();
        let serve = cmd
            .get_subcommands()
            .find(|s| s.get_name() == "serve")
            .expect("serve subcommand must exist");
        let arg = serve
            .get_arguments()
            .find(|a| a.get_id() == "project")
            .expect("--project arg must exist");
        assert_eq!(
            arg.get_env().expect("--project must have env").to_str(),
            Some(ENV_PROJECT_ID),
        );
    }
}
