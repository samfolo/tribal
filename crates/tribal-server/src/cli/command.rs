//! Clap command and argument definitions for the Tribal CLI.

use clap::{ArgAction, Args, CommandFactory, Parser, Subcommand, error::ErrorKind};
use tribal_config::{CliOverrides, ServerCliOverrides};

use super::{default_values::DEFAULT_CONFIG_PATH, styles::STYLES, transport::Transport};

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
    Setup,

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
    pub transport: Option<Transport>,

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
                transport: self.transport.map(Into::into),
                bind_address: self.bind,
            }),
        };

        let overrides = CliOverrides { server };
        (overrides, self.project)
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
    List,
}

/// Arguments for `project register`.
#[derive(Debug, Args)]
pub struct ProjectRegisterArgs {}

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

    /// List active tokens.
    List,

    /// Revoke a specific token by prefix.
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
pub struct TokenCreateArgs {}

/// Arguments for `token revoke`.
#[derive(Debug, Args)]
pub struct TokenRevokeArgs {}

/// Arguments for `token revoke-all`.
#[derive(Debug, Args)]
pub struct TokenRevokeAllArgs {}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use clap::{CommandFactory, Parser};
    use tribal_config::TransportKind;

    use super::*;

    const TEST_BIND_ADDR: &str = "127.0.0.1:7077";

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
        let Some(Command::Serve { args }) = cli.command else {
            unreachable!();
        };
        assert_eq!(args.transport, Some(Transport::Http));
    }

    #[test]
    fn test_serve_bind_parsed_as_string() {
        let cli = Cli::try_parse_from(["tribal", "serve", "--bind", TEST_BIND_ADDR]).unwrap();
        let Some(Command::Serve { args }) = cli.command else {
            unreachable!();
        };
        assert_eq!(args.bind.as_deref(), Some(TEST_BIND_ADDR));
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
            transport: Some(Transport::Sse),
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
            bind: Some(TEST_BIND_ADDR.into()),
            project: None,
        };
        let (overrides, _) = args.into_cli_overrides();
        let server = overrides.server.unwrap();
        assert!(server.transport.is_none());
        assert_eq!(server.bind_address.as_deref(), Some(TEST_BIND_ADDR));
    }

    #[test]
    fn test_into_cli_overrides_both_flags() {
        let args = ServeArgs {
            transport: Some(Transport::Http),
            bind: Some(TEST_BIND_ADDR.into()),
            project: None,
        };
        let (overrides, _) = args.into_cli_overrides();
        let server = overrides.server.unwrap();
        assert_eq!(server.transport, Some(TransportKind::Http));
        assert_eq!(server.bind_address.as_deref(), Some(TEST_BIND_ADDR));
    }

    // -- Invalid input ------------------------------------------------------

    #[test]
    fn test_serve_invalid_transport_rejected() {
        let result = Cli::try_parse_from(["tribal", "serve", "--transport", "grpc"]);
        assert!(result.is_err());
    }

    // -- No subcommand ------------------------------------------------------

    #[test]
    fn test_no_subcommand_is_none() {
        let cli = Cli::try_parse_from(["tribal"]).unwrap();
        assert!(cli.command.is_none());
    }
}
