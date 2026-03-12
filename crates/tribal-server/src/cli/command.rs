//! Clap command and argument definitions for the Tribal CLI.

use std::net::SocketAddr;

use clap::{Args, Parser, Subcommand, error::ErrorKind};

use super::{default_values::DEFAULT_CONFIG_PATH, transport::Transport};

// ---------------------------------------------------------------------------
// Root
// ---------------------------------------------------------------------------

/// The Tribal knowledge server.
#[derive(Debug, Parser)]
#[command(name = "tribal", version, about)]
pub struct Cli {
    /// The subcommand to execute.
    #[command(subcommand)]
    pub command: Option<Command>,
}

// ---------------------------------------------------------------------------
// Top-level subcommands
// ---------------------------------------------------------------------------

/// Available subcommands.
#[derive(Debug, Subcommand)]
pub enum Command {
    /// Run first-time database setup and migrations.
    Setup,

    /// Start the MCP server.
    Serve {
        /// Arguments for the serve subcommand.
        #[command(flatten)]
        args: ServeArgs,
    },

    /// Manage projects.
    #[command(subcommand)]
    Project(ProjectCommand),

    /// Manage authentication tokens.
    #[command(subcommand)]
    Token(TokenCommand),
}

// ---------------------------------------------------------------------------
// Serve
// ---------------------------------------------------------------------------

/// Arguments for the `serve` subcommand.
#[derive(Debug, Args)]
pub struct ServeArgs {
    /// Transport protocol for the MCP server.
    #[arg(long, default_value = "stdio")]
    pub transport: Transport,

    /// Project ID (`proj_`-prefixed) to scope the session to.
    #[arg(long)]
    pub project: Option<String>,

    /// Socket address to bind the HTTP/SSE listener to.
    #[arg(long)]
    pub bind: Option<SocketAddr>,

    /// Path to the configuration file.
    #[arg(long, default_value = DEFAULT_CONFIG_PATH)]
    pub config: String,
}

impl ServeArgs {
    /// Validates transport/bind constraints.
    ///
    /// # Errors
    ///
    /// Returns a [`clap::Error`] if `--bind` is supplied with `--transport
    /// stdio`. The `stdio` transport communicates over stdin/stdout and cannot
    /// listen on a network address.
    pub fn validate(&self) -> Result<(), clap::Error> {
        if self.bind.is_some() && self.transport == Transport::Stdio {
            return Err(clap::Error::raw(
                ErrorKind::ArgumentConflict,
                "--bind cannot be used with --transport stdio\n",
            ));
        }
        Ok(())
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

/// Arguments for `project register` (placeholder — defined by ticket 6.8).
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

/// Arguments for `token create` (placeholder — defined by ticket 7.5).
#[derive(Debug, Args)]
pub struct TokenCreateArgs {}

/// Arguments for `token revoke` (placeholder — defined by ticket 7.5).
#[derive(Debug, Args)]
pub struct TokenRevokeArgs {}

/// Arguments for `token revoke-all` (placeholder — defined by ticket 7.5).
#[derive(Debug, Args)]
pub struct TokenRevokeAllArgs {}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use std::net::{Ipv4Addr, SocketAddrV4};

    use clap::Parser;

    use super::*;

    const TEST_BIND_ADDR: &str = "127.0.0.1:7077";

    fn test_bind_addr() -> SocketAddr {
        SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 7077))
    }

    // -- Defaults -----------------------------------------------------------

    #[test]
    fn test_serve_defaults() {
        let cli = Cli::try_parse_from(["tribal", "serve"]).unwrap();
        assert!(matches!(cli.command, Some(Command::Serve { ref args })
            if args.transport == Transport::Stdio
            && args.project.is_none()
            && args.bind.is_none()
            && args.config == DEFAULT_CONFIG_PATH
        ));
    }

    // -- Transport/bind validation ------------------------------------------

    #[test]
    fn test_serve_stdio_with_bind_rejected() {
        let cli = Cli::try_parse_from([
            "tribal",
            "serve",
            "--transport",
            "stdio",
            "--bind",
            TEST_BIND_ADDR,
        ])
        .unwrap();
        let Some(Command::Serve { args }) = cli.command else {
            unreachable!();
        };
        assert!(args.validate().is_err());
    }

    #[test]
    fn test_serve_bind_without_transport_rejected() {
        let cli = Cli::try_parse_from(["tribal", "serve", "--bind", TEST_BIND_ADDR]).unwrap();
        let Some(Command::Serve { args }) = cli.command else {
            unreachable!();
        };
        assert!(args.validate().is_err());
    }

    #[test]
    fn test_serve_http_with_bind_accepted() {
        let cli = Cli::try_parse_from([
            "tribal",
            "serve",
            "--transport",
            "http",
            "--bind",
            TEST_BIND_ADDR,
        ])
        .unwrap();
        let Some(Command::Serve { args }) = cli.command else {
            unreachable!();
        };
        assert!(args.validate().is_ok());
        assert_eq!(args.bind, Some(test_bind_addr()));
    }

    #[test]
    fn test_serve_sse_with_bind_accepted() {
        let cli = Cli::try_parse_from([
            "tribal",
            "serve",
            "--transport",
            "sse",
            "--bind",
            TEST_BIND_ADDR,
        ])
        .unwrap();
        let Some(Command::Serve { args }) = cli.command else {
            unreachable!();
        };
        assert!(args.validate().is_ok());
        assert_eq!(args.bind, Some(test_bind_addr()));
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
