//! Clap command and argument definitions for the Tribal CLI.

use clap::{ArgAction, Args, CommandFactory, Parser, Subcommand, error::ErrorKind};
use tribal_config::{
    CliOverrides, DatabaseCliOverrides, EmbeddingCliOverrides, InferenceCliOverrides,
    InferenceStageCliOverrides, ServerCliOverrides, TelemetryCliOverrides, TransportKind,
    default_config_file_path,
};
use tribal_domain::ProviderKind;

use super::{flags::PersistableFlag, styles::STYLES};

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
        default_value_t = default_config_file_path(),
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
    /// Bootstrap a project end-to-end: database setup, token, project
    /// registration, and MCP wire-up snippet in one invocation.
    #[command(display_order = 0)]
    Bootstrap {
        /// Arguments for the bootstrap subcommand.
        #[command(flatten)]
        args: BootstrapArgs,
    },

    /// Run readiness diagnostics across config, database, project,
    /// token, advertised URL, and binary uniqueness.
    #[command(display_order = 1)]
    Check {
        /// Arguments for the check subcommand.
        #[command(flatten)]
        args: CheckArgs,
    },

    /// Start the MCP server.
    #[command(display_order = 2)]
    Serve {
        /// Arguments for the serve subcommand.
        #[command(flatten)]
        args: ServeArgs,
    },

    /// Run first-time database setup and migrations.
    #[command(display_order = 3)]
    Setup {
        /// Arguments for the setup subcommand.
        #[command(flatten)]
        args: SetupArgs,
    },

    /// Manage projects.
    #[command(subcommand, display_order = 4)]
    Project(ProjectCommand),

    /// Manage authentication tokens.
    #[command(subcommand, display_order = 5)]
    Token(TokenCommand),

    /// Interact with the resolved configuration.
    #[command(subcommand, display_order = 6)]
    Config(ConfigCommand),

    /// Print an MCP server-config entry for the active project to
    /// stdout.
    #[command(name = "mcp-config", display_order = 7)]
    McpConfig {
        /// Arguments for the mcp-config subcommand.
        #[command(flatten)]
        args: McpConfigArgs,
    },
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
            ..CliOverrides::default()
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
#[derive(Debug, Default, Args)]
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
            database,
            ..CliOverrides::default()
        }
    }
}

// ---------------------------------------------------------------------------
// Shared provider arguments
// ---------------------------------------------------------------------------

/// Provider and model selection flags shared across CLI commands.
///
/// Flattened into command-specific args structs via `#[command(flatten)]`.
/// The `into_cli_overrides` method projects each flag into the figment
/// overlay layer.
///
/// API keys are intentionally absent: the cascade picks them up from
/// `OPENAI_API_KEY` / `ANTHROPIC_API_KEY` or from `TRIBAL_*__API_KEY`,
/// never from flags.
#[derive(Debug, Default, Args)]
pub struct ProviderArgs {
    /// Embedding provider override.
    #[arg(
        long = PersistableFlag::EmbeddingProvider.flag_name(),
        value_parser = clap::value_parser!(ProviderKind),
        help_heading = "Providers",
    )]
    pub embedding_provider: Option<ProviderKind>,

    /// Embedding model name override.
    #[arg(
        long = PersistableFlag::EmbeddingModel.flag_name(),
        help_heading = "Providers",
    )]
    pub embedding_model: Option<String>,

    /// Extraction-stage inference provider override.
    #[arg(
        long = PersistableFlag::InferenceExtractionProvider.flag_name(),
        value_parser = clap::value_parser!(ProviderKind),
        help_heading = "Providers",
    )]
    pub inference_extraction_provider: Option<ProviderKind>,

    /// Extraction-stage inference model name override.
    #[arg(
        long = PersistableFlag::InferenceExtractionModel.flag_name(),
        help_heading = "Providers",
    )]
    pub inference_extraction_model: Option<String>,

    /// Triage-stage inference provider override.
    #[arg(
        long = PersistableFlag::InferenceTriageProvider.flag_name(),
        value_parser = clap::value_parser!(ProviderKind),
        help_heading = "Providers",
    )]
    pub inference_triage_provider: Option<ProviderKind>,

    /// Triage-stage inference model name override.
    #[arg(
        long = PersistableFlag::InferenceTriageModel.flag_name(),
        help_heading = "Providers",
    )]
    pub inference_triage_model: Option<String>,

    /// Relation-stage inference provider override.
    #[arg(
        long = PersistableFlag::InferenceRelationProvider.flag_name(),
        value_parser = clap::value_parser!(ProviderKind),
        help_heading = "Providers",
    )]
    pub inference_relation_provider: Option<ProviderKind>,

    /// Relation-stage inference model name override.
    #[arg(
        long = PersistableFlag::InferenceRelationModel.flag_name(),
        help_heading = "Providers",
    )]
    pub inference_relation_model: Option<String>,
}

impl ProviderArgs {
    /// Builds [`CliOverrides`] from explicitly-passed CLI flags.
    ///
    /// Each subtree (`embedding`, `inference.*`) is populated only when at
    /// least one of its flags was supplied, so absent flags never mask
    /// lower-precedence layers.
    pub fn into_cli_overrides(self) -> CliOverrides {
        let embedding = match (self.embedding_provider, self.embedding_model) {
            (None, None) => None,
            (provider, model) => Some(EmbeddingCliOverrides { provider, model }),
        };

        let extraction = inference_stage_overrides(
            self.inference_extraction_provider,
            self.inference_extraction_model,
        );
        let triage =
            inference_stage_overrides(self.inference_triage_provider, self.inference_triage_model);
        let relation = inference_stage_overrides(
            self.inference_relation_provider,
            self.inference_relation_model,
        );

        let inference = if extraction.is_none() && triage.is_none() && relation.is_none() {
            None
        } else {
            Some(InferenceCliOverrides {
                extraction,
                triage,
                relation,
            })
        };

        CliOverrides {
            embedding,
            inference,
            ..CliOverrides::default()
        }
    }
}

/// Projects a `(provider, model)` pair into an
/// [`InferenceStageCliOverrides`], returning `None` when both are absent so
/// the subtree is omitted from the figment overlay.
fn inference_stage_overrides(
    provider: Option<ProviderKind>,
    model: Option<String>,
) -> Option<InferenceStageCliOverrides> {
    match (provider, model) {
        (None, None) => None,
        (provider, model) => Some(InferenceStageCliOverrides { provider, model }),
    }
}

// ---------------------------------------------------------------------------
// Shared telemetry arguments
// ---------------------------------------------------------------------------

/// Telemetry flags shared across CLI commands.
///
/// Flattened into command-specific args structs via `#[command(flatten)]`.
#[derive(Debug, Default, Args)]
pub struct TelemetryArgs {
    /// OTLP exporter endpoint override.
    #[arg(
        long = PersistableFlag::TelemetryOtlpEndpoint.flag_name(),
        help_heading = "Telemetry",
    )]
    pub telemetry_otlp_endpoint: Option<String>,
}

impl TelemetryArgs {
    /// Builds [`CliOverrides`] from explicitly-passed CLI flags.
    pub fn into_cli_overrides(self) -> CliOverrides {
        let telemetry = self
            .telemetry_otlp_endpoint
            .map(|otlp_endpoint| TelemetryCliOverrides {
                otlp_endpoint: Some(otlp_endpoint),
            });
        CliOverrides {
            telemetry,
            ..CliOverrides::default()
        }
    }
}

// ---------------------------------------------------------------------------
// Bootstrap
// ---------------------------------------------------------------------------

/// Arguments for the `bootstrap` subcommand.
///
/// Flattens [`DatabaseArgs`], [`ProviderArgs`], and [`TelemetryArgs`]
/// alongside its own session-scoped flags so a single invocation can mint
/// a token, register a project, and emit the wire-up snippet.
#[derive(Debug, Default, Args)]
pub struct BootstrapArgs {
    /// Transport mode for the generated MCP config snippet. Controls
    /// the snippet shape: stdio uses `command`/`args`, while http and
    /// sse use `url` with optional `headers`. Defaults to stdio.
    #[arg(long, help_heading = "Bootstrap")]
    pub transport: Option<TransportKind>,

    /// Git remote URL to register. Detected from the current repository
    /// if omitted.
    #[arg(long, help_heading = "Bootstrap")]
    pub remote: Option<String>,

    /// Human-friendly project name. Derived from the git remote path
    /// if omitted.
    #[arg(long, help_heading = "Bootstrap")]
    pub name: Option<String>,

    /// Principal key to associate with the bearer token (e.g.
    /// `user:sam`). Defaults to `principal:local` if omitted; the
    /// `principal:local` row is always ensured regardless.
    #[arg(long, help_heading = "Bootstrap")]
    pub principal: Option<String>,

    /// Token lifetime in hours. Overrides the config default for this
    /// token only.
    #[arg(long, help_heading = "Bootstrap")]
    pub ttl: Option<u64>,

    /// Emit a single JSON object describing the resolved wire-up
    /// (bearer token, project, MCP snippet) instead of the polished
    /// human output. Suitable for scripting.
    #[arg(long, help_heading = "Output")]
    pub json: bool,

    /// Database connection options.
    #[command(flatten)]
    pub database: DatabaseArgs,

    /// Provider and model selection.
    #[command(flatten)]
    pub provider: ProviderArgs,

    /// Telemetry options.
    #[command(flatten)]
    pub telemetry: TelemetryArgs,
}

impl BootstrapArgs {
    /// Builds [`CliOverrides`] by delegating to each flattened arg's
    /// own impl and overlaying the populated subtrees.
    ///
    /// Each constituent `into_cli_overrides` populates only its own
    /// section, so combining them is a flat field-by-field assembly
    /// rather than a deep merge.
    pub fn into_cli_overrides(self) -> CliOverrides {
        let Self {
            transport,
            remote: _,
            name: _,
            principal: _,
            ttl: _,
            json: _,
            database,
            provider,
            telemetry,
        } = self;

        let database = database.into_cli_overrides();
        let provider = provider.into_cli_overrides();
        let telemetry = telemetry.into_cli_overrides();

        // `--transport` must flow into the validated in-memory config so
        // `validate_server` reconciles the choice against `bind_address`
        // (which may be set via env/file). `CliOverrides::persisted()`
        // drops server fields, so this affects only the live invocation,
        // not the first-run config file.
        let server = transport.map(|t| ServerCliOverrides {
            transport: Some(t),
            bind_address: None,
        });

        CliOverrides {
            server,
            database: database.database,
            embedding: provider.embedding,
            inference: provider.inference,
            telemetry: telemetry.telemetry,
        }
    }
}

// ---------------------------------------------------------------------------
// Check
// ---------------------------------------------------------------------------

/// Arguments for the `check` subcommand.
#[derive(Debug, Default, Args)]
pub struct CheckArgs {
    /// Run fatal probes against the configured embedding and
    /// inference providers.
    #[arg(long, help_heading = "Check")]
    pub providers: bool,

    /// Project ID to verify directly, bypassing the
    /// `TRIBAL_PROJECT_ID` / git-remote cascade.
    #[arg(long, help_heading = "Check")]
    pub project: Option<String>,

    /// Bearer token to verify, overriding the
    /// `TRIBAL_AUTH_TOKEN` / `credentials.json` resolution order.
    #[arg(long, help_heading = "Check")]
    pub token: Option<String>,

    /// Emit a single JSON object on stdout instead of the human
    /// form on stderr.
    #[arg(long, help_heading = "Output")]
    pub json: bool,
}

// ---------------------------------------------------------------------------
// Setup
// ---------------------------------------------------------------------------

/// Arguments for the `setup` subcommand.
#[derive(Debug, Args)]
pub struct SetupArgs {
    /// Principal key to associate with the bearer token (e.g.
    /// `user:sam`). Defaults to `principal:local` if omitted; the
    /// `principal:local` row is always ensured regardless.
    #[arg(long, help_heading = "Setup")]
    pub principal: Option<String>,

    /// Token lifetime in hours. Overrides the config default for this
    /// token only.
    #[arg(long, help_heading = "Setup")]
    pub ttl: Option<u64>,

    /// Database connection options.
    #[command(flatten)]
    pub database: DatabaseArgs,
}

impl SetupArgs {
    /// Builds [`CliOverrides`] from explicitly-passed CLI flags.
    ///
    /// `--principal` and `--ttl` affect only the bearer token minted by
    /// this setup run, so they do not appear in [`CliOverrides`].
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

    /// Output a bare MCP server config entry as JSON to stdout,
    /// suitable for piping into `claude mcp add-json`. The snippet
    /// shape varies by transport.
    #[arg(long, help_heading = "Output")]
    pub json: bool,

    /// Transport mode for the generated MCP config snippet. Controls
    /// the snippet shape: stdio uses `command`/`args`, while http and
    /// sse use `url` with optional `headers`. Defaults to stdio.
    #[arg(long, help_heading = "Output")]
    pub transport: Option<TransportKind>,

    /// Bearer token to embed in HTTP/SSE config snippets. Validated
    /// against the database unless `--skip-validation` is set. Falls
    /// back to the `TRIBAL_AUTH_TOKEN` environment variable if omitted.
    #[arg(long, help_heading = "Output")]
    pub token: Option<String>,

    /// Skip database validation of the bearer token. Use when the
    /// token belongs to a different environment or when embedding a
    /// value that will be resolved later. Also permits generating
    /// HTTP/SSE snippets without a token, but note that the default
    /// server configuration requires bearer auth — a tokenless
    /// snippet will need manual auth configuration to work.
    #[arg(long, help_heading = "Output")]
    pub skip_validation: bool,

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
// Config
// ---------------------------------------------------------------------------

/// Configuration subcommands.
#[derive(Debug, Subcommand)]
pub enum ConfigCommand {
    /// Print the fully resolved configuration as YAML. Sensitive fields
    /// (database URL, API keys) are redacted unless `--show-secrets`
    /// is passed.
    Show {
        /// Arguments for config show.
        #[command(flatten)]
        args: ConfigShowArgs,
    },
}

/// Arguments for `config show`.
#[derive(Debug, Clone, Copy, Args)]
pub struct ConfigShowArgs {
    /// Reveal sensitive values (database URL, API keys) instead of
    /// redacting them.
    #[arg(long)]
    pub show_secrets: bool,
}

// ---------------------------------------------------------------------------
// MCP config
// ---------------------------------------------------------------------------

/// Arguments for the `mcp-config` subcommand.
///
/// Renders the wire-up snippet bootstrap emits, reconstructed from the
/// resolved project and the persisted bearer token. The database is
/// reached through the same overlay cascade as every other command so
/// `--project` typos surface immediately rather than at server start.
#[derive(Debug, Args)]
pub struct McpConfigArgs {
    /// Transport mode for the generated snippet. Falls back to
    /// `server.transport` from the resolved configuration when omitted.
    #[arg(long, help_heading = "Output")]
    pub transport: Option<TransportKind>,

    /// Project ID (`proj_`-prefixed) to render the snippet for. Falls
    /// back to `TRIBAL_PROJECT_ID` and then to git-remote detection.
    #[arg(long, env = "TRIBAL_PROJECT_ID", help_heading = "Session")]
    pub project: Option<String>,

    /// Bearer token override for http/sse snippets: embeds this exact
    /// token. Ignored for stdio.
    #[arg(long, help_heading = "Output")]
    pub token: Option<String>,

    /// Embed the persisted static bearer token in the http/sse snippet.
    ///
    /// The default http/sse snippet on a loopback deployment is URL-only
    /// and relies on OAuth, which suits OAuth-capable harnesses. Pass this
    /// for a harness that authenticates only with a static `Authorization`
    /// header and so cannot perform the OAuth flow. Mutually exclusive with
    /// `--token`. Ignored for stdio.
    #[arg(long, conflicts_with = "token", help_heading = "Output")]
    pub static_token: bool,

    /// Database connection options.
    #[command(flatten)]
    pub database: DatabaseArgs,
}

impl McpConfigArgs {
    /// Builds [`CliOverrides`] from explicitly-passed CLI flags.
    ///
    /// `--transport`, `--project`, `--token`, and `--static-token` affect
    /// only this single rendering and do not flow into [`CliOverrides`].
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
        assert_eq!(cli.global.config, default_config_file_path());
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

    // -- BootstrapArgs into_cli_overrides -----------------------------------

    #[test]
    fn test_bootstrap_into_cli_overrides_no_transport() {
        let overrides = BootstrapArgs::default().into_cli_overrides();
        assert!(overrides.server.is_none());
    }

    #[test]
    fn test_bootstrap_into_cli_overrides_threads_transport() {
        let args = BootstrapArgs {
            transport: Some(TransportKind::Http),
            ..BootstrapArgs::default()
        };
        let overrides = args.into_cli_overrides();
        let server = overrides
            .server
            .expect("transport override populates server slot");
        assert_eq!(server.transport, Some(TransportKind::Http));
        assert!(
            server.bind_address.is_none(),
            "bootstrap never overrides bind_address",
        );
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

    // -- ProviderArgs / TelemetryArgs ---------------------------------------

    /// Parser harness flattening [`ProviderArgs`] for standalone clap tests.
    #[derive(Debug, Parser)]
    #[command(no_binary_name = true)]
    struct ProviderArgsHarness {
        #[command(flatten)]
        args: ProviderArgs,
    }

    /// Parser harness flattening [`TelemetryArgs`] for standalone clap tests.
    #[derive(Debug, Parser)]
    #[command(no_binary_name = true)]
    struct TelemetryArgsHarness {
        #[command(flatten)]
        args: TelemetryArgs,
    }

    #[test]
    fn test_provider_args_parses_all_flags() {
        let parsed = ProviderArgsHarness::try_parse_from([
            "--embedding-provider",
            "openai",
            "--embedding-model",
            "text-embedding-3-small",
            "--inference-extraction-provider",
            "anthropic",
            "--inference-extraction-model",
            "claude-opus-4",
            "--inference-triage-provider",
            "openai",
            "--inference-triage-model",
            "gpt-5",
            "--inference-relation-provider",
            "anthropic",
            "--inference-relation-model",
            "claude-haiku-5",
        ])
        .unwrap();
        let args = parsed.args;
        assert_eq!(args.embedding_provider, Some(ProviderKind::OpenAi));
        assert_eq!(
            args.embedding_model.as_deref(),
            Some("text-embedding-3-small"),
        );
        assert_eq!(
            args.inference_extraction_provider,
            Some(ProviderKind::Anthropic),
        );
        assert_eq!(
            args.inference_extraction_model.as_deref(),
            Some("claude-opus-4"),
        );
        assert_eq!(args.inference_triage_provider, Some(ProviderKind::OpenAi));
        assert_eq!(args.inference_triage_model.as_deref(), Some("gpt-5"));
        assert_eq!(
            args.inference_relation_provider,
            Some(ProviderKind::Anthropic),
        );
        assert_eq!(
            args.inference_relation_model.as_deref(),
            Some("claude-haiku-5"),
        );
    }

    #[test]
    fn test_provider_args_invalid_provider_rejected() {
        let result = ProviderArgsHarness::try_parse_from(["--embedding-provider", "grpc"]);
        assert!(result.is_err());
    }

    #[test]
    fn test_provider_args_into_cli_overrides_no_flags() {
        let parsed = ProviderArgsHarness::try_parse_from(std::iter::empty::<&str>()).unwrap();
        let overrides = parsed.args.into_cli_overrides();
        assert!(overrides.embedding.is_none());
        assert!(overrides.inference.is_none());
    }

    #[test]
    fn test_provider_args_into_cli_overrides_populated() {
        let parsed = ProviderArgsHarness::try_parse_from([
            "--embedding-provider",
            "openai",
            "--inference-triage-model",
            "gpt-5",
        ])
        .unwrap();
        let overrides = parsed.args.into_cli_overrides();

        let embedding = overrides.embedding.expect("embedding subtree populated");
        assert_eq!(embedding.provider, Some(ProviderKind::OpenAi));
        assert!(embedding.model.is_none());

        let inference = overrides.inference.expect("inference subtree populated");
        assert!(inference.extraction.is_none());
        assert!(inference.relation.is_none());
        let triage = inference.triage.expect("triage stage populated");
        assert!(triage.provider.is_none());
        assert_eq!(triage.model.as_deref(), Some("gpt-5"));
    }

    #[test]
    fn test_telemetry_args_parses_endpoint() {
        let parsed = TelemetryArgsHarness::try_parse_from([
            "--telemetry-otlp-endpoint",
            "http://collector.internal:4317",
        ])
        .unwrap();
        assert_eq!(
            parsed.args.telemetry_otlp_endpoint.as_deref(),
            Some("http://collector.internal:4317"),
        );
    }

    #[test]
    fn test_telemetry_args_into_cli_overrides_populated() {
        let parsed = TelemetryArgsHarness::try_parse_from([
            "--telemetry-otlp-endpoint",
            "http://collector.internal:4317",
        ])
        .unwrap();
        let overrides = parsed.args.into_cli_overrides();
        let telemetry = overrides.telemetry.expect("telemetry subtree populated");
        assert_eq!(
            telemetry.otlp_endpoint.as_deref(),
            Some("http://collector.internal:4317"),
        );
    }

    #[test]
    fn test_telemetry_args_into_cli_overrides_no_flags() {
        let parsed = TelemetryArgsHarness::try_parse_from(std::iter::empty::<&str>()).unwrap();
        let overrides = parsed.args.into_cli_overrides();
        assert!(overrides.telemetry.is_none());
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
            principal: None,
            ttl: None,
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
                && !args.json
                && args.transport.is_none()
                && args.token.is_none()
                && !args.skip_validation
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
            "--json",
            "--transport",
            "http",
            "--token",
            "test-token",
            "--skip-validation",
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
                && args.json
                && args.transport == Some(TransportKind::Http)
                && args.token.as_deref() == Some("test-token")
                && args.skip_validation
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

    // -- Config show ---------------------------------------------------------

    #[test]
    fn test_config_show_parses() {
        let cli = Cli::try_parse_from(["tribal", "config", "show"]).unwrap();
        assert!(matches!(
            cli.command,
            Some(Command::Config(ConfigCommand::Show { args }))
            if !args.show_secrets
        ));
    }

    #[test]
    fn test_config_show_parses_show_secrets() {
        let cli = Cli::try_parse_from(["tribal", "config", "show", "--show-secrets"]).unwrap();
        assert!(matches!(
            cli.command,
            Some(Command::Config(ConfigCommand::Show { args }))
            if args.show_secrets
        ));
    }

    // -- MCP config ----------------------------------------------------------

    #[test]
    fn test_mcp_config_parses_without_flags() {
        let cli = Cli::try_parse_from(["tribal", "mcp-config"]).unwrap();
        assert!(matches!(
            cli.command,
            Some(Command::McpConfig { ref args })
            if args.transport.is_none()
                && args.token.is_none()
                && args.database.database_url.is_none()
        ));
    }

    #[test]
    fn test_mcp_config_parses_all_flags() {
        let cli = Cli::try_parse_from([
            "tribal",
            "mcp-config",
            "--transport",
            "http",
            "--project",
            "proj_00000000-0000-0000-0000-000000000001",
            "--token",
            "test-token",
            "-d",
            "postgres://h/db",
        ])
        .unwrap();
        assert!(matches!(
            cli.command,
            Some(Command::McpConfig { ref args })
            if args.transport == Some(TransportKind::Http)
                && args.project.as_deref() == Some("proj_00000000-0000-0000-0000-000000000001")
                && args.token.as_deref() == Some("test-token")
                && args.database.database_url.as_deref() == Some("postgres://h/db")
        ));
    }

    #[test]
    fn test_mcp_config_into_cli_overrides_delegates_to_database_args() {
        let args = McpConfigArgs {
            transport: None,
            project: None,
            token: None,
            static_token: false,
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
