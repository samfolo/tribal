//! Clap command and argument definitions for the Tribal CLI.

use clap::{ArgAction, Args, CommandFactory, Parser, Subcommand, ValueEnum, error::ErrorKind};
use tribal_config::{CliOverrides, ServerCliOverrides, default_config_file_path};
use tribal_domain::{AuthTokenId, ProjectId, ProviderKind, Scope, TaskType, is_mintable_scope};
use tribal_wire::management::{CredentialSourceId, EndpointSelection, KnownModelId, TransportKind};

use super::styles::STYLES;

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
        args: Box<BootstrapArgs>,
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

    /// Control the runtime-independent local management authority.
    #[command(subcommand, display_order = 3)]
    Manager(ManagerCommand),

    /// Control the runtime owned by the current management authority.
    #[command(subcommand, display_order = 4)]
    Runtime(RuntimeCommand),

    /// Discover and select curated models.
    #[command(subcommand, display_order = 5)]
    Models(ModelsCommand),

    /// Discover credentials for an exact product operation.
    #[command(subcommand, display_order = 6)]
    Credential(CredentialCommand),

    /// Inspect graph configuration choices.
    #[command(subcommand, display_order = 7)]
    Graph(GraphCommand),

    /// Manage the configured database.
    #[command(subcommand, display_order = 5)]
    Database(DatabaseCommand),

    /// Manage projects.
    #[command(subcommand, display_order = 4)]
    Project(ProjectCommand),

    /// Manage authentication tokens.
    #[command(subcommand, display_order = 5)]
    Token(TokenCommand),

    /// Interact with the resolved configuration.
    #[command(subcommand, display_order = 6)]
    Config(ConfigCommand),

    /// Render integration configuration from manager-owned facts.
    #[command(subcommand, display_order = 7)]
    Integration(IntegrationCommand),

    /// Migrate the embedding space: run, cancel, or prune a reindex.
    #[command(subcommand, display_order = 8)]
    Reindex(ReindexCommand),

    /// Manage durable agent threads.
    #[command(subcommand, display_order = 9)]
    Threads(ThreadsCommand),
}

/// Manager process and lifecycle subcommands.
#[derive(Debug, Subcommand)]
pub enum ManagerCommand {
    /// Run the local management authority in the foreground.
    Run {
        /// Arguments for the management authority.
        #[command(flatten)]
        args: ManageArgs,
    },
    /// Shut down the manager for the configured path.
    Shutdown,
}

/// Arguments for `tribal manager run`.
#[derive(Debug, Args)]
pub struct ManageArgs {
    /// Emit one bounded machine-readable launch record to stdout.
    #[arg(long)]
    pub announce_json: bool,
}

/// Runtime lifecycle subcommands.
#[derive(Debug, Subcommand)]
pub enum RuntimeCommand {
    /// Start the managed runtime.
    Start {
        #[command(flatten)]
        output: OutputArgs,
    },
    /// Stop the managed runtime.
    Stop {
        #[command(flatten)]
        output: OutputArgs,
    },
    /// Restart the managed runtime.
    Restart {
        #[command(flatten)]
        output: OutputArgs,
    },
    /// Print the latest lifecycle snapshot.
    Status {
        #[command(flatten)]
        output: OutputArgs,
    },
}

/// Output selection shared by typed command projections.
#[derive(Debug, Clone, Copy, Default, Args)]
pub struct OutputArgs {
    /// Emit the exact management response as JSON.
    #[arg(long, help_heading = "Output")]
    pub json: bool,
}

/// Curated model discovery subcommands.
#[derive(Debug, Subcommand)]
pub enum ModelsCommand {
    /// List model identities accepted by product actions.
    List {
        #[command(flatten)]
        output: OutputArgs,
    },
}

/// Credential capability discovery subcommands.
#[derive(Debug, Subcommand)]
pub enum CredentialCommand {
    /// List stored sources compatible with an exact use.
    #[command(subcommand)]
    Sources(CredentialSourcesCommand),
}

/// Use-bound credential source queries.
#[derive(Debug, Subcommand)]
pub enum CredentialSourcesCommand {
    /// Discover sources for a curated model selection.
    Model {
        #[command(flatten)]
        args: ModelCredentialSourceArgs,
    },
    /// Discover sources for graph genesis.
    Genesis {
        #[command(flatten)]
        args: GenesisCredentialSourceArgs,
    },
}

/// Inference stage accepted at the command boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum InferenceStageArg {
    Extraction,
    Triage,
    Relation,
}

/// One inference stage and its endpoint transition.
#[derive(Debug, Clone, PartialEq)]
pub struct StageEndpointArg {
    pub stage: InferenceStageArg,
    pub endpoint: EndpointSelection,
}

impl std::str::FromStr for StageEndpointArg {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let (stage, endpoint) = value
            .split_once('=')
            .ok_or_else(|| "expected stage=preserve|provider-default|URL".to_owned())?;
        let stage = parse_inference_stage(stage)?;
        let endpoint = match endpoint {
            "preserve" => EndpointSelection::Preserve,
            "provider-default" => EndpointSelection::ProviderDefault,
            "" => return Err("endpoint must not be empty".to_owned()),
            value => EndpointSelection::Custom {
                value: value.to_owned(),
            },
        };
        Ok(Self { stage, endpoint })
    }
}

/// One inference stage and a use-bound stored credential source.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StageCredentialSourceArg {
    pub stage: InferenceStageArg,
    pub source: CredentialSourceId,
}

impl std::str::FromStr for StageCredentialSourceArg {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let (stage, source) = value
            .split_once('=')
            .ok_or_else(|| "expected stage=credential-source-id".to_owned())?;
        Ok(Self {
            stage: parse_inference_stage(stage)?,
            source: source
                .parse()
                .map_err(|error| format!("invalid credential source id: {error}"))?,
        })
    }
}

fn parse_inference_stage(value: &str) -> Result<InferenceStageArg, String> {
    match value {
        "extraction" => Ok(InferenceStageArg::Extraction),
        "triage" => Ok(InferenceStageArg::Triage),
        "relation" => Ok(InferenceStageArg::Relation),
        _ => Err(format!("unknown inference stage '{value}'")),
    }
}

/// Arguments for model credential discovery.
#[derive(Debug, Args)]
pub struct ModelCredentialSourceArgs {
    /// Curated model identity from `tribal models list`.
    #[arg(long)]
    pub model: KnownModelId,
    /// Stage receiving the selected model; repeat for multiple stages.
    #[arg(long, value_enum, required = true)]
    pub stage: Vec<InferenceStageArg>,
    /// Use the provider's default endpoint instead of preserving the current one.
    #[arg(long, conflicts_with = "endpoint")]
    pub provider_default: bool,
    /// Select a custom endpoint.
    #[arg(long, value_name = "URL")]
    pub endpoint: Option<String>,
    /// Output selection.
    #[command(flatten)]
    pub output: OutputArgs,
}

/// Arguments for genesis credential discovery.
#[derive(Debug, Args)]
pub struct GenesisCredentialSourceArgs {
    /// Embedding provider.
    #[arg(long, value_parser = clap::value_parser!(ProviderKind))]
    pub provider: ProviderKind,
    /// Embedding model.
    #[arg(long)]
    pub model: String,
    /// Embedding output dimensions.
    #[arg(long)]
    pub dimensions: Option<u32>,
    /// Embedding endpoint base URL.
    #[arg(long = "base-url")]
    pub base_url: Option<String>,
    /// Output selection.
    #[command(flatten)]
    pub output: OutputArgs,
}

/// Graph discovery subcommands.
#[derive(Debug, Subcommand)]
pub enum GraphCommand {
    /// List the valid graph-genesis inputs.
    GenesisOptions {
        #[command(flatten)]
        output: OutputArgs,
    },
}

/// Database administration subcommands.
#[derive(Debug, Subcommand)]
pub enum DatabaseCommand {
    /// Apply migrations and ensure the local principal exists.
    Initialise {
        #[command(flatten)]
        output: OutputArgs,
    },
}

// ---------------------------------------------------------------------------
// Serve
// ---------------------------------------------------------------------------

/// Arguments for the `serve` subcommand.
///
/// Transport and bind-address environment variables (`TRIBAL_TRANSPORT`,
/// `TRIBAL_BIND_ADDRESS`) are handled by the configuration loading layer.
/// Ambient project selection is resolved only after explicit process mode.
#[derive(Debug, Args)]
pub struct ServeArgs {
    /// Transport protocol for the MCP server.
    #[arg(long, help_heading = "Transport")]
    pub transport: Option<TransportKind>,

    /// Socket address to bind the HTTP/SSE listener to.
    #[arg(long, help_heading = "Transport")]
    pub bind: Option<String>,

    /// Project ID (`proj_`-prefixed) to scope the session to.
    #[arg(long, help_heading = "Session")]
    pub project: Option<String>,

    /// Disable ambient and working-tree project selection.
    #[arg(long, conflicts_with = "project", help_heading = "Session")]
    pub unscoped: bool,
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
// Bootstrap
// ---------------------------------------------------------------------------

/// Arguments for the manager-owned `bootstrap` composition.
#[derive(Debug, Default, Args)]
pub struct BootstrapArgs {
    /// External database selected and persisted by bootstrap.
    #[arg(long = "database-url", help_heading = "Storage")]
    pub database_url: Option<String>,

    /// Curated model assignment in `stage=model-id` form; repeat as needed.
    #[arg(
        long = "model-selection",
        value_name = "STAGE=MODEL",
        help_heading = "Models"
    )]
    pub model_selections: Vec<String>,

    /// Endpoint transition in `stage=preserve|provider-default|URL` form.
    #[arg(
        long = "model-endpoint",
        value_name = "STAGE=ENDPOINT",
        help_heading = "Models"
    )]
    pub model_endpoints: Vec<StageEndpointArg>,

    /// Model credential as `stage=ENVIRONMENT_VARIABLE`; repeat by stage.
    #[arg(
        long = "model-credential-env",
        value_name = "STAGE=VARIABLE",
        help_heading = "Models"
    )]
    pub model_credential_env: Vec<String>,

    /// Stored credential capability in `stage=credential-source-id` form.
    #[arg(
        long = "model-credential-source",
        value_name = "STAGE=SOURCE_ID",
        help_heading = "Models"
    )]
    pub model_credential_sources: Vec<StageCredentialSourceArg>,

    /// Read one model stage's credential from stdin.
    #[arg(
        long = "model-credential-stdin",
        value_enum,
        conflicts_with = "genesis_credential_stdin",
        help_heading = "Models"
    )]
    pub model_credential_stdin: Option<InferenceStageArg>,

    /// Embedding provider for graph genesis.
    #[arg(long, requires = "genesis_model", help_heading = "Genesis")]
    pub genesis_provider: Option<ProviderKind>,

    /// Embedding model for graph genesis.
    #[arg(long, requires = "genesis_provider", help_heading = "Genesis")]
    pub genesis_model: Option<String>,

    /// Embedding output dimensions.
    #[arg(long, requires = "genesis_provider", help_heading = "Genesis")]
    pub genesis_dimensions: Option<u32>,

    /// Embedding endpoint base URL.
    #[arg(long, requires = "genesis_provider", help_heading = "Genesis")]
    pub genesis_base_url: Option<String>,

    /// Environment variable containing the explicit genesis credential.
    #[arg(
        long = "genesis-credential-env",
        value_name = "VARIABLE",
        requires = "genesis_provider",
        conflicts_with_all = ["genesis_credential_source", "genesis_credential_stdin", "genesis_reuse_stage"],
        help_heading = "Genesis"
    )]
    pub genesis_credential_env: Option<String>,

    /// Stored credential capability for graph genesis.
    #[arg(
        long = "genesis-credential-source",
        value_name = "SOURCE_ID",
        requires = "genesis_provider",
        conflicts_with_all = ["genesis_credential_env", "genesis_credential_stdin", "genesis_reuse_stage"],
        help_heading = "Genesis"
    )]
    pub genesis_credential_source: Option<CredentialSourceId>,

    /// Read the explicit genesis credential from stdin.
    #[arg(
        long = "genesis-credential-stdin",
        requires = "genesis_provider",
        conflicts_with_all = ["genesis_credential_env", "genesis_credential_source", "genesis_reuse_stage", "model_credential_stdin"],
        help_heading = "Genesis"
    )]
    pub genesis_credential_stdin: bool,

    /// Reuse the selected inference stage credential for genesis.
    #[arg(
        long = "genesis-reuse-stage",
        value_enum,
        requires = "genesis_provider",
        conflicts_with_all = ["genesis_credential_env", "genesis_credential_source", "genesis_credential_stdin"],
        help_heading = "Genesis"
    )]
    pub genesis_reuse_stage: Option<InferenceStageArg>,

    /// Absolute OTLP endpoint to persist.
    #[arg(long, help_heading = "Telemetry")]
    pub otlp_endpoint: Option<String>,

    /// Working tree to register; omission leaves project registration out.
    #[arg(long, value_name = "DIRECTORY", help_heading = "Project")]
    pub project_path: Option<String>,

    /// Project display name.
    #[arg(long, requires = "project_path", help_heading = "Project")]
    pub project_name: Option<String>,

    /// Project default branch.
    #[arg(long, requires = "project_path", help_heading = "Project")]
    pub project_branch: Option<String>,

    /// Principal receiving the bootstrap credential.
    #[arg(long, help_heading = "Token")]
    pub principal: Option<String>,

    /// Token lifetime in hours.
    #[arg(long, help_heading = "Token")]
    pub ttl: Option<u64>,

    /// Scope to grant, repeatable.
    #[arg(long = "scope", value_parser = parse_mintable_scope, help_heading = "Token")]
    pub scope: Vec<Scope>,

    /// Always issue a new token instead of ensuring the local default.
    #[arg(long, help_heading = "Token")]
    pub create_token: bool,

    /// Explicit integration transport; omission uses configured policy.
    #[arg(long, help_heading = "Integration")]
    pub transport: Option<TransportKind>,

    /// Network authentication and secret-export policy.
    #[arg(
        long,
        value_enum,
        default_value = "oauth",
        help_heading = "Integration"
    )]
    pub auth: IntegrationAuthArg,

    /// Emit the exact bootstrap receipt as JSON.
    #[arg(long, help_heading = "Output")]
    pub json: bool,
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

    /// Emit a single JSON object on stdout instead of the human
    /// form on stderr.
    #[arg(long, help_heading = "Output")]
    pub json: bool,
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
    /// Working tree to inspect; defaults to the current directory.
    #[arg(long, value_name = "DIRECTORY", help_heading = "Project")]
    pub path: Option<String>,

    /// Human-friendly project name. Derived from the git remote path
    /// if omitted.
    #[arg(long, help_heading = "Project")]
    pub name: Option<String>,

    /// Default branch name.
    #[arg(long, help_heading = "Project")]
    pub branch: Option<String>,

    /// Output selection.
    #[command(flatten)]
    pub output: OutputArgs,
}

/// Arguments for `project list`.
#[derive(Debug, Args)]
pub struct ProjectListArgs {
    /// Maximum rows in one page.
    #[arg(long, default_value_t = 50)]
    pub limit: u16,
    /// Opaque continuation cursor from a prior page.
    #[arg(long)]
    pub after: Option<String>,
    /// Output selection.
    #[command(flatten)]
    pub output: OutputArgs,
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

    /// Revoke a specific token by stable id.
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

/// Clap value parser for `--scope`: parses a raw scope, then rejects any
/// the CLI is not permitted to mint (root or uncatalogued `execute`).
///
/// Returns the message string clap renders as the value-validation error.
fn parse_mintable_scope(raw: &str) -> Result<Scope, String> {
    let scope = Scope::parse(raw).map_err(|err| err.to_string())?;
    if is_mintable_scope(&scope) {
        Ok(scope)
    } else {
        Err(format!(
            "{raw:?} cannot be minted here; execute access is limited to {}",
            Scope::EMBEDDING_EXECUTE,
        ))
    }
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

    /// Scope to grant, repeatable. Each must be mintable: any read or
    /// write scope, plus `tribal.embedding:execute`. When omitted, the
    /// token receives full read and write access.
    #[arg(long = "scope", value_name = "SCOPE", value_parser = parse_mintable_scope, help_heading = "Token")]
    pub scope: Vec<Scope>,

    /// Persist this token as the manager namespace's default credential.
    #[arg(long, help_heading = "Token")]
    pub persist_as_default: bool,

    /// Output selection.
    #[command(flatten)]
    pub output: OutputArgs,
}

/// Arguments for `token list`.
#[derive(Debug, Args)]
pub struct TokenListArgs {
    /// Maximum rows in one page.
    #[arg(long, default_value_t = 50)]
    pub limit: u16,
    /// Opaque continuation cursor from a prior page.
    #[arg(long)]
    pub after: Option<String>,
    /// Output selection.
    #[command(flatten)]
    pub output: OutputArgs,
}

/// Arguments for `token revoke`.
#[derive(Debug, Args)]
pub struct TokenRevokeArgs {
    /// Stable token id from `tribal token list`.
    #[arg(value_name = "TOKEN_ID")]
    pub id: AuthTokenId,
    /// Output selection.
    #[command(flatten)]
    pub output: OutputArgs,
}

/// Arguments for `token revoke-all`.
#[derive(Debug, Args)]
pub struct TokenRevokeAllArgs {
    /// Revoke only tokens belonging to this principal.
    #[arg(long, help_heading = "Token")]
    pub principal: Option<String>,

    /// Output selection.
    #[command(flatten)]
    pub output: OutputArgs,
}

// ---------------------------------------------------------------------------
// Reindex
// ---------------------------------------------------------------------------

/// Reindex (embedding-space migration) subcommands.
#[derive(Debug, Subcommand)]
pub enum ReindexCommand {
    /// Preview or apply a migration to a new embedding identity.
    Run {
        /// Arguments for the reindex run.
        #[command(flatten)]
        args: ReindexRunArgs,
    },

    /// Cancel the live reindex run, if any.
    Cancel {
        #[command(flatten)]
        output: OutputArgs,
    },

    /// Supersede the prunable profiles and reclaim their storage.
    Prune {
        #[command(flatten)]
        args: ReindexPruneArgs,
    },
}

/// Arguments for `reindex run`.
#[derive(Debug, Args)]
pub struct ReindexRunArgs {
    /// Target embedding provider.
    #[arg(long, value_parser = clap::value_parser!(ProviderKind), help_heading = "Reindex")]
    pub provider: ProviderKind,

    /// Target embedding model.
    #[arg(long, help_heading = "Reindex")]
    pub model: String,

    /// Target output dimension. When omitted, the provider/model native
    /// dimension is resolved.
    #[arg(long, help_heading = "Reindex")]
    pub dimensions: Option<u32>,

    /// Target endpoint base URL. When omitted, the provider's canonical
    /// endpoint is used.
    #[arg(long = "base-url", help_heading = "Reindex")]
    pub base_url: Option<String>,

    /// Apply the migration; omission previews it without mutation.
    #[arg(long, help_heading = "Reindex")]
    pub apply: bool,

    /// Output selection.
    #[command(flatten)]
    pub output: OutputArgs,
}

/// Arguments for `reindex prune`.
#[derive(Debug, Args)]
pub struct ReindexPruneArgs {
    /// Delete the candidate profiles; omission previews them.
    #[arg(long, help_heading = "Reindex")]
    pub apply: bool,
    /// Output selection.
    #[command(flatten)]
    pub output: OutputArgs,
}

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

/// Configuration subcommands.
#[derive(Debug, Subcommand)]
pub enum ConfigCommand {
    /// Print the redacted resolved configuration.
    Show {
        /// Arguments for config show.
        #[command(flatten)]
        args: ConfigShowArgs,
    },
    /// Read one effective configuration value.
    Get {
        #[command(flatten)]
        args: ConfigGetArgs,
    },
    /// Validate and persist one configuration value.
    Set {
        #[command(flatten)]
        args: ConfigSetArgs,
    },
    /// Validate one proposed value without persistence.
    Validate {
        #[command(flatten)]
        args: ConfigValidateArgs,
    },
    /// Print the canonical configuration path.
    Path {
        #[command(flatten)]
        output: OutputArgs,
    },
}

/// Arguments for `config show`.
#[derive(Debug, Clone, Copy, Args)]
pub struct ConfigShowArgs {
    /// Output selection.
    #[command(flatten)]
    pub output: OutputArgs,
}

/// Arguments for `config get`.
#[derive(Debug, Args)]
pub struct ConfigGetArgs {
    /// Validated dotted configuration field path.
    pub key: String,
    /// Output selection.
    #[command(flatten)]
    pub output: OutputArgs,
}

/// Arguments for `config set`.
#[derive(Debug, Args)]
pub struct ConfigSetArgs {
    /// Validated dotted configuration field path.
    pub key: String,
    /// JSON value, or a bare string when JSON parsing fails.
    pub value: String,
    /// Output selection.
    #[command(flatten)]
    pub output: OutputArgs,
}

/// Arguments for `config validate`.
#[derive(Debug, Args)]
pub struct ConfigValidateArgs {
    /// Validated dotted configuration field path.
    pub key: String,
    /// JSON value, or a bare string when JSON parsing fails.
    pub value: String,
    /// Output selection.
    #[command(flatten)]
    pub output: OutputArgs,
}

// ---------------------------------------------------------------------------
// Integration
// ---------------------------------------------------------------------------

/// Integration rendering subcommands.
#[derive(Debug, Subcommand)]
pub enum IntegrationCommand {
    /// Render one MCP client configuration document.
    McpConfig {
        #[command(flatten)]
        args: IntegrationMcpConfigArgs,
    },
}

/// Authentication policy for network integration exports.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, ValueEnum)]
pub enum IntegrationAuthArg {
    #[value(name = "oauth")]
    #[default]
    OAuth,
    PersistedBearer,
}

/// Arguments for `integration mcp-config`.
#[derive(Debug, Args)]
pub struct IntegrationMcpConfigArgs {
    /// Explicit transport; omission uses the configured transport.
    #[arg(long)]
    pub transport: Option<TransportKind>,

    /// Project id for an explicit stdio target.
    #[arg(long, conflicts_with = "unscoped")]
    pub project: Option<ProjectId>,

    /// Render an explicitly unscoped stdio target.
    #[arg(long, conflicts_with = "project")]
    pub unscoped: bool,

    /// Network authentication and secret-export policy.
    #[arg(long, value_enum, default_value = "oauth")]
    pub auth: IntegrationAuthArg,

    /// Output selection.
    #[command(flatten)]
    pub output: OutputArgs,
}

// ---------------------------------------------------------------------------
// Threads
// ---------------------------------------------------------------------------

/// Durable agent-thread subcommands.
#[derive(Debug, Subcommand)]
pub enum ThreadsCommand {
    /// Delete terminal threads and their records by explicit criteria.
    /// Spend rows in the ledger survive with their thread references
    /// cleared. Without `--cascade` any candidate with descendants is
    /// refused; with it, a pass extends to the terminal descendants of
    /// accepted candidates, refusing only candidates whose subtree still
    /// holds a live thread. Omission previews what a pass would collect.
    Prune {
        /// Arguments for the prune pass.
        #[command(flatten)]
        args: ThreadsPruneArgs,
    },
}

/// Arguments for `threads prune`.
#[derive(Debug, Args)]
pub struct ThreadsPruneArgs {
    /// Prune threads whose terminal commit is older than this many days.
    #[arg(long = "older-than-days", help_heading = "Threads")]
    pub older_than_days: u32,

    /// Restrict the pass to one pipeline stage (extraction, triage,
    /// relation).
    #[arg(long, value_parser = clap::value_parser!(TaskType), help_heading = "Threads")]
    pub stage: Option<TaskType>,

    /// Also delete the terminal descendants of accepted candidates; a
    /// live descendant still refuses its whole subtree.
    #[arg(long, help_heading = "Threads")]
    pub cascade: bool,

    /// Apply the deletion; omission previews it without mutation.
    #[arg(long, help_heading = "Threads")]
    pub apply: bool,

    /// Output selection.
    #[command(flatten)]
    pub output: OutputArgs,
}
