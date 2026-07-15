//! Internal data layer for `tribal check`.
//!
//! Each check returns a [`CheckOutcome`] carrying a typed [`CheckDetail`]
//! and an optional [`CheckRemediation`].  Detail describes the state;
//! remediation carries the action — neither duplicates the other.
//! Variants own their data so rendering is a single match per enum.

use std::path::PathBuf;

use tribal_config::{
    Diagnostics, ProviderConnectionResolutionError, ProviderStage, ValidationError,
    standard_env_var_name,
};
use tribal_domain::{ProjectId, ProviderConnectionName, ProviderKind, ReindexRunState};
use tribal_wire::operator_check::CheckName;

use crate::error::FIRST_RUN_REQUIRED;

// ---------------------------------------------------------------------------
// CheckOutcome
// ---------------------------------------------------------------------------

/// What a single check produces.
///
/// The check name is derived from the carried [`CheckDetail`] via
/// [`CheckDetail::name`], so a pairing of `(Pass, DatabaseReachable)`
/// is impossible to build with a `ConfigLoaded` detail.
#[derive(Debug, Clone, PartialEq)]
pub(in crate::management::operator_check) enum CheckOutcome {
    Pass {
        detail: CheckDetail,
    },
    Warn {
        detail: CheckDetail,
        remediation: CheckRemediation,
    },
    Fail {
        detail: CheckDetail,
        remediation: CheckRemediation,
    },
    Skip {
        detail: CheckDetail,
    },
}

// ---------------------------------------------------------------------------
// CheckOutcomes
// ---------------------------------------------------------------------------

/// Ordered collection of [`CheckOutcome`] values produced during a run.
///
/// Centralises the discipline of "what counts as ok" and the projection
/// to the wire format — both are facts about the collection, not about
/// any single outcome.
#[derive(Debug, Default, Clone, PartialEq)]
pub(in crate::management::operator_check) struct CheckOutcomes(Vec<CheckOutcome>);

impl CheckOutcomes {
    pub(in crate::management::operator_check) fn new() -> Self {
        Self::default()
    }

    pub(in crate::management::operator_check) fn push(&mut self, outcome: CheckOutcome) {
        self.0.push(outcome);
    }

    pub(in crate::management::operator_check) fn iter(&self) -> std::slice::Iter<'_, CheckOutcome> {
        self.0.iter()
    }
}

// ---------------------------------------------------------------------------
// CheckDetail
// ---------------------------------------------------------------------------

/// Typed description of what a check observed.
#[derive(Debug, Clone, PartialEq)]
pub(in crate::management::operator_check) enum CheckDetail {
    /// Config file parsed successfully from `path`.
    ConfigLoaded { path: PathBuf },
    /// Config file failed to load or deserialise.
    ConfigParseFailed { error: String, path: PathBuf },
    /// All configuration invariants passed.
    AllInvariantsSatisfied,
    /// One or more configuration invariants failed.
    ValidationFailed { diagnostics: Diagnostics },
    /// Database connection succeeded.
    DatabaseReachable,
    /// Database connection failed; `error` is the underlying sqlx error.
    DatabaseUnreachable { error: String },
    /// Database migration head matches the binary's compile-time head.
    MigrationsMatch,
    /// Database is older than the binary expects.
    MigrationsBehind { expected: i64, found: i64 },
    /// Database is newer than the binary expects.
    MigrationsAhead { expected: i64, found: i64 },
    /// `_sqlx_migrations` table does not exist — database was never set up.
    MigrationsTableMissing,
    /// Migration head query failed unexpectedly; `error` is the rendered cause.
    MigrationsQueryFailed { error: String },
    /// Project resolved from CLI override, env var, or git remote.
    ProjectFound { project_id: ProjectId, name: String },
    /// User-supplied project (CLI flag or `TRIBAL_PROJECT_ID`) could
    /// not be resolved; `error` is the reason.
    ProjectNotFound { error: String },
    /// No project resolvable from any cascade step.
    ProjectCascadeMissing,
    /// Project resolution failed at the infrastructure layer.
    ProjectQueryFailed { error: String },
    /// Stdio transport with no `--token` supplied; verification skipped.
    TokenSkippedStdio,
    /// Token verified against the database.
    TokenVerified { transport: TokenTransport },
    /// Token verification failed.
    TokenVerificationFailed {
        transport: TokenTransport,
        reason: TokenFailureReason,
    },
    /// Network transport on a loopback surface with no static token.
    /// Network clients authenticate via OAuth, so a static token is
    /// optional and its absence is not a finding.
    TokenSkippedLoopbackOauth,
    /// Network transport on a routable surface with no static token.
    /// Open registration is refused there, so an absent static token
    /// leaves clients with no authentication path.
    TokenMissingRoutable,
    /// The namespaced credential envelope is unreadable; absence instead
    /// resolves by deployment topology.
    CredentialsUnreadable { error: String },
    /// Stdio transport has no advertised URL — nothing to probe.
    AdvertisedUrlSkippedStdio,
    /// Advertised URL responded; `status` is the HTTP response code.
    AdvertisedUrlReachable { url: String, status: u16 },
    /// Advertised URL did not respond; `error` describes the failure.
    AdvertisedUrlUnreachable { url: String, error: String },
    /// Exactly one `tribal` binary resolved on PATH.
    BinaryUnique { path: PathBuf },
    /// Multiple `tribal` binaries resolved on PATH — shadowing risk.
    BinaryDuplicate { paths: Vec<PathBuf> },
    /// No `tribal` binary resolved on PATH — the binary running this
    /// check is not discoverable through PATH lookup.
    BinaryAbsent,
    /// A check did not run because a prior phase or invariant precludes
    /// it.  `name` is the skipped check; `reason` carries the cause.
    DependencySkipped { name: CheckName, reason: SkipReason },
    /// Provider probe against the named target succeeded.
    ProviderProbePassed {
        target: ProviderStage,
        provider: ProviderKind,
    },
    /// Provider probe against the named target failed; `error` is the
    /// underlying [`tribal_inference::InferenceError`] rendered.
    ProviderProbeFailed {
        target: ProviderStage,
        provider: ProviderKind,
        error: String,
    },
    /// No active embedding profile exists yet; first server boot provisions
    /// it from `init.embedding`.
    EmbeddingProfilePending,
    /// The active embedding profile is healthy. `genesis_drift` carries the
    /// genesis seed identity when it differs from the live profile, reported
    /// as informational state (the seed is stale by design once a corpus
    /// exists).
    EmbeddingProfileLive {
        provider: ProviderKind,
        model: String,
        dimensions: u32,
        genesis_drift: Option<String>,
    },
    /// The active profile's provider endpoint has no resolvable connection.
    EmbeddingCredentialUnresolved {
        provider: ProviderKind,
        base_url: String,
        source: ProviderConnectionResolutionError,
    },
    /// A reindex run is live and/or items are quarantined out of the active
    /// space, conditions that warrant operator attention.
    EmbeddingReindexAttention {
        live_run: Option<(String, ReindexRunState)>,
        quarantined: u64,
    },
    /// A database query backing the embedding-profile check failed after
    /// connectivity was confirmed.
    EmbeddingProfileQueryFailed { error: String },
}

// ---------------------------------------------------------------------------
// CheckName from provider stage
// ---------------------------------------------------------------------------

pub(in crate::management::operator_check) fn check_name_for_provider_stage(
    stage: ProviderStage,
) -> CheckName {
    match stage {
        ProviderStage::Embedding => CheckName::ProviderEmbedding,
        ProviderStage::Extraction => CheckName::ProviderExtraction,
        ProviderStage::Triage => CheckName::ProviderTriage,
        ProviderStage::Relation => CheckName::ProviderRelation,
    }
}

// ---------------------------------------------------------------------------
// SkipReason
// ---------------------------------------------------------------------------

/// Why a downstream check did not run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::management::operator_check) enum SkipReason {
    /// The config file could not be loaded; nothing downstream can run.
    ConfigParseFailed,
    /// A `server.bind_address` validation error blocks advertised-URL
    /// resolution, so the probe is skipped.
    ConfigValidateFailedTransportBind,
    /// The named target has an invalid provider connection, so its probe is skipped.
    ConfigValidateFailedApiKey { target: ProviderStage },
    /// The database was not reachable, so checks that require a
    /// connection are skipped.
    DatabaseUnreachable,
}

// ---------------------------------------------------------------------------
// TokenTransport / TokenFailureReason
// ---------------------------------------------------------------------------

/// The transport context against which a token was verified.
///
/// `Http` covers both `http` and `sse` transports — the two share token
/// verification semantics.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::management::operator_check) enum TokenTransport {
    Stdio,
    Http,
}

/// Why token verification failed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::management::operator_check) enum TokenFailureReason {
    /// No matching token row in the database.
    Invalid,
    /// The token row has been revoked.
    Revoked,
    /// The token has expired.
    Expired,
    /// Token row resolves but its `principal_id` has no matching row.
    PrincipalMissing,
    /// Database error during verification.
    DatabaseUnavailable { context: String },
}

impl CheckDetail {
    /// Returns the [`CheckName`] each detail variant belongs to.
    ///
    /// The exhaustive match is the single audit point: adding a new
    /// detail variant forces the compiler to assign it to a name here.
    pub(in crate::management::operator_check) fn name(&self) -> CheckName {
        match self {
            Self::ConfigLoaded { .. } | Self::ConfigParseFailed { .. } => CheckName::ConfigParse,
            Self::AllInvariantsSatisfied | Self::ValidationFailed { .. } => {
                CheckName::ConfigValidate
            }
            Self::DatabaseReachable | Self::DatabaseUnreachable { .. } => {
                CheckName::DatabaseReachable
            }
            Self::MigrationsMatch
            | Self::MigrationsBehind { .. }
            | Self::MigrationsAhead { .. }
            | Self::MigrationsTableMissing
            | Self::MigrationsQueryFailed { .. } => CheckName::MigrationsCurrent,
            Self::ProjectFound { .. }
            | Self::ProjectNotFound { .. }
            | Self::ProjectCascadeMissing
            | Self::ProjectQueryFailed { .. } => CheckName::ProjectResolution,
            Self::TokenSkippedStdio
            | Self::TokenSkippedLoopbackOauth
            | Self::TokenMissingRoutable
            | Self::TokenVerified { .. }
            | Self::TokenVerificationFailed { .. }
            | Self::CredentialsUnreadable { .. } => CheckName::ValidTokenExists,
            Self::AdvertisedUrlSkippedStdio
            | Self::AdvertisedUrlReachable { .. }
            | Self::AdvertisedUrlUnreachable { .. } => CheckName::AdvertisedUrlReachable,
            Self::BinaryUnique { .. } | Self::BinaryDuplicate { .. } | Self::BinaryAbsent => {
                CheckName::BinaryUniqueness
            }
            Self::DependencySkipped { name, .. } => *name,
            Self::ProviderProbePassed { target, .. } | Self::ProviderProbeFailed { target, .. } => {
                check_name_for_provider_stage(*target)
            }
            Self::EmbeddingProfilePending
            | Self::EmbeddingProfileLive { .. }
            | Self::EmbeddingCredentialUnresolved { .. }
            | Self::EmbeddingReindexAttention { .. }
            | Self::EmbeddingProfileQueryFailed { .. } => CheckName::EmbeddingProfile,
        }
    }

    /// Renders the detail as the wire-format string.
    pub(in crate::management::operator_check) fn render(&self) -> String {
        match self {
            Self::ConfigLoaded { path } => format!("config loaded from {}", path.display()),
            Self::ConfigParseFailed { error, path } => {
                format!("config at {} failed to load: {error}", path.display())
            }
            Self::AllInvariantsSatisfied => "all configuration invariants satisfied".into(),
            Self::ValidationFailed { diagnostics } => diagnostics
                .iter()
                .map(ValidationError::to_string)
                .collect::<Vec<_>>()
                .join("\n"),
            Self::DatabaseReachable => "database connection succeeded".into(),
            Self::DatabaseUnreachable { error } => format!("database unreachable: {error}"),
            Self::MigrationsMatch => "migrations are current".into(),
            Self::MigrationsBehind { expected, found } => {
                format!("binary expects migration {expected}, database is at {found}")
            }
            Self::MigrationsAhead { expected, found } => {
                format!("database is at migration {found}; binary expects {expected}")
            }
            Self::MigrationsTableMissing => FIRST_RUN_REQUIRED.into(),
            Self::MigrationsQueryFailed { error } => {
                format!("migration check query failed: {error}")
            }
            Self::ProjectFound { project_id, name } => {
                format!("resolved project `{name}` ({project_id})")
            }
            Self::ProjectNotFound { error } => format!("project not found: {error}"),
            Self::ProjectCascadeMissing => {
                "no project resolved from --project, TRIBAL_PROJECT_ID, or the git remote".into()
            }
            Self::ProjectQueryFailed { error } => {
                format!("project lookup failed: {error}")
            }
            Self::TokenSkippedStdio => {
                "stdio transport authenticates as `principal:local`; bearer tokens are not used \
                 at runtime"
                    .into()
            }
            Self::TokenVerified { transport } => match transport {
                TokenTransport::Stdio => format!("token verified {STDIO_QUALIFIER}"),
                TokenTransport::Http => "token verified".into(),
            },
            Self::TokenVerificationFailed { transport, reason } => {
                let base = match reason {
                    TokenFailureReason::Invalid => "token is invalid".to_owned(),
                    TokenFailureReason::Revoked => "token is revoked".to_owned(),
                    TokenFailureReason::Expired => "token is expired".to_owned(),
                    TokenFailureReason::PrincipalMissing => {
                        "token's principal not found".to_owned()
                    }
                    TokenFailureReason::DatabaseUnavailable { context } => {
                        format!("token verification failed; database unavailable: {context}")
                    }
                };
                match transport {
                    TokenTransport::Stdio => format!("{base} {STDIO_QUALIFIER}"),
                    TokenTransport::Http => base,
                }
            }
            Self::TokenSkippedLoopbackOauth => {
                "no static token configured; network clients authenticate via OAuth on this \
                 loopback deployment, so a static token is not required"
                    .into()
            }
            Self::TokenMissingRoutable => {
                "no static token configured and automatic client registration is unavailable \
                 for this deployment (it is routable, or DCR is disabled), so clients have no \
                 authentication path"
                    .into()
            }
            Self::CredentialsUnreadable { error } => {
                format!("the namespaced credential envelope is unreadable: {error}")
            }
            Self::AdvertisedUrlSkippedStdio => "stdio transport: no advertised URL to probe".into(),
            Self::AdvertisedUrlReachable { url, status } => {
                format!("{url} responded with HTTP {status}")
            }
            Self::AdvertisedUrlUnreachable { url, error } => {
                format!("{url} unreachable: {error}")
            }
            Self::BinaryUnique { path } => format!("`tribal` resolves to {}", path.display()),
            Self::BinaryDuplicate { paths } => {
                let rendered: Vec<String> = paths.iter().map(|p| p.display().to_string()).collect();
                format!(
                    "multiple `tribal` binaries on PATH: {}",
                    rendered.join(", "),
                )
            }
            Self::BinaryAbsent => {
                "no `tribal` binary resolves from PATH (running via an absolute or relative \
                 path bypasses PATH lookup)"
                    .into()
            }
            Self::DependencySkipped { name: _, reason } => match reason {
                SkipReason::ConfigParseFailed => {
                    "skipped because the configuration file could not be loaded".into()
                }
                SkipReason::ConfigValidateFailedTransportBind => {
                    "skipped because `server.bind_address` validation failed".into()
                }
                SkipReason::ConfigValidateFailedApiKey { target } => format!(
                    "skipped because `{}` validation failed",
                    target.section_path().extend("connection")
                ),
                SkipReason::DatabaseUnreachable => {
                    "skipped because the database is unreachable".into()
                }
            },
            Self::ProviderProbePassed { target, provider } => format!(
                "{} provider ({provider}) probe passed",
                target.section_path()
            ),
            Self::ProviderProbeFailed {
                target,
                provider,
                error,
            } => format!(
                "{} provider ({provider}) probe failed: {error}",
                target.section_path()
            ),
            Self::EmbeddingProfilePending => {
                "no active embedding profile yet; first server boot provisions it from \
                 init.embedding"
                    .into()
            }
            Self::EmbeddingProfileLive {
                provider,
                model,
                dimensions,
                genesis_drift,
            } => render_embedding_profile_live(
                *provider,
                model,
                *dimensions,
                genesis_drift.as_deref(),
            ),
            Self::EmbeddingCredentialUnresolved {
                provider,
                base_url,
                source,
            } => render_embedding_credential_unresolved(*provider, base_url, source),
            Self::EmbeddingReindexAttention {
                live_run,
                quarantined,
            } => render_reindex_attention(live_run.as_ref(), *quarantined),
            Self::EmbeddingProfileQueryFailed { error } => {
                format!("embedding-profile check query failed: {error}")
            }
        }
    }
}

/// Renders an active-profile detail, appending the genesis divergence note as
/// informational state when the seed no longer matches the live model.
fn render_embedding_profile_live(
    provider: ProviderKind,
    model: &str,
    dimensions: u32,
    genesis_drift: Option<&str>,
) -> String {
    let base = format!("active embedding profile: {provider}/{model} ({dimensions}d)");
    match genesis_drift {
        Some(genesis) => format!(
            "{base}; genesis seed init.embedding ({genesis}) differs from the live model \
             (informational; update init.embedding to mirror it)"
        ),
        None => base,
    }
}

/// Renders the credential-unresolved detail, naming the endpoint and the cause
/// that distinguishes a missing entry from an entry with an empty key.
fn render_embedding_credential_unresolved(
    provider: ProviderKind,
    base_url: &str,
    source: &ProviderConnectionResolutionError,
) -> String {
    format!("the active embedding profile expects {provider} at {base_url}; {source}")
}

/// Renders the reindex-attention warning, joining the live-run and
/// quarantine lines that apply.
fn render_reindex_attention(
    live_run: Option<&(String, ReindexRunState)>,
    quarantined: u64,
) -> String {
    let mut lines = Vec::new();
    if let Some((run_id, state)) = live_run {
        lines.push(format!("reindex run {run_id} is live ({state})"));
    }
    if quarantined > 0 {
        lines.push(format!(
            "{quarantined} item(s) quarantined out of the active embedding space"
        ));
    }
    lines.join("\n")
}

/// Suffix appended to stdio token-verification renderings — stdio
/// transport does not actually consume the token at runtime, so the
/// outcome describes the verification only.
const STDIO_QUALIFIER: &str =
    "(checked against --token; stdio transport does not use this token at runtime)";

// ---------------------------------------------------------------------------
// CheckRemediation
// ---------------------------------------------------------------------------

/// Typed action a user takes to resolve a failing or warning check.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::management::operator_check) enum CheckRemediation {
    /// Inspect the config file at `path` for parse errors.
    InspectConfigFile { path: PathBuf },
    /// One or more targeted hints for the validation errors collected.
    FixConfigInvariant { hints: Vec<String> },
    /// Verify database availability with `pg_isready` and review the
    /// `database.url` field.
    CheckPgIsready,
    /// Run `tribal database initialise` to initialise the database or apply pending
    /// migrations.
    RunDatabaseInitialise,
    /// Upgrade the `tribal` binary to a version that knows about the
    /// migrations already applied to the database.
    UpgradeBinary,
    /// Register a project with `tribal project register` or set
    /// `TRIBAL_PROJECT_ID`.
    RegisterProjectOrSetEnv,
    /// Mint a new bearer token with `tribal token create`.
    RunTribalTokenCreate,
    /// Re-run `tribal bootstrap` to regenerate the namespaced default credential.
    RerunBootstrap,
    /// A query failed after connectivity was confirmed; distinct from
    /// [`Self::CheckPgIsready`] which would mislead the operator here.
    ConsultUnderlyingError,
    /// Start `tribal serve` so it binds the advertised URL.
    StartServeOnAdvertisedUrl,
    /// Remove the duplicate `tribal` binaries from PATH or reorder so
    /// the intended one wins.
    PruneDuplicateBinaries { paths: Vec<PathBuf> },
    /// Ensure `tribal`'s install directory is on PATH so other tooling
    /// can discover it.
    EnsureTribalOnPath,
    /// Verify the supplied project ID or register a new project with
    /// `tribal project register`.
    VerifyProjectIdOrRegister,
    /// Inspect the provider configuration for the named target — the
    /// `api_key` (for cloud providers), the `base_url`, and the
    /// upstream env-var conventions — or run `tribal serve` to see the
    /// underlying startup probe warning.
    FixProviderConfig {
        target: ProviderStage,
        provider: ProviderKind,
    },
    /// The genesis-embedding probe failed; name `init.embedding.base_url`
    /// (the genesis endpoint) and the catalogue credential that backs it,
    /// because the embedding credential resolves through the catalogue, not
    /// an `init.embedding.api_key` field.
    FixEmbeddingProviderConfig { provider: ProviderKind },
    /// Add a catalogue credential for the active embedding provider's
    /// endpoint so the next server boot resolves it.
    AddEmbeddingCredential { provider: ProviderKind },
    /// A catalogue entry already matches the active endpoint, but its `api_key`
    /// is empty; set the key on the existing connection rather than adding one.
    SetEmbeddingCredentialKey {
        connection: ProviderConnectionName,
        provider: ProviderKind,
    },
    /// Inspect the live reindex run and any quarantined items.
    InspectReindexRun,
}

impl CheckRemediation {
    /// Renders the remediation as the wire-format string.
    pub(in crate::management::operator_check) fn render(&self) -> String {
        match self {
            Self::InspectConfigFile { path } => {
                format!("inspect {} for syntax errors", path.display())
            }
            Self::FixConfigInvariant { hints } => hints.join("\n"),
            Self::CheckPgIsready => {
                "run `pg_isready` against the configured database URL and verify the host, \
                 port, and credentials"
                    .into()
            }
            Self::RunDatabaseInitialise => {
                "run `tribal database initialise` to initialise or migrate the database".into()
            }
            Self::UpgradeBinary => {
                "upgrade the `tribal` binary to a version that includes the database's \
                 applied migrations"
                    .into()
            }
            Self::RegisterProjectOrSetEnv => {
                "register a project with `tribal project register` or set `TRIBAL_PROJECT_ID`"
                    .into()
            }
            Self::RunTribalTokenCreate => {
                "mint a new bearer token with `tribal token create`".into()
            }
            Self::RerunBootstrap => {
                "re-run `tribal bootstrap` to regenerate the namespaced default credential".into()
            }
            Self::ConsultUnderlyingError => {
                "inspect the underlying error above; the database itself is reachable".into()
            }
            Self::StartServeOnAdvertisedUrl => {
                "start `tribal serve` so it binds the advertised URL".into()
            }
            Self::PruneDuplicateBinaries { paths } => {
                let rendered: Vec<String> = paths.iter().map(|p| p.display().to_string()).collect();
                format!(
                    "remove or reorder the duplicate `tribal` entries on PATH: {}",
                    rendered.join(", "),
                )
            }
            Self::EnsureTribalOnPath => {
                "ensure `tribal`'s install directory is on PATH so other tooling can discover it"
                    .into()
            }
            Self::VerifyProjectIdOrRegister => {
                "verify the supplied project ID or register a new project with \
                 `tribal project register`"
                    .into()
            }
            Self::FixProviderConfig { target, provider } => {
                let path = target.section_path().extend("connection");
                format!(
                    "check `{path}` and its `provider_connections` entry for {provider}, or run \
                     `tribal serve` to see the underlying startup probe warning"
                )
            }
            Self::FixEmbeddingProviderConfig { provider } => {
                let connection_path = ProviderStage::Embedding.section_path().extend("connection");
                format!(
                    "check `{connection_path}` and {}, or run `tribal serve` to see the \
                     underlying startup probe warning",
                    embedding_credential_phrase(*provider),
                )
            }
            Self::AddEmbeddingCredential { provider } => embedding_credential_phrase(*provider),
            Self::SetEmbeddingCredentialKey {
                connection,
                provider,
            } => {
                let path = format!("provider_connections.{connection}.api_key");
                match standard_env_var_name(*provider) {
                    Some(standard) => format!(
                        "the `{connection}` connection exists but has no key; set `{path}` (or \
                         export `TRIBAL_PROVIDER_CONNECTIONS__{}__API_KEY` / `{standard}`)",
                        connection.as_str().to_uppercase()
                    ),
                    None => format!(
                        "the `{connection}` connection exists but has no key; set its `api_key`"
                    ),
                }
            }
            Self::InspectReindexRun => {
                "confirm the reindex worker is running (cancel a stalled run with `tribal reindex \
                 cancel`), then re-run `tribal reindex` once any quarantined items are addressed"
                    .into()
            }
        }
    }
}

/// Renders the "set the catalogue credential" phrase shared by the
/// embedding-credential remediations, naming the `<provider>_default`
/// connection and the env-override paths that satisfy it.
fn embedding_credential_phrase(provider: ProviderKind) -> String {
    let connection = format!("{provider}_default");
    let path = format!("provider_connections.{connection}.api_key");
    match standard_env_var_name(provider) {
        Some(standard) => format!(
            "set `{path}` (or export `TRIBAL_PROVIDER_CONNECTIONS__{}__API_KEY` / `{standard}`) so the \
             endpoint resolves",
            connection.to_uppercase()
        ),
        None => format!("add `provider_connections.{connection}` for the endpoint"),
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_check_detail_renders_path_variant() {
        let detail = CheckDetail::ConfigLoaded {
            path: PathBuf::from("/etc/tribal/config.yaml"),
        };
        assert_eq!(
            detail.render(),
            "config loaded from /etc/tribal/config.yaml"
        );
    }

    #[test]
    fn test_check_detail_renders_config_parse_failed() {
        let detail = CheckDetail::ConfigParseFailed {
            error: "expected a string at line 3".into(),
            path: PathBuf::from("/etc/tribal/config.yaml"),
        };
        assert_eq!(
            detail.render(),
            "config at /etc/tribal/config.yaml failed to load: expected a string at line 3"
        );
    }

    #[test]
    fn test_check_remediation_renders_inspect_config_file() {
        let remediation = CheckRemediation::InspectConfigFile {
            path: PathBuf::from("/etc/tribal/config.yaml"),
        };
        assert_eq!(
            remediation.render(),
            "inspect /etc/tribal/config.yaml for syntax errors"
        );
    }

    #[test]
    fn test_fix_embedding_provider_config_names_the_live_sections_not_the_removed_ones() {
        let rendered = CheckRemediation::FixEmbeddingProviderConfig {
            provider: ProviderKind::OpenAi,
        }
        .render();
        assert!(
            rendered.contains("init.embedding.connection"),
            "should name the genesis connection field: {rendered}",
        );
        assert!(
            rendered.contains("provider_connections.openai_default.api_key"),
            "should name the provider connection credential: {rendered}",
        );
        assert!(
            !rendered.contains("embedding.api_key") && !rendered.contains("TRIBAL_EMBEDDING__"),
            "must not name the removed embedding.* shape: {rendered}",
        );
    }
}
