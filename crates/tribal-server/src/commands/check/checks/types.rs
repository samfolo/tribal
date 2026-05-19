//! Internal data layer for `tribal check`.
//!
//! Each check returns a [`CheckOutcome`] carrying a typed [`CheckDetail`]
//! and an optional [`CheckRemediation`].  Variants own their data so
//! rendering — to the wire format or the human form — is a single match
//! per enum.

use std::path::PathBuf;

use serde::{Deserialize, Serialize};
use tribal_config::ProviderKind;
use tribal_domain::ProjectId;

// ---------------------------------------------------------------------------
// CheckName
// ---------------------------------------------------------------------------

/// Identifier for a single check.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CheckName {
    ConfigParse,
    ConfigValidate,
    DatabaseReachable,
    MigrationsCurrent,
    ProjectResolution,
    ValidTokenExists,
    AdvertisedUrlReachable,
    BinaryUniqueness,
    ProviderEmbedding,
    ProviderExtraction,
    ProviderTriage,
    ProviderRelation,
}

impl CheckName {
    /// Snake-case identifier matching the serde wire form.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::ConfigParse => "config_parse",
            Self::ConfigValidate => "config_validate",
            Self::DatabaseReachable => "database_reachable",
            Self::MigrationsCurrent => "migrations_current",
            Self::ProjectResolution => "project_resolution",
            Self::ValidTokenExists => "valid_token_exists",
            Self::AdvertisedUrlReachable => "advertised_url_reachable",
            Self::BinaryUniqueness => "binary_uniqueness",
            Self::ProviderEmbedding => "provider_embedding",
            Self::ProviderExtraction => "provider_extraction",
            Self::ProviderTriage => "provider_triage",
            Self::ProviderRelation => "provider_relation",
        }
    }
}

// ---------------------------------------------------------------------------
// CheckOutcome
// ---------------------------------------------------------------------------

/// What a single check produces.
///
/// The check name is derived from the carried [`CheckDetail`] via
/// [`CheckDetail::name`], so a pairing of `(Pass, DatabaseReachable)`
/// is impossible to build with a `ConfigLoaded` detail.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::commands::check) enum CheckOutcome {
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
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub(in crate::commands::check) struct CheckOutcomes(Vec<CheckOutcome>);

impl CheckOutcomes {
    pub(in crate::commands::check) fn new() -> Self {
        Self::default()
    }

    pub(in crate::commands::check) fn push(&mut self, outcome: CheckOutcome) {
        self.0.push(outcome);
    }

    pub(in crate::commands::check) fn iter(&self) -> std::slice::Iter<'_, CheckOutcome> {
        self.0.iter()
    }
}

// ---------------------------------------------------------------------------
// CheckDetail
// ---------------------------------------------------------------------------

/// Typed description of what a check observed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::commands::check) enum CheckDetail {
    /// Config file parsed successfully from `path`.
    ConfigLoaded { path: PathBuf },
    /// Config file failed to load or deserialise.
    ConfigParseFailed { error: String, path: PathBuf },
    /// All configuration invariants passed.
    AllInvariantsSatisfied,
    /// One or more configuration invariants failed.
    ValidationFailed { errors: Vec<String> },
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
    /// No token resolvable from any source, but the database has at
    /// least one active token.
    TokenAggregateWarn,
    /// No token resolvable from any source and the database has no
    /// active tokens.
    NoActiveTokens,
    /// The aggregate any-active query failed at the infrastructure layer.
    TokenAggregateQueryFailed { error: String },
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
        target: ProviderProbeTarget,
        provider: ProviderKind,
    },
    /// Provider probe against the named target failed; `error` is the
    /// underlying [`tribal_inference::InferenceError`] rendered.
    ProviderProbeFailed {
        target: ProviderProbeTarget,
        provider: ProviderKind,
        error: String,
    },
}

// ---------------------------------------------------------------------------
// ProviderProbeTarget / SkipReason
// ---------------------------------------------------------------------------

/// Which provider-backed configuration block a probe is targeting.
///
/// The three inference variants are pipeline stages; embedding is not a
/// stage (it runs both at query time and within triage) but shares the
/// config-shape (`provider` + `api_key`) and so groups with them here.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::commands::check) enum ProviderProbeTarget {
    Embedding,
    Extraction,
    Triage,
    Relation,
}

impl ProviderProbeTarget {
    pub(in crate::commands::check) fn config_path(self) -> &'static str {
        match self {
            Self::Embedding => "embedding",
            Self::Extraction => "inference.extraction",
            Self::Triage => "inference.triage",
            Self::Relation => "inference.relation",
        }
    }

    pub(in crate::commands::check) fn check_name(self) -> CheckName {
        match self {
            Self::Embedding => CheckName::ProviderEmbedding,
            Self::Extraction => CheckName::ProviderExtraction,
            Self::Triage => CheckName::ProviderTriage,
            Self::Relation => CheckName::ProviderRelation,
        }
    }
}

/// Why a downstream check did not run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::commands::check) enum SkipReason {
    /// The config file could not be loaded; nothing downstream can run.
    ConfigParseFailed,
    /// A `server.bind_address` validation error blocks advertised-URL
    /// resolution, so the probe is skipped.
    ConfigValidateFailedTransportBind,
    /// The named target has a missing `api_key` validation error, so
    /// its provider probe is skipped.
    ConfigValidateFailedApiKey { target: ProviderProbeTarget },
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
pub(in crate::commands::check) enum TokenTransport {
    Stdio,
    Http,
}

/// Why token verification failed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::commands::check) enum TokenFailureReason {
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
    pub(in crate::commands::check) fn name(&self) -> CheckName {
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
            | Self::TokenVerified { .. }
            | Self::TokenVerificationFailed { .. }
            | Self::TokenAggregateWarn
            | Self::NoActiveTokens
            | Self::TokenAggregateQueryFailed { .. } => CheckName::ValidTokenExists,
            Self::AdvertisedUrlSkippedStdio
            | Self::AdvertisedUrlReachable { .. }
            | Self::AdvertisedUrlUnreachable { .. } => CheckName::AdvertisedUrlReachable,
            Self::BinaryUnique { .. } | Self::BinaryDuplicate { .. } | Self::BinaryAbsent => {
                CheckName::BinaryUniqueness
            }
            Self::DependencySkipped { name, .. } => *name,
            Self::ProviderProbePassed { target, .. } | Self::ProviderProbeFailed { target, .. } => {
                target.check_name()
            }
        }
    }

    /// Renders the detail as the wire-format string.
    pub(in crate::commands::check) fn render(&self) -> String {
        match self {
            Self::ConfigLoaded { path } => format!("config loaded from {}", path.display()),
            Self::ConfigParseFailed { error, path } => {
                format!("config at {} failed to load: {error}", path.display())
            }
            Self::AllInvariantsSatisfied => "all configuration invariants satisfied".into(),
            Self::ValidationFailed { errors } => errors.join("\n"),
            Self::DatabaseReachable => "database connection succeeded".into(),
            Self::DatabaseUnreachable { error } => format!("database unreachable: {error}"),
            Self::MigrationsMatch => "migrations are current".into(),
            Self::MigrationsBehind { expected, found } => format!(
                "database is at migration {found}; binary expects {expected}; database is behind"
            ),
            Self::MigrationsAhead { expected, found } => format!(
                "database is at migration {found}; binary expects {expected}; database is ahead"
            ),
            Self::MigrationsTableMissing => {
                "database is uninitialised; run `tribal setup` first".into()
            }
            Self::MigrationsQueryFailed { error } => {
                format!("migration check query failed: {error}")
            }
            Self::ProjectFound { project_id, name } => {
                format!("resolved project `{name}` ({project_id})")
            }
            Self::ProjectNotFound { error } => format!("project not found: {error}"),
            Self::ProjectCascadeMissing => {
                "no project resolved from CLI flag, environment, or git remote".into()
            }
            Self::ProjectQueryFailed { error } => {
                format!("project lookup failed: {error}")
            }
            Self::TokenSkippedStdio => {
                "stdio transport: no `--token` supplied; verification skipped".into()
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
            Self::TokenAggregateWarn => {
                "no token resolvable, but at least one active token exists in the database".into()
            }
            Self::NoActiveTokens => {
                "no token resolvable and no active tokens exist in the database".into()
            }
            Self::TokenAggregateQueryFailed { error } => {
                format!("token aggregate check failed: {error}")
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
                    "skipped because `{}.api_key` validation failed",
                    target.config_path()
                ),
                SkipReason::DatabaseUnreachable => {
                    "skipped because the database is unreachable".into()
                }
            },
            Self::ProviderProbePassed { target, provider } => format!(
                "{} provider ({provider}) probe passed",
                target.config_path()
            ),
            Self::ProviderProbeFailed {
                target,
                provider,
                error,
            } => format!(
                "{} provider ({provider}) probe failed: {error}",
                target.config_path()
            ),
        }
    }
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
pub(in crate::commands::check) enum CheckRemediation {
    /// Inspect the config file at `path` for parse errors.
    InspectConfigFile { path: PathBuf },
    /// One or more targeted hints for the validation errors collected.
    FixConfigInvariant { hints: Vec<String> },
    /// Verify database availability with `pg_isready` and review the
    /// `database.url` field.
    CheckPgIsready,
    /// Run `tribal setup` to initialise the database or apply pending
    /// migrations.
    RunTribalSetup,
    /// Upgrade the `tribal` binary to a version that knows about the
    /// migrations already applied to the database.
    UpgradeBinary,
    /// Register a project with `tribal project register` or set
    /// `TRIBAL_PROJECT_ID`.
    RegisterProjectOrSetEnv,
    /// Mint a new bearer token with `tribal token create`.
    RunTribalTokenCreate,
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
        target: ProviderProbeTarget,
        provider: ProviderKind,
    },
}

impl CheckRemediation {
    /// Renders the remediation as the wire-format string.
    pub(in crate::commands::check) fn render(&self) -> String {
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
            Self::RunTribalSetup => {
                "run `tribal setup` to initialise or migrate the database".into()
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
                let path = target.config_path();
                match provider.standard_env_var_name() {
                    Some(env) => {
                        format!("check `{path}.api_key` (or export `{env}`) and `{path}.base_url`")
                    }
                    None => {
                        format!("check `{path}.base_url` and confirm the provider is reachable")
                    }
                }
            }
        }
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
}
