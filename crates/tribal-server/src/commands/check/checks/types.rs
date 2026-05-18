//! Internal data layer for `tribal check`.
//!
//! Each check returns a [`CheckOutcome`] carrying a typed [`CheckDetail`]
//! and an optional [`CheckRemediation`].  Variants own their data so
//! rendering — to the wire format or the human form — is a single match
//! per enum.

use std::path::PathBuf;

use serde::{Deserialize, Serialize};
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

// ---------------------------------------------------------------------------
// CheckOutcome
// ---------------------------------------------------------------------------

/// What a single check produces.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::commands::check) enum CheckOutcome {
    Pass {
        name: CheckName,
        detail: CheckDetail,
    },
    Warn {
        name: CheckName,
        detail: CheckDetail,
        remediation: Option<CheckRemediation>,
    },
    Fail {
        name: CheckName,
        detail: CheckDetail,
        remediation: Option<CheckRemediation>,
    },
    Skip {
        name: CheckName,
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
