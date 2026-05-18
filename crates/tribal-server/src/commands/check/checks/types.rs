//! Internal data layer for `tribal check`.
//!
//! Each check returns a [`CheckOutcome`] carrying a typed [`CheckDetail`]
//! and an optional [`CheckRemediation`].  Variants own their data so
//! rendering — to the wire format or the human form — is a single match
//! per enum.

use std::path::PathBuf;

use serde::{Deserialize, Serialize};

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
    Fail {
        name: CheckName,
        detail: CheckDetail,
        remediation: Option<CheckRemediation>,
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
        }
    }
}

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
