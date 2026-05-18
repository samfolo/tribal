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
// CheckDetail
// ---------------------------------------------------------------------------

/// Typed description of what a check observed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::commands::check) enum CheckDetail {
    /// Config file parsed successfully from `path`.
    ConfigLoaded { path: PathBuf },
    /// Config file failed to load or deserialise.
    ConfigParseFailed { error: String, path: PathBuf },
}

impl CheckDetail {
    /// Renders the detail as the wire-format string.
    pub(in crate::commands::check) fn render(&self) -> String {
        match self {
            Self::ConfigLoaded { path } => format!("config loaded from {}", path.display()),
            Self::ConfigParseFailed { error, path } => {
                format!("config at {} failed to load: {error}", path.display())
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
}

impl CheckRemediation {
    /// Renders the remediation as the wire-format string.
    pub(in crate::commands::check) fn render(&self) -> String {
        match self {
            Self::InspectConfigFile { path } => {
                format!("inspect {} for syntax errors", path.display())
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
