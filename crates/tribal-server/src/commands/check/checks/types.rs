//! Internal data layer for `tribal check`.
//!
//! Each check returns a [`CheckOutcome`] carrying a typed [`CheckDetail`]
//! and optional [`CheckRemediation`].  The variants own their data so
//! presentation (rendering to the wire format, rendering to the human
//! form) can be a single match per enum rather than concatenated string
//! literals scattered across per-check files.

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
// CheckStatus
// ---------------------------------------------------------------------------

/// Status discriminant shared between the internal outcome and the
/// wire format.
///
/// Serialised lowercase — `pass | warn | fail | skip` — matching the
/// frozen JSON schema consumed by downstream tooling (Docker
/// healthcheck, install-path tests, operational skill).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum CheckStatus {
    Pass,
    Warn,
    Fail,
    Skip,
}

// ---------------------------------------------------------------------------
// CheckOutcome
// ---------------------------------------------------------------------------

/// What a single check produces.
///
/// `detail` and `remediation` are typed; the wire format renders them
/// via the conversion in `super::super::output`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::commands::check) struct CheckOutcome {
    pub name: CheckName,
    pub status: CheckStatus,
    pub detail: CheckDetail,
    pub remediation: Option<CheckRemediation>,
}

// ---------------------------------------------------------------------------
// CheckDetail
// ---------------------------------------------------------------------------

/// Typed description of what a check observed.
///
/// New variants are added by the per-check work as it lands; rendering
/// is a single exhaustive `match` in [`CheckDetail::render`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::commands::check) enum CheckDetail {
    /// Config file parsed successfully from `path`.
    ConfigLoaded { path: PathBuf },
    /// All configuration invariants passed.
    AllInvariantsSatisfied,
}

impl CheckDetail {
    /// Renders the detail as the wire-format string.
    pub(in crate::commands::check) fn render(&self) -> String {
        match self {
            Self::ConfigLoaded { path } => format!("config loaded from {}", path.display()),
            Self::AllInvariantsSatisfied => "all config invariants satisfied".to_owned(),
        }
    }
}

// ---------------------------------------------------------------------------
// CheckRemediation
// ---------------------------------------------------------------------------

/// Typed remediation hint paired with a non-`Pass` outcome.
///
/// New variants are added by the per-check work as it lands.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::commands::check) enum CheckRemediation {
    /// Direct the operator at the resolved config file path.
    InspectConfigFile { path: PathBuf },
    /// Direct the operator to run `tribal setup`.
    RunTribalSetup,
}

impl CheckRemediation {
    /// Renders the remediation as the wire-format string.
    pub(in crate::commands::check) fn render(&self) -> String {
        match self {
            Self::InspectConfigFile { path } => {
                format!("inspect the config file at {}", path.display())
            }
            Self::RunTribalSetup => "run `tribal setup`".to_owned(),
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
    fn test_check_status_serialises_lowercase() {
        assert_eq!(
            serde_json::to_string(&CheckStatus::Pass).expect("serialise"),
            "\"pass\"",
        );
        assert_eq!(
            serde_json::to_string(&CheckStatus::Warn).expect("serialise"),
            "\"warn\"",
        );
        assert_eq!(
            serde_json::to_string(&CheckStatus::Fail).expect("serialise"),
            "\"fail\"",
        );
        assert_eq!(
            serde_json::to_string(&CheckStatus::Skip).expect("serialise"),
            "\"skip\"",
        );
    }

    #[test]
    fn test_check_status_round_trips_through_json() {
        for status in [
            CheckStatus::Pass,
            CheckStatus::Warn,
            CheckStatus::Fail,
            CheckStatus::Skip,
        ] {
            let json = serde_json::to_string(&status).expect("serialise");
            let back: CheckStatus = serde_json::from_str(&json).expect("deserialise");
            assert_eq!(status, back);
        }
    }

    #[test]
    fn test_check_detail_renders_no_field_variant() {
        assert_eq!(
            CheckDetail::AllInvariantsSatisfied.render(),
            "all config invariants satisfied",
        );
    }

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
    fn test_check_remediation_renders_no_field_variant() {
        assert_eq!(
            CheckRemediation::RunTribalSetup.render(),
            "run `tribal setup`"
        );
    }

    #[test]
    fn test_check_remediation_renders_path_variant() {
        let remediation = CheckRemediation::InspectConfigFile {
            path: PathBuf::from("/etc/tribal/config.yaml"),
        };
        assert_eq!(
            remediation.render(),
            "inspect the config file at /etc/tribal/config.yaml",
        );
    }
}
