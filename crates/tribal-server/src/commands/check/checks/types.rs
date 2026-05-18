//! Internal data layer for `tribal check`.
//!
//! Each check returns a [`CheckOutcome`] carrying a typed [`CheckDetail`].
//! The variants own their data so presentation (rendering to the wire
//! format, rendering to the human form) is a single match per enum
//! rather than concatenated string literals scattered across per-check
//! files.  New variants land alongside their producers.

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
#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::commands::check) struct CheckOutcome {
    pub name: CheckName,
    pub status: CheckStatus,
    pub detail: CheckDetail,
}

// ---------------------------------------------------------------------------
// CheckDetail
// ---------------------------------------------------------------------------

/// Typed description of what a check observed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::commands::check) enum CheckDetail {
    /// Config file parsed successfully from `path`.
    ConfigLoaded { path: PathBuf },
}

impl CheckDetail {
    /// Renders the detail as the wire-format string.
    pub(in crate::commands::check) fn render(&self) -> String {
        match self {
            Self::ConfigLoaded { path } => format!("config loaded from {}", path.display()),
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
    fn test_check_detail_renders_path_variant() {
        let detail = CheckDetail::ConfigLoaded {
            path: PathBuf::from("/etc/tribal/config.yaml"),
        };
        assert_eq!(
            detail.render(),
            "config loaded from /etc/tribal/config.yaml"
        );
    }
}
