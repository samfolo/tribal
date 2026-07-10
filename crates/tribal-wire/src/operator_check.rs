//! Check identity and row vocabulary shared by local operator contracts.

use serde::{Deserialize, Serialize};

/// Identifier for a single check row.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum CheckName {
    /// Config file parse/read check.
    ConfigParse,
    /// Config invariant validation check.
    ConfigValidate,
    /// Database connectivity check.
    DatabaseReachable,
    /// Migration head check.
    MigrationsCurrent,
    /// Project resolution check.
    ProjectResolution,
    /// Static token verification check.
    ValidTokenExists,
    /// Advertised URL reachability check.
    AdvertisedUrlReachable,
    /// PATH binary uniqueness check.
    BinaryUniqueness,
    /// Active embedding profile check.
    EmbeddingProfile,
    /// Embedding provider probe check.
    ProviderEmbedding,
    /// Extraction provider probe check.
    ProviderExtraction,
    /// Triage provider probe check.
    ProviderTriage,
    /// Relation provider probe check.
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
            Self::EmbeddingProfile => "embedding_profile",
            Self::ProviderEmbedding => "provider_embedding",
            Self::ProviderExtraction => "provider_extraction",
            Self::ProviderTriage => "provider_triage",
            Self::ProviderRelation => "provider_relation",
        }
    }
}

/// One row of the check report.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(tag = "status", rename_all = "lowercase")]
pub enum CheckResult {
    /// The check passed.
    Pass {
        /// Which check produced the row.
        name: CheckName,
        /// Human-readable detail for the observed state.
        detail: String,
    },
    /// The check surfaced a warning.
    Warn {
        /// Which check produced the row.
        name: CheckName,
        /// Human-readable detail for the observed state.
        detail: String,
        /// Suggested operator action.
        remediation: String,
    },
    /// The check failed.
    Fail {
        /// Which check produced the row.
        name: CheckName,
        /// Human-readable detail for the observed state.
        detail: String,
        /// Suggested operator action.
        remediation: String,
    },
    /// The check was skipped because a precondition did not hold.
    Skip {
        /// Which check produced the row.
        name: CheckName,
        /// Human-readable detail for the observed state.
        detail: String,
    },
}
