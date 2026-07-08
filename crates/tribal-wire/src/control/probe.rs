//! One-shot diagnostics that power the config facade.

use serde::{Deserialize, Serialize};
use tribal_domain::ProviderKind;

// ---------------------------------------------------------------------------
// database.probe
// ---------------------------------------------------------------------------

/// Parameters for `database.probe`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct DatabaseProbeRequest {
    /// `PostgreSQL` URL to test.
    pub url: String,
}

/// Result of probing a database URL.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum DatabaseProbe {
    /// The database accepted a connection.
    Reachable,
    /// The endpoint responded but rejected credentials.
    Unauthorized {
        /// The underlying refusal, rendered for operators.
        detail: String,
    },
    /// The endpoint could not be reached.
    Unreachable {
        /// The underlying connection failure, rendered for operators.
        detail: String,
    },
}

// ---------------------------------------------------------------------------
// credential.probe
// ---------------------------------------------------------------------------

/// Parameters for `credential.probe`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct CredentialProbeRequest {
    /// Provider whose configured credential should be probed.
    pub provider: ProviderKind,
}

/// Result of probing a configured provider credential.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum CredentialProbe {
    /// The provider accepted the configured credential.
    Healthy,
    /// The provider rejected the configured credential.
    Unauthorized {
        /// The underlying refusal, rendered for operators.
        detail: String,
    },
    /// The provider could not be probed from the current configuration.
    Unknown {
        /// Why the probe could not determine credential health.
        detail: String,
    },
}

// ---------------------------------------------------------------------------
// graph.embedding_profile
// ---------------------------------------------------------------------------

/// Stable summary of an active embedding profile.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct EmbeddingProfileSummary {
    /// Provider used by the profile.
    pub provider: ProviderKind,
    /// Normalised endpoint for the profile.
    pub base_url: String,
    /// Embedding model.
    pub model: String,
    /// Embedding dimensionality.
    pub dimensions: u32,
}

/// Result of reading the active embedding profile.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum GraphEmbeddingProfile {
    /// No active profile exists yet.
    NoProfile,
    /// The active profile exists.
    Active {
        /// The active profile identity.
        profile: EmbeddingProfileSummary,
        /// The genesis seed identity when it differs from the active profile.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        genesis_drift: Option<String>,
    },
    /// The database could not answer profile state.
    Unknown {
        /// Why the profile state is unknown.
        detail: String,
    },
}
