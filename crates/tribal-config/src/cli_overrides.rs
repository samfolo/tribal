//! CLI flag overrides for the configuration cascade.
//!
//! Only explicitly-passed values participate in the merge; absent fields
//! are skipped via `skip_serializing_if` so lower-precedence layers are
//! not masked.
//!
//! The same shape doubles as the on-disk projection that
//! [`crate::render_persisted_config`] writes during `tribal bootstrap`.

use std::collections::BTreeMap;

use serde::Serialize;
use tribal_domain::ProviderKind;

use crate::sections::TransportKind;

// ---------------------------------------------------------------------------
// Top-level overrides
// ---------------------------------------------------------------------------

/// CLI flag overrides merged at the highest precedence.
#[derive(Debug, Default, Clone, Serialize)]
pub struct CliOverrides {
    /// Server-related CLI overrides.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub server: Option<ServerCliOverrides>,

    /// Database-related CLI overrides.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub database: Option<DatabaseCliOverrides>,

    /// Genesis-seed CLI overrides (`init.embedding`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub init: Option<InitCliOverrides>,

    /// Inference-stage CLI overrides.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub inference: Option<InferenceCliOverrides>,

    /// Telemetry CLI overrides.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub telemetry: Option<TelemetryCliOverrides>,

    /// Credential catalogue skeleton, synthesised only during persistence.
    ///
    /// No CLI flag populates this; [`crate::render_persisted_config`] fills it
    /// with the genesis `<provider>_default` connection when a key-requiring
    /// embedding provider is seeded, so the runtime has a catalogue entry to
    /// resolve the credential into. The key itself is never persisted.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub credentials: Option<BTreeMap<String, PersistedCredentialEntry>>,
}

// ---------------------------------------------------------------------------
// Section overrides
// ---------------------------------------------------------------------------

/// Server-related CLI flag overrides.
#[derive(Debug, Clone, Serialize)]
pub struct ServerCliOverrides {
    /// Transport override from `--transport`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub transport: Option<TransportKind>,

    /// Bind address override from `--bind`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bind_address: Option<String>,
}

/// Database-related CLI flag overrides.
#[derive(Debug, Clone, Serialize)]
pub struct DatabaseCliOverrides {
    /// Database URL override from `--database-url`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
}

/// Genesis-seed CLI flag overrides, serialised under `init`.
#[derive(Debug, Clone, Serialize)]
pub struct InitCliOverrides {
    /// Genesis embedding identity override.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub embedding: Option<EmbeddingCliOverrides>,
}

/// Genesis embedding-identity CLI flag overrides, serialised under
/// `init.embedding`.
#[derive(Debug, Clone, Serialize)]
pub struct EmbeddingCliOverrides {
    /// Provider override from `--embedding-provider`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub provider: Option<ProviderKind>,

    /// Model name override from `--embedding-model`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
}

/// A persisted credential-catalogue entry skeleton: the endpoint identity
/// without its key. Serialise-only; the runtime deserialises it back into a
/// [`crate::CredentialEntry`] whose `api_key` defaults to absent.
#[derive(Debug, Clone, Serialize)]
pub struct PersistedCredentialEntry {
    /// The provider kind this connection serves.
    pub provider_kind: ProviderKind,

    /// The endpoint base URL.
    pub base_url: String,
}

/// Inference-stage CLI flag overrides.
#[derive(Debug, Clone, Serialize)]
pub struct InferenceCliOverrides {
    /// Extraction-stage overrides.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub extraction: Option<InferenceStageCliOverrides>,

    /// Triage-stage overrides.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub triage: Option<InferenceStageCliOverrides>,

    /// Relation-stage overrides.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub relation: Option<InferenceStageCliOverrides>,
}

/// Per-stage inference CLI flag overrides.
#[derive(Debug, Clone, Serialize)]
pub struct InferenceStageCliOverrides {
    /// Provider override.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub provider: Option<ProviderKind>,

    /// Model name override.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
}

/// Telemetry CLI flag overrides.
#[derive(Debug, Clone, Serialize)]
pub struct TelemetryCliOverrides {
    /// OTLP exporter endpoint override from `--telemetry-otlp-endpoint`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub otlp_endpoint: Option<String>,
}
