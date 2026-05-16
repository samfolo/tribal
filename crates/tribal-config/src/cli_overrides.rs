//! CLI flag overrides for the configuration cascade.
//!
//! Only explicitly-passed values participate in the merge; absent fields
//! are skipped via `skip_serializing_if` so lower-precedence layers are
//! not masked.
//!
//! The same shape doubles as the on-disk projection that
//! [`crate::render_persisted_config`] writes during `tribal bootstrap` —
//! see the [`crate::Persisted`] trait for the conversion that pins
//! resolved values into each populated slot.

use serde::Serialize;

use crate::sections::{ProviderKind, TransportKind};

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

    /// Embedding-stage CLI overrides.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub embedding: Option<EmbeddingCliOverrides>,

    /// Inference-stage CLI overrides.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub inference: Option<InferenceCliOverrides>,

    /// Telemetry CLI overrides.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub telemetry: Option<TelemetryCliOverrides>,
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

/// Embedding-stage CLI flag overrides.
#[derive(Debug, Clone, Serialize)]
pub struct EmbeddingCliOverrides {
    /// Provider override from `--embedding-provider`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub provider: Option<ProviderKind>,

    /// Model name override from `--embedding-model`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
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
