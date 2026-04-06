//! System fingerprint computation: hash derivation and construction.
//!
//! The system fingerprint captures all inference-affecting configuration
//! values as a content-addressed SHA-256 hash. This module provides the
//! functions to compute that hash and build the repository input type.

use std::collections::HashMap;

use sqlx::PgConnection;
use tribal_common::sha256_hex;
use tribal_config::TribalConfig;
use tribal_db::{DbError, NewSystemFingerprint};
use tribal_domain::{
    EmbeddingParameters, InferenceParameters, McpErrorCode, PipelineParameters, PromptVersion,
    PromptVersionId, StageParameters,
};
use tribal_inference::ProviderIdentity;

use crate::{
    error::{IntoMcpError, McpToolError},
    server_handler::{ActivePromptVersions, ConnectionRepositories},
};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

pub(crate) const MISSING_PROMPT_VERSIONS: &str =
    "one or more active prompt versions could not be found in the database";

// ---------------------------------------------------------------------------
// PromptContentHashes
// ---------------------------------------------------------------------------

/// Named prompt content hashes in canonical ordering.
///
/// Maps each (stage, role) pair to its content hash, making the
/// ordering self-documenting and preventing silent ordering bugs
/// between call sites.
pub(crate) struct PromptContentHashes {
    pub extraction_system: String,
    pub extraction_user: String,
    pub triage_system: String,
    pub triage_user: String,
    pub relation_system: String,
    pub relation_user: String,
}

impl PromptContentHashes {
    /// Constructs content hashes from active prompt version IDs and the
    /// corresponding `PromptVersion` records looked up from the database.
    ///
    /// Returns `None` if any of the 6 required versions is missing.
    pub(crate) fn from_active(
        active: &ActivePromptVersions,
        versions: &[PromptVersion],
    ) -> Option<Self> {
        let by_id: HashMap<PromptVersionId, &str> = versions
            .iter()
            .map(|v| (v.id(), v.content_hash()))
            .collect();

        Some(Self {
            extraction_system: by_id
                .get(&active.extraction_system_prompt_version_id)?
                .to_string(),
            extraction_user: by_id
                .get(&active.extraction_user_prompt_version_id)?
                .to_string(),
            triage_system: by_id
                .get(&active.triage_system_prompt_version_id)?
                .to_string(),
            triage_user: by_id
                .get(&active.triage_user_prompt_version_id)?
                .to_string(),
            relation_system: by_id
                .get(&active.relation_system_prompt_version_id)?
                .to_string(),
            relation_user: by_id
                .get(&active.relation_user_prompt_version_id)?
                .to_string(),
        })
    }

    /// Returns hashes in canonical order, matching the
    /// `SystemFingerprint` struct field declaration order.
    fn as_ordered_slice(&self) -> [&str; 6] {
        [
            &self.extraction_system,
            &self.extraction_user,
            &self.triage_system,
            &self.triage_user,
            &self.relation_system,
            &self.relation_user,
        ]
    }
}

// ---------------------------------------------------------------------------
// PipelineProviderIdentities
// ---------------------------------------------------------------------------

/// Provider identities for all inference and embedding providers.
///
/// Reuses [`ProviderIdentity`] from the inference crate — the same
/// type returned by each provider's `identity()` method.
pub(crate) struct PipelineProviderIdentities {
    pub extraction: ProviderIdentity,
    pub triage: ProviderIdentity,
    pub relation: ProviderIdentity,
    pub embedding: ProviderIdentity,
}

// ---------------------------------------------------------------------------
// Construction helpers
// ---------------------------------------------------------------------------

/// Builds [`InferenceParameters`] from the resolved configuration.
#[must_use]
pub fn build_inference_parameters(config: &TribalConfig) -> InferenceParameters {
    InferenceParameters {
        extraction: StageParameters {
            temperature: config.inference.extraction.temperature,
            max_tokens: config.inference.extraction.max_tokens,
        },
        triage: StageParameters {
            temperature: config.inference.triage.temperature,
            max_tokens: config.inference.triage.max_tokens,
        },
        relation: StageParameters {
            temperature: config.inference.relation.temperature,
            max_tokens: config.inference.relation.max_tokens,
        },
        embedding: EmbeddingParameters {
            dimensions: config.embedding.dimensions,
        },
        pipeline: PipelineParameters {
            max_candidates_per_job: config.worker.max_candidates_per_job,
            triage_search_limit: config.worker.triage_search_limit,
            tag_similarity_threshold: config.worker.tag_similarity_threshold,
        },
    }
}

// ---------------------------------------------------------------------------
// Hash computation
// ---------------------------------------------------------------------------

/// Computes the system fingerprint SHA-256 hash.
///
/// Input fields are concatenated with newline separators: prompt
/// content hashes, then provider identity strings, then build version,
/// then canonical JSON of `InferenceParameters`.
pub(crate) fn compute_fingerprint_hash(
    hashes: &PromptContentHashes,
    provider_identities: &PipelineProviderIdentities,
    build_version: &str,
    params: &InferenceParameters,
) -> String {
    let ordered = hashes.as_ordered_slice();

    let parts: Vec<&str> = ordered
        .iter()
        .copied()
        .chain([
            provider_identities.extraction.name.as_str(),
            provider_identities.extraction.model.as_str(),
            provider_identities.triage.name.as_str(),
            provider_identities.triage.model.as_str(),
            provider_identities.relation.name.as_str(),
            provider_identities.relation.model.as_str(),
            provider_identities.embedding.name.as_str(),
            provider_identities.embedding.model.as_str(),
            build_version,
        ])
        .collect();

    let json = params.to_canonical_json();

    let mut concatenated = parts.join("\n");
    concatenated.push('\n');
    concatenated.push_str(&json);

    sha256_hex(&concatenated)
}

/// Builds a [`NewSystemFingerprint`] from all component values.
pub(crate) fn build_new_system_fingerprint(
    content_hash: &str,
    build_version: &str,
    active: &ActivePromptVersions,
    provider_identities: &PipelineProviderIdentities,
    params: &InferenceParameters,
) -> NewSystemFingerprint {
    NewSystemFingerprint::builder()
        .content_hash(content_hash.to_owned())
        .build_version(build_version.to_owned())
        .extraction_system_prompt_version_id(active.extraction_system_prompt_version_id)
        .extraction_user_prompt_version_id(active.extraction_user_prompt_version_id)
        .triage_system_prompt_version_id(active.triage_system_prompt_version_id)
        .triage_user_prompt_version_id(active.triage_user_prompt_version_id)
        .relation_system_prompt_version_id(active.relation_system_prompt_version_id)
        .relation_user_prompt_version_id(active.relation_user_prompt_version_id)
        .extraction_inference_provider(provider_identities.extraction.name.clone())
        .extraction_inference_model(provider_identities.extraction.model.clone())
        .triage_inference_provider(provider_identities.triage.name.clone())
        .triage_inference_model(provider_identities.triage.model.clone())
        .relation_inference_provider(provider_identities.relation.name.clone())
        .relation_inference_model(provider_identities.relation.model.clone())
        .embedding_provider(provider_identities.embedding.name.clone())
        .embedding_model(provider_identities.embedding.model.clone())
        .inference_parameters(
            serde_json::to_value(params).expect("InferenceParameters serialisation is infallible"),
        )
        .build()
}

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// Errors from fingerprint computation and upsert.
#[derive(Debug, thiserror::Error)]
pub(crate) enum FingerprintError {
    #[error(transparent)]
    Db(#[from] DbError),

    #[error("{message}")]
    MissingPromptVersions {
        /// Describes which invariant was violated.
        message: &'static str,
    },
}

impl IntoMcpError for FingerprintError {
    fn into_mcp_error(self) -> McpToolError {
        match self {
            Self::Db(e) => e.into_mcp_error(),
            Self::MissingPromptVersions { message } => McpToolError {
                code: McpErrorCode::Internal,
                message: message.to_owned(),
                details: serde_json::json!({}),
            },
        }
    }
}

// ---------------------------------------------------------------------------
// Combined compute + upsert
// ---------------------------------------------------------------------------

/// Looks up prompt content hashes, computes the fingerprint hash,
/// upserts the fingerprint record, and returns the content hash.
pub(crate) async fn compute_and_upsert_fingerprint(
    conn: &mut PgConnection,
    repositories: &ConnectionRepositories,
    active_prompts: &ActivePromptVersions,
    provider_identities: &PipelineProviderIdentities,
    build_version: &str,
    inference_parameters: &InferenceParameters,
) -> Result<String, FingerprintError> {
    let version_ids = active_prompts.version_ids();

    let prompt_versions = repositories
        .prompt_version
        .find_by_ids(conn, &version_ids)
        .await?;

    let content_hashes = PromptContentHashes::from_active(active_prompts, &prompt_versions).ok_or(
        FingerprintError::MissingPromptVersions {
            message: MISSING_PROMPT_VERSIONS,
        },
    )?;

    let fingerprint_hash = compute_fingerprint_hash(
        &content_hashes,
        provider_identities,
        build_version,
        inference_parameters,
    );

    let new_fingerprint = build_new_system_fingerprint(
        &fingerprint_hash,
        build_version,
        active_prompts,
        provider_identities,
        inference_parameters,
    );

    repositories
        .system_fingerprint
        .upsert(conn, &new_fingerprint)
        .await?;

    Ok(fingerprint_hash)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use tribal_common::SHA256_HEX_LENGTH;

    use super::*;

    fn test_hashes() -> PromptContentHashes {
        PromptContentHashes {
            extraction_system: "a".repeat(64),
            extraction_user: "b".repeat(64),
            triage_system: "c".repeat(64),
            triage_user: "d".repeat(64),
            relation_system: "e".repeat(64),
            relation_user: "f".repeat(64),
        }
    }

    fn test_provider_identities() -> PipelineProviderIdentities {
        PipelineProviderIdentities {
            extraction: ProviderIdentity {
                name: "ollama".into(),
                model: "llama3:70b".into(),
            },
            triage: ProviderIdentity {
                name: "ollama".into(),
                model: "llama3:8b".into(),
            },
            relation: ProviderIdentity {
                name: "ollama".into(),
                model: "llama3:8b".into(),
            },
            embedding: ProviderIdentity {
                name: "ollama".into(),
                model: "nomic-embed-text".into(),
            },
        }
    }

    fn test_params() -> InferenceParameters {
        InferenceParameters {
            extraction: StageParameters {
                temperature: 0.2,
                max_tokens: 4096,
            },
            triage: StageParameters {
                temperature: 0.1,
                max_tokens: 2048,
            },
            relation: StageParameters {
                temperature: 0.1,
                max_tokens: 2048,
            },
            embedding: EmbeddingParameters { dimensions: 768 },
            pipeline: PipelineParameters {
                max_candidates_per_job: 20,
                triage_search_limit: 10,
                tag_similarity_threshold: 0.85,
            },
        }
    }

    #[test]
    fn test_hash_produces_valid_hex() {
        let hash = compute_fingerprint_hash(
            &test_hashes(),
            &test_provider_identities(),
            "v0.1.0-abc123",
            &test_params(),
        );

        assert_eq!(hash.len(), SHA256_HEX_LENGTH);
        assert!(hash.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn test_hash_is_deterministic() {
        let a = compute_fingerprint_hash(
            &test_hashes(),
            &test_provider_identities(),
            "v0.1.0",
            &test_params(),
        );
        let b = compute_fingerprint_hash(
            &test_hashes(),
            &test_provider_identities(),
            "v0.1.0",
            &test_params(),
        );

        assert_eq!(a, b);
    }

    #[test]
    fn test_changing_build_version_changes_hash() {
        let a = compute_fingerprint_hash(
            &test_hashes(),
            &test_provider_identities(),
            "v0.1.0",
            &test_params(),
        );
        let b = compute_fingerprint_hash(
            &test_hashes(),
            &test_provider_identities(),
            "v0.2.0",
            &test_params(),
        );

        assert_ne!(a, b);
    }

    #[test]
    fn test_changing_prompt_hash_changes_fingerprint() {
        let mut hashes = test_hashes();
        let a = compute_fingerprint_hash(
            &hashes,
            &test_provider_identities(),
            "v0.1.0",
            &test_params(),
        );

        hashes.extraction_system = "0".repeat(64);
        let b = compute_fingerprint_hash(
            &hashes,
            &test_provider_identities(),
            "v0.1.0",
            &test_params(),
        );

        assert_ne!(a, b);
    }

    #[test]
    fn test_changing_model_changes_hash() {
        let mut identities = test_provider_identities();
        let a = compute_fingerprint_hash(&test_hashes(), &identities, "v0.1.0", &test_params());

        identities.triage.model = "different-model".into();
        let b = compute_fingerprint_hash(&test_hashes(), &identities, "v0.1.0", &test_params());

        assert_ne!(a, b);
    }

    #[test]
    fn test_changing_inference_param_changes_hash() {
        let mut params = test_params();
        let a = compute_fingerprint_hash(
            &test_hashes(),
            &test_provider_identities(),
            "v0.1.0",
            &params,
        );

        params.extraction.temperature = 0.9;
        let b = compute_fingerprint_hash(
            &test_hashes(),
            &test_provider_identities(),
            "v0.1.0",
            &params,
        );

        assert_ne!(a, b);
    }

    #[test]
    fn test_build_inference_parameters_from_config() {
        let config = TribalConfig::default();
        let params = build_inference_parameters(&config);

        assert!(
            (params.extraction.temperature - config.inference.extraction.temperature).abs()
                < f64::EPSILON
        );
        assert_eq!(
            params.extraction.max_tokens,
            config.inference.extraction.max_tokens
        );
        assert_eq!(params.embedding.dimensions, config.embedding.dimensions);
        assert_eq!(
            params.pipeline.max_candidates_per_job,
            config.worker.max_candidates_per_job
        );
    }
}
