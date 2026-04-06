//! System fingerprint entity — content-addressed configuration snapshot.
//!
//! Each fingerprint captures the full set of inference-affecting
//! configuration values at a point in time. Identical configurations
//! produce the same `content_hash`, enabling deduplication.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use typed_builder::TypedBuilder;

use crate::{InferenceParameters, PromptVersionId, SystemFingerprintId};

/// A content-addressed system configuration snapshot.
///
/// The `content_hash` (SHA-256, hex-encoded, 64 chars) uniquely identifies
/// the combination of prompt versions, model identifiers, build version,
/// and inference parameters active when a job or feedback record was
/// created.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TypedBuilder)]
pub struct SystemFingerprint {
    /// Unique identifier with `sfp_` prefix.
    id: SystemFingerprintId,
    /// SHA-256 hash of all fingerprint inputs.
    content_hash: String,
    /// Git-describe version of the build.
    build_version: String,

    // -- Prompt version IDs ---------------------------------------------------
    /// Extraction system prompt version.
    extraction_system_prompt_version_id: PromptVersionId,
    /// Extraction user prompt version.
    extraction_user_prompt_version_id: PromptVersionId,
    /// Triage system prompt version.
    triage_system_prompt_version_id: PromptVersionId,
    /// Triage user prompt version.
    triage_user_prompt_version_id: PromptVersionId,
    /// Relation system prompt version.
    relation_system_prompt_version_id: PromptVersionId,
    /// Relation user prompt version.
    relation_user_prompt_version_id: PromptVersionId,

    // -- Model identifiers ----------------------------------------------------
    /// Extraction inference provider name.
    extraction_inference_provider: String,
    /// Extraction inference model name.
    extraction_inference_model: String,
    /// Triage inference provider name.
    triage_inference_provider: String,
    /// Triage inference model name.
    triage_inference_model: String,
    /// Relation inference provider name.
    relation_inference_provider: String,
    /// Relation inference model name.
    relation_inference_model: String,
    /// Embedding provider name.
    embedding_provider: String,
    /// Embedding model name.
    embedding_model: String,

    // -- Inference parameters --------------------------------------------------
    /// Full inference-affecting parameters as typed JSONB.
    inference_parameters: InferenceParameters,

    /// When this fingerprint was first recorded.
    created_at: DateTime<Utc>,
}

impl SystemFingerprint {
    /// Returns the fingerprint identifier.
    pub fn id(&self) -> SystemFingerprintId {
        self.id
    }

    /// Returns the content hash.
    pub fn content_hash(&self) -> &str {
        &self.content_hash
    }

    /// Returns the build version.
    pub fn build_version(&self) -> &str {
        &self.build_version
    }

    /// Returns the extraction system prompt version ID.
    pub fn extraction_system_prompt_version_id(&self) -> PromptVersionId {
        self.extraction_system_prompt_version_id
    }

    /// Returns the extraction user prompt version ID.
    pub fn extraction_user_prompt_version_id(&self) -> PromptVersionId {
        self.extraction_user_prompt_version_id
    }

    /// Returns the triage system prompt version ID.
    pub fn triage_system_prompt_version_id(&self) -> PromptVersionId {
        self.triage_system_prompt_version_id
    }

    /// Returns the triage user prompt version ID.
    pub fn triage_user_prompt_version_id(&self) -> PromptVersionId {
        self.triage_user_prompt_version_id
    }

    /// Returns the relation system prompt version ID.
    pub fn relation_system_prompt_version_id(&self) -> PromptVersionId {
        self.relation_system_prompt_version_id
    }

    /// Returns the relation user prompt version ID.
    pub fn relation_user_prompt_version_id(&self) -> PromptVersionId {
        self.relation_user_prompt_version_id
    }

    /// Returns the extraction inference provider.
    pub fn extraction_inference_provider(&self) -> &str {
        &self.extraction_inference_provider
    }

    /// Returns the extraction inference model.
    pub fn extraction_inference_model(&self) -> &str {
        &self.extraction_inference_model
    }

    /// Returns the triage inference provider.
    pub fn triage_inference_provider(&self) -> &str {
        &self.triage_inference_provider
    }

    /// Returns the triage inference model.
    pub fn triage_inference_model(&self) -> &str {
        &self.triage_inference_model
    }

    /// Returns the relation inference provider.
    pub fn relation_inference_provider(&self) -> &str {
        &self.relation_inference_provider
    }

    /// Returns the relation inference model.
    pub fn relation_inference_model(&self) -> &str {
        &self.relation_inference_model
    }

    /// Returns the embedding provider.
    pub fn embedding_provider(&self) -> &str {
        &self.embedding_provider
    }

    /// Returns the embedding model.
    pub fn embedding_model(&self) -> &str {
        &self.embedding_model
    }

    /// Returns the inference parameters.
    pub fn inference_parameters(&self) -> &InferenceParameters {
        &self.inference_parameters
    }

    /// Returns when this fingerprint was first recorded.
    pub fn created_at(&self) -> DateTime<Utc> {
        self.created_at
    }
}
