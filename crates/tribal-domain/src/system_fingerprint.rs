//! System fingerprint entity — content-addressed configuration snapshot.
//!
//! Each fingerprint captures the full set of inference-affecting
//! configuration values at a point in time. Identical configurations
//! produce the same `content_hash`, enabling deduplication. Per-stage
//! configuration (prompts, provider, model, sampling parameters) is
//! subsumed by that stage's content-addressed binding version; the
//! fingerprint composes the three binding hashes with what no stage
//! binding carries — the build version, the job-level pipeline
//! parameters, and the embedding identity.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use typed_builder::TypedBuilder;

use crate::{PipelineParameters, SystemFingerprintId};

/// A content-addressed system configuration snapshot.
///
/// The `content_hash` (SHA-256, hex-encoded, 64 chars) uniquely
/// identifies the combination of stage binding versions, build version,
/// pipeline parameters, and embedding identity active when a job or
/// feedback record was created.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TypedBuilder)]
pub struct SystemFingerprint {
    /// Unique identifier with `sfp_` prefix.
    id: SystemFingerprintId,
    /// SHA-256 hash of all fingerprint inputs.
    content_hash: String,
    /// Git-describe version of the build.
    build_version: String,

    // -- Stage binding versions ------------------------------------------------
    /// The extraction stage's binding-version content address.
    extraction_binding_hash: String,
    /// The triage stage's binding-version content address.
    triage_binding_hash: String,
    /// The relation stage's binding-version content address.
    relation_binding_hash: String,

    // -- Embedding identity ------------------------------------------------
    /// Embedding provider name.
    embedding_provider: String,
    /// Embedding model name.
    embedding_model: String,
    /// Embedding vector dimensionality.
    embedding_dimensions: u32,

    // -- Pipeline parameters ----------------------------------------------
    /// Job-level parameters subsumed by no stage binding, as typed JSONB.
    pipeline_parameters: PipelineParameters,

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

    /// Returns the extraction binding-version content address.
    pub fn extraction_binding_hash(&self) -> &str {
        &self.extraction_binding_hash
    }

    /// Returns the triage binding-version content address.
    pub fn triage_binding_hash(&self) -> &str {
        &self.triage_binding_hash
    }

    /// Returns the relation binding-version content address.
    pub fn relation_binding_hash(&self) -> &str {
        &self.relation_binding_hash
    }

    /// Returns the embedding provider.
    pub fn embedding_provider(&self) -> &str {
        &self.embedding_provider
    }

    /// Returns the embedding model.
    pub fn embedding_model(&self) -> &str {
        &self.embedding_model
    }

    /// Returns the embedding vector dimensionality.
    pub fn embedding_dimensions(&self) -> u32 {
        self.embedding_dimensions
    }

    /// Returns the pipeline parameters.
    pub fn pipeline_parameters(&self) -> &PipelineParameters {
        &self.pipeline_parameters
    }

    /// Returns when this fingerprint was first recorded.
    pub fn created_at(&self) -> DateTime<Utc> {
        self.created_at
    }
}
