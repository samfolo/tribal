//! Extraction result entity.
//!
//! Stores the structured output of the extraction stage as JSONB for
//! consumption by triage and relation stages. One row per job.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use typed_builder::TypedBuilder;

use crate::{ExtractionResultId, JobId};

/// The persisted output of the extraction stage.
///
/// Contains the full candidate list and any intra-batch relation hints
/// produced by the extraction agent. Triage workers index into
/// `candidates` by `batch_index`; the relation worker reads the
/// complete `relation_hints` array.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TypedBuilder)]
pub struct ExtractionResult {
    /// Unique identifier with `exr_` prefix.
    id: ExtractionResultId,
    /// The job this extraction result belongs to.
    job_id: JobId,
    /// JSONB array of candidate objects.
    candidates: serde_json::Value,
    /// JSONB array of relation hint objects.
    relation_hints: serde_json::Value,
    /// When this result was created.
    created_at: DateTime<Utc>,
}

impl ExtractionResult {
    /// Returns the extraction result identifier.
    pub fn id(&self) -> ExtractionResultId {
        self.id
    }

    /// Returns the job identifier.
    pub fn job_id(&self) -> JobId {
        self.job_id
    }

    /// Returns the candidates JSONB array.
    pub fn candidates(&self) -> &serde_json::Value {
        &self.candidates
    }

    /// Returns the relation hints JSONB array.
    pub fn relation_hints(&self) -> &serde_json::Value {
        &self.relation_hints
    }

    /// Returns when this result was created.
    pub fn created_at(&self) -> DateTime<Utc> {
        self.created_at
    }
}
