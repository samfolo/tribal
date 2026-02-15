//! Pipeline stage classification.
//!
//! Identifies which stage of the ingest pipeline produced a given record.
//! Distinct from [`TaskType`](crate::TaskType) — `TaskType` classifies
//! the unit of work; `PipelineStage` classifies the origin of a prompt
//! version or token usage record, and includes `Embedding` which is not
//! a standalone task type.

use serde::{Deserialize, Serialize};

/// The pipeline stage that produced a record.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PipelineStage {
    /// The extraction stage — extracting candidates from raw input.
    Extraction,
    /// The triage stage — classifying a single candidate.
    Triage,
    /// The relation stage — creating relations across triaged items.
    Relation,
    /// The embedding stage — generating vector embeddings.
    Embedding,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::enum_serde_tests;

    enum_serde_tests!(test_pipeline_stage_serde_roundtrip, PipelineStage {
        PipelineStage::Extraction => "extraction",
        PipelineStage::Triage => "triage",
        PipelineStage::Relation => "relation",
        PipelineStage::Embedding => "embedding",
    });
}
