//! Prompt stage classification.
//!
//! Identifies which pipeline stage a prompt version applies to. Distinct
//! from [`PipelineStage`](crate::PipelineStage) which includes `Embedding`
//! — prompt versions exist only for stages that use LLM calls.

use serde::{Deserialize, Serialize};

/// The pipeline stage a prompt version applies to.
///
/// A strict subset of [`PipelineStage`](crate::PipelineStage) — excludes
/// `Embedding` because embeddings use a model API, not a prompt template.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PromptStage {
    /// The extraction stage — extracting candidates from raw input.
    Extraction,
    /// The triage stage — classifying a single candidate.
    Triage,
    /// The relation stage — creating relations across triaged items.
    Relation,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::enum_serde_tests;

    enum_serde_tests!(test_prompt_stage_serde_roundtrip, PromptStage {
        PromptStage::Extraction => "extraction",
        PromptStage::Triage => "triage",
        PromptStage::Relation => "relation",
    });
}
