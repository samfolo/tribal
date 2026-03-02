//! Prompt assembly for pipeline stages.

mod extraction;
mod relation;
mod triage;
pub(crate) mod variables;

pub(crate) use extraction::assemble_extraction_prompt;
pub(crate) use relation::{
    CandidateOutcome, RelationPromptContext, SimilarItemDecisionContext, assemble_relation_prompt,
};
pub(crate) use triage::{SimilarItemContext, assemble_triage_prompt};
