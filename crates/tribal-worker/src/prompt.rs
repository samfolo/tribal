//! Prompt assembly for pipeline stages.

mod extraction;
mod legends;
mod relation;
mod renderer;
mod snapshots;
mod triage;
mod validation;
pub(crate) mod variables;

pub(crate) use extraction::{assemble_extraction_prompt, extraction_user_context};
pub(crate) use legends::SimilarityBand;
pub(crate) use relation::{
    CandidateOutcome, RelationPromptContext, SimilarItemDecisionContext, assemble_relation_prompt,
    relation_user_context,
};
pub(crate) use triage::{SimilarItemContext, assemble_triage_prompt, triage_user_context};
pub use validation::synthetic_validation_context;
pub use variables::reserved_keys;
