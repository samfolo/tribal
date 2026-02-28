//! Prompt assembly for pipeline stages.

mod extraction;
mod triage;

pub(crate) use extraction::assemble_extraction_prompt;
#[allow(unused_imports)]
pub(crate) use triage::{SimilarItemContext, assemble_triage_prompt};
