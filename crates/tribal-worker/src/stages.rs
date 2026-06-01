//! Pipeline stage implementations (extraction, triage, relation).

mod common;
mod extraction;
mod relation;
mod triage;

pub(crate) use common::{
    StageCommit, StageOutput, TriageCommitDecision, load_active_embedding_profile,
    record_prompt_version_ids,
};
pub(crate) use relation::RelationCommitDecision;
