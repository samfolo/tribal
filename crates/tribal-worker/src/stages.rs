//! Pipeline stage implementations (extraction, triage, relation).

mod common;
mod extraction;
mod relation;
mod triage;

pub(crate) use common::{
    StageCommit, StageOutput, TriageCommitDecision, record_prompt_version_ids,
};
pub(crate) use relation::RelationCommitDecision;
