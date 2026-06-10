//! Pipeline stage implementations (extraction, triage, relation).

mod common;
mod extraction;
mod relation;
mod triage;

pub(crate) use common::{
    StageCommit, TriageCommitDecision, load_active_embedding_profile, map_facade_error,
    record_prompt_version_ids, stage_attribution,
};
pub(crate) use relation::RelationCommitDecision;
