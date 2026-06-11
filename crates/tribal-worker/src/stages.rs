//! Pipeline stage implementations (extraction, triage, relation).

mod common;
mod extraction;
mod relation;
mod triage;

pub(crate) use common::{
    StageCommit, StageRun, TriageCommitDecision, finish_thread, load_active_embedding_profile,
    map_gateway_error, prompt_version_ids_for_task, record_prompt_version_ids, stage_attribution,
};
pub(crate) use relation::RelationCommitDecision;
