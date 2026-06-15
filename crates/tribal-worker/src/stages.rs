//! Pipeline stage implementations (extraction, triage, relation).

mod common;
mod extraction;
mod relation;
mod triage;
mod triage_loop;
mod triage_submission;

pub(crate) use common::{
    StageCommit, StageRun, StageTerminal, TriageCommitDecision, finish_thread,
    load_active_embedding_profile, map_gateway_error, prompt_version_ids_for_task,
    record_prompt_version_ids, stage_attribution,
};
pub(crate) use relation::RelationCommitDecision;
pub(crate) use triage_submission::{
    TriageSubmissionPipeline, VerifierContext, reconstruct_candidate_scores,
};
