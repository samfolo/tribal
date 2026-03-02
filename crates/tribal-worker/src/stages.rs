//! Pipeline stage implementations (extraction, triage, relation).

mod common;
mod extraction;
mod relation;
mod triage;

pub(crate) use common::{StageCommit, StageOutput, TriageCommitDecision};
pub(crate) use relation::{RelationEdge, RelationOutput, RelationTarget};
