//! Pipeline stage implementations (extraction, triage, relation).

mod common;
mod extraction;
mod relation;
mod triage;

pub(crate) use extraction::{StageCommit, StageOutput};
