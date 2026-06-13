//! Response parsing for pipeline stages.

mod deserialise;
mod extraction;
mod relation;
mod triage;
mod triage_submission;

#[cfg(test)]
mod schema_dialect_snapshots;

pub(crate) use extraction::{ExtractionOutput, parse_extraction_response};
pub(crate) use relation::{
    IngestionRelationKind, RelationEdge, RelationOutput, RelationTarget, parse_relation_response,
};
pub(crate) use triage::{
    SimilarItemClassification, TriageClassification, TriageDecision, TriageItemReference,
    parse_triage_response,
};
pub(crate) use triage_submission::{
    HANDOFF_MAX_CHARS, TriageSubmission, TriageSubmissionDecision, triage_submission_schema,
};
