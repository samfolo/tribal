//! Response parsing for pipeline stages.

mod extraction;
mod relation;
mod triage;

pub(crate) use extraction::{ExtractionOutput, parse_extraction_response};
// IngestionRelationKind is a field type on RelationEdge — re-exported for test access.
#[allow(unused_imports)]
pub(crate) use relation::IngestionRelationKind;
pub(crate) use relation::{RelationEdge, RelationOutput, RelationTarget, parse_relation_response};
pub(crate) use triage::{
    SimilarItemClassification, TriageClassification, TriageDecision, parse_triage_response,
};
