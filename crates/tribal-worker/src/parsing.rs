//! Response parsing for pipeline stages.

mod extraction;
mod relation;
mod triage;

pub(crate) use extraction::{ExtractionOutput, parse_extraction_response};
#[cfg(test)]
pub(crate) use relation::IngestionRelationKind;
pub(crate) use relation::{RelationEdge, RelationOutput, RelationTarget, parse_relation_response};
pub(crate) use triage::{
    SimilarItemClassification, TriageClassification, TriageDecision, TriageItemReference,
    parse_triage_response,
};
