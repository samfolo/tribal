//! Response parsing for pipeline stages.

mod deserialise;
mod extraction;
mod relation;
mod triage;

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
