//! Response parsing for pipeline stages.

mod extraction;
mod relation;
mod triage;

pub(crate) use extraction::{ExtractionOutput, parse_extraction_response};
pub(crate) use relation::parse_relation_response;
pub(crate) use triage::{
    SimilarItemClassification, TriageClassification, TriageDecision, parse_triage_response,
};
