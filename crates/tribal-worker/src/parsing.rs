//! Response parsing for pipeline stages.

mod extraction;
mod triage;

pub(crate) use extraction::{ExtractionOutput, parse_extraction_response};
pub(crate) use triage::parse_triage_response;
