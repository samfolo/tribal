//! Response parsing for pipeline stages.

mod extraction;

pub(crate) use extraction::{ExtractionOutput, parse_extraction_response};
