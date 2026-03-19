pub mod extraction;
pub mod relation;
pub mod triage;

pub use extraction::{
    CandidateSpec, ExtractionFixture, ExtractionFixtureBuilder, ReferenceSpec, RelationHintSpec,
    candidate, hint,
};
pub use relation::{EdgeSpec, RelationFixture, RelationFixtureBuilder, intra_batch, to_existing};
pub use triage::{SimilarItemSpec, TriageFixtureBuilder, duplicate, novel};
