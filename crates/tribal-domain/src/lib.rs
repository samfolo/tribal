#![warn(clippy::pedantic)]
#![deny(warnings)]
//! Core domain types, ID newtypes, shared error types, and configuration
//! structs for Tribal.

mod database_config;
mod discovery;
mod error_code;
mod ids;
mod job;
mod knowledge;
mod reference_kind;
mod relation;
mod task;

pub use discovery::{Direction, DiscoveryField, ExplorationField};
pub use error_code::McpErrorCode;
pub use ids::{
    EpisodeId, IdParseError, JobId, KnowledgeItemId, ProjectId, ReferenceId, RetrievalFeedbackId,
    SessionId, TaskId,
};
pub use job::{JobOutcome, JobStatus};
pub use knowledge::{Confidence, KnowledgeKind};
pub use reference_kind::ReferenceKind;
pub use relation::{RelationHintType, RelationKind, RelationSuggestion};
pub use task::{TaskErrorKind, TaskStatus, TaskType};

/// Generates a serde roundtrip test for an enum with a compile-time
/// exhaustiveness check.
///
/// If a variant is added to the enum but not listed in the macro invocation,
/// the embedded `match` becomes non-exhaustive and the build fails.
#[cfg(test)]
macro_rules! enum_serde_tests {
    ($test_name:ident, $type:ty { $($variant:path => $json:literal),+ $(,)? }) => {
        #[test]
        fn $test_name() {
            // Compile-time exhaustiveness guard: every variant must be listed.
            #[allow(dead_code)]
            fn check_exhaustiveness(v: $type) {
                match v {
                    $( $variant => {} )+
                }
            }

            let variants: &[($type, &str)] = &[
                $( ($variant, $json), )+
            ];
            for &(variant, expected_json) in variants {
                let json = serde_json::to_string(&variant).expect("should serialise");
                assert_eq!(json, format!("\"{expected_json}\""), "serialised form of {variant:?}");
                let parsed: $type = serde_json::from_str(&json).expect("should deserialise");
                assert_eq!(parsed, variant);
            }
        }
    };
}

#[cfg(test)]
pub(crate) use enum_serde_tests;
