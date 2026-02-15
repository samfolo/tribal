#![warn(clippy::pedantic)]
#![deny(warnings)]
//! Core domain types, ID newtypes, shared error types, and configuration
//! structs for Tribal.

mod auth_token;
mod database_config;
mod discovery;
mod embedding;
mod embedding_purpose;
mod error_code;
mod feedback_rating;
mod ids;
mod item_observation;
mod job;
mod knowledge;
mod pipeline_stage;
mod principal;
mod project;
mod prompt_version;
mod reference;
mod reference_kind;
mod relation;
mod retrieval_feedback;
mod source_type;
mod standing;
mod tag_registry;
mod task;
mod token_usage;
mod triage;

pub use auth_token::AuthToken;
pub use database_config::DatabaseConfig;
pub use discovery::{Direction, DiscoveryField, ExplorationField};
pub use embedding::Embedding;
pub use embedding_purpose::EmbeddingPurpose;
pub use error_code::McpErrorCode;
pub use feedback_rating::FeedbackRating;
pub use ids::{
    AuthTokenId, EmbeddingId, EpisodeId, IdParseError, ItemObservationId, JobId, KnowledgeItemId,
    PrincipalId, ProjectId, PromptVersionId, ReferenceId, RelationBatchId, RelationId,
    RetrievalFeedbackId, SessionId, TaskId, TokenUsageId, TriageResultId,
    TriageSimilarItemDecisionId,
};
pub use item_observation::ItemObservation;
pub use job::{Job, JobOutcome, JobStatus};
pub use knowledge::{Confidence, KnowledgeItem, KnowledgeKind};
pub use pipeline_stage::PipelineStage;
pub use principal::Principal;
pub use project::Project;
pub use prompt_version::PromptVersion;
pub use reference::Reference;
pub use reference_kind::ReferenceKind;
pub use relation::{KnowledgeItemRelation, RelationHintType, RelationKind, RelationSuggestion};
pub use retrieval_feedback::RetrievalFeedback;
pub use source_type::SourceType;
pub use standing::Standing;
pub use tag_registry::TagRegistryEntry;
pub use task::{Task, TaskErrorKind, TaskStatus, TaskType};
pub use token_usage::TokenUsage;
pub use triage::{SimilarItem, TriageOutcome, TriageResult, TriageSimilarItemDecision};

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
