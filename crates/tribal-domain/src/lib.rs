#![warn(clippy::pedantic)]
#![allow(clippy::must_use_candidate)]
#![deny(warnings)]
//! Core domain types, ID newtypes, shared error types, and configuration
//! structs for Tribal.

/// Generates `as_str()`, [`Display`], and [`FromStr`] implementations for a
/// simple enum whose variants map one-to-one to database TEXT values.
///
/// Includes a compile-time exhaustiveness guard: if a variant is added to
/// the enum but not listed in the macro invocation, the embedded `match`
/// becomes non-exhaustive and the build fails.
macro_rules! enum_text_conversions {
    ($type:ty { $($variant:path => $str:literal),+ $(,)? }) => {
        impl $type {
            /// Returns the database string representation.
            #[must_use]
            pub fn as_str(&self) -> &'static str {
                match self {
                    $( $variant => $str, )+
                }
            }
        }

        impl std::fmt::Display for $type {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str(self.as_str())
            }
        }

        impl std::str::FromStr for $type {
            type Err = String;

            fn from_str(s: &str) -> Result<Self, Self::Err> {
                match s {
                    $( $str => Ok($variant), )+
                    other => Err(format!(
                        "unknown {}: {other}",
                        stringify!($type),
                    )),
                }
            }
        }
    };
}

mod api_key;
mod auth_token;
mod candidate;
mod discovery;
mod embedding;
mod embedding_purpose;
mod error_code;
mod extraction_result;
mod feedback_rating;
mod git;
mod ids;
mod inference_parameters;
mod item_observation;
mod job;
mod knowledge;
mod pipeline_stage;
mod principal;
mod project;
mod prompt_role;
mod prompt_stage;
mod prompt_version;
mod reference;
mod reference_kind;
mod relation;
mod retrieval_feedback;
mod scope;
mod source_type;
pub mod span_attrs;
mod standing;
mod system_fingerprint;
mod tag_registry;
mod tag_similarity_result;
mod task;
mod token_usage;
mod triage;

pub use api_key::ApiKey;
pub use auth_token::{AuthToken, AuthTokenBuilder};
pub use candidate::{Candidate, RelationHint, SuggestedReference};
pub use discovery::{Direction, DiscoveryField, ExplorationField};
pub use embedding::{Embedding, EmbeddingBuilder};
pub use embedding_purpose::EmbeddingPurpose;
pub use error_code::McpErrorCode;
pub use extraction_result::{ExtractionResult, ExtractionResultBuilder};
pub use feedback_rating::FeedbackRating;
pub use git::{GitRemote, GitRemoteParseError};
pub use ids::{
    AuthTokenId, EmbeddingId, EpisodeId, ExtractionResultId, IdParseError, ItemObservationId,
    JobId, KnowledgeItemId, PrincipalId, ProjectId, PromptVersionId, ReferenceId, RelationBatchId,
    RelationId, RetrievalFeedbackId, SessionId, SystemFingerprintId, TaskId, TokenUsageId,
    TriageResultId, TriageSimilarItemDecisionId,
};
pub use inference_parameters::{
    EmbeddingParameters, InferenceParameters, PipelineParameters, StageParameters,
};
pub use item_observation::{ItemObservation, ItemObservationBuilder};
pub use job::{Job, JobBuilder, JobOutcome, JobState, JobStatus};
pub use knowledge::{Confidence, KnowledgeItem, KnowledgeItemBuilder, KnowledgeKind};
pub use pipeline_stage::PipelineStage;
pub use principal::{LOCAL_PRINCIPAL_KEY, Principal, PrincipalBuilder};
pub use project::{Project, ProjectBuilder};
pub use prompt_role::PromptRole;
pub use prompt_stage::PromptStage;
pub use prompt_version::{PromptVersion, PromptVersionBuilder};
pub use reference::{Reference, ReferenceBuilder};
pub use reference_kind::ReferenceKind;
pub use relation::{
    KnowledgeItemRelation, KnowledgeItemRelationBuilder, RelationHintType, RelationKind,
    RelationSuggestion,
};
pub use retrieval_feedback::{RetrievalFeedback, RetrievalFeedbackBuilder};
pub use scope::{Scope, ScopeParseError, full_access_scopes, is_authorised};
pub use source_type::SourceType;
pub use standing::{Standing, StandingBuilder};
pub use system_fingerprint::{SystemFingerprint, SystemFingerprintBuilder};
pub use tag_registry::{TagRegistryEntry, TagRegistryEntryBuilder};
pub use tag_similarity_result::TagSimilarityResult;
pub use task::{Task, TaskBuilder, TaskErrorKind, TaskStatus, TaskType};
pub use token_usage::{TokenUsage, TokenUsageBuilder, TokenUsageStage};
pub use triage::{
    SimilarItem, SimilarItemBuilder, TriageOutcome, TriageResult, TriageResultBuilder,
    TriageSimilarItemDecision, TriageSimilarItemDecisionBuilder,
};

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

/// Generates an `as_str()` / `FromStr` roundtrip test for an enum with a
/// compile-time exhaustiveness check, plus an invalid-input assertion.
#[cfg(test)]
macro_rules! enum_text_tests {
    ($test_name:ident, $type:ty { $($variant:path => $str:literal),+ $(,)? }) => {
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
                $( ($variant, $str), )+
            ];
            for &(variant, expected_str) in variants {
                assert_eq!(variant.as_str(), expected_str, "as_str for {variant:?}");
                assert_eq!(variant.to_string(), expected_str, "Display for {variant:?}");
                let parsed: $type = expected_str.parse().expect("should parse");
                assert_eq!(parsed, variant, "FromStr for {expected_str:?}");
            }

            assert!(
                "bogus_not_a_real_variant".parse::<$type>().is_err(),
                "invalid input should fail"
            );
        }
    };
}

#[cfg(test)]
pub(crate) use enum_text_tests;
