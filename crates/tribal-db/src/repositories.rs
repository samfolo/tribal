//! Repository traits and Postgres implementations for the database layer.
//!
//! Each entity has a trait defining its data access operations and a
//! zero-sized Postgres implementation struct. All methods take
//! `&mut PgConnection` as an explicit executor parameter, keeping
//! repositories pool-agnostic.

mod auth_token;
mod common;
mod embedding;
mod extraction_result;
mod item_observation;
mod job;
mod knowledge_item;
mod migration;
mod oauth_authorization_code;
mod oauth_client;
mod principal;
mod project;
mod prompt_version;
mod reference;
mod relation;
mod retrieval_feedback;
mod standing;
mod system_fingerprint;
mod tag_embedding;
mod tag_registry;
mod task;
mod token_usage;
mod triage_result;
mod triage_similar_item_decision;

pub use auth_token::{AuthTokenRepository, NewAuthToken, PgAuthTokenRepository};
pub use common::cursor::encode_cursor;
pub use embedding::{EmbeddingRepository, NewEmbedding, PgEmbeddingRepository};
pub use extraction_result::{
    ExtractionResultRepository, NewExtractionResult, PgExtractionResultRepository,
};
pub use item_observation::{
    ItemObservationRepository, NewItemObservation, PgItemObservationRepository,
};
#[cfg(feature = "test-helpers")]
pub use job::JobStateOverride;
pub use job::{JobRepository, JobStatusTransition, NewJob, PgJobRepository};
pub use knowledge_item::{
    KnowledgeItemRepository, NewKnowledgeItem, PgKnowledgeItemRepository, SemanticSearchParams,
    SemanticSearchResponse, SemanticSearchResult,
};
pub use migration::{MigrationHeadStatus, MigrationRepository, PgMigrationRepository};
pub use oauth_authorization_code::{
    NewOauthAuthorizationCode, OauthAuthorizationCodeRepository, PgOauthAuthorizationCodeRepository,
};
pub use oauth_client::{NewOauthClient, OauthClientRepository, PgOauthClientRepository};
pub use principal::{NewPrincipal, PgPrincipalRepository, PrincipalRepository};
pub use project::{NewProject, PgProjectRepository, ProjectRepository};
pub use prompt_version::{NewPromptVersion, PgPromptVersionRepository, PromptVersionRepository};
pub use reference::{NewReference, PgReferenceRepository, ReferenceRepository};
pub use relation::{
    NewKnowledgeItemRelation, PgRelationRepository, RelationRepository, TraversalDirection,
    TraversalNode, TraversalResponse,
};
pub use retrieval_feedback::{
    NewRetrievalFeedback, PgRetrievalFeedbackRepository, RetrievalFeedbackRepository,
};
pub use standing::{PgStandingRepository, StandingRepository};
pub use system_fingerprint::{
    NewSystemFingerprint, PgSystemFingerprintRepository, SystemFingerprintRepository,
};
pub use tag_embedding::{NewTagEmbedding, PgTagEmbeddingRepository, TagEmbeddingRepository};
pub use tag_registry::{PgTagRegistryRepository, TagRegistryRepository};
pub use task::{NewTask, PgTaskRepository, ReclaimOutcome, TaskRepository, TaskStatusCount};
pub use token_usage::{NewTokenUsage, PgTokenUsageRepository, TokenUsageRepository};
pub use triage_result::{NewTriageResult, PgTriageResultRepository, TriageResultRepository};
pub use triage_similar_item_decision::{
    NewTriageSimilarItemDecision, PgTriageSimilarItemDecisionRepository,
    TriageSimilarItemDecisionRepository,
};
