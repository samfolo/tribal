//! Repository traits and Postgres implementations for the database layer.
//!
//! Each entity has a trait defining its data access operations and a
//! zero-sized Postgres implementation struct. All methods take
//! `&mut PgConnection` as an explicit executor parameter, keeping
//! repositories pool-agnostic.

mod advisory_lock;
mod agent_binding_version;
mod agent_driver_task;
mod agent_thread;
mod agent_thread_record;
mod auth_token;
mod common;
mod embedding;
mod embedding_index;
mod embedding_profile;
mod extraction_result;
mod item_observation;
mod job;
mod knowledge_item;
mod local_default_credential;
mod migration;
mod oauth_authorization_code;
mod oauth_client;
mod principal;
mod project;
mod prompt_version;
mod reference;
mod reindex_quarantine;
mod reindex_run;
mod reindex_task;
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

pub use advisory_lock::{AdvisoryLockRepository, PgAdvisoryLockRepository};
pub use agent_binding_version::{
    AgentBindingVersionRepository, NewAgentBindingVersion, PgAgentBindingVersionRepository,
};
pub use agent_driver_task::{
    AgentDriverTaskRepository, NewAgentDriverTask, PgAgentDriverTaskRepository,
};
pub use agent_thread::{
    AgentThreadRepository, DrivingTaskRef, NewAgentThread, PgAgentThreadRepository,
    ThreadPruneCriteria, ThreadPruneOutcome,
};
pub use agent_thread_record::{
    AgentThreadRecordRepository, JobTriageSubmission, NewAgentThreadRecord,
    PgAgentThreadRecordRepository,
};
pub use auth_token::{
    AuthTokenInventoryRow, AuthTokenPageKey, AuthTokenRepository, NewAuthToken,
    PgAuthTokenRepository,
};
pub use common::cursor::encode_cursor;
pub use embedding::{EmbeddingRepository, NewEmbedding, PgEmbeddingRepository};
pub use embedding_index::{
    EmbeddingIndexRepository, EmbeddingTable, IndexState, PgEmbeddingIndexRepository,
};
pub use embedding_profile::{
    EmbeddingProfileRepository, NewEmbeddingProfile, PgEmbeddingProfileRepository,
};
pub use extraction_result::{
    ExtractionResultRepository, NewExtractionResult, PgExtractionResultRepository,
};
pub use item_observation::{
    ItemObservationRepository, NewItemObservation, PgItemObservationRepository,
};
#[cfg(feature = "test-helpers")]
pub use job::JobStateOverride;
pub use job::{
    IngestInsertOutcome, IngestJobRepository, JobRepository, JobStatusTransition, NewJob,
    PgJobRepository, RecentIngestionCursor, RecentIngestionPage, RecentIngestionSummary,
    RecentIngestionsQuery,
};
pub use knowledge_item::{
    KnowledgeItemRepository, NewKnowledgeItem, PgKnowledgeItemRepository, SemanticSearchParams,
    SemanticSearchResponse, SemanticSearchResult,
};
pub use local_default_credential::{
    LocalDefaultCredential, LocalDefaultCredentialRepository, PgLocalDefaultCredentialRepository,
};
pub use migration::{MigrationHeadStatus, MigrationRepository, PgMigrationRepository};
pub use oauth_authorization_code::{
    NewOauthAuthorizationCode, OauthAuthorizationCodeRepository, PgOauthAuthorizationCodeRepository,
};
pub use oauth_client::{NewOauthClient, OauthClientRepository, PgOauthClientRepository};
pub use principal::{NewPrincipal, PgPrincipalRepository, PrincipalRepository};
pub use project::{NewProject, PgProjectRepository, ProjectPageKey, ProjectRepository};
pub use prompt_version::{NewPromptVersion, PgPromptVersionRepository, PromptVersionRepository};
pub use reference::{NewReference, PgReferenceRepository, ReferenceRepository};
pub use reindex_quarantine::{
    NewReindexQuarantine, PgReindexQuarantineRepository, ReindexQuarantineRepository,
};
pub use reindex_run::{NewReindexRun, PgReindexRunRepository, ReindexRunRepository};
pub use reindex_task::{
    NewReindexTask, PgReindexTaskRepository, ReindexTaskRepository, ReindexTaskStateCount,
};
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
