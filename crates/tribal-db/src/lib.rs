//! Database layer for Tribal: repository traits and implementations,
//! sqlx queries, migrations, and connection pool management.

pub mod advisory_locks;
mod error;
mod pool;
mod repositories;
mod tables;

pub use tables::APPLICATION_TABLES;

/// Compiled migrations for the Tribal database schema.
///
/// Embeds all SQL migration files from `crates/tribal-db/migrations/` at
/// compile time. Used by the test harness to build the migrated template
/// database that every test clones from.
pub static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!();

pub use error::DbError;
pub use pool::create_pool;
#[cfg(feature = "test-helpers")]
pub use repositories::JobStateOverride;
pub use repositories::{
    AdvisoryLockRepository, AgentBindingVersionRepository, AgentDriverTaskRepository,
    AgentThreadRecordRepository, AgentThreadRepository, AuthTokenInventoryRow, AuthTokenPageKey,
    AuthTokenRepository, DatabaseCreation, DatabaseProvisionRepository, DrivingTaskRef,
    EmbeddingIndexRepository, EmbeddingProfileRepository, EmbeddingRepository, EmbeddingTable,
    EnsurePrincipalOutcome, EnsureSystemOutcome, ExtractionResultRepository,
    GraphIdentityRepository, IndexState, IngestInsertOutcome, IngestJobRepository,
    ItemObservationRepository, JobRepository, JobStatusTransition, JobTriageSubmission,
    KnowledgeItemRepository, LocalDefaultCredential, LocalDefaultCredentialRepository,
    MigrationHeadStatus, MigrationRepository, NewAgentBindingVersion, NewAgentDriverTask,
    NewAgentThread, NewAgentThreadRecord, NewAuthToken, NewEmbedding, NewEmbeddingProfile,
    NewExtractionResult, NewGitProject, NewItemObservation, NewJob, NewKnowledgeItem,
    NewKnowledgeItemRelation, NewOauthAuthorizationCode, NewOauthClient, NewPrincipal,
    NewPromptVersion, NewReference, NewReindexQuarantine, NewReindexRun, NewReindexTask,
    NewRetrievalFeedback, NewSystemFingerprint, NewTagEmbedding, NewTask, NewTokenUsage,
    NewTriageResult, NewTriageSimilarItemDecision, OauthAuthorizationCodeRepository,
    OauthClientRepository, PgAdvisoryLockRepository, PgAgentBindingVersionRepository,
    PgAgentDriverTaskRepository, PgAgentThreadRecordRepository, PgAgentThreadRepository,
    PgAuthTokenRepository, PgDatabaseProvisionRepository, PgEmbeddingIndexRepository,
    PgEmbeddingProfileRepository, PgEmbeddingRepository, PgExtractionResultRepository,
    PgGraphIdentityRepository, PgItemObservationRepository, PgJobRepository,
    PgKnowledgeItemRepository, PgLocalDefaultCredentialRepository, PgMigrationRepository,
    PgOauthAuthorizationCodeRepository, PgOauthClientRepository, PgPrincipalRepository,
    PgProjectRepository, PgPromptVersionRepository, PgReferenceRepository,
    PgReindexQuarantineRepository, PgReindexRunRepository, PgReindexTaskRepository,
    PgRelationRepository, PgRetrievalFeedbackRepository, PgStandingRepository,
    PgSystemFingerprintRepository, PgTagEmbeddingRepository, PgTagRegistryRepository,
    PgTaskRepository, PgTokenUsageRepository, PgTriageResultRepository,
    PgTriageSimilarItemDecisionRepository, PrincipalRepository, ProjectPageKey, ProjectRepository,
    PromptVersionRepository, RecentIngestionCursor, RecentIngestionPage, RecentIngestionSummary,
    RecentIngestionsQuery, ReclaimOutcome, ReferenceRepository, ReindexQuarantineRepository,
    ReindexRunRepository, ReindexTaskRepository, ReindexTaskStateCount, RelationRepository,
    RetrievalFeedbackRepository, SemanticSearchParams, SemanticSearchResponse,
    SemanticSearchResult, StandingRepository, SystemFingerprintRepository, TagEmbeddingRepository,
    TagRegistryRepository, TaskRepository, TaskStatusCount, ThreadPruneCriteria,
    ThreadPruneOutcome, TokenUsageRepository, TraversalDirection, TraversalNode, TraversalResponse,
    TriageResultRepository, TriageSimilarItemDecisionRepository, encode_cursor,
};
