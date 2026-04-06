#![warn(clippy::pedantic)]
#![deny(warnings)]
//! Database layer for Tribal: repository traits and implementations,
//! sqlx queries, migrations, and connection pool management.

mod error;
mod pool;
mod repositories;
mod tables;

pub use tables::APPLICATION_TABLES;

/// Compiled migrations for the Tribal database schema.
///
/// Embeds all SQL migration files from `crates/tribal-db/migrations/` at
/// compile time. Used by [`tribal_test_utils::TestContext`] to run
/// migrations against test databases.
pub static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!();

pub use error::DbError;
pub use pool::create_pool;
#[cfg(feature = "test-helpers")]
pub use repositories::JobStateOverride;
pub use repositories::{
    AuthTokenRepository, EmbeddingRepository, ExtractionResultRepository,
    ItemObservationRepository, JobRepository, JobStatusTransition, KnowledgeItemRepository,
    MigrationRepository, NewAuthToken, NewEmbedding, NewExtractionResult, NewItemObservation,
    NewJob, NewKnowledgeItem, NewKnowledgeItemRelation, NewPrincipal, NewProject, NewPromptVersion,
    NewReference, NewRetrievalFeedback, NewSystemFingerprint, NewTagEmbedding, NewTask,
    NewTokenUsage, NewTriageResult,
    NewTriageSimilarItemDecision, PgAuthTokenRepository, PgEmbeddingRepository,
    PgExtractionResultRepository, PgItemObservationRepository, PgJobRepository,
    PgKnowledgeItemRepository, PgMigrationRepository, PgPrincipalRepository, PgProjectRepository,
    PgPromptVersionRepository, PgReferenceRepository, PgRelationRepository,
    PgRetrievalFeedbackRepository, PgStandingRepository, PgSystemFingerprintRepository,
    PgTagEmbeddingRepository,
    PgTagRegistryRepository, PgTaskRepository, PgTokenUsageRepository, PgTriageResultRepository,
    PgTriageSimilarItemDecisionRepository, PrincipalRepository, ProjectRepository,
    PromptVersionRepository, ReclaimOutcome, ReferenceRepository, RelationRepository,
    RetrievalFeedbackRepository, SemanticSearchParams, SemanticSearchResponse,
    SemanticSearchResult, StandingRepository, SystemFingerprintRepository,
    TagEmbeddingRepository, TagRegistryRepository,
    TaskRepository, TaskStatusCount, TokenUsageRepository, TraversalDirection, TraversalNode,
    TraversalResponse, TriageResultRepository, TriageSimilarItemDecisionRepository,
};
