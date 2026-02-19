//! Repository traits and Postgres implementations for the database layer.
//!
//! Each entity has a trait defining its data access operations and a
//! zero-sized Postgres implementation struct. All methods take
//! `&mut PgConnection` as an explicit executor parameter, keeping
//! repositories pool-agnostic.

mod common;
mod embedding;
mod item_observation;
mod job;
mod knowledge_item;
mod principal;
mod project;
mod reference;
mod relation;
mod standing;
mod tag_registry;
mod task;
mod triage_result;
mod triage_similar_item_decision;

pub use embedding::{EmbeddingRepository, NewEmbedding, PgEmbeddingRepository};
pub use item_observation::{
    ItemObservationRepository, NewItemObservation, PgItemObservationRepository,
};
pub use job::{JobRepository, JobStatusTransition, NewJob, PgJobRepository};
pub use knowledge_item::{
    KnowledgeItemRepository, NewKnowledgeItem, PgKnowledgeItemRepository, SemanticSearchParams,
    SemanticSearchResponse, SemanticSearchResult,
};
pub use principal::{NewPrincipal, PgPrincipalRepository, PrincipalRepository};
pub use project::{NewProject, PgProjectRepository, ProjectRepository};
pub use reference::{NewReference, PgReferenceRepository, ReferenceRepository};
pub use relation::{
    NewKnowledgeItemRelation, PgRelationRepository, RelationRepository, TraversalNode,
    TraversalResponse,
};
pub use standing::{PgStandingRepository, StandingRepository};
pub use tag_registry::{PgTagRegistryRepository, TagRegistryRepository};
pub use task::{NewTask, PgTaskRepository, TaskRepository};
pub use triage_result::{NewTriageResult, PgTriageResultRepository, TriageResultRepository};
pub use triage_similar_item_decision::{
    NewTriageSimilarItemDecision, PgTriageSimilarItemDecisionRepository,
    TriageSimilarItemDecisionRepository,
};
