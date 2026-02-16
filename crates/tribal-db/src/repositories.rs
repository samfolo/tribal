//! Repository traits and Postgres implementations for the database layer.
//!
//! Each entity has a trait defining its data access operations and a
//! zero-sized Postgres implementation struct. All methods take
//! `&mut PgConnection` as an explicit executor parameter, keeping
//! repositories pool-agnostic.

mod common;
mod knowledge_item;
mod principal;
mod project;

pub use knowledge_item::{
    KnowledgeItemRepository, NewKnowledgeItem, PgKnowledgeItemRepository, SemanticSearchParams,
    SemanticSearchResponse, SemanticSearchResult,
};
pub use principal::{NewPrincipal, PgPrincipalRepository, PrincipalRepository};
pub use project::{NewProject, PgProjectRepository, ProjectRepository};
