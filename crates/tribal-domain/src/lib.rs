#![warn(clippy::pedantic)]
#![deny(warnings)]
//! Core domain types, ID newtypes, shared error types, and configuration
//! structs for Tribal.

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
pub use task::{TaskStatus, TaskType};
