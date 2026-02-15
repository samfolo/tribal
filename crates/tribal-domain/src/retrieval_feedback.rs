//! Retrieval feedback entity — user ratings of retrieval quality.
//!
//! A single rating covers the entire retrieval session (discovery,
//! exploration, etc.), not individual tool calls. Records are
//! self-sufficient after telemetry rotation — they include enough data
//! to remain a usable evaluation dataset.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use typed_builder::TypedBuilder;

use crate::{FeedbackRating, KnowledgeItemId, PrincipalId, RetrievalFeedbackId};

/// User feedback on a retrieval session.
///
/// Contains the original query, model info, returned items, and a
/// positive/negative rating. Designed to remain useful as an evaluation
/// dataset after OpenTelemetry traces are rotated.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, TypedBuilder)]
pub struct RetrievalFeedback {
    /// Unique identifier with `fb_` prefix.
    id: RetrievalFeedbackId,
    /// Links to the full OpenTelemetry trace (while it exists).
    trace_id: String,
    /// The original discovery query (entry point).
    query_text: String,
    /// Which embedding model was used for the query.
    embedding_model: String,
    /// Items returned by the discovery query.
    #[builder(default)]
    returned_item_ids: Vec<KnowledgeItemId>,
    /// Items used as exploration anchors.
    #[builder(default)]
    explored_anchor_ids: Vec<KnowledgeItemId>,
    /// System config or prompt version if tracked.
    #[builder(default)]
    policy_version: Option<String>,
    /// The principal who provided the feedback.
    principal_id: PrincipalId,
    /// The feedback rating.
    rating: FeedbackRating,
    /// Optional reasoning for the rating.
    #[builder(default)]
    notes: Option<String>,
    /// When the feedback was submitted.
    created_at: DateTime<Utc>,
}

impl RetrievalFeedback {
    /// Returns the feedback identifier.
    pub fn id(&self) -> RetrievalFeedbackId {
        self.id
    }

    /// Returns the trace identifier.
    pub fn trace_id(&self) -> &str {
        &self.trace_id
    }

    /// Returns the original query text.
    pub fn query_text(&self) -> &str {
        &self.query_text
    }

    /// Returns the embedding model used.
    pub fn embedding_model(&self) -> &str {
        &self.embedding_model
    }

    /// Returns the returned item identifiers.
    pub fn returned_item_ids(&self) -> &[KnowledgeItemId] {
        &self.returned_item_ids
    }

    /// Returns the explored anchor identifiers.
    pub fn explored_anchor_ids(&self) -> &[KnowledgeItemId] {
        &self.explored_anchor_ids
    }

    /// Returns the policy version, if tracked.
    pub fn policy_version(&self) -> Option<&str> {
        self.policy_version.as_deref()
    }

    /// Returns the principal who provided the feedback.
    pub fn principal_id(&self) -> PrincipalId {
        self.principal_id
    }

    /// Returns the feedback rating.
    pub fn rating(&self) -> FeedbackRating {
        self.rating
    }

    /// Returns the optional notes.
    pub fn notes(&self) -> Option<&str> {
        self.notes.as_deref()
    }

    /// Returns when the feedback was submitted.
    pub fn created_at(&self) -> DateTime<Utc> {
        self.created_at
    }
}
