//! Item observation entity — deduplication signal for knowledge items.
//!
//! Created when triage classifies a candidate as a duplicate of an
//! existing item. Records the re-encounter without creating a new item.
//! Item weight is derived from observation count at query time.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use typed_builder::TypedBuilder;

use crate::{ItemObservationId, KnowledgeItemId, PrincipalId, SourceType};

/// A record of an existing knowledge item being re-observed.
///
/// Weight derivation: item weight = `1 + COUNT(observations)`.
/// Computed at query time — no stored weight columns.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, TypedBuilder)]
pub struct ItemObservation {
    /// Unique identifier with `obs_` prefix.
    id: ItemObservationId,
    /// The existing knowledge item that was re-observed.
    knowledge_item_id: KnowledgeItemId,
    /// The principal who captured this observation.
    principal_id: PrincipalId,
    /// How the re-observation was captured.
    source_type: SourceType,
    /// When the re-observation occurred.
    observed_at: DateTime<Utc>,
}

impl ItemObservation {
    /// Returns the observation identifier.
    pub fn id(&self) -> ItemObservationId {
        self.id
    }

    /// Returns the knowledge item that was re-observed.
    pub fn knowledge_item_id(&self) -> KnowledgeItemId {
        self.knowledge_item_id
    }

    /// Returns the principal who captured this observation.
    pub fn principal_id(&self) -> PrincipalId {
        self.principal_id
    }

    /// Returns how the re-observation was captured.
    pub fn source_type(&self) -> SourceType {
        self.source_type
    }

    /// Returns when the re-observation occurred.
    pub fn observed_at(&self) -> DateTime<Utc> {
        self.observed_at
    }
}
