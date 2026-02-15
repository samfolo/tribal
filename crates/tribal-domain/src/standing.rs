//! Standing — computed evidential profile of a knowledge item.
//!
//! Standing summarises an item's position in the knowledge graph.
//! It is always computed at query time from the relationship graph
//! and item observations — never stored. `DerivedFrom` edges are
//! excluded from standing counts (provenance ≠ reliability).

use serde::{Deserialize, Serialize};
use typed_builder::TypedBuilder;

use crate::KnowledgeItemId;

/// Computed evidential standing of a knowledge item.
///
/// Summarises support, contradiction, supersession, observation frequency,
/// and diversity of the support base. Computed at query time from the
/// relationship graph — consistent with the "outcome derived from graph,
/// not stored on item" principle.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, TypedBuilder)]
pub struct Standing {
    /// Number of distinct items supporting this item.
    supporting_count: u32,
    /// Number of distinct items contradicting this item.
    contradicting_count: u32,
    /// If superseded, which item replaced this one.
    #[builder(default)]
    superseded_by: Option<KnowledgeItemId>,
    /// Re-encounter frequency (1 + `item_observations` count).
    observation_count: u32,
    /// Most recent supporting item (actionable ID).
    #[builder(default)]
    newest_supporting_id: Option<KnowledgeItemId>,
    /// Most recent contradicting item (actionable ID).
    #[builder(default)]
    newest_contradicting_id: Option<KnowledgeItemId>,
    /// Distinct episodes providing support.
    supporting_episode_count: u32,
    /// Distinct projects providing support.
    supporting_project_count: u32,
}

impl Standing {
    /// Returns the number of distinct supporting items.
    pub fn supporting_count(&self) -> u32 {
        self.supporting_count
    }

    /// Returns the number of distinct contradicting items.
    pub fn contradicting_count(&self) -> u32 {
        self.contradicting_count
    }

    /// Returns the superseding item, if this item has been superseded.
    pub fn superseded_by(&self) -> Option<KnowledgeItemId> {
        self.superseded_by
    }

    /// Returns the observation count (re-encounter frequency).
    pub fn observation_count(&self) -> u32 {
        self.observation_count
    }

    /// Returns the most recent supporting item identifier.
    pub fn newest_supporting_id(&self) -> Option<KnowledgeItemId> {
        self.newest_supporting_id
    }

    /// Returns the most recent contradicting item identifier.
    pub fn newest_contradicting_id(&self) -> Option<KnowledgeItemId> {
        self.newest_contradicting_id
    }

    /// Returns the number of distinct supporting episodes.
    pub fn supporting_episode_count(&self) -> u32 {
        self.supporting_episode_count
    }

    /// Returns the number of distinct supporting projects.
    pub fn supporting_project_count(&self) -> u32 {
        self.supporting_project_count
    }
}
