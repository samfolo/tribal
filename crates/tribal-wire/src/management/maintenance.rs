//! Reindex and thread-retention administration DTOs.

use serde::{Deserialize, Serialize};
use tribal_domain::{ProviderKind, ReindexRunId, TaskType};

use super::{ConfigRevision, GenesisEmbeddingInput, Revisioned};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct ReindexRunRequest {
    pub expected_revision: ConfigRevision,
    pub target: GenesisEmbeddingInput,
    pub mode: MutationMode,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum MutationMode {
    Preview,
    Apply,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct ReindexPlan {
    pub provider: ProviderKind,
    pub model: String,
    pub dimensions: u32,
    pub normalised_base_url: String,
    pub estimated_items: u64,
    pub estimated_tags: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(tag = "resolution", content = "data", rename_all = "snake_case")]
pub enum ReindexApplyResolution {
    Created { run_id: ReindexRunId },
    AlreadyLive { run_id: ReindexRunId },
    Unchanged,
    LockContended,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(tag = "outcome", content = "data", rename_all = "snake_case")]
pub enum ReindexRunOutcome {
    Preview {
        plan: ReindexPlan,
    },
    Applied {
        plan: ReindexPlan,
        resolution: ReindexApplyResolution,
    },
}

pub type ReindexRunResult = Revisioned<ReindexRunOutcome>;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct ReindexCancelRequest {
    pub expected_revision: ConfigRevision,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(tag = "outcome", content = "data", rename_all = "snake_case")]
pub enum ReindexCancelOutcome {
    Cancelled { run_id: ReindexRunId },
    NoLiveRun,
}

pub type ReindexCancelResult = Revisioned<ReindexCancelOutcome>;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct ReindexPruneRequest {
    pub expected_revision: ConfigRevision,
    pub mode: MutationMode,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct ReindexPruneCounts {
    pub profiles: u64,
    pub embeddings: u64,
    pub tag_embeddings: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(tag = "outcome", content = "data", rename_all = "snake_case")]
pub enum ReindexPruneOutcome {
    Preview { candidates: ReindexPruneCounts },
    Applied { deleted: ReindexPruneCounts },
}

pub type ReindexPruneResult = Revisioned<ReindexPruneOutcome>;

/// Positive retention interval measured by the manager clock.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(try_from = "u32", into = "u32")]
pub struct RetentionDays(u32);

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("retention days must be greater than zero")]
pub struct RetentionDaysError;

impl TryFrom<u32> for RetentionDays {
    type Error = RetentionDaysError;

    fn try_from(value: u32) -> Result<Self, Self::Error> {
        if value == 0 {
            Err(RetentionDaysError)
        } else {
            Ok(Self(value))
        }
    }
}

impl From<RetentionDays> for u32 {
    fn from(value: RetentionDays) -> Self {
        value.0
    }
}

impl RetentionDays {
    #[must_use]
    pub const fn get(self) -> u32 {
        self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct ThreadPruneRequest {
    pub expected_revision: ConfigRevision,
    pub older_than: RetentionDays,
    pub stage: Option<TaskType>,
    pub cascade: bool,
    pub mode: MutationMode,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct ThreadPrunePlan {
    pub eligible: u64,
    pub refused: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct ThreadPruneApplied {
    pub pruned: u64,
    pub refused: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(tag = "outcome", content = "data", rename_all = "snake_case")]
pub enum ThreadPruneOutcome {
    Preview { plan: ThreadPrunePlan },
    Applied { result: ThreadPruneApplied },
}

pub type ThreadPruneResult = Revisioned<ThreadPruneOutcome>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_retention_days_rejects_zero_during_deserialisation() {
        assert!(serde_json::from_str::<RetentionDays>("0").is_err());
        assert_eq!(serde_json::from_str::<RetentionDays>("1").unwrap().get(), 1);
    }
}
