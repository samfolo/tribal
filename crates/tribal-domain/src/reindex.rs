//! Reindex run and task domain types.
//!
//! A reindex run is one `tribal reindex` invocation: the operator-facing
//! lifecycle and monitoring surface. Its work is leased out as reindex tasks,
//! which reuse the ingestion lease invariants (claim implies a token, owner,
//! and heartbeat) against a distinct state set.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use typed_builder::TypedBuilder;
use uuid::Uuid;

use crate::{EmbeddingErrorClass, EmbeddingProfileId, PrincipalId, ReindexRunId, ReindexTaskId};

// ---------------------------------------------------------------------------
// Enums
// ---------------------------------------------------------------------------

/// Lifecycle state of a reindex run.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReindexRunState {
    /// Created, not yet picked up by the worker.
    Queued,
    /// Actively backfilling, sweeping, or cutting over.
    Running,
    /// Finished successfully; its target profile is active.
    Completed,
    /// Retired by a later run (reserved; runs are append-only history).
    Superseded,
    /// Cancelled by the operator.
    Aborted,
    /// Ended in failure (retry exhaustion, quarantine cap, or mid-build drift).
    Failed,
}

enum_text_conversions!(ReindexRunState {
    ReindexRunState::Queued => "queued",
    ReindexRunState::Running => "running",
    ReindexRunState::Completed => "completed",
    ReindexRunState::Superseded => "superseded",
    ReindexRunState::Aborted => "aborted",
    ReindexRunState::Failed => "failed",
});

/// Lifecycle state of a reindex task.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReindexTaskState {
    /// Available to claim.
    Pending,
    /// Held by a worker under a claim token.
    Claimed,
    /// Finished successfully.
    Completed,
    /// Exhausted its transient retries.
    DeadLetter,
}

enum_text_conversions!(ReindexTaskState {
    ReindexTaskState::Pending => "pending",
    ReindexTaskState::Claimed => "claimed",
    ReindexTaskState::Completed => "completed",
    ReindexTaskState::DeadLetter => "dead_letter",
});

/// Whether a reindex task embeds knowledge items or tags.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReindexEntityKind {
    /// Knowledge-item embeddings.
    Item,
    /// Tag embeddings.
    Tag,
}

enum_text_conversions!(ReindexEntityKind {
    ReindexEntityKind::Item => "item",
    ReindexEntityKind::Tag => "tag",
});

// ---------------------------------------------------------------------------
// ReindexRun
// ---------------------------------------------------------------------------

/// One reindex run: the lifecycle, tallies, and provenance of a model
/// migration.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TypedBuilder)]
pub struct ReindexRun {
    id: ReindexRunId,
    target_profile_id: EmbeddingProfileId,
    epoch: i64,
    state: ReindexRunState,
    initiated_by_principal_id: PrincipalId,
    #[builder(default)]
    items_enumerated: Option<u32>,
    items_embedded: u32,
    #[builder(default)]
    tags_enumerated: Option<u32>,
    tags_embedded: u32,
    items_quarantined: u32,
    tags_quarantined: u32,
    #[builder(default)]
    error_message: Option<String>,
    #[builder(default)]
    trace_context: Option<String>,
    started_at: DateTime<Utc>,
    #[builder(default)]
    completed_at: Option<DateTime<Utc>>,
}

impl ReindexRun {
    /// Returns the run identifier.
    #[must_use]
    pub fn id(&self) -> ReindexRunId {
        self.id
    }

    /// Returns the profile this run is building.
    #[must_use]
    pub fn target_profile_id(&self) -> EmbeddingProfileId {
        self.target_profile_id
    }

    /// Returns the target profile's epoch.
    #[must_use]
    pub fn epoch(&self) -> i64 {
        self.epoch
    }

    /// Returns the run's lifecycle state.
    #[must_use]
    pub fn state(&self) -> ReindexRunState {
        self.state
    }

    /// Returns the principal that initiated the run.
    #[must_use]
    pub fn initiated_by_principal_id(&self) -> PrincipalId {
        self.initiated_by_principal_id
    }

    /// Returns the first-enumeration item backlog count, if enumerated.
    #[must_use]
    pub fn items_enumerated(&self) -> Option<u32> {
        self.items_enumerated
    }

    /// Returns the count of item rows present for the profile.
    #[must_use]
    pub fn items_embedded(&self) -> u32 {
        self.items_embedded
    }

    /// Returns the first-enumeration tag backlog count, if enumerated.
    #[must_use]
    pub fn tags_enumerated(&self) -> Option<u32> {
        self.tags_enumerated
    }

    /// Returns the count of tag rows present for the profile.
    #[must_use]
    pub fn tags_embedded(&self) -> u32 {
        self.tags_embedded
    }

    /// Returns the count of permanently-quarantined items.
    #[must_use]
    pub fn items_quarantined(&self) -> u32 {
        self.items_quarantined
    }

    /// Returns the count of permanently-quarantined tags.
    #[must_use]
    pub fn tags_quarantined(&self) -> u32 {
        self.tags_quarantined
    }

    /// Returns the failure message, if the run failed.
    #[must_use]
    pub fn error_message(&self) -> Option<&str> {
        self.error_message.as_deref()
    }

    /// Returns the propagated trace context, if any.
    #[must_use]
    pub fn trace_context(&self) -> Option<&str> {
        self.trace_context.as_deref()
    }

    /// Returns when the run started.
    #[must_use]
    pub fn started_at(&self) -> DateTime<Utc> {
        self.started_at
    }

    /// Returns when the run reached a terminal state, if it has.
    #[must_use]
    pub fn completed_at(&self) -> Option<DateTime<Utc>> {
        self.completed_at
    }
}

// ---------------------------------------------------------------------------
// ReindexTask
// ---------------------------------------------------------------------------

/// One unit of reindex work: a backfill batch range or a catch-up singleton.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TypedBuilder)]
pub struct ReindexTask {
    id: ReindexTaskId,
    reindex_run_id: ReindexRunId,
    kind: ReindexEntityKind,
    target_ref: String,
    state: ReindexTaskState,
    attempt: u32,
    max_attempts: u32,
    available_at: DateTime<Utc>,
    #[builder(default)]
    claim_token: Option<Uuid>,
    #[builder(default)]
    claimed_by: Option<String>,
    #[builder(default)]
    claimed_at: Option<DateTime<Utc>>,
    #[builder(default)]
    heartbeat_at: Option<DateTime<Utc>>,
    #[builder(default)]
    last_error: Option<String>,
    #[builder(default)]
    last_error_class: Option<EmbeddingErrorClass>,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
    #[builder(default)]
    completed_at: Option<DateTime<Utc>>,
}

impl ReindexTask {
    /// Returns the task identifier.
    #[must_use]
    pub fn id(&self) -> ReindexTaskId {
        self.id
    }

    /// Returns the owning run.
    #[must_use]
    pub fn reindex_run_id(&self) -> ReindexRunId {
        self.reindex_run_id
    }

    /// Returns whether the task embeds items or tags.
    #[must_use]
    pub fn kind(&self) -> ReindexEntityKind {
        self.kind
    }

    /// Returns the batch range key or singleton entity reference.
    #[must_use]
    pub fn target_ref(&self) -> &str {
        &self.target_ref
    }

    /// Returns the task's lifecycle state.
    #[must_use]
    pub fn state(&self) -> ReindexTaskState {
        self.state
    }

    /// Returns the number of attempts made so far.
    #[must_use]
    pub fn attempt(&self) -> u32 {
        self.attempt
    }

    /// Returns the maximum attempts before dead-lettering.
    #[must_use]
    pub fn max_attempts(&self) -> u32 {
        self.max_attempts
    }

    /// Returns when the task next becomes available to claim.
    #[must_use]
    pub fn available_at(&self) -> DateTime<Utc> {
        self.available_at
    }

    /// Returns the claim token of the current owner, if claimed.
    #[must_use]
    pub fn claim_token(&self) -> Option<Uuid> {
        self.claim_token
    }

    /// Returns the current owner instance id, if claimed.
    #[must_use]
    pub fn claimed_by(&self) -> Option<&str> {
        self.claimed_by.as_deref()
    }

    /// Returns when the task was claimed, if claimed.
    #[must_use]
    pub fn claimed_at(&self) -> Option<DateTime<Utc>> {
        self.claimed_at
    }

    /// Returns the last heartbeat, if claimed.
    #[must_use]
    pub fn heartbeat_at(&self) -> Option<DateTime<Utc>> {
        self.heartbeat_at
    }

    /// Returns the last error message, if any.
    #[must_use]
    pub fn last_error(&self) -> Option<&str> {
        self.last_error.as_deref()
    }

    /// Returns the last error's class, if any.
    #[must_use]
    pub fn last_error_class(&self) -> Option<EmbeddingErrorClass> {
        self.last_error_class
    }

    /// Returns when the task was created.
    #[must_use]
    pub fn created_at(&self) -> DateTime<Utc> {
        self.created_at
    }

    /// Returns when the task was last updated.
    #[must_use]
    pub fn updated_at(&self) -> DateTime<Utc> {
        self.updated_at
    }

    /// Returns when the task completed, if it has.
    #[must_use]
    pub fn completed_at(&self) -> Option<DateTime<Utc>> {
        self.completed_at
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{enum_serde_tests, enum_text_tests};

    enum_serde_tests!(test_reindex_run_state_serde_roundtrip, ReindexRunState {
        ReindexRunState::Queued => "queued",
        ReindexRunState::Running => "running",
        ReindexRunState::Completed => "completed",
        ReindexRunState::Superseded => "superseded",
        ReindexRunState::Aborted => "aborted",
        ReindexRunState::Failed => "failed",
    });

    enum_text_tests!(test_reindex_run_state_text_roundtrip, ReindexRunState {
        ReindexRunState::Queued => "queued",
        ReindexRunState::Running => "running",
        ReindexRunState::Completed => "completed",
        ReindexRunState::Superseded => "superseded",
        ReindexRunState::Aborted => "aborted",
        ReindexRunState::Failed => "failed",
    });

    enum_serde_tests!(test_reindex_task_state_serde_roundtrip, ReindexTaskState {
        ReindexTaskState::Pending => "pending",
        ReindexTaskState::Claimed => "claimed",
        ReindexTaskState::Completed => "completed",
        ReindexTaskState::DeadLetter => "dead_letter",
    });

    enum_text_tests!(test_reindex_task_state_text_roundtrip, ReindexTaskState {
        ReindexTaskState::Pending => "pending",
        ReindexTaskState::Claimed => "claimed",
        ReindexTaskState::Completed => "completed",
        ReindexTaskState::DeadLetter => "dead_letter",
    });

    enum_serde_tests!(test_reindex_entity_kind_serde_roundtrip, ReindexEntityKind {
        ReindexEntityKind::Item => "item",
        ReindexEntityKind::Tag => "tag",
    });

    enum_text_tests!(test_reindex_entity_kind_text_roundtrip, ReindexEntityKind {
        ReindexEntityKind::Item => "item",
        ReindexEntityKind::Tag => "tag",
    });
}
