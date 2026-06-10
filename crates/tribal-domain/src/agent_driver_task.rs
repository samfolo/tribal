//! Agent driver tasks: the queue family for threads with no stage task.
//!
//! Delegation children, verifiers, and deferred tool work are driven by
//! rows here rather than by launched stage tasks, with the same lease
//! mechanics the stage family uses: claim token, heartbeat, availability,
//! `FOR UPDATE SKIP LOCKED`, reclaim, retry, and dead-letter. The reindex
//! family is the precedent.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use typed_builder::TypedBuilder;

use crate::{AgentDriverTaskId, AgentThreadId};

/// The lifecycle state of a driver task.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AgentDriverTaskState {
    /// Waiting to be claimed.
    Pending,
    /// A worker owns this row and is actively processing it.
    Claimed,
    /// Terminal: the work finished.
    Completed,
    /// Terminal: the retry budget is exhausted.
    DeadLetter,
}

enum_text_conversions!(AgentDriverTaskState {
    AgentDriverTaskState::Pending => "pending",
    AgentDriverTaskState::Claimed => "claimed",
    AgentDriverTaskState::Completed => "completed",
    AgentDriverTaskState::DeadLetter => "dead_letter",
});

impl AgentDriverTaskState {
    /// Returns `true` for the two terminal states.
    #[must_use]
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Completed | Self::DeadLetter)
    }
}

/// What a driver task executes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AgentDriverTaskKind {
    /// Drives a thread's turns.
    Drive,
    /// Executes one deferred tool call.
    DeferredTool,
}

enum_text_conversions!(AgentDriverTaskKind {
    AgentDriverTaskKind::Drive => "drive",
    AgentDriverTaskKind::DeferredTool => "deferred_tool",
});

/// A driver-family task row.
///
/// # Invariants
///
/// - `claim_token`, `claimed_by`, and `heartbeat_at` are set only while
///   `Claimed`.
/// - Dead-letter condition: `attempt > max_attempts` after increment.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TypedBuilder)]
pub struct AgentDriverTask {
    /// Unique identifier with `adrv_` prefix.
    id: AgentDriverTaskId,
    /// The thread this row drives or works for.
    thread_id: AgentThreadId,
    /// What this row executes.
    kind: AgentDriverTaskKind,
    /// Current lifecycle state.
    state: AgentDriverTaskState,
    /// Failed attempts so far.
    attempt: u32,
    /// The retry budget.
    max_attempts: u32,
    /// The earliest claimable time.
    available_at: DateTime<Utc>,
    /// The ownership claim token, while claimed.
    #[builder(default)]
    claim_token: Option<uuid::Uuid>,
    /// The claiming worker identity, while claimed.
    #[builder(default)]
    claimed_by: Option<String>,
    /// When this row was claimed.
    #[builder(default)]
    claimed_at: Option<DateTime<Utc>>,
    /// The most recent heartbeat, while claimed.
    #[builder(default)]
    heartbeat_at: Option<DateTime<Utc>>,
    /// The most recent failure message, if any.
    #[builder(default)]
    last_error: Option<String>,
    /// When this row was created.
    created_at: DateTime<Utc>,
    /// When this row was last updated.
    updated_at: DateTime<Utc>,
    /// When this row reached a terminal state.
    #[builder(default)]
    completed_at: Option<DateTime<Utc>>,
}

impl AgentDriverTask {
    /// Returns the driver-task identifier.
    pub fn id(&self) -> AgentDriverTaskId {
        self.id
    }

    /// Returns the thread this row drives or works for.
    pub fn thread_id(&self) -> AgentThreadId {
        self.thread_id
    }

    /// Returns what this row executes.
    pub fn kind(&self) -> AgentDriverTaskKind {
        self.kind
    }

    /// Returns the current lifecycle state.
    pub fn state(&self) -> AgentDriverTaskState {
        self.state
    }

    /// Returns the failed attempts so far.
    pub fn attempt(&self) -> u32 {
        self.attempt
    }

    /// Returns the retry budget.
    pub fn max_attempts(&self) -> u32 {
        self.max_attempts
    }

    /// Returns the earliest claimable time.
    pub fn available_at(&self) -> DateTime<Utc> {
        self.available_at
    }

    /// Returns the ownership claim token, while claimed.
    pub fn claim_token(&self) -> Option<uuid::Uuid> {
        self.claim_token
    }

    /// Returns the claiming worker identity, while claimed.
    pub fn claimed_by(&self) -> Option<&str> {
        self.claimed_by.as_deref()
    }

    /// Returns when this row was claimed.
    pub fn claimed_at(&self) -> Option<DateTime<Utc>> {
        self.claimed_at
    }

    /// Returns the most recent heartbeat, while claimed.
    pub fn heartbeat_at(&self) -> Option<DateTime<Utc>> {
        self.heartbeat_at
    }

    /// Returns the most recent failure message, if any.
    pub fn last_error(&self) -> Option<&str> {
        self.last_error.as_deref()
    }

    /// Returns when this row was created.
    pub fn created_at(&self) -> DateTime<Utc> {
        self.created_at
    }

    /// Returns when this row was last updated.
    pub fn updated_at(&self) -> DateTime<Utc> {
        self.updated_at
    }

    /// Returns when this row reached a terminal state.
    pub fn completed_at(&self) -> Option<DateTime<Utc>> {
        self.completed_at
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{enum_serde_tests, enum_text_tests};

    enum_serde_tests!(test_driver_state_serde_roundtrip, AgentDriverTaskState {
        AgentDriverTaskState::Pending => "pending",
        AgentDriverTaskState::Claimed => "claimed",
        AgentDriverTaskState::Completed => "completed",
        AgentDriverTaskState::DeadLetter => "dead_letter",
    });

    enum_text_tests!(test_driver_state_text_roundtrip, AgentDriverTaskState {
        AgentDriverTaskState::Pending => "pending",
        AgentDriverTaskState::Claimed => "claimed",
        AgentDriverTaskState::Completed => "completed",
        AgentDriverTaskState::DeadLetter => "dead_letter",
    });

    enum_serde_tests!(test_driver_kind_serde_roundtrip, AgentDriverTaskKind {
        AgentDriverTaskKind::Drive => "drive",
        AgentDriverTaskKind::DeferredTool => "deferred_tool",
    });

    enum_text_tests!(test_driver_kind_text_roundtrip, AgentDriverTaskKind {
        AgentDriverTaskKind::Drive => "drive",
        AgentDriverTaskKind::DeferredTool => "deferred_tool",
    });

    #[test]
    fn test_driver_state_terminality_partition() {
        assert!(!AgentDriverTaskState::Pending.is_terminal());
        assert!(!AgentDriverTaskState::Claimed.is_terminal());
        assert!(AgentDriverTaskState::Completed.is_terminal());
        assert!(AgentDriverTaskState::DeadLetter.is_terminal());
    }
}
