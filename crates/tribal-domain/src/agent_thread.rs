//! Agent threads: the durable unit of one agent run.
//!
//! A thread is an append-only sequence of records committed as they happen;
//! the committed record is truth, and every other view (resume, evaluation,
//! observability) derives from it. Thread-row fields such as status and
//! suspension are projections of the log, committed in the same transaction
//! as the record that implies them. The lifecycle vocabulary is deliberately
//! isomorphic to the A2A task lifecycle.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use typed_builder::TypedBuilder;

use crate::{
    AgentBindingVersionId, AgentDriverTaskId, AgentThreadId, AgentThreadRecordId, PipelineStage,
    PrincipalId, TaskId,
};

/// The serialisation shape of thread-owned structures (record `content`
/// and the suspension payload). Bumped when the serialised form changes;
/// distinct from the binding version, which pins behaviour.
pub const AGENT_THREAD_FORMAT_VERSION: u32 = 1;

// ---------------------------------------------------------------------------
// Status
// ---------------------------------------------------------------------------

/// The lifecycle status of an agent thread.
///
/// All four terminal states are terminal: nothing transitions out of them,
/// and any resolution arriving at a terminal thread is recorded and
/// discarded. A thread is dead-lettered only from `Running`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AgentThreadStatus {
    /// Created, not yet driven.
    Queued,
    /// A driving task holds the thread.
    Running,
    /// Data at rest awaiting its typed cause's resolution.
    Suspended,
    /// Terminal: the run completed.
    Completed,
    /// Terminal: a non-recoverable stage error.
    Failed,
    /// Terminal: a durable cancellation intent was observed.
    Cancelled,
    /// Terminal: thread recovery exhausted.
    DeadLetter,
}

enum_text_conversions!(AgentThreadStatus {
    AgentThreadStatus::Queued => "queued",
    AgentThreadStatus::Running => "running",
    AgentThreadStatus::Suspended => "suspended",
    AgentThreadStatus::Completed => "completed",
    AgentThreadStatus::Failed => "failed",
    AgentThreadStatus::Cancelled => "cancelled",
    AgentThreadStatus::DeadLetter => "dead_letter",
});

impl AgentThreadStatus {
    /// Returns `true` for the four terminal states.
    #[must_use]
    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Completed | Self::Failed | Self::Cancelled | Self::DeadLetter
        )
    }
}

/// The four terminal statuses, as their own type.
///
/// Carried wherever an outcome is terminal by construction (the
/// disposition decision, terminal-transition commits), so a live status
/// in a terminal position is unrepresentable rather than asserted.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AgentThreadTerminal {
    /// The run completed.
    Completed,
    /// A non-recoverable stage error.
    Failed,
    /// A durable cancellation intent was observed.
    Cancelled,
    /// Thread recovery exhausted.
    DeadLetter,
}

impl From<AgentThreadTerminal> for AgentThreadStatus {
    fn from(terminal: AgentThreadTerminal) -> Self {
        match terminal {
            AgentThreadTerminal::Completed => Self::Completed,
            AgentThreadTerminal::Failed => Self::Failed,
            AgentThreadTerminal::Cancelled => Self::Cancelled,
            AgentThreadTerminal::DeadLetter => Self::DeadLetter,
        }
    }
}

// ---------------------------------------------------------------------------
// Suspension
// ---------------------------------------------------------------------------

/// The typed cause a suspended thread is waiting on.
///
/// One state, one resume path: the resolving event is appended to the
/// thread and the driving task is made available. Serialised to the thread
/// row's `suspension` column under [`AGENT_THREAD_FORMAT_VERSION`]; the
/// timer cause carries no payload because the row's `wake_at` column is
/// the authority the availability sweep drives.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "cause", rename_all = "snake_case")]
pub enum AgentThreadSuspension {
    /// Pending human input (the A2A `input-required` analogue).
    HumanInput {
        /// The model-facing request shown to whoever resolves it.
        prompt: String,
    },
    /// Pending deferred tool results.
    DeferredToolResults {
        /// The assistant message whose calls are outstanding; scopes the
        /// tool-result fence.
        requesting_seq: AgentThreadRecordSeq,
        /// The call ids still awaiting a result; resolution is
        /// completeness-checked against this set.
        pending_tool_call_ids: Vec<String>,
    },
    /// Sleeping until the row's `wake_at` instant.
    Timer,
    /// An execution budget was exhausted; the admission check re-runs on
    /// wake, and a bounded number of unchanged re-checks fails the thread.
    BudgetExhaustion {
        /// Consecutive wakes whose re-check found the budget still
        /// exhausted.
        unchanged_rechecks: u32,
    },
}

// ---------------------------------------------------------------------------
// Records
// ---------------------------------------------------------------------------

/// A record's position in its thread's append-only log.
///
/// `seq` is the counter vocabulary for log positions; derivation happens
/// under the same locked read snapshot that selected the work, so a
/// conflict loser re-reads the tail rather than guessing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct AgentThreadRecordSeq(i64);

impl AgentThreadRecordSeq {
    /// The first position in a thread's log.
    pub const FIRST: Self = Self(0);

    /// Wraps a raw log position.
    #[must_use]
    pub fn new(position: i64) -> Self {
        Self(position)
    }

    /// Returns the raw log position.
    #[must_use]
    pub fn inner(self) -> i64 {
        self.0
    }

    /// Returns the position immediately after this one.
    #[must_use]
    pub fn next(self) -> Self {
        Self(self.0 + 1)
    }
}

impl std::fmt::Display for AgentThreadRecordSeq {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

/// What kind of event one committed record is.
///
/// Conversation records (assistant message, tool result, input,
/// submission) project into the model-facing conversation; control records
/// (suspension, cancellation) are runtime bookkeeping the model never
/// sees. `ObservedToolEvent` carries an external agent's transcript
/// observations, which never claim the executed-tool-result fence.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AgentThreadRecordKind {
    /// A model response, with any tool-call requests it carries.
    AssistantMessage,
    /// One executed tool call's result.
    ToolResult,
    /// Input injected into the conversation (the initial prompt, a human
    /// resolution, a child's hand-back).
    Input,
    /// A suspension control record.
    Suspension,
    /// A cancellation control record.
    Cancellation,
    /// An agentic stage's typed submission.
    Submission,
    /// An external agent's observed (not executed) tool activity.
    ObservedToolEvent,
}

enum_text_conversions!(AgentThreadRecordKind {
    AgentThreadRecordKind::AssistantMessage => "assistant_message",
    AgentThreadRecordKind::ToolResult => "tool_result",
    AgentThreadRecordKind::Input => "input",
    AgentThreadRecordKind::Suspension => "suspension",
    AgentThreadRecordKind::Cancellation => "cancellation",
    AgentThreadRecordKind::Submission => "submission",
    AgentThreadRecordKind::ObservedToolEvent => "observed_tool_event",
});

impl AgentThreadRecordKind {
    /// Returns `true` for records that project into the model-facing
    /// conversation.
    #[must_use]
    pub fn is_conversation(self) -> bool {
        matches!(
            self,
            Self::AssistantMessage | Self::ToolResult | Self::Input | Self::Submission
        )
    }
}

/// One committed record in a thread's append-only log.
///
/// # Invariants
///
/// - `(thread_id, seq)` is unique; the log has no gaps a reader must
///   tolerate, only a tail it must re-read after losing a conflict.
/// - `requesting_seq` and `tool_call_id` are set exactly when `kind` is
///   [`AgentThreadRecordKind::ToolResult`]; the partial unique fence over
///   them makes executed results exactly-once per requested call.
/// - Records are immutable once committed.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TypedBuilder)]
pub struct AgentThreadRecord {
    /// Unique identifier with `arec_` prefix.
    id: AgentThreadRecordId,
    /// The thread this record belongs to.
    thread_id: AgentThreadId,
    /// Position in the thread's log.
    seq: AgentThreadRecordSeq,
    /// What kind of event this record is.
    kind: AgentThreadRecordKind,
    /// The requesting assistant message's position, for executed tool
    /// results.
    #[builder(default)]
    requesting_seq: Option<AgentThreadRecordSeq>,
    /// The provider call id this result answers, for executed tool
    /// results.
    #[builder(default)]
    tool_call_id: Option<String>,
    /// The event payload, shaped per the thread's format version.
    content: serde_json::Value,
    /// Token usage observed for the call this record captures, where one
    /// occurred.
    #[builder(default)]
    usage: Option<serde_json::Value>,
    /// The trace this record was observed under.
    #[builder(default)]
    trace_id: Option<String>,
    /// The span this record was observed under.
    #[builder(default)]
    span_id: Option<String>,
    /// When this record was committed.
    committed_at: DateTime<Utc>,
}

impl AgentThreadRecord {
    /// Returns the record identifier.
    pub fn id(&self) -> AgentThreadRecordId {
        self.id
    }

    /// Returns the owning thread's identifier.
    pub fn thread_id(&self) -> AgentThreadId {
        self.thread_id
    }

    /// Returns the record's position in the log.
    pub fn seq(&self) -> AgentThreadRecordSeq {
        self.seq
    }

    /// Returns the record kind.
    pub fn kind(&self) -> AgentThreadRecordKind {
        self.kind
    }

    /// Returns the requesting assistant message's position, for executed
    /// tool results.
    pub fn requesting_seq(&self) -> Option<AgentThreadRecordSeq> {
        self.requesting_seq
    }

    /// Returns the answered call id, for executed tool results.
    pub fn tool_call_id(&self) -> Option<&str> {
        self.tool_call_id.as_deref()
    }

    /// Returns the event payload.
    pub fn content(&self) -> &serde_json::Value {
        &self.content
    }

    /// Returns the observed token usage, where one call occurred.
    pub fn usage(&self) -> Option<&serde_json::Value> {
        self.usage.as_ref()
    }

    /// Returns the observing trace id.
    pub fn trace_id(&self) -> Option<&str> {
        self.trace_id.as_deref()
    }

    /// Returns the observing span id.
    pub fn span_id(&self) -> Option<&str> {
        self.span_id.as_deref()
    }

    /// Returns the commit time.
    pub fn committed_at(&self) -> DateTime<Utc> {
        self.committed_at
    }
}

// ---------------------------------------------------------------------------
// Thread
// ---------------------------------------------------------------------------

/// A durable agent run.
///
/// # Invariants
///
/// - Exactly one of `stage_task_id` and `driver_task_id` is set: every
///   thread has a real driving row for guards to target.
/// - `suspension` is set exactly when `status` is `Suspended`.
/// - `recovery_attempts` accumulates across recovery cycles and never
///   resets; it drives the escalating per-cycle backoff.
/// - `completed_at` is set exactly when `status` is terminal.
/// - The cancellation intent columns are seq-free and idempotent: writing
///   them can never be lost to a racing record commit.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TypedBuilder)]
pub struct AgentThread {
    /// Unique identifier with `athr_` prefix.
    id: AgentThreadId,
    /// The parent thread, for delegation lineage.
    #[builder(default)]
    parent_thread_id: Option<AgentThreadId>,
    /// The pipeline stage this thread executes.
    pipeline_stage: PipelineStage,
    /// The content-addressed binding this thread was admitted under.
    binding_version_id: AgentBindingVersionId,
    /// The launched stage task driving this thread, when stage-bound.
    #[builder(default)]
    stage_task_id: Option<TaskId>,
    /// The driver-family task driving this thread, when not stage-bound.
    #[builder(default)]
    driver_task_id: Option<AgentDriverTaskId>,
    /// The principal this run is attributed and metered to.
    principal_id: PrincipalId,
    /// Current lifecycle status.
    status: AgentThreadStatus,
    /// The typed cause a suspended thread awaits.
    #[builder(default)]
    suspension: Option<AgentThreadSuspension>,
    /// When cancellation was durably requested, if it was.
    #[builder(default)]
    cancel_requested_at: Option<DateTime<Utc>>,
    /// Who durably requested cancellation, if anyone.
    #[builder(default)]
    cancel_requested_by: Option<String>,
    /// Recovery cycles consumed; accumulates, never resets.
    recovery_attempts: u32,
    /// The serialisation shape of this thread's owned structures.
    format_version: u32,
    /// The instant a timer or budget suspension wakes at.
    #[builder(default)]
    wake_at: Option<DateTime<Utc>>,
    /// Capabilities claimed at initialisation and fidelity observed in the
    /// stream, finalised at completion.
    #[builder(default)]
    fidelity: Option<serde_json::Value>,
    /// The committed-record projection of this thread's spend.
    #[builder(default)]
    execution_spend: Option<serde_json::Value>,
    /// When this thread reached a terminal status.
    #[builder(default)]
    completed_at: Option<DateTime<Utc>>,
    /// When this thread was created.
    created_at: DateTime<Utc>,
    /// When this thread was last updated.
    updated_at: DateTime<Utc>,
}

impl AgentThread {
    /// Returns the thread identifier.
    pub fn id(&self) -> AgentThreadId {
        self.id
    }

    /// Returns the parent thread, for delegation lineage.
    pub fn parent_thread_id(&self) -> Option<AgentThreadId> {
        self.parent_thread_id
    }

    /// Returns the pipeline stage this thread executes.
    pub fn pipeline_stage(&self) -> PipelineStage {
        self.pipeline_stage
    }

    /// Returns the binding version this thread was admitted under.
    pub fn binding_version_id(&self) -> AgentBindingVersionId {
        self.binding_version_id
    }

    /// Returns the driving stage task, when stage-bound.
    pub fn stage_task_id(&self) -> Option<TaskId> {
        self.stage_task_id
    }

    /// Returns the driving driver-family task, when not stage-bound.
    pub fn driver_task_id(&self) -> Option<AgentDriverTaskId> {
        self.driver_task_id
    }

    /// Returns the principal this run is attributed to.
    pub fn principal_id(&self) -> PrincipalId {
        self.principal_id
    }

    /// Returns the current lifecycle status.
    pub fn status(&self) -> AgentThreadStatus {
        self.status
    }

    /// Returns the suspension cause, while suspended.
    pub fn suspension(&self) -> Option<&AgentThreadSuspension> {
        self.suspension.as_ref()
    }

    /// Returns when cancellation was durably requested, if it was.
    pub fn cancel_requested_at(&self) -> Option<DateTime<Utc>> {
        self.cancel_requested_at
    }

    /// Returns who durably requested cancellation, if anyone.
    pub fn cancel_requested_by(&self) -> Option<&str> {
        self.cancel_requested_by.as_deref()
    }

    /// Returns the accumulated recovery-cycle count.
    pub fn recovery_attempts(&self) -> u32 {
        self.recovery_attempts
    }

    /// Returns the serialisation shape of this thread's owned structures.
    pub fn format_version(&self) -> u32 {
        self.format_version
    }

    /// Returns the wake instant, while timer- or budget-suspended.
    pub fn wake_at(&self) -> Option<DateTime<Utc>> {
        self.wake_at
    }

    /// Returns the claimed-versus-observed fidelity report.
    pub fn fidelity(&self) -> Option<&serde_json::Value> {
        self.fidelity.as_ref()
    }

    /// Returns the committed-record projection of spend.
    pub fn execution_spend(&self) -> Option<&serde_json::Value> {
        self.execution_spend.as_ref()
    }

    /// Returns when this thread reached a terminal status.
    pub fn completed_at(&self) -> Option<DateTime<Utc>> {
        self.completed_at
    }

    /// Returns when this thread was created.
    pub fn created_at(&self) -> DateTime<Utc> {
        self.created_at
    }

    /// Returns when this thread was last updated.
    pub fn updated_at(&self) -> DateTime<Utc> {
        self.updated_at
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{enum_serde_tests, enum_text_tests};

    enum_serde_tests!(test_thread_status_serde_roundtrip, AgentThreadStatus {
        AgentThreadStatus::Queued => "queued",
        AgentThreadStatus::Running => "running",
        AgentThreadStatus::Suspended => "suspended",
        AgentThreadStatus::Completed => "completed",
        AgentThreadStatus::Failed => "failed",
        AgentThreadStatus::Cancelled => "cancelled",
        AgentThreadStatus::DeadLetter => "dead_letter",
    });

    enum_text_tests!(test_thread_status_text_roundtrip, AgentThreadStatus {
        AgentThreadStatus::Queued => "queued",
        AgentThreadStatus::Running => "running",
        AgentThreadStatus::Suspended => "suspended",
        AgentThreadStatus::Completed => "completed",
        AgentThreadStatus::Failed => "failed",
        AgentThreadStatus::Cancelled => "cancelled",
        AgentThreadStatus::DeadLetter => "dead_letter",
    });

    enum_serde_tests!(test_thread_terminal_serde_roundtrip, AgentThreadTerminal {
        AgentThreadTerminal::Completed => "completed",
        AgentThreadTerminal::Failed => "failed",
        AgentThreadTerminal::Cancelled => "cancelled",
        AgentThreadTerminal::DeadLetter => "dead_letter",
    });

    enum_serde_tests!(test_record_kind_serde_roundtrip, AgentThreadRecordKind {
        AgentThreadRecordKind::AssistantMessage => "assistant_message",
        AgentThreadRecordKind::ToolResult => "tool_result",
        AgentThreadRecordKind::Input => "input",
        AgentThreadRecordKind::Suspension => "suspension",
        AgentThreadRecordKind::Cancellation => "cancellation",
        AgentThreadRecordKind::Submission => "submission",
        AgentThreadRecordKind::ObservedToolEvent => "observed_tool_event",
    });

    enum_text_tests!(test_record_kind_text_roundtrip, AgentThreadRecordKind {
        AgentThreadRecordKind::AssistantMessage => "assistant_message",
        AgentThreadRecordKind::ToolResult => "tool_result",
        AgentThreadRecordKind::Input => "input",
        AgentThreadRecordKind::Suspension => "suspension",
        AgentThreadRecordKind::Cancellation => "cancellation",
        AgentThreadRecordKind::Submission => "submission",
        AgentThreadRecordKind::ObservedToolEvent => "observed_tool_event",
    });

    #[test]
    fn test_terminal_maps_onto_terminal_statuses() {
        for (terminal, status) in [
            (AgentThreadTerminal::Completed, AgentThreadStatus::Completed),
            (AgentThreadTerminal::Failed, AgentThreadStatus::Failed),
            (AgentThreadTerminal::Cancelled, AgentThreadStatus::Cancelled),
            (
                AgentThreadTerminal::DeadLetter,
                AgentThreadStatus::DeadLetter,
            ),
        ] {
            let mapped = AgentThreadStatus::from(terminal);
            assert_eq!(mapped, status);
            assert!(mapped.is_terminal());
        }
    }

    #[test]
    fn test_thread_status_terminality_partition() {
        assert!(!AgentThreadStatus::Queued.is_terminal());
        assert!(!AgentThreadStatus::Running.is_terminal());
        assert!(!AgentThreadStatus::Suspended.is_terminal());
        assert!(AgentThreadStatus::Completed.is_terminal());
        assert!(AgentThreadStatus::Failed.is_terminal());
        assert!(AgentThreadStatus::Cancelled.is_terminal());
        assert!(AgentThreadStatus::DeadLetter.is_terminal());
    }

    #[test]
    fn test_record_kind_conversation_partition() {
        assert!(AgentThreadRecordKind::AssistantMessage.is_conversation());
        assert!(AgentThreadRecordKind::ToolResult.is_conversation());
        assert!(AgentThreadRecordKind::Input.is_conversation());
        assert!(AgentThreadRecordKind::Submission.is_conversation());
        assert!(!AgentThreadRecordKind::Suspension.is_conversation());
        assert!(!AgentThreadRecordKind::Cancellation.is_conversation());
        assert!(!AgentThreadRecordKind::ObservedToolEvent.is_conversation());
    }

    #[test]
    fn test_suspension_serialises_with_a_cause_tag() {
        let suspension = AgentThreadSuspension::DeferredToolResults {
            requesting_seq: AgentThreadRecordSeq::new(4),
            pending_tool_call_ids: vec!["call_1".to_owned(), "call_2".to_owned()],
        };

        let json = serde_json::to_value(&suspension).expect("suspension serialises");
        assert_eq!(json["cause"], "deferred_tool_results");
        assert_eq!(json["requesting_seq"], 4);

        let back: AgentThreadSuspension =
            serde_json::from_value(json).expect("suspension round-trips");
        assert_eq!(back, suspension);
    }

    #[test]
    fn test_seq_orders_and_advances() {
        let first = AgentThreadRecordSeq::FIRST;
        assert_eq!(first.inner(), 0);
        assert!(first < first.next());
        assert_eq!(first.next().inner(), 1);
    }
}
