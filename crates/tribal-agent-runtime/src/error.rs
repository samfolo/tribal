//! Crate-level error type for the agent runtime.

use thiserror::Error;
use tribal_db::DrivingTaskRef;
use tribal_domain::{AgentThreadId, AgentThreadStatus, TaskId};

/// Errors from the runtime's thread-store and turn operations.
#[derive(Debug, Error)]
pub enum AgentRuntimeError {
    /// A database operation failed.
    #[error("agent runtime database operation failed: {context}")]
    Database {
        /// What the runtime was doing.
        context: String,
        /// The underlying database error.
        #[source]
        source: tribal_db::DbError,
    },

    /// A status compare-and-set missed: the thread moved under the
    /// actor, which abandons its write — ownership is lost, or the next
    /// sweep cycle converges what this attempt could not.
    #[error("thread {thread_id} status CAS missed: expected {expected}, the row moved")]
    StatusCasMissed {
        /// The thread whose status moved.
        thread_id: AgentThreadId,
        /// The status the actor expected to move from.
        expected: AgentThreadStatus,
    },

    /// Waking a suspended thread found its driving task not blocked, so
    /// re-queueing it affected zero rows. Committing the wake would leave
    /// the thread running with an unclaimable task, so the whole wake
    /// rolls back and a sweep or retry re-attempts it.
    #[error("stage task {task_id} was not blocked when its thread woke")]
    DrivingTaskNotBlocked {
        /// The stage task the wake expected to find blocked.
        task_id: TaskId,
    },

    /// The claim-guarded driving-task write affected zero rows: the
    /// lease was lost and the whole commit must roll back. Carries the
    /// driving task — stage or driver — so both families' guards report
    /// the same way.
    #[error("{driving_task} lease lost during a thread-runtime commit")]
    LeaseLost {
        /// The driving task whose claim token no longer matches.
        driving_task: DrivingTaskRef,
    },

    /// A stage task's thread vanished between operations — a consistency
    /// fault, since threads are never deleted while their task is live.
    #[error("no thread found for stage task {task_id}")]
    ThreadMissing {
        /// The orphaned stage task.
        task_id: TaskId,
    },

    /// Serialising a thread-owned structure (record content, suspension)
    /// failed — a format-version bug, not an operational fault.
    #[error("serialising thread content failed: {context}")]
    ContentSerialisation {
        /// What was being serialised.
        context: String,
        /// The underlying serde error.
        #[source]
        source: serde_json::Error,
    },

    /// A loop inference call failed — a provider fault that routes to
    /// the stage-error path, never the conversation. Boxed so the
    /// provider error's width never inflates every runtime result.
    #[error("loop inference call failed: {context}")]
    Inference {
        /// What the loop was doing.
        context: String,
        /// The underlying provider error.
        #[source]
        source: Box<tribal_inference::InferenceError>,
    },

    /// A tool's system half failed — routed to the stage-error path,
    /// never the conversation; the recoverable half returns in-band by
    /// construction and can never reach this variant.
    #[error("tool execution failed: {context}")]
    ToolExecution {
        /// The tool's operator-facing context.
        context: String,
    },

    /// The committed log violated a structural expectation of the
    /// projection — a consistency fault, not an operational one.
    #[error("thread log projection failed: {context}")]
    LogProjection {
        /// What the projection found.
        context: String,
    },
}

impl AgentRuntimeError {
    /// Wraps a database error with the runtime's context vocabulary.
    #[must_use]
    pub fn database(context: impl Into<String>, source: tribal_db::DbError) -> Self {
        Self::Database {
            context: context.into(),
            source,
        }
    }
}
