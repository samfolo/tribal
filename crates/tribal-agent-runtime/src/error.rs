//! Crate-level error type for the agent runtime.

use thiserror::Error;
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

    /// A status compare-and-set missed: the thread moved under the actor.
    /// Bounded retry loops treat this as their retry signal; a terminal
    /// status ends every loop.
    #[error("thread {thread_id} status CAS missed: expected {expected}, the row moved")]
    StatusCasMissed {
        /// The thread whose status moved.
        thread_id: AgentThreadId,
        /// The status the actor expected to move from.
        expected: AgentThreadStatus,
    },

    /// The claim-guarded task write affected zero rows: the lease was
    /// lost and the whole commit must roll back.
    #[error("task {task_id} lease lost during a thread-runtime commit")]
    LeaseLost {
        /// The task whose claim token no longer matches.
        task_id: TaskId,
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
