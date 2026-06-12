//! Error types for the worker loop and pipeline stages.

use tribal_domain::TaskErrorKind;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

pub(crate) const SEMAPHORE_CLOSED: &str = "semaphore closed unexpectedly";
pub(crate) const STAGE_PRE_DISPATCH: &str = "pre-dispatch";
pub(crate) const STAGE_EXTRACTION: &str = "extraction";
pub(crate) const STAGE_TRIAGE: &str = "triage";
pub(crate) const STAGE_RELATION: &str = "relation";

// ---------------------------------------------------------------------------
// WorkerError
// ---------------------------------------------------------------------------

/// Errors produced by the worker loop itself (not by individual stages).
#[derive(Debug, thiserror::Error)]
pub enum WorkerError {
    /// The database pool could not be acquired.
    #[error("worker pool acquisition failed: {source}")]
    PoolExhausted {
        /// The underlying sqlx error.
        #[source]
        source: sqlx::Error,
    },

    /// The claim query failed.
    #[error("claim query failed: {context}")]
    ClaimFailed {
        /// Human-readable description of what the claim was trying to do.
        context: String,
        /// The underlying database error.
        #[source]
        source: tribal_db::DbError,
    },

    /// The worker was cancelled via its cancellation token.
    #[error("worker cancelled")]
    Cancelled,

    /// A reclaim operation (startup or periodic sweep) failed.
    #[error("reclaim failed: {context}")]
    ReclaimFailed {
        /// Human-readable description of the reclaim operation.
        context: String,
        /// The underlying database error.
        #[source]
        source: tribal_db::DbError,
    },
}

// ---------------------------------------------------------------------------
// StageError
// ---------------------------------------------------------------------------

/// Errors produced by individual pipeline stages.
///
/// Each variant maps to a [`TaskErrorKind`] for structured persistence
/// via [`StageError::to_error_kind`].
#[derive(Debug, thiserror::Error)]
pub(crate) enum StageError {
    /// The inference provider returned an error.
    #[error("provider error during {context}")]
    Provider {
        /// Human-readable description of the stage operation.
        context: String,
        /// The underlying inference error.
        #[source]
        source: tribal_inference::InferenceError,
    },

    /// Timed out waiting for a provider semaphore permit.
    #[error("semaphore timeout for provider {provider_key}")]
    SemaphoreTimeout {
        /// The provider key that timed out.
        provider_key: String,
    },

    /// The provider response could not be parsed into the expected shape.
    #[error("parse error during {context}")]
    Parse {
        /// Human-readable description of what was being parsed.
        context: String,
        /// The raw response text, if available.
        raw_response: Option<String>,
    },

    /// The task's claim token no longer matches (another worker reclaimed).
    #[error("ownership lost")]
    OwnershipLost,

    /// The task exceeded its timeout.
    #[error("task timed out after {timeout_millis}ms")]
    Timeout {
        /// The timeout that was exceeded, in milliseconds.
        timeout_millis: u64,
    },

    /// A prompt template could not be rendered.
    #[error("template render failed: {context}")]
    TemplateRender {
        /// Human-readable description of what was being rendered.
        context: String,
        /// The underlying Tera error.
        #[source]
        source: tera::Error,
    },

    /// A thread-runtime operation failed for a reason that is neither a
    /// lost lease nor a database fault (content serialisation, a missing
    /// thread): an internal consistency error.
    #[error("thread runtime failure during {context}")]
    Runtime {
        /// Human-readable description of the stage operation.
        context: String,
        /// The underlying runtime error.
        #[source]
        source: tribal_agent_runtime::AgentRuntimeError,
    },

    /// An execution budget exhausted in a way no retry can change: the
    /// turn cap, the execution deadline, or the bounded budget
    /// re-checks running dry. Terminal — the thread fails with its task.
    #[error("execution budget exhausted in {stage}: {context}")]
    BudgetExhausted {
        /// The stage whose thread exhausted its budget.
        stage: String,
        /// Which budget, with its numbers.
        context: String,
    },

    /// A database operation failed during stage execution.
    #[error("database error in {stage}: {context}")]
    Database {
        /// The pipeline stage where the error occurred.
        stage: String,
        /// Human-readable description of the operation.
        context: String,
        /// The underlying database error.
        #[source]
        source: tribal_db::DbError,
    },
}

impl StageError {
    /// Maps this error to the corresponding [`TaskErrorKind`] for
    /// persistence in the task's `error_kind` column.
    pub(crate) fn to_error_kind(&self) -> TaskErrorKind {
        match self {
            Self::Provider { .. } => TaskErrorKind::ProviderError,
            Self::SemaphoreTimeout { .. } => TaskErrorKind::SemaphoreTimeout,
            Self::Parse { .. } => TaskErrorKind::ParseError,
            Self::OwnershipLost => TaskErrorKind::OwnershipLost,
            Self::Timeout { .. } => TaskErrorKind::Timeout,
            Self::TemplateRender { .. } | Self::Runtime { .. } => TaskErrorKind::InternalError,
            Self::BudgetExhausted { .. } => TaskErrorKind::BudgetExhausted,
            Self::Database { .. } => TaskErrorKind::DatabaseError,
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_stage_error_to_error_kind_mapping() {
        let cases: Vec<(StageError, TaskErrorKind)> = vec![
            (
                StageError::Provider {
                    context: "extraction".into(),
                    source: tribal_inference::InferenceError::provider_unavailable("test", "down"),
                },
                TaskErrorKind::ProviderError,
            ),
            (
                StageError::SemaphoreTimeout {
                    provider_key: "ollama".into(),
                },
                TaskErrorKind::SemaphoreTimeout,
            ),
            (
                StageError::Parse {
                    context: "triage".into(),
                    raw_response: None,
                },
                TaskErrorKind::ParseError,
            ),
            (StageError::OwnershipLost, TaskErrorKind::OwnershipLost),
            (
                StageError::Timeout {
                    timeout_millis: 300_000,
                },
                TaskErrorKind::Timeout,
            ),
            (
                StageError::TemplateRender {
                    context: "rendering extraction prompt".into(),
                    source: tera::Error::msg("unknown variable"),
                },
                TaskErrorKind::InternalError,
            ),
            (
                StageError::Database {
                    stage: "extraction".into(),
                    context: "inserting result".into(),
                    source: tribal_db::DbError::NotFound {
                        entity: "job",
                        id: "test".into(),
                    },
                },
                TaskErrorKind::DatabaseError,
            ),
        ];

        for (error, expected_kind) in cases {
            assert_eq!(error.to_error_kind(), expected_kind, "mismatch for {error}");
        }
    }

    #[test]
    fn test_worker_error_display() {
        let cancelled = WorkerError::Cancelled;
        assert_eq!(cancelled.to_string(), "worker cancelled");

        let pool = WorkerError::PoolExhausted {
            source: sqlx::Error::PoolTimedOut,
        };
        assert!(
            pool.to_string()
                .starts_with("worker pool acquisition failed:"),
            "unexpected display: {pool}",
        );

        let claim = WorkerError::ClaimFailed {
            context: "claiming tasks".into(),
            source: tribal_db::DbError::NotFound {
                entity: "task",
                id: "test".into(),
            },
        };
        assert_eq!(claim.to_string(), "claim query failed: claiming tasks");

        let reclaim = WorkerError::ReclaimFailed {
            context: "startup reclaim".into(),
            source: tribal_db::DbError::NotFound {
                entity: "task",
                id: "test".into(),
            },
        };
        assert_eq!(reclaim.to_string(), "reclaim failed: startup reclaim");
    }
}
