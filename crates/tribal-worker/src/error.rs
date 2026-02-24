//! Error types for the worker loop and pipeline stages.

use tribal_domain::TaskErrorKind;

// ---------------------------------------------------------------------------
// WorkerError
// ---------------------------------------------------------------------------

/// Errors produced by the worker loop itself (not by individual stages).
#[derive(Debug, thiserror::Error)]
pub enum WorkerError {
    /// The connection pool has no available connections.
    #[error("connection pool exhausted")]
    PoolExhausted {
        /// The underlying sqlx error.
        #[source]
        source: sqlx::Error,
    },

    /// A task claim operation failed.
    #[error("claim failed: {context}")]
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
    #[error("task timed out after {timeout_seconds}s")]
    Timeout {
        /// The timeout that was exceeded, in seconds.
        timeout_seconds: u64,
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
                    source: tribal_inference::InferenceError::ProviderUnavailable {
                        provider: "test".into(),
                        reason: "down".into(),
                    },
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
                    timeout_seconds: 300,
                },
                TaskErrorKind::Timeout,
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
        assert_eq!(pool.to_string(), "connection pool exhausted");

        let claim = WorkerError::ClaimFailed {
            context: "claiming tasks".into(),
            source: tribal_db::DbError::NotFound {
                entity: "task",
                id: "test".into(),
            },
        };
        assert_eq!(claim.to_string(), "claim failed: claiming tasks");
    }
}
