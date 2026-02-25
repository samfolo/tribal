//! Error types for `tribal-test-utils`.
//!
//! Each class of test infrastructure has its own scoped error enum for
//! compile-time exhaustiveness. [`TestDbError`] covers database
//! infrastructure failures: container startup, migration execution, and
//! connection pool creation.

use thiserror::Error;

/// Errors produced by the database test infrastructure.
///
/// All variants use named fields. `#[source]` preserves the error chain
/// for debugging. These errors are intentionally **not** recoverable — a
/// test infrastructure failure should cause the test to abort with a
/// clear diagnostic message.
#[derive(Debug, Error)]
pub enum TestDbError {
    /// The testcontainers container failed to start, or host resolution
    /// failed after startup.
    ///
    /// Common causes: Docker daemon not running, image pull failure,
    /// or port conflict.
    #[error("container startup failed: {context}")]
    ContainerStart {
        /// Human-readable description of what went wrong.
        context: String,
        /// The underlying testcontainers error.
        #[source]
        source: testcontainers::TestcontainersError,
    },

    /// Failed to retrieve the mapped host port from the container.
    #[error("failed to retrieve host port for container port {container_port}")]
    PortMapping {
        /// The container-side port that could not be mapped.
        container_port: u16,
        /// The underlying testcontainers error.
        #[source]
        source: testcontainers::TestcontainersError,
    },

    /// Database migration failed against the test container.
    #[error("migration failed against test database")]
    Migration {
        /// The underlying sqlx migration error.
        #[from]
        #[source]
        source: sqlx::migrate::MigrateError,
    },

    /// Failed to create a connection pool to the test database.
    #[error("failed to create test connection pool: {context}")]
    PoolCreation {
        /// Human-readable description of the failure.
        context: String,
        /// The underlying sqlx error.
        #[source]
        source: sqlx::Error,
    },

    /// Failed to open a raw (non-pooled) connection.
    #[error("failed to open raw connection")]
    ConnectionFailed {
        /// The underlying sqlx error.
        #[source]
        source: sqlx::Error,
    },

    /// Failed to begin a test transaction.
    #[error("failed to begin test transaction")]
    TransactionBegin {
        /// The underlying sqlx error.
        #[source]
        source: sqlx::Error,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_display_migration() {
        let err = TestDbError::Migration {
            source: sqlx::migrate::MigrateError::VersionMissing(1),
        };
        assert_eq!(err.to_string(), "migration failed against test database");
    }

    #[test]
    fn test_display_pool_creation() {
        let err = TestDbError::PoolCreation {
            context: "connecting to 127.0.0.1:54321".to_owned(),
            source: sqlx::Error::RowNotFound,
        };
        assert_eq!(
            err.to_string(),
            "failed to create test connection pool: connecting to 127.0.0.1:54321",
        );
    }

    #[test]
    fn test_display_transaction_begin() {
        let err = TestDbError::TransactionBegin {
            source: sqlx::Error::RowNotFound,
        };
        assert_eq!(err.to_string(), "failed to begin test transaction");
    }

    #[test]
    fn test_from_migrate_error() {
        let migrate_err = sqlx::migrate::MigrateError::VersionMissing(42);
        let err = TestDbError::from(migrate_err);
        assert!(matches!(err, TestDbError::Migration { .. }));
    }
}
