//! Crate-level error type for `tribal-db`.
//!
//! [`DbError`] is the single error enum for the database layer.  All
//! variants use named fields; wrapped errors carry `#[source]` for error
//! chain propagation.

use thiserror::Error;

/// Errors produced by the database layer.
///
/// All variants use named fields.  `#[source]` preserves the error chain
/// for tracing and debugging.  [`QueryFailed`](DbError::QueryFailed) is
/// constructed explicitly by repository code with a meaningful context
/// string; it intentionally does **not** implement `From<sqlx::Error>`.
#[derive(Debug, Error)]
pub enum DbError {
    /// A database query failed.
    ///
    /// Constructed explicitly by repository code with a meaningful context
    /// string alongside the underlying sqlx error.
    #[error("query failed: {context}")]
    QueryFailed {
        /// Human-readable description of what the query was trying to do.
        context: String,
        /// The underlying sqlx error.
        #[source]
        source: sqlx::Error,
    },

    /// A unique constraint was violated during an insert or update.
    #[error("unique constraint violated on {table}.{constraint}: {detail}")]
    UniqueViolation {
        /// The table on which the constraint exists.
        table: String,
        /// The constraint name (e.g. `"knowledge_items_pkey"`).
        constraint: String,
        /// Additional detail from the database error message.
        detail: String,
    },

    /// An expected row was not found.
    #[error("row not found: {entity} with id {id}")]
    NotFound {
        /// The entity type being looked up (e.g. `"job"`, `"knowledge_item"`).
        entity: &'static str,
        /// The string representation of the identifier that was not found.
        id: String,
    },

    /// A pagination cursor could not be decoded.
    ///
    /// Returned when a cursor is malformed, truncated, or otherwise
    /// unparseable.  This is a client input error, distinct from a
    /// database failure.
    #[error("invalid cursor: {detail}")]
    InvalidCursor {
        /// Human-readable description of why the cursor is invalid.
        detail: String,
    },

    /// A connection pool has no available connections.
    ///
    /// Maps to the `resource_exhausted` MCP error code at the boundary.
    #[error("connection pool exhausted: {pool_name}")]
    PoolExhausted {
        /// Which pool ran out of connections (`"mcp"` or `"worker"`).
        pool_name: &'static str,
    },

    /// A database migration failed.
    #[error("migration failed")]
    Migration {
        /// The underlying migration error.
        #[from]
        #[source]
        source: sqlx::migrate::MigrateError,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_display_query_failed() {
        let err = DbError::QueryFailed {
            context: "fetching job by id".to_owned(),
            source: sqlx::Error::RowNotFound,
        };
        assert_eq!(err.to_string(), "query failed: fetching job by id");
    }

    #[test]
    fn test_display_unique_violation() {
        let err = DbError::UniqueViolation {
            table: "knowledge_items".to_owned(),
            constraint: "knowledge_items_pkey".to_owned(),
            detail: "key (id)=(ki_abc) already exists".to_owned(),
        };
        assert_eq!(
            err.to_string(),
            "unique constraint violated on knowledge_items.knowledge_items_pkey: \
             key (id)=(ki_abc) already exists"
        );
    }

    #[test]
    fn test_display_not_found() {
        let err = DbError::NotFound {
            entity: "job",
            id: "job_550e8400-e29b-41d4-a716-446655440000".to_owned(),
        };
        assert_eq!(
            err.to_string(),
            "row not found: job with id job_550e8400-e29b-41d4-a716-446655440000"
        );
    }

    #[test]
    fn test_display_invalid_cursor() {
        let err = DbError::InvalidCursor {
            detail: "expected 20 bytes, got 4".to_owned(),
        };
        assert_eq!(err.to_string(), "invalid cursor: expected 20 bytes, got 4");
    }

    #[test]
    fn test_display_pool_exhausted() {
        let err = DbError::PoolExhausted { pool_name: "mcp" };
        assert_eq!(err.to_string(), "connection pool exhausted: mcp");
    }

    #[test]
    fn test_display_migration() {
        let err = DbError::Migration {
            source: sqlx::migrate::MigrateError::VersionMissing(1),
        };
        assert_eq!(err.to_string(), "migration failed");
    }

    #[test]
    fn test_from_migrate_error() {
        let migrate_err = sqlx::migrate::MigrateError::VersionMissing(42);
        let err = DbError::from(migrate_err);
        assert!(matches!(err, DbError::Migration { .. }));
    }
}
