//! Error factory functions for mock repository dispatch.
//!
//! Each factory returns a boxed closure that produces a fresh
//! [`DbError`] on each invocation, following the same pattern as
//! the inference error factories.

use tribal_db::DbError;

use crate::mock::async_dispatch::core::ErrorFactory;

/// Repository-specific error factory type alias.
pub type DbErrorFactory = ErrorFactory<DbError>;

/// Produces a closure returning [`DbError::NotFound`].
pub fn a_not_found(entity: &'static str, id: impl Into<String>) -> DbErrorFactory {
    let id = id.into();
    Box::new(move || DbError::NotFound {
        entity,
        id: id.clone(),
    })
}

/// Produces a closure returning [`DbError::QueryFailed`] with a
/// synthetic `sqlx::Error::RowNotFound` source.
pub fn a_query_failed(context: impl Into<String>) -> DbErrorFactory {
    let context = context.into();
    Box::new(move || DbError::QueryFailed {
        context: context.clone(),
        source: sqlx::Error::RowNotFound,
    })
}

/// Produces a closure returning [`DbError::UniqueViolation`].
pub fn a_unique_violation(
    table: impl Into<String>,
    constraint: impl Into<String>,
    detail: impl Into<String>,
) -> DbErrorFactory {
    let table = table.into();
    let constraint = constraint.into();
    let detail = detail.into();
    Box::new(move || DbError::UniqueViolation {
        table: table.clone(),
        constraint: constraint.clone(),
        detail: detail.clone(),
    })
}

/// Produces a closure returning [`DbError::PoolExhausted`].
pub fn a_pool_exhausted(pool_name: &'static str) -> DbErrorFactory {
    Box::new(move || DbError::PoolExhausted { pool_name })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_not_found_factory() {
        let err = a_not_found("project", "proj_abc123")();
        assert!(matches!(
            err,
            DbError::NotFound { entity: "project", ref id }
            if id == "proj_abc123"
        ));
    }

    #[test]
    fn test_query_failed_factory() {
        let err = a_query_failed("fetching project by id")();
        assert!(matches!(
            err,
            DbError::QueryFailed { ref context, .. }
            if context == "fetching project by id"
        ));
    }

    #[test]
    fn test_unique_violation_factory() {
        let err = a_unique_violation("projects", "projects_git_remote_key", "duplicate")();
        assert!(matches!(
            err,
            DbError::UniqueViolation { ref table, ref constraint, ref detail }
            if table == "projects" && constraint == "projects_git_remote_key" && detail == "duplicate"
        ));
    }

    #[test]
    fn test_pool_exhausted_factory() {
        let err = a_pool_exhausted("mcp")();
        assert!(matches!(err, DbError::PoolExhausted { pool_name: "mcp" }));
    }

    #[test]
    fn test_factory_produces_fresh_error_per_invocation() {
        let factory = a_not_found("job", "job_123");
        let err_a = factory();
        let err_b = factory();
        assert_eq!(format!("{err_a}"), format!("{err_b}"));
    }
}
