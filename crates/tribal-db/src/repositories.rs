//! Repository traits and Postgres implementations for the database layer.
//!
//! Each entity has a trait defining its data access operations and a
//! zero-sized Postgres implementation struct. All methods take
//! `&mut PgConnection` as an explicit executor parameter, keeping
//! repositories pool-agnostic.

mod principal;
mod project;

pub use principal::{NewPrincipal, PgPrincipalRepository, PrincipalRepository};
pub use project::{NewProject, PgProjectRepository, ProjectRepository};

use crate::DbError;

/// Postgres SQLSTATE code for a unique constraint violation.
const PG_UNIQUE_VIOLATION: &str = "23505";

/// Checks whether an sqlx error is a Postgres unique violation (SQLSTATE 23505)
/// and maps it to [`DbError::UniqueViolation`].
///
/// Returns `Some(DbError::UniqueViolation { ... })` if the error matches,
/// or `None` for any other error kind.  Takes `&sqlx::Error` (not owned) so
/// the caller retains ownership for the `QueryFailed` fallback path.
fn try_into_unique_violation(err: &sqlx::Error) -> Option<DbError> {
    let sqlx::Error::Database(db_err) = err else {
        return None;
    };
    if db_err.code().as_deref() != Some(PG_UNIQUE_VIOLATION) {
        return None;
    }
    let table = db_err.table().unwrap_or("unknown").to_owned();
    let constraint = db_err.constraint().unwrap_or("unknown").to_owned();
    // detail() is Postgres-specific — downcast for full message.
    let detail = db_err
        .try_downcast_ref::<sqlx::postgres::PgDatabaseError>()
        .and_then(|pg| pg.detail())
        .unwrap_or_else(|| db_err.message())
        .to_owned();
    Some(DbError::UniqueViolation {
        table,
        constraint,
        detail,
    })
}
