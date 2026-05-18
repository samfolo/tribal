//! Shared state threaded through phase-3 check functions.

use sqlx::PgPool;

/// Resources built once for the duration of a `tribal check` run.
///
/// Constructed after the database connection is verified; dropped when
/// the run exits.
pub(in crate::commands::check) struct CheckContext {
    pub pool: PgPool,
}
