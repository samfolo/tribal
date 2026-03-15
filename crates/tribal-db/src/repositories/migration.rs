//! Migration infrastructure repository: first-run detection and advisory
//! lock coordination.

use sqlx::PgConnection;

use crate::error::DbError;

// ---------------------------------------------------------------------------
// Trait
// ---------------------------------------------------------------------------

/// Data access operations for migration infrastructure.
pub trait MigrationRepository {
    /// Returns `true` if the `_sqlx_migrations` table exists.
    fn has_migrations_table(
        &self,
        conn: &mut PgConnection,
    ) -> impl Future<Output = Result<bool, DbError>> + Send;

    /// Attempts to acquire the Postgres advisory lock identified by
    /// `lock_id`.  Returns `true` if the lock was acquired, `false` if
    /// it is already held by another session.
    fn try_advisory_lock(
        &self,
        conn: &mut PgConnection,
        lock_id: i64,
    ) -> impl Future<Output = Result<bool, DbError>> + Send;

    /// Releases the Postgres advisory lock identified by `lock_id`.
    /// Returns `true` if the lock was held and released, `false` if it
    /// was not held.
    fn release_advisory_lock(
        &self,
        conn: &mut PgConnection,
        lock_id: i64,
    ) -> impl Future<Output = Result<bool, DbError>> + Send;
}

// ---------------------------------------------------------------------------
// Postgres implementation
// ---------------------------------------------------------------------------

/// Postgres implementation of [`MigrationRepository`].
pub struct PgMigrationRepository;

impl MigrationRepository for PgMigrationRepository {
    async fn has_migrations_table(
        &self,
        conn: &mut PgConnection,
    ) -> Result<bool, DbError> {
        let exists: bool = sqlx::query_scalar(
            "SELECT EXISTS (
                 SELECT 1 FROM information_schema.tables
                 WHERE table_name = '_sqlx_migrations'
             )",
        )
        .fetch_one(&mut *conn)
        .await
        .map_err(|source| DbError::QueryFailed {
            context: "check for _sqlx_migrations table".into(),
            source,
        })?;

        Ok(exists)
    }

    async fn try_advisory_lock(
        &self,
        conn: &mut PgConnection,
        lock_id: i64,
    ) -> Result<bool, DbError> {
        let acquired: bool =
            sqlx::query_scalar("SELECT pg_try_advisory_lock($1)")
                .bind(lock_id)
                .fetch_one(&mut *conn)
                .await
                .map_err(|source| DbError::QueryFailed {
                    context: "acquire migration advisory lock".into(),
                    source,
                })?;

        Ok(acquired)
    }

    async fn release_advisory_lock(
        &self,
        conn: &mut PgConnection,
        lock_id: i64,
    ) -> Result<bool, DbError> {
        let released: bool =
            sqlx::query_scalar("SELECT pg_advisory_unlock($1)")
                .bind(lock_id)
                .fetch_one(&mut *conn)
                .await
                .map_err(|source| DbError::QueryFailed {
                    context: "release migration advisory lock".into(),
                    source,
                })?;

        Ok(released)
    }
}
