//! Migration infrastructure repository: first-run detection, head-version
//! reconciliation, and advisory lock coordination.

use std::cmp::Ordering;

use async_trait::async_trait;
use sqlx::PgConnection;

use crate::error::DbError;

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/// Result of comparing the binary's compile-time head migration against
/// what the database has recorded in `_sqlx_migrations`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MigrationHeadStatus {
    /// The recorded head matches the expected head — no action needed.
    Matches,
    /// The database is older than the binary expects.  The binary
    /// includes migrations the database has not yet applied.  An empty
    /// `_sqlx_migrations` table folds here with `found: 0` — the
    /// remediation (`tribal database initialise`) is the same as for any other
    /// Behind, and a separate "empty" arm would add no actionable
    /// information.
    Behind { expected: i64, found: i64 },
    /// The database is newer than the binary expects.  Another binary
    /// version has applied migrations this binary does not know about.
    Ahead { expected: i64, found: i64 },
    /// The `_sqlx_migrations` table does not exist — no migrations have
    /// ever been applied against this database.
    MissingTable,
}

// ---------------------------------------------------------------------------
// Trait
// ---------------------------------------------------------------------------

/// Data access operations for migration infrastructure.
///
/// All methods take `&mut PgConnection` as an explicit executor,
/// keeping the repository pool-agnostic.
#[async_trait]
pub trait MigrationRepository {
    /// Returns `true` if the `_sqlx_migrations` table exists.
    async fn has_migrations_table(&self, conn: &mut PgConnection) -> Result<bool, DbError>;

    /// Compares the recorded head version in `_sqlx_migrations` against
    /// `expected` (the binary's compile-time head from
    /// `tribal_db::MIGRATOR.iter().last()`).
    ///
    /// Returns [`MigrationHeadStatus::MissingTable`] when no migrations
    /// table exists; otherwise reports `Matches`, `Behind`, or `Ahead`.
    async fn current_head_matches(
        &self,
        conn: &mut PgConnection,
        expected: i64,
    ) -> Result<MigrationHeadStatus, DbError>;

    /// Attempts to acquire the Postgres advisory lock identified by
    /// `lock_id`.  Returns `true` if the lock was acquired, `false` if
    /// it is already held by another session.
    async fn try_advisory_lock(
        &self,
        conn: &mut PgConnection,
        lock_id: i64,
    ) -> Result<bool, DbError>;

    /// Releases the Postgres advisory lock identified by `lock_id`.
    /// Returns `true` if the lock was held and released, `false` if it
    /// was not held.
    async fn release_advisory_lock(
        &self,
        conn: &mut PgConnection,
        lock_id: i64,
    ) -> Result<bool, DbError>;
}

// ---------------------------------------------------------------------------
// Postgres implementation
// ---------------------------------------------------------------------------

/// Postgres implementation of [`MigrationRepository`].
pub struct PgMigrationRepository;

#[async_trait]
impl MigrationRepository for PgMigrationRepository {
    async fn has_migrations_table(&self, conn: &mut PgConnection) -> Result<bool, DbError> {
        let exists: bool = sqlx::query_scalar("SELECT to_regclass('_sqlx_migrations') IS NOT NULL")
            .fetch_one(&mut *conn)
            .await
            .map_err(|source| DbError::QueryFailed {
                context: "check for _sqlx_migrations table".into(),
                source,
            })?;

        Ok(exists)
    }

    async fn current_head_matches(
        &self,
        conn: &mut PgConnection,
        expected: i64,
    ) -> Result<MigrationHeadStatus, DbError> {
        if !self.has_migrations_table(&mut *conn).await? {
            return Ok(MigrationHeadStatus::MissingTable);
        }

        let found: Option<i64> = sqlx::query_scalar(
            "SELECT version FROM _sqlx_migrations ORDER BY version DESC LIMIT 1",
        )
        .fetch_optional(&mut *conn)
        .await
        .map_err(|source| DbError::QueryFailed {
            context: "read latest applied migration version".into(),
            source,
        })?;

        // A migrations table exists but contains no rows.  Treat this as
        // "behind from zero" so the operator is steered to `tribal database initialise`,
        // matching the remediation path for any other Behind variant.
        let found = found.unwrap_or(0);

        Ok(match found.cmp(&expected) {
            Ordering::Equal => MigrationHeadStatus::Matches,
            Ordering::Less => MigrationHeadStatus::Behind { expected, found },
            Ordering::Greater => MigrationHeadStatus::Ahead { expected, found },
        })
    }

    async fn try_advisory_lock(
        &self,
        conn: &mut PgConnection,
        lock_id: i64,
    ) -> Result<bool, DbError> {
        let acquired: bool = sqlx::query_scalar("SELECT pg_try_advisory_lock($1)")
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
        let released: bool = sqlx::query_scalar("SELECT pg_advisory_unlock($1)")
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

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

#[cfg(feature = "test-helpers")]
impl PgMigrationRepository {
    /// Inserts a synthetic row into `_sqlx_migrations` at the given
    /// `version`.  Intended for integration tests that need to simulate
    /// a database whose recorded head is newer than the binary expects
    /// (the [`MigrationHeadStatus::Ahead`] arm).
    ///
    /// # Errors
    ///
    /// Returns [`DbError::QueryFailed`] on database errors.
    pub async fn insert_synthetic_row_for_test(
        &self,
        conn: &mut PgConnection,
        version: i64,
    ) -> Result<(), DbError> {
        sqlx::query(
            "INSERT INTO _sqlx_migrations \
             (version, description, installed_on, execution_time, success, checksum) \
             VALUES ($1, 'synthetic test row', now(), 0, true, '\\x00')",
        )
        .bind(version)
        .execute(&mut *conn)
        .await
        .map_err(|source| DbError::QueryFailed {
            context: "insert synthetic _sqlx_migrations row".into(),
            source,
        })?;

        Ok(())
    }

    /// Drops the `_sqlx_migrations` table.  Intended for integration
    /// tests that need to simulate a fresh database where migrations
    /// have never been applied (the [`MigrationHeadStatus::MissingTable`]
    /// arm).  The caller is expected to run inside a rolled-back
    /// transaction so the drop is reversed at end-of-test.
    ///
    /// # Errors
    ///
    /// Returns [`DbError::QueryFailed`] on database errors.
    pub async fn drop_migrations_table_for_test(
        &self,
        conn: &mut PgConnection,
    ) -> Result<(), DbError> {
        sqlx::query("DROP TABLE _sqlx_migrations")
            .execute(&mut *conn)
            .await
            .map_err(|source| DbError::QueryFailed {
                context: "drop _sqlx_migrations table".into(),
                source,
            })?;

        Ok(())
    }

    /// Deletes every row from `_sqlx_migrations` while leaving the
    /// table in place.  Intended for tests that pin the empty-table
    /// fold into [`MigrationHeadStatus::Behind`] `{ found: 0 }`.
    ///
    /// # Errors
    ///
    /// Returns [`DbError::QueryFailed`] on database errors.
    pub async fn truncate_migrations_table_for_test(
        &self,
        conn: &mut PgConnection,
    ) -> Result<(), DbError> {
        sqlx::query("DELETE FROM _sqlx_migrations")
            .execute(&mut *conn)
            .await
            .map_err(|source| DbError::QueryFailed {
                context: "truncate _sqlx_migrations table".into(),
                source,
            })?;

        Ok(())
    }
}
