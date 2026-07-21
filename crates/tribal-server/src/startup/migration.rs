//! First-run guard and migration runner with advisory-lock coordination.

use sqlx::{PgConnection, PgPool};
use tribal_common::random_duration_in_range;
use tribal_db::{MigrationHeadStatus, MigrationRepository, PgMigrationRepository, advisory_locks};

use super::{
    POOL_NAME_MCP,
    constants::{MIGRATION_MAX_ATTEMPTS, MIGRATION_RETRY_SLEEP_MAX, MIGRATION_RETRY_SLEEP_MIN},
};
use crate::error::AppError;

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Checks whether the database has been initialised through the manager.
///
/// Returns `Ok(())` if the `_sqlx_migrations` table exists, or
/// `Err(AppError::FirstRunRequired)` otherwise.
pub(crate) async fn check_first_run(pool: &PgPool) -> Result<(), AppError> {
    let repo = PgMigrationRepository;
    let mut conn = pool
        .acquire()
        .await
        .map_err(|e| AppError::pool_acquire(POOL_NAME_MCP, "first-run check", e))?;

    let exists = repo
        .has_migrations_table(&mut conn)
        .await
        .map_err(|source| AppError::Database { source })?;

    if exists {
        Ok(())
    } else {
        Err(AppError::FirstRunRequired)
    }
}

/// Database-observed result of a migration run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MigrationRunOutcome {
    /// At least one migration remained when this call held the arbiter lock.
    Applied,
    /// The database was already at the compiled head under the lock.
    AlreadyCurrent,
}

/// Runs pending migrations under a `Postgres` advisory lock.
///
/// Retries lock acquisition up to [`MIGRATION_MAX_ATTEMPTS`] times with
/// jittered sleep between attempts. If the lock cannot be acquired after
/// all attempts, returns `AppError::MigrationLockFailed` (exit code 75).
pub(crate) async fn run_migrations(pool: &PgPool) -> Result<MigrationRunOutcome, AppError> {
    let repo = PgMigrationRepository;
    let expected_head = tribal_db::MIGRATOR
        .iter()
        .last()
        .ok_or(AppError::EmptyMigrationCatalogue)?
        .version;

    for attempt in 1..=MIGRATION_MAX_ATTEMPTS {
        let mut conn = pool
            .acquire()
            .await
            .map_err(|e| AppError::pool_acquire(POOL_NAME_MCP, "migration", e))?;

        let acquired = repo
            .try_advisory_lock(&mut conn, advisory_locks::MIGRATION)
            .await
            .map_err(|source| AppError::Database { source })?;

        if acquired {
            // Every fallible step below must release the advisory lock
            // before propagating: the session returns to the pool on drop
            // and would otherwise hold the lock indefinitely.
            let observed = match repo.current_head_matches(&mut conn, expected_head).await {
                Ok(observed) => observed,
                Err(source) => {
                    release_migration_lock(&repo, &mut conn).await;
                    return Err(AppError::Database { source });
                }
            };
            let outcome = if matches!(observed, MigrationHeadStatus::Matches) {
                MigrationRunOutcome::AlreadyCurrent
            } else {
                MigrationRunOutcome::Applied
            };
            // Detach the connection from the pool so the migrator can
            // acquire a pool slot, while we retain the session that
            // holds the advisory lock.
            let mut lock_conn = conn.detach();

            let result = tribal_db::MIGRATOR.run(pool).await;

            release_migration_lock(&repo, &mut lock_conn).await;

            result.map_err(|source| AppError::MigrationFailed { source })?;
            return Ok(outcome);
        }

        if attempt < MIGRATION_MAX_ATTEMPTS {
            let sleep =
                random_duration_in_range(MIGRATION_RETRY_SLEEP_MIN, MIGRATION_RETRY_SLEEP_MAX);
            tracing::warn!(
                attempt,
                max_attempts = MIGRATION_MAX_ATTEMPTS,
                retry_ms = sleep.as_millis(),
                "could not acquire migration lock, retrying",
            );
            tokio::time::sleep(sleep).await;
        }
    }

    eprintln!("tribal: database migration in progress by another instance — retry in ~30s");
    Err(AppError::MigrationLockFailed {
        attempts: MIGRATION_MAX_ATTEMPTS,
    })
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Releases the migration advisory lock, downgrading failures to warnings:
/// the caller's own result must not be masked by release trouble.
async fn release_migration_lock(repo: &PgMigrationRepository, conn: &mut PgConnection) {
    match repo
        .release_advisory_lock(conn, advisory_locks::MIGRATION)
        .await
    {
        Ok(true) => {}
        Ok(false) => {
            tracing::warn!("advisory lock was not held when release was attempted");
        }
        Err(e) => {
            tracing::warn!(%e, "failed to release migration advisory lock");
        }
    }
}
