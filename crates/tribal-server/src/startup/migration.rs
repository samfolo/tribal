//! First-run guard and migration runner with advisory-lock coordination.

use sqlx::PgPool;
use tribal_common::random_duration_in_range;
use tribal_db::{MigrationRepository, PgMigrationRepository};

use super::constants::{
    ADVISORY_LOCK_ID, MIGRATION_MAX_ATTEMPTS, MIGRATION_RETRY_SLEEP_MAX, MIGRATION_RETRY_SLEEP_MIN,
};
use crate::error::AppError;

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Checks whether the database has been initialised with `tribal setup`.
///
/// Returns `Ok(())` if the `_sqlx_migrations` table exists, or
/// `Err(AppError::FirstRunRequired)` otherwise.
pub(crate) async fn check_first_run(pool: &PgPool) -> Result<(), AppError> {
    let repo = PgMigrationRepository;
    let mut conn = pool
        .acquire()
        .await
        .map_err(|e| AppError::pool_acquire("first-run check", e))?;

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

/// Runs pending migrations under a `Postgres` advisory lock.
///
/// Retries lock acquisition up to [`MIGRATION_MAX_ATTEMPTS`] times with
/// jittered sleep between attempts. If the lock cannot be acquired after
/// all attempts, returns `AppError::MigrationLockFailed` (exit code 75).
pub(crate) async fn run_migrations(pool: &PgPool) -> Result<(), AppError> {
    let repo = PgMigrationRepository;

    for attempt in 1..=MIGRATION_MAX_ATTEMPTS {
        let mut conn = pool
            .acquire()
            .await
            .map_err(|e| AppError::pool_acquire("migration", e))?;

        let acquired = repo
            .try_advisory_lock(&mut conn, ADVISORY_LOCK_ID)
            .await
            .map_err(|source| AppError::Database { source })?;

        if acquired {
            // Detach the connection from the pool so the migrator can
            // acquire a pool slot, while we retain the session that
            // holds the advisory lock.
            let mut lock_conn = conn.detach();

            let result = tribal_db::MIGRATOR.run(pool).await;

            // Release the lock on the original session, then drop.
            match repo
                .release_advisory_lock(&mut lock_conn, ADVISORY_LOCK_ID)
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

            return result.map_err(|source| AppError::MigrationFailed { source });
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
