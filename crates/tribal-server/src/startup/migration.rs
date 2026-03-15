//! First-run guard and migration runner with advisory-lock coordination.

use rand::Rng;
use sqlx::PgPool;
use tokio::time::Duration;

use crate::error::AppError;

use super::constants::{
    ADVISORY_LOCK_ID, MIGRATION_LOCK_TIMEOUT, MIGRATION_MAX_ATTEMPTS,
    MIGRATION_RETRY_SLEEP_MAX_SECS, MIGRATION_RETRY_SLEEP_MIN_SECS,
};

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Checks whether the database has been initialised with `tribal setup`.
///
/// Returns `Ok(())` if the `_sqlx_migrations` table exists, or
/// `Err(AppError::FirstRunRequired)` otherwise.
pub(crate) async fn check_first_run(pool: &PgPool) -> Result<(), AppError> {
    let exists: bool = sqlx::query_scalar(
        "SELECT EXISTS (
             SELECT 1 FROM information_schema.tables
             WHERE table_name = '_sqlx_migrations'
         )",
    )
    .fetch_one(pool)
    .await
    .map_err(|e| AppError::Database {
        source: tribal_db::DbError::QueryFailed {
            context: "check for _sqlx_migrations table".into(),
            source: e,
        },
    })?;

    if exists {
        Ok(())
    } else {
        Err(AppError::FirstRunRequired)
    }
}

/// Runs pending migrations under a Postgres advisory lock.
///
/// Retries lock acquisition up to [`MIGRATION_MAX_ATTEMPTS`] times with
/// jittered sleep between attempts. If the lock cannot be acquired after
/// all attempts, returns `AppError::MigrationLockFailed` (exit code 75).
pub(crate) async fn run_migrations(pool: &PgPool) -> Result<(), AppError> {
    for attempt in 1..=MIGRATION_MAX_ATTEMPTS {
        if try_acquire_lock(pool).await? {
            let result = tribal_db::MIGRATOR.run(pool).await;

            // Always release the lock, even on migration failure.
            release_lock(pool).await;

            return result.map_err(|source| AppError::MigrationFailed { source });
        }

        if attempt < MIGRATION_MAX_ATTEMPTS {
            let jitter = rand::rng().random_range(
                MIGRATION_RETRY_SLEEP_MIN_SECS..=MIGRATION_RETRY_SLEEP_MAX_SECS,
            );
            tracing::warn!(
                attempt,
                max_attempts = MIGRATION_MAX_ATTEMPTS,
                retry_secs = jitter,
                "could not acquire migration lock, retrying",
            );
            tokio::time::sleep(Duration::from_secs(jitter)).await;
        }
    }

    eprintln!(
        "error: could not acquire migration lock after {} attempts",
        MIGRATION_MAX_ATTEMPTS,
    );
    Err(AppError::MigrationLockFailed {
        attempts: MIGRATION_MAX_ATTEMPTS,
    })
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Attempts to acquire the advisory lock with a statement-level timeout.
async fn try_acquire_lock(pool: &PgPool) -> Result<bool, AppError> {
    let timeout_ms = i32::try_from(MIGRATION_LOCK_TIMEOUT.as_millis()).unwrap_or(i32::MAX);

    // Set a statement timeout so the lock attempt does not block indefinitely.
    sqlx::query(&format!("SET LOCAL statement_timeout = {timeout_ms}"))
        .execute(pool)
        .await
        .map_err(|e| AppError::Database {
            source: tribal_db::DbError::QueryFailed {
                context: "set migration lock timeout".into(),
                source: e,
            },
        })?;

    let acquired: bool =
        sqlx::query_scalar("SELECT pg_try_advisory_lock($1)")
            .bind(ADVISORY_LOCK_ID)
            .fetch_one(pool)
            .await
            .map_err(|e| AppError::Database {
                source: tribal_db::DbError::QueryFailed {
                    context: "acquire migration advisory lock".into(),
                    source: e,
                },
            })?;

    Ok(acquired)
}

/// Releases the advisory lock. Logs a warning on failure but does not
/// propagate — the lock is session-scoped and will be released when the
/// connection is returned to the pool.
async fn release_lock(pool: &PgPool) {
    let result: Result<bool, _> =
        sqlx::query_scalar("SELECT pg_advisory_unlock($1)")
            .bind(ADVISORY_LOCK_ID)
            .fetch_one(pool)
            .await;

    match result {
        Ok(true) => {}
        Ok(false) => {
            tracing::warn!("advisory lock was not held when release was attempted");
        }
        Err(e) => {
            tracing::warn!(%e, "failed to release migration advisory lock");
        }
    }
}
