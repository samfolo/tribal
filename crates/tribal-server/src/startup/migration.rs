//! First-run guard and migration runner with advisory-lock coordination.

use sqlx::{PgConnection, PgPool};
use tribal_common::random_duration_in_range;
use tribal_db::{
    AdvisoryLockRepository, DbError, MigrationHeadStatus, MigrationRepository,
    PgAdvisoryLockRepository, PgMigrationRepository, advisory_locks,
};

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
    /// A storage transition holds this database; no schema work began.
    GraphTransitionInProgress,
}

/// Runs pending migrations under a `Postgres` advisory lock, admitted only
/// while no storage transition holds this database — a refused admission
/// returns [`MigrationRunOutcome::GraphTransitionInProgress`] before any
/// schema work.
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

        let acquired = PgAdvisoryLockRepository
            .try_acquire_exclusive(&mut conn, advisory_locks::MIGRATION)
            .await
            .map_err(|source| AppError::Database { source })?;

        if acquired {
            // Every fallible step below must release the advisory locks
            // before propagating: the session returns to the pool on drop
            // and would otherwise hold them indefinitely.
            match acquire_transition_admission(&mut conn).await {
                Ok(true) => {}
                Ok(false) => {
                    release_migration_lock(&mut conn).await;
                    return Ok(MigrationRunOutcome::GraphTransitionInProgress);
                }
                Err(source) => {
                    release_migration_lock(&mut conn).await;
                    return Err(AppError::Database { source });
                }
            }
            let observed = match repo.current_head_matches(&mut conn, expected_head).await {
                Ok(observed) => observed,
                Err(source) => {
                    release_transition_admission(&mut conn).await;
                    release_migration_lock(&mut conn).await;
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

            release_transition_admission(&mut lock_conn).await;
            release_migration_lock(&mut lock_conn).await;

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

/// Try-acquires the shared form of both storage-transition locks, so a switch
/// in flight on this database refuses schema work before it begins. A partial
/// acquisition is released before reporting refusal.
async fn acquire_transition_admission(conn: &mut PgConnection) -> Result<bool, DbError> {
    if !PgAdvisoryLockRepository
        .try_acquire_shared(conn, advisory_locks::GRAPH_TRANSITION)
        .await?
    {
        return Ok(false);
    }
    match PgAdvisoryLockRepository
        .try_acquire_shared(conn, advisory_locks::TARGET_MIGRATION_RESERVATION)
        .await
    {
        Ok(true) => Ok(true),
        // Both the refusal and the failure leave the session holding
        // nothing: a pooled connection must never be returned with a lock.
        Ok(false) => {
            release_shared_lock(conn, advisory_locks::GRAPH_TRANSITION).await;
            Ok(false)
        }
        Err(source) => {
            release_shared_lock(conn, advisory_locks::GRAPH_TRANSITION).await;
            Err(source)
        }
    }
}

/// Releases both shared admission locks after the migration's terminal result.
async fn release_transition_admission(conn: &mut PgConnection) {
    release_shared_lock(conn, advisory_locks::TARGET_MIGRATION_RESERVATION).await;
    release_shared_lock(conn, advisory_locks::GRAPH_TRANSITION).await;
}

/// Releases one shared lock, downgrading failures to warnings.
async fn release_shared_lock(conn: &mut PgConnection, lock_id: i64) {
    match PgAdvisoryLockRepository.release_shared(conn, lock_id).await {
        Ok(true) => {}
        Ok(false) => {
            tracing::warn!(
                lock_id,
                "shared advisory lock was not held when release was attempted"
            );
        }
        Err(e) => {
            tracing::warn!(%e, lock_id, "failed to release shared advisory lock");
        }
    }
}

/// Releases the migration advisory lock, downgrading failures to warnings:
/// the caller's own result must not be masked by release trouble.
async fn release_migration_lock(conn: &mut PgConnection) {
    match PgAdvisoryLockRepository
        .release_exclusive(conn, advisory_locks::MIGRATION)
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

#[cfg(test)]
mod tests {
    use tribal_test_utils::TestDb;

    use super::*;

    #[tokio::test]
    async fn test_run_migrations_refused_while_a_transition_holds_either_lock() {
        let ctx = TestDb::new_unmigrated().await;
        let mut holder = ctx.raw_connection().await.expect("holder session");
        assert!(
            PgAdvisoryLockRepository
                .try_acquire_exclusive(&mut holder, advisory_locks::GRAPH_TRANSITION)
                .await
                .expect("hold the source transition lock"),
        );

        let outcome = run_migrations(ctx.pool())
            .await
            .expect("refused, not failed");
        assert_eq!(outcome, MigrationRunOutcome::GraphTransitionInProgress);
        let mut probe = ctx.raw_connection().await.expect("probe session");
        assert!(
            !PgMigrationRepository
                .has_migrations_table(&mut probe)
                .await
                .expect("probe for the migrations table"),
            "no schema work begins under a refused admission",
        );

        assert!(
            PgAdvisoryLockRepository
                .release_exclusive(&mut holder, advisory_locks::GRAPH_TRANSITION)
                .await
                .expect("release the source transition lock"),
        );
        assert!(
            PgAdvisoryLockRepository
                .try_acquire_exclusive(&mut holder, advisory_locks::TARGET_MIGRATION_RESERVATION)
                .await
                .expect("hold the candidate reservation"),
        );
        let outcome = run_migrations(ctx.pool())
            .await
            .expect("refused on the reservation");
        assert_eq!(outcome, MigrationRunOutcome::GraphTransitionInProgress);
        // The partially acquired shared source lock must have been released.
        let mut rival = ctx.raw_connection().await.expect("rival session");
        assert!(
            PgAdvisoryLockRepository
                .try_acquire_exclusive(&mut rival, advisory_locks::GRAPH_TRANSITION)
                .await
                .expect("probe the source lock"),
            "a refused admission releases its partial acquisition",
        );
        assert!(
            PgAdvisoryLockRepository
                .release_exclusive(&mut rival, advisory_locks::GRAPH_TRANSITION)
                .await
                .expect("release the rival probe"),
        );

        assert!(
            PgAdvisoryLockRepository
                .release_exclusive(&mut holder, advisory_locks::TARGET_MIGRATION_RESERVATION)
                .await
                .expect("release the candidate reservation"),
        );
        let outcome = run_migrations(ctx.pool())
            .await
            .expect("migrate once admission is clear");
        assert_eq!(outcome, MigrationRunOutcome::Applied);
    }
}
