//! Reindex run repository: the operator-facing lifecycle of a model migration.
//!
//! Runs are append-only history (prune never deletes them), so they double as
//! the progress and audit surface a monitoring client polls. Raw
//! `sqlx::query()` is used because the row maps onto domain enums parsed as
//! `TEXT`.

use async_trait::async_trait;
use sqlx::{PgConnection, Row};
use tribal_domain::{EmbeddingProfileId, PrincipalId, ReindexRun, ReindexRunId, ReindexRunState};
use typed_builder::TypedBuilder;

use super::common::columns::Columns;
use crate::DbError;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const COLUMNS: Columns = Columns(&[
    "id",
    "target_profile_id",
    "epoch",
    "state",
    "initiated_by_principal_id",
    "items_enumerated",
    "items_embedded",
    "tags_enumerated",
    "tags_embedded",
    "items_quarantined",
    "tags_quarantined",
    "error_message",
    "trace_context",
    "started_at",
    "completed_at",
]);

const UNKNOWN_STATE_IN_DB: &str = "unrecognised reindex run state in database: schema mismatch";
const EPOCH_OVERFLOW: &str = "negative count in database: data corruption";
const COUNT_EXCEEDS_I32: &str = "count exceeds i32::MAX";

// ---------------------------------------------------------------------------
// Input types
// ---------------------------------------------------------------------------

/// Input for creating a reindex run. The run is inserted `queued`; `id`,
/// tallies, and `started_at` are server-defaulted.
#[derive(Debug, Clone, TypedBuilder)]
pub struct NewReindexRun {
    /// The building profile this run targets.
    pub target_profile_id: EmbeddingProfileId,
    /// The target profile's epoch.
    pub epoch: i64,
    /// The principal that initiated the run.
    pub initiated_by_principal_id: PrincipalId,
    /// Propagated trace context, if any.
    #[builder(default)]
    pub trace_context: Option<String>,
}

// ---------------------------------------------------------------------------
// Trait
// ---------------------------------------------------------------------------

/// Data access operations for reindex runs.
#[async_trait]
pub trait ReindexRunRepository {
    /// Inserts a `queued` run.
    ///
    /// # Errors
    ///
    /// Returns [`DbError::QueryFailed`] on database errors.
    async fn insert(
        &self,
        conn: &mut PgConnection,
        new: &NewReindexRun,
    ) -> Result<ReindexRun, DbError>;

    /// Finds a run by id.
    ///
    /// # Errors
    ///
    /// Returns [`DbError::QueryFailed`] on database errors.
    async fn find_by_id(
        &self,
        conn: &mut PgConnection,
        id: ReindexRunId,
    ) -> Result<Option<ReindexRun>, DbError>;

    /// Returns the single live (`queued` or `running`) run, if any. The
    /// single-flight invariant guarantees at most one.
    ///
    /// # Errors
    ///
    /// Returns [`DbError::QueryFailed`] on database errors.
    async fn find_live(&self, conn: &mut PgConnection) -> Result<Option<ReindexRun>, DbError>;

    /// Transitions a run from `from` to `to`, stamping `completed_at` on
    /// terminal states and recording `error_message` on failure. The `from`
    /// guard makes the change a compare-and-set: it returns `true` only if the
    /// run was in `from`, so a promote cannot resurrect a run another path
    /// (cancel) moved to a terminal state.
    ///
    /// # Errors
    ///
    /// Returns [`DbError::QueryFailed`] on database errors.
    async fn transition(
        &self,
        conn: &mut PgConnection,
        id: ReindexRunId,
        from: ReindexRunState,
        to: ReindexRunState,
        error_message: Option<&str>,
    ) -> Result<bool, DbError>;

    /// Records the first-enumeration backlog counts.
    ///
    /// # Errors
    ///
    /// Returns [`DbError::QueryFailed`] on database errors.
    async fn set_enumerated(
        &self,
        conn: &mut PgConnection,
        id: ReindexRunId,
        items: u32,
        tags: u32,
    ) -> Result<(), DbError>;

    /// Adds to the embedded-row tallies.
    ///
    /// # Errors
    ///
    /// Returns [`DbError::QueryFailed`] on database errors.
    async fn bump_embedded(
        &self,
        conn: &mut PgConnection,
        id: ReindexRunId,
        items_delta: u32,
        tags_delta: u32,
    ) -> Result<(), DbError>;

    /// Adds to the quarantine tallies.
    ///
    /// # Errors
    ///
    /// Returns [`DbError::QueryFailed`] on database errors.
    async fn bump_quarantined(
        &self,
        conn: &mut PgConnection,
        id: ReindexRunId,
        items_delta: u32,
        tags_delta: u32,
    ) -> Result<(), DbError>;
}

// ---------------------------------------------------------------------------
// Postgres implementation
// ---------------------------------------------------------------------------

/// Postgres implementation of [`ReindexRunRepository`].
pub struct PgReindexRunRepository;

#[async_trait]
impl ReindexRunRepository for PgReindexRunRepository {
    async fn insert(
        &self,
        conn: &mut PgConnection,
        new: &NewReindexRun,
    ) -> Result<ReindexRun, DbError> {
        let row = sqlx::query(&format!(
            "INSERT INTO reindex_runs \
                 (target_profile_id, epoch, state, initiated_by_principal_id, trace_context) \
             VALUES ($1, $2, 'queued', $3, $4) \
             RETURNING {COLUMNS}"
        ))
        .bind(new.target_profile_id.inner())
        .bind(new.epoch)
        .bind(new.initiated_by_principal_id.inner())
        .bind(&new.trace_context)
        .fetch_one(&mut *conn)
        .await
        .map_err(|e| DbError::QueryFailed {
            context: "inserting reindex run".to_owned(),
            source: e,
        })?;

        Ok(map_reindex_run_row(&row))
    }

    async fn find_by_id(
        &self,
        conn: &mut PgConnection,
        id: ReindexRunId,
    ) -> Result<Option<ReindexRun>, DbError> {
        let row = sqlx::query(&format!("SELECT {COLUMNS} FROM reindex_runs WHERE id = $1"))
            .bind(id.inner())
            .fetch_optional(&mut *conn)
            .await
            .map_err(|e| DbError::QueryFailed {
                context: format!("finding reindex run {id}"),
                source: e,
            })?;

        Ok(row.as_ref().map(map_reindex_run_row))
    }

    async fn find_live(&self, conn: &mut PgConnection) -> Result<Option<ReindexRun>, DbError> {
        let row = sqlx::query(&format!(
            "SELECT {COLUMNS} FROM reindex_runs \
             WHERE state IN ('queued', 'running') ORDER BY started_at DESC LIMIT 1"
        ))
        .fetch_optional(&mut *conn)
        .await
        .map_err(|e| DbError::QueryFailed {
            context: "finding live reindex run".to_owned(),
            source: e,
        })?;

        Ok(row.as_ref().map(map_reindex_run_row))
    }

    async fn transition(
        &self,
        conn: &mut PgConnection,
        id: ReindexRunId,
        from: ReindexRunState,
        to: ReindexRunState,
        error_message: Option<&str>,
    ) -> Result<bool, DbError> {
        // Terminal states stamp completed_at (preserved if already set, so a
        // superseded run keeps its original completion time); live states leave
        // it null. The chk_terminal_completed CHECK enforces the pairing.
        let completed_at_sql = if to.is_terminal() {
            "completed_at = COALESCE(completed_at, now())"
        } else {
            "completed_at = NULL"
        };

        let result = sqlx::query(&format!(
            "UPDATE reindex_runs SET state = $2, error_message = $3, {completed_at_sql} \
             WHERE id = $1 AND state = $4"
        ))
        .bind(id.inner())
        .bind(to.as_str())
        .bind(error_message)
        .bind(from.as_str())
        .execute(&mut *conn)
        .await
        .map_err(|e| DbError::QueryFailed {
            context: format!("transitioning reindex run {id} from {from} to {to}"),
            source: e,
        })?;

        Ok(result.rows_affected() > 0)
    }

    async fn set_enumerated(
        &self,
        conn: &mut PgConnection,
        id: ReindexRunId,
        items: u32,
        tags: u32,
    ) -> Result<(), DbError> {
        sqlx::query(
            "UPDATE reindex_runs SET items_enumerated = $2, tags_enumerated = $3 WHERE id = $1",
        )
        .bind(id.inner())
        .bind(i32::try_from(items).expect(COUNT_EXCEEDS_I32))
        .bind(i32::try_from(tags).expect(COUNT_EXCEEDS_I32))
        .execute(&mut *conn)
        .await
        .map_err(|e| DbError::QueryFailed {
            context: format!("recording enumeration for reindex run {id}"),
            source: e,
        })?;
        Ok(())
    }

    async fn bump_embedded(
        &self,
        conn: &mut PgConnection,
        id: ReindexRunId,
        items_delta: u32,
        tags_delta: u32,
    ) -> Result<(), DbError> {
        sqlx::query(
            "UPDATE reindex_runs \
             SET items_embedded = items_embedded + $2, tags_embedded = tags_embedded + $3 \
             WHERE id = $1",
        )
        .bind(id.inner())
        .bind(i64::from(items_delta))
        .bind(i64::from(tags_delta))
        .execute(&mut *conn)
        .await
        .map_err(|e| DbError::QueryFailed {
            context: format!("bumping embedded tallies for reindex run {id}"),
            source: e,
        })?;
        Ok(())
    }

    async fn bump_quarantined(
        &self,
        conn: &mut PgConnection,
        id: ReindexRunId,
        items_delta: u32,
        tags_delta: u32,
    ) -> Result<(), DbError> {
        sqlx::query(
            "UPDATE reindex_runs \
             SET items_quarantined = items_quarantined + $2, \
                 tags_quarantined = tags_quarantined + $3 \
             WHERE id = $1",
        )
        .bind(id.inner())
        .bind(i64::from(items_delta))
        .bind(i64::from(tags_delta))
        .execute(&mut *conn)
        .await
        .map_err(|e| DbError::QueryFailed {
            context: format!("bumping quarantine tallies for reindex run {id}"),
            source: e,
        })?;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Row mapping
// ---------------------------------------------------------------------------

fn map_reindex_run_row(r: &sqlx::postgres::PgRow) -> ReindexRun {
    ReindexRun::builder()
        .id(ReindexRunId::from(r.get::<uuid::Uuid, _>("id")))
        .target_profile_id(EmbeddingProfileId::from(
            r.get::<uuid::Uuid, _>("target_profile_id"),
        ))
        .epoch(r.get::<i64, _>("epoch"))
        .state(
            r.get::<String, _>("state")
                .parse::<ReindexRunState>()
                .expect(UNKNOWN_STATE_IN_DB),
        )
        .initiated_by_principal_id(PrincipalId::from(
            r.get::<uuid::Uuid, _>("initiated_by_principal_id"),
        ))
        .items_enumerated(map_count(r, "items_enumerated"))
        .items_embedded(r.get::<i64, _>("items_embedded"))
        .tags_enumerated(map_count(r, "tags_enumerated"))
        .tags_embedded(r.get::<i64, _>("tags_embedded"))
        .items_quarantined(r.get::<i64, _>("items_quarantined"))
        .tags_quarantined(r.get::<i64, _>("tags_quarantined"))
        .error_message(r.get::<Option<String>, _>("error_message"))
        .trace_context(r.get::<Option<String>, _>("trace_context"))
        .started_at(r.get("started_at"))
        .completed_at(r.get("completed_at"))
        .build()
}

fn map_count(r: &sqlx::postgres::PgRow, column: &str) -> Option<u32> {
    r.get::<Option<i32>, _>(column)
        .map(|v| u32::try_from(v).expect(EPOCH_OVERFLOW))
}
