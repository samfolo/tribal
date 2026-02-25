//! Test-only repository for seed infrastructure operations.
//!
//! Centralises all raw SQL used by the seed executor: timestamp
//! backdating and relation commitment scaffolding. Consolidating
//! these operations in one place means schema changes to the
//! underlying tables only need updating here.
//!
//! Uses raw `sqlx::query()` because the operations (backdating
//! timestamps, inserting structural scaffolding) sit outside the
//! regular repository layer.

use std::fmt;

use async_trait::async_trait;
use sqlx::PgConnection;
use tribal_domain::{
    ItemObservationId, JobId, KnowledgeItemId, PrincipalId, ProjectId, RelationBatchId, RelationId,
    TaskId,
};

// ---------------------------------------------------------------------------
// Column formatting
// ---------------------------------------------------------------------------

/// A static list of column names for SQL query formatting.
struct Columns(&'static [&'static str]);

impl fmt::Display for Columns {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut iter = self.0.iter();
        if let Some(first) = iter.next() {
            write!(f, "{first}")?;
            for col in iter {
                write!(f, ", {col}")?;
            }
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Columns inserted when creating commitment scaffolding for a
/// `prompt_version` row.
const PROMPT_VERSION_INSERT_COLUMNS: Columns = Columns(&["stage", "content_hash", "content"]);

/// Columns inserted when creating a completed `job` row for relation
/// batch commitment.
const JOB_INSERT_COLUMNS: Columns = Columns(&[
    "project_id",
    "principal_id",
    "source_context",
    "status",
    "outcome",
    "committed_batch_id",
    "extraction_prompt_version_id",
    "triage_prompt_version_id",
    "relation_prompt_version_id",
]);

// ---------------------------------------------------------------------------
// Trait
// ---------------------------------------------------------------------------

/// Repository for seed-infrastructure database operations that sit
/// outside the regular repository layer (backdating, scaffolding).
#[async_trait]
pub(crate) trait SeedRepository {
    /// Backdates a knowledge item's `created_at` timestamp.
    async fn backdate_item(
        &self,
        conn: &mut PgConnection,
        id: KnowledgeItemId,
        ts: chrono::DateTime<chrono::Utc>,
    );

    /// Backdates an observation's `observed_at` timestamp.
    async fn backdate_observation(
        &self,
        conn: &mut PgConnection,
        id: ItemObservationId,
        ts: chrono::DateTime<chrono::Utc>,
    );

    /// Backdates the `created_at` timestamp on a batch of relations.
    async fn backdate_relations(
        &self,
        conn: &mut PgConnection,
        ids: &[RelationId],
        ts: chrono::DateTime<chrono::Utc>,
    );

    /// Backdates a task's `heartbeat_at` by the given duration.
    async fn backdate_task_heartbeat(
        &self,
        conn: &mut PgConnection,
        id: TaskId,
        duration: std::time::Duration,
    );

    /// Creates commitment scaffolding for a relation batch: a
    /// `prompt_version` row and a completed `job` row with
    /// `committed_batch_id = batch_id`.
    ///
    /// Returns the [`JobId`] of the created job.
    async fn commit_relation_batch(
        &self,
        conn: &mut PgConnection,
        project_id: ProjectId,
        principal_id: PrincipalId,
        batch_id: RelationBatchId,
    ) -> JobId;
}

// ---------------------------------------------------------------------------
// Postgres implementation
// ---------------------------------------------------------------------------

/// Postgres implementation of [`SeedRepository`].
pub(crate) struct PgSeedRepository;

#[async_trait]
impl SeedRepository for PgSeedRepository {
    async fn backdate_item(
        &self,
        conn: &mut PgConnection,
        id: KnowledgeItemId,
        ts: chrono::DateTime<chrono::Utc>,
    ) {
        sqlx::query("UPDATE knowledge_items SET created_at = $1 WHERE id = $2")
            .bind(ts)
            .bind(id.inner())
            .execute(&mut *conn)
            .await
            .expect("seed: backdate item");
    }

    async fn backdate_observation(
        &self,
        conn: &mut PgConnection,
        id: ItemObservationId,
        ts: chrono::DateTime<chrono::Utc>,
    ) {
        sqlx::query("UPDATE item_observations SET observed_at = $1 WHERE id = $2")
            .bind(ts)
            .bind(id.inner())
            .execute(&mut *conn)
            .await
            .expect("seed: backdate observation");
    }

    async fn backdate_relations(
        &self,
        conn: &mut PgConnection,
        ids: &[RelationId],
        ts: chrono::DateTime<chrono::Utc>,
    ) {
        if ids.is_empty() {
            return;
        }
        let raw_ids: Vec<&uuid::Uuid> = ids.iter().map(RelationId::inner).collect();
        sqlx::query("UPDATE knowledge_item_relations SET created_at = $1 WHERE id = ANY($2)")
            .bind(ts)
            .bind(&raw_ids)
            .execute(&mut *conn)
            .await
            .expect("seed: backdate relations");
    }

    async fn backdate_task_heartbeat(
        &self,
        conn: &mut PgConnection,
        id: TaskId,
        duration: std::time::Duration,
    ) {
        let secs = duration.as_secs_f64();
        sqlx::query(
            "UPDATE tasks SET heartbeat_at = now() - make_interval(secs => $1) WHERE id = $2",
        )
        .bind(secs)
        .bind(id.inner())
        .execute(&mut *conn)
        .await
        .expect("seed: backdate task heartbeat");
    }

    async fn commit_relation_batch(
        &self,
        conn: &mut PgConnection,
        project_id: ProjectId,
        principal_id: PrincipalId,
        batch_id: RelationBatchId,
    ) -> JobId {
        let content_hash = format!("{:064x}", uuid::Uuid::new_v4().as_u128());

        let pv_sql = format!(
            "INSERT INTO prompt_versions ({PROMPT_VERSION_INSERT_COLUMNS}) \
             VALUES ($1, $2, $3) RETURNING id"
        );
        let prompt_version_id: uuid::Uuid = sqlx::query_scalar(&pv_sql)
            .bind("extraction")
            .bind(&content_hash)
            .bind("test")
            .fetch_one(&mut *conn)
            .await
            .expect("seed: insert prompt_version");

        let job_sql = format!(
            "INSERT INTO jobs ({JOB_INSERT_COLUMNS}) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $7, $7) RETURNING id"
        );
        let job_id: uuid::Uuid = sqlx::query_scalar(&job_sql)
            .bind(project_id.inner())
            .bind(principal_id.inner())
            .bind(serde_json::json!({}))
            .bind("completed")
            .bind("success")
            .bind(batch_id.inner())
            .bind(prompt_version_id)
            .fetch_one(&mut *conn)
            .await
            .expect("seed: insert job");

        JobId::from(job_id)
    }
}

// ---------------------------------------------------------------------------
// Public helper
// ---------------------------------------------------------------------------

/// Backdates a task's `heartbeat_at` by the given duration.
///
/// Intended for simulating stale heartbeats in tests that exercise
/// the reclaim sweep and startup reclaim paths.
pub async fn backdate_task_heartbeat(
    conn: &mut PgConnection,
    id: TaskId,
    duration: std::time::Duration,
) {
    PgSeedRepository
        .backdate_task_heartbeat(conn, id, duration)
        .await;
}

/// Creates commitment scaffolding for a relation batch: a
/// `prompt_version` row and a completed `job` row with
/// `committed_batch_id = batch_id`.
///
/// Returns the [`JobId`] of the created job. Intended for test setup
/// where relations need to appear committed without a full pipeline
/// run.
pub async fn commit_relation_batch(
    conn: &mut PgConnection,
    project_id: ProjectId,
    principal_id: PrincipalId,
    batch_id: RelationBatchId,
) -> JobId {
    PgSeedRepository
        .commit_relation_batch(conn, project_id, principal_id, batch_id)
        .await
}
