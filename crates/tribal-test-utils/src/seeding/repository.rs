//! Test-only repository for seed infrastructure operations.
//!
//! Centralises raw SQL used by the seed executor for timestamp
//! backdating, plus delegates to the setup module and production
//! repositories for relation commitment scaffolding.
//!
//! Backdating uses raw `sqlx::query()` because the operations sit
//! outside the regular repository layer.

use async_trait::async_trait;
use sqlx::PgConnection;
use tribal_db::{JobStateOverride, PgJobRepository};
use tribal_domain::{
    ItemObservationId, JobId, JobOutcome, JobStatus, KnowledgeItemId, PrincipalId, ProjectId,
    RelationBatchId, RelationId,
};

use crate::{a_new_job, a_new_prompt_version, insert_prompt_version};

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

    async fn commit_relation_batch(
        &self,
        conn: &mut PgConnection,
        project_id: ProjectId,
        principal_id: PrincipalId,
        batch_id: RelationBatchId,
    ) -> JobId {
        let pv_id = insert_prompt_version(conn, &a_new_prompt_version().build()).await;

        let job = PgJobRepository
            .insert_for_test(
                conn,
                &a_new_job()
                    .project_id(project_id)
                    .principal_id(principal_id)
                    .extraction_system_prompt_version_id(pv_id)
                    .extraction_user_prompt_version_id(pv_id)
                    .triage_system_prompt_version_id(pv_id)
                    .triage_user_prompt_version_id(pv_id)
                    .relation_system_prompt_version_id(pv_id)
                    .relation_user_prompt_version_id(pv_id)
                    .build(),
                &JobStateOverride::builder()
                    .status(JobStatus::Completed)
                    .outcome(Some(JobOutcome::Success))
                    .committed_batch_id(Some(batch_id))
                    .build(),
            )
            .await
            .expect("seed: insert completed job");

        job.id()
    }
}

// ---------------------------------------------------------------------------
// Public helper
// ---------------------------------------------------------------------------

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
