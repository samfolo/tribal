//! Standing repository: trait definition and Postgres implementation.
//!
//! Standing is the computed evidential profile of a knowledge item —
//! support, contradiction, supersession, observation frequency, and
//! diversity metrics.  It is never stored; the single SQL query
//! derives all fields from the relationship graph and item observations
//! at query time.

use std::collections::HashMap;

use async_trait::async_trait;
use sqlx::{PgConnection, Row};
use tribal_domain::{KnowledgeItemId, Standing};

use crate::DbError;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const COUNT_EXCEEDS_U32: &str = "aggregate count exceeds u32 — unexpected row volume";

// ---------------------------------------------------------------------------
// Trait
// ---------------------------------------------------------------------------

/// Data access operations for computing knowledge item standing.
///
/// All methods take `&mut PgConnection` as an explicit executor,
/// keeping the repository pool-agnostic.
#[async_trait]
pub trait StandingRepository {
    /// Computes the standing for each requested knowledge item.
    ///
    /// Returns standings in the same order as the input `ids`.  Items
    /// that do not exist in the database receive a zero-valued standing.
    /// Returns an empty vec without issuing a query if `ids` is empty.
    ///
    /// # Errors
    ///
    /// Returns [`DbError::QueryFailed`] on database errors.
    async fn compute(
        &self,
        conn: &mut PgConnection,
        ids: &[KnowledgeItemId],
    ) -> Result<Vec<Standing>, DbError>;
}

// ---------------------------------------------------------------------------
// Postgres implementation
// ---------------------------------------------------------------------------

/// Postgres implementation of [`StandingRepository`].
///
/// A zero-sized type with no internal state.
pub struct PgStandingRepository;

/// Maps a raw `sqlx::Row` from the standing aggregation query into a
/// `(KnowledgeItemId, Standing)` tuple.
fn map_standing_row(r: &sqlx::postgres::PgRow) -> (KnowledgeItemId, Standing) {
    let ki_id = KnowledgeItemId::from(r.get::<uuid::Uuid, _>("id"));

    let supporting_count =
        u32::try_from(r.get::<i64, _>("supporting_count")).expect(COUNT_EXCEEDS_U32);
    let contradicting_count =
        u32::try_from(r.get::<i64, _>("contradicting_count")).expect(COUNT_EXCEEDS_U32);
    let observation_count =
        u32::try_from(r.get::<i64, _>("observation_count")).expect(COUNT_EXCEEDS_U32);
    let supporting_episode_count =
        u32::try_from(r.get::<i64, _>("supporting_episode_count")).expect(COUNT_EXCEEDS_U32);
    let supporting_project_count =
        u32::try_from(r.get::<i64, _>("supporting_project_count")).expect(COUNT_EXCEEDS_U32);

    let superseded_by = r
        .get::<Option<uuid::Uuid>, _>("superseded_by")
        .map(KnowledgeItemId::from);
    let newest_supporting_id = r
        .get::<Option<uuid::Uuid>, _>("newest_supporting_id")
        .map(KnowledgeItemId::from);
    let newest_contradicting_id = r
        .get::<Option<uuid::Uuid>, _>("newest_contradicting_id")
        .map(KnowledgeItemId::from);

    let standing = Standing::builder()
        .supporting_count(supporting_count)
        .contradicting_count(contradicting_count)
        .superseded_by(superseded_by)
        .observation_count(observation_count)
        .newest_supporting_id(newest_supporting_id)
        .newest_contradicting_id(newest_contradicting_id)
        .supporting_episode_count(supporting_episode_count)
        .supporting_project_count(supporting_project_count)
        .build();

    (ki_id, standing)
}

#[async_trait]
impl StandingRepository for PgStandingRepository {
    async fn compute(
        &self,
        conn: &mut PgConnection,
        ids: &[KnowledgeItemId],
    ) -> Result<Vec<Standing>, DbError> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }

        let uuid_ids: Vec<uuid::Uuid> = ids.iter().map(|id| *id.inner()).collect();

        let rows = sqlx::query(
            "SELECT \
                 ki.id, \
                 COUNT(DISTINCT r.source_id) \
                     FILTER (WHERE r.relation_type = 'supports') \
                     AS supporting_count, \
                 COUNT(DISTINCT r.source_id) \
                     FILTER (WHERE r.relation_type = 'contradicts') \
                     AS contradicting_count, \
                 (SELECT r2.source_id FROM knowledge_item_relations r2 \
                  JOIN jobs j2 ON j2.committed_batch_id = r2.relation_batch_id \
                  WHERE r2.target_id = ki.id AND r2.relation_type = 'supersedes' \
                  LIMIT 1) AS superseded_by, \
                 (SELECT COUNT(*) FROM item_observations io \
                  WHERE io.knowledge_item_id = ki.id) AS observation_count, \
                 (SELECT r3.source_id FROM knowledge_item_relations r3 \
                  JOIN jobs j3 ON j3.committed_batch_id = r3.relation_batch_id \
                  WHERE r3.target_id = ki.id AND r3.relation_type = 'supports' \
                  ORDER BY r3.created_at DESC LIMIT 1) AS newest_supporting_id, \
                 (SELECT r4.source_id FROM knowledge_item_relations r4 \
                  JOIN jobs j4 ON j4.committed_batch_id = r4.relation_batch_id \
                  WHERE r4.target_id = ki.id AND r4.relation_type = 'contradicts' \
                  ORDER BY r4.created_at DESC LIMIT 1) AS newest_contradicting_id, \
                 COUNT(DISTINCT sup_ki.episode_id) \
                     FILTER (WHERE r.relation_type = 'supports') \
                     AS supporting_episode_count, \
                 COUNT(DISTINCT sup_ki.project_id) \
                     FILTER (WHERE r.relation_type = 'supports') \
                     AS supporting_project_count \
             FROM knowledge_items ki \
             LEFT JOIN ( \
                 knowledge_item_relations r \
                 JOIN jobs j ON j.committed_batch_id = r.relation_batch_id \
             ) ON r.target_id = ki.id \
             LEFT JOIN knowledge_items sup_ki ON sup_ki.id = r.source_id \
             WHERE ki.id = ANY($1) \
             GROUP BY ki.id",
        )
        .bind(&uuid_ids)
        .fetch_all(&mut *conn)
        .await
        .map_err(|e| DbError::QueryFailed {
            context: format!("computing standing for {} items", ids.len()),
            source: e,
        })?;

        let map: HashMap<KnowledgeItemId, Standing> =
            rows.iter().map(map_standing_row).collect();

        let zero = Standing::builder()
            .supporting_count(0)
            .contradicting_count(0)
            .observation_count(0)
            .supporting_episode_count(0)
            .supporting_project_count(0)
            .build();

        Ok(ids
            .iter()
            .map(|id| map.get(id).cloned().unwrap_or_else(|| zero.clone()))
            .collect())
    }
}
