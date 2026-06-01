//! Embedding profile repository: trait definition and Postgres implementation.
//!
//! `embedding_profiles` is an append-only activation log. The active profile is
//! derived as the highest-epoch `complete` row. Raw `sqlx::query()` is used
//! because the row maps onto domain enums parsed as `TEXT`, which the
//! compile-time `sqlx::query!` macro cannot construct.

use async_trait::async_trait;
use sqlx::{PgConnection, Row};
use tribal_domain::{
    DistanceMetric, EmbeddingProfile, EmbeddingProfileId, EmbeddingProfileState, ProviderKind,
};
use typed_builder::TypedBuilder;

use super::common::columns::Columns;
use crate::DbError;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const COLUMNS: Columns = Columns(&[
    "id",
    "epoch",
    "provider_kind",
    "normalised_base_url",
    "model",
    "dimensions",
    "distance_metric",
    "revision_token",
    "probe_digest",
    "state",
    "fingerprint_hash",
    "created_at",
    "completed_at",
]);

const DIMENSIONS_EXCEEDS_I32: &str = "profile dimensions exceeds i32::MAX";
const DIMENSIONS_OVERFLOW: &str = "negative dimensions in database: data corruption";
const UNKNOWN_PROVIDER_KIND_IN_DB: &str = "unknown provider_kind in database: data corruption";
const UNKNOWN_DISTANCE_METRIC_IN_DB: &str = "unknown distance_metric in database: data corruption";
const UNKNOWN_PROFILE_STATE_IN_DB: &str = "unknown profile state in database: data corruption";

// ---------------------------------------------------------------------------
// Input types
// ---------------------------------------------------------------------------

/// Input for creating a new embedding profile.
///
/// A profile is always inserted in the `building` state; `id`, `epoch`,
/// `created_at`, and `completed_at` are server-generated. The genesis profile
/// and every reindex target are created through this shape.
#[derive(Debug, Clone, TypedBuilder)]
pub struct NewEmbeddingProfile {
    /// The provider that produces this geometry.
    pub provider_kind: ProviderKind,
    /// The provider endpoint, normalised for credential and registry lookup.
    pub normalised_base_url: String,
    /// The embedding model name.
    pub model: String,
    /// Vector dimension.
    pub dimensions: u32,
    /// The distance metric the index is built for.
    #[builder(default)]
    pub distance_metric: DistanceMetric,
    /// A provider-exposed revision signal; empty when none.
    #[builder(default)]
    pub revision_token: String,
    /// The quantised probe digest, when one was resolved.
    #[builder(default)]
    pub probe_digest: Option<String>,
    /// Stable handle over the identity tuple.
    pub fingerprint_hash: String,
}

// ---------------------------------------------------------------------------
// Trait
// ---------------------------------------------------------------------------

/// Data access operations for embedding profiles.
///
/// All methods take `&mut PgConnection` as an explicit executor, keeping the
/// repository pool-agnostic.
#[async_trait]
pub trait EmbeddingProfileRepository {
    /// Inserts a new profile in the `building` state.
    ///
    /// # Errors
    ///
    /// Returns [`DbError::QueryFailed`] on database errors.
    async fn insert(
        &self,
        conn: &mut PgConnection,
        new: &NewEmbeddingProfile,
    ) -> Result<EmbeddingProfile, DbError>;

    /// Returns the active profile: the highest-epoch `complete` row, or `None`
    /// before any profile has completed.
    ///
    /// # Errors
    ///
    /// Returns [`DbError::QueryFailed`] on database errors.
    async fn find_active(
        &self,
        conn: &mut PgConnection,
    ) -> Result<Option<EmbeddingProfile>, DbError>;

    /// Returns the profile with the given id, or `None` if no such profile
    /// exists (e.g. it was pruned).
    ///
    /// # Errors
    ///
    /// Returns [`DbError::QueryFailed`] on database errors.
    async fn find_by_id(
        &self,
        conn: &mut PgConnection,
        id: EmbeddingProfileId,
    ) -> Result<Option<EmbeddingProfile>, DbError>;

    /// Returns the highest-epoch `building` profile, or `None`.
    ///
    /// Single-flight makes at most one `building` profile live at a time;
    /// first-boot provisioning uses this to adopt a genesis profile left by a
    /// prior crashed run.
    ///
    /// # Errors
    ///
    /// Returns [`DbError::QueryFailed`] on database errors.
    async fn find_building(
        &self,
        conn: &mut PgConnection,
    ) -> Result<Option<EmbeddingProfile>, DbError>;

    /// Transitions a `building` profile to `complete`, stamping `completed_at`.
    ///
    /// Returns `true` if this call performed the transition, `false` if the
    /// profile was not in the `building` state (already completed or absent),
    /// so the caller can reason about a crash-resumed provisioning.
    ///
    /// # Errors
    ///
    /// Returns [`DbError::QueryFailed`] on database errors.
    async fn mark_complete(
        &self,
        conn: &mut PgConnection,
        id: EmbeddingProfileId,
    ) -> Result<bool, DbError>;

    /// Transitions a `building` profile to `failed`, stamping `completed_at`.
    ///
    /// Used when a reindex aborts or a transient failure dead-letters, and to
    /// reconcile an orphan building profile on boot. Returns `true` if this call
    /// performed the transition, `false` if the profile was not `building`.
    ///
    /// # Errors
    ///
    /// Returns [`DbError::QueryFailed`] on database errors.
    async fn mark_failed(
        &self,
        conn: &mut PgConnection,
        id: EmbeddingProfileId,
    ) -> Result<bool, DbError>;

    /// Supersedes every prunable profile in one statement: all `failed`
    /// profiles and every `complete` profile below the active (highest-epoch
    /// `complete`) one. The active is never superseded. `completed_at` is
    /// preserved, since prunable profiles already carry it. Returns the epochs
    /// of the superseded profiles, whose partial indexes the caller then drops.
    ///
    /// # Errors
    ///
    /// Returns [`DbError::QueryFailed`] on database errors.
    async fn supersede_prunable(&self, conn: &mut PgConnection) -> Result<Vec<i64>, DbError>;

    /// Returns the epochs of every `superseded` profile whose partial HNSW index
    /// still exists in either embedding table. This is the full set still
    /// eligible for index cleanup: newly superseded epochs plus any whose prior
    /// drop failed and whose dead index lingers, so a later prune retries it
    /// rather than only re-attempting the freshly-transitioned set.
    ///
    /// # Errors
    ///
    /// Returns [`DbError::QueryFailed`] on database errors.
    async fn prunable_index_epochs(&self, conn: &mut PgConnection) -> Result<Vec<i64>, DbError>;
}

/// Transitions a building profile to a terminal state, stamping `completed_at`.
/// Returns whether a row was transitioned (the guard `from` state did not match
/// otherwise). Shared by [`mark_complete`] and [`mark_failed`]; the `failed →
/// superseded` step is `supersede_prunable`, which preserves `completed_at`.
async fn transition_to_terminal(
    conn: &mut PgConnection,
    id: EmbeddingProfileId,
    from: EmbeddingProfileState,
    to: EmbeddingProfileState,
    verb: &str,
) -> Result<bool, DbError> {
    let result = sqlx::query(
        "UPDATE embedding_profiles SET state = $2, completed_at = now() \
         WHERE id = $1 AND state = $3",
    )
    .bind(id.inner())
    .bind(to.as_str())
    .bind(from.as_str())
    .execute(&mut *conn)
    .await
    .map_err(|e| DbError::QueryFailed {
        context: format!("{verb} embedding profile {id}"),
        source: e,
    })?;

    Ok(result.rows_affected() > 0)
}

// ---------------------------------------------------------------------------
// Postgres implementation
// ---------------------------------------------------------------------------

/// Postgres implementation of [`EmbeddingProfileRepository`].
///
/// A zero-sized type with no internal state.
pub struct PgEmbeddingProfileRepository;

#[async_trait]
impl EmbeddingProfileRepository for PgEmbeddingProfileRepository {
    async fn insert(
        &self,
        conn: &mut PgConnection,
        new: &NewEmbeddingProfile,
    ) -> Result<EmbeddingProfile, DbError> {
        let dimensions = i32::try_from(new.dimensions).expect(DIMENSIONS_EXCEEDS_I32);

        let row = sqlx::query(&format!(
            "INSERT INTO embedding_profiles \
                 (provider_kind, normalised_base_url, model, dimensions, distance_metric, \
                  revision_token, probe_digest, state, fingerprint_hash) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, 'building', $8) \
             RETURNING {COLUMNS}"
        ))
        .bind(new.provider_kind.as_str())
        .bind(&new.normalised_base_url)
        .bind(&new.model)
        .bind(dimensions)
        .bind(new.distance_metric.as_str())
        .bind(&new.revision_token)
        .bind(&new.probe_digest)
        .bind(&new.fingerprint_hash)
        .fetch_one(&mut *conn)
        .await
        .map_err(|e| DbError::QueryFailed {
            context: "inserting embedding profile".to_owned(),
            source: e,
        })?;

        Ok(map_embedding_profile_row(&row))
    }

    async fn find_active(
        &self,
        conn: &mut PgConnection,
    ) -> Result<Option<EmbeddingProfile>, DbError> {
        let row = sqlx::query(&format!(
            "SELECT {COLUMNS} FROM embedding_profiles \
             WHERE state = 'complete' ORDER BY epoch DESC LIMIT 1"
        ))
        .fetch_optional(&mut *conn)
        .await
        .map_err(|e| DbError::QueryFailed {
            context: "finding active embedding profile".to_owned(),
            source: e,
        })?;

        Ok(row.as_ref().map(map_embedding_profile_row))
    }

    async fn find_by_id(
        &self,
        conn: &mut PgConnection,
        id: EmbeddingProfileId,
    ) -> Result<Option<EmbeddingProfile>, DbError> {
        let row = sqlx::query(&format!(
            "SELECT {COLUMNS} FROM embedding_profiles WHERE id = $1"
        ))
        .bind(id.inner())
        .fetch_optional(&mut *conn)
        .await
        .map_err(|e| DbError::QueryFailed {
            context: format!("finding embedding profile {id}"),
            source: e,
        })?;

        Ok(row.as_ref().map(map_embedding_profile_row))
    }

    async fn find_building(
        &self,
        conn: &mut PgConnection,
    ) -> Result<Option<EmbeddingProfile>, DbError> {
        let row = sqlx::query(&format!(
            "SELECT {COLUMNS} FROM embedding_profiles \
             WHERE state = 'building' ORDER BY epoch DESC LIMIT 1"
        ))
        .fetch_optional(&mut *conn)
        .await
        .map_err(|e| DbError::QueryFailed {
            context: "finding building embedding profile".to_owned(),
            source: e,
        })?;

        Ok(row.as_ref().map(map_embedding_profile_row))
    }

    async fn mark_complete(
        &self,
        conn: &mut PgConnection,
        id: EmbeddingProfileId,
    ) -> Result<bool, DbError> {
        transition_to_terminal(
            conn,
            id,
            EmbeddingProfileState::Building,
            EmbeddingProfileState::Complete,
            "completing",
        )
        .await
    }

    async fn mark_failed(
        &self,
        conn: &mut PgConnection,
        id: EmbeddingProfileId,
    ) -> Result<bool, DbError> {
        transition_to_terminal(
            conn,
            id,
            EmbeddingProfileState::Building,
            EmbeddingProfileState::Failed,
            "failing",
        )
        .await
    }

    async fn supersede_prunable(&self, conn: &mut PgConnection) -> Result<Vec<i64>, DbError> {
        // The active profile is the highest-epoch `complete` one; a `complete`
        // profile below it, and every `failed` profile, is prunable. Preserve
        // `completed_at` (prunable profiles already carry it). The returned
        // epochs name the now-superseded profiles whose partial indexes the
        // caller drops to reclaim their storage.
        let epochs = sqlx::query_scalar(
            "UPDATE embedding_profiles SET state = 'superseded' \
             WHERE state = 'failed' \
                OR (state = 'complete' \
                    AND epoch < (SELECT MAX(epoch) FROM embedding_profiles \
                                 WHERE state = 'complete')) \
             RETURNING epoch",
        )
        .fetch_all(&mut *conn)
        .await
        .map_err(|e| DbError::QueryFailed {
            context: "superseding prunable embedding profiles".to_owned(),
            source: e,
        })?;
        Ok(epochs)
    }

    async fn prunable_index_epochs(&self, conn: &mut PgConnection) -> Result<Vec<i64>, DbError> {
        // A superseded profile is eligible for index cleanup while either of its
        // partial HNSW indexes still exists. `to_regclass` resolves the
        // epoch-named index (see `EmbeddingTable::hnsw_index_name`) to NULL when
        // absent, so an epoch whose drops all succeeded drops out of the set and
        // one whose drop failed remains, which is what lets a later prune retry.
        let epochs = sqlx::query_scalar(
            "SELECT epoch FROM embedding_profiles \
             WHERE state = 'superseded' \
               AND (to_regclass('idx_embeddings_hnsw_epoch' || epoch) IS NOT NULL \
                 OR to_regclass('idx_tag_embeddings_hnsw_epoch' || epoch) IS NOT NULL) \
             ORDER BY epoch",
        )
        .fetch_all(&mut *conn)
        .await
        .map_err(|e| DbError::QueryFailed {
            context: "finding prunable superseded index epochs".to_owned(),
            source: e,
        })?;
        Ok(epochs)
    }
}

// ---------------------------------------------------------------------------
// Row mapping
// ---------------------------------------------------------------------------

fn map_embedding_profile_row(r: &sqlx::postgres::PgRow) -> EmbeddingProfile {
    EmbeddingProfile::builder()
        .id(EmbeddingProfileId::from(r.get::<uuid::Uuid, _>("id")))
        .epoch(r.get::<i64, _>("epoch"))
        .provider_kind(
            r.get::<String, _>("provider_kind")
                .parse::<ProviderKind>()
                .expect(UNKNOWN_PROVIDER_KIND_IN_DB),
        )
        .normalised_base_url(r.get::<String, _>("normalised_base_url"))
        .model(r.get::<String, _>("model"))
        .dimensions(u32::try_from(r.get::<i32, _>("dimensions")).expect(DIMENSIONS_OVERFLOW))
        .distance_metric(
            r.get::<String, _>("distance_metric")
                .parse::<DistanceMetric>()
                .expect(UNKNOWN_DISTANCE_METRIC_IN_DB),
        )
        .revision_token(r.get::<String, _>("revision_token"))
        .probe_digest(r.get::<Option<String>, _>("probe_digest"))
        .state(
            r.get::<String, _>("state")
                .parse::<EmbeddingProfileState>()
                .expect(UNKNOWN_PROFILE_STATE_IN_DB),
        )
        .fingerprint_hash(r.get::<String, _>("fingerprint_hash"))
        .created_at(r.get("created_at"))
        .completed_at(r.get("completed_at"))
        .build()
}
