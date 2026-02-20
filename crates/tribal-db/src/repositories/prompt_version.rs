//! Prompt version repository: trait definition and Postgres implementation.
//!
//! Prompt versions are content-addressed and immutable.  The upsert
//! operation either creates a new version or returns the existing row
//! when the `(stage, content_hash)` pair already exists.

use async_trait::async_trait;
use sqlx::{PgConnection, Row};
use tribal_domain::{PromptStage, PromptVersion, PromptVersionId};
use typed_builder::TypedBuilder;

use crate::DbError;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const UNKNOWN_PROMPT_STAGE_IN_DB: &str =
    "unrecognised prompt stage in database — schema mismatch";

// ---------------------------------------------------------------------------
// Input types
// ---------------------------------------------------------------------------

/// Input for upserting a prompt version.
///
/// Contains only caller-provided fields.  Server-generated values
/// (`id`, `created_at`) are produced by Postgres via `DEFAULT`
/// clauses and returned via `RETURNING`.
#[derive(Debug, TypedBuilder)]
pub struct NewPromptVersion {
    /// The pipeline stage this prompt applies to.
    pub stage: PromptStage,
    /// SHA-256 hex-encoded content hash (64 chars, lowercase).
    pub content_hash: String,
    /// The full prompt template content.
    pub content: String,
}

// ---------------------------------------------------------------------------
// Trait
// ---------------------------------------------------------------------------

/// Data access operations for prompt versions.
///
/// All methods take `&mut PgConnection` as an explicit executor,
/// keeping the repository pool-agnostic.  Prompt versions are
/// content-addressed and immutable — the upsert returns the existing
/// row when the content hash matches.
#[async_trait]
pub trait PromptVersionRepository {
    /// Inserts a new prompt version or returns the existing row when
    /// the `(stage, content_hash)` pair already exists.
    ///
    /// Uses a two-step approach: INSERT ON CONFLICT DO NOTHING, then
    /// SELECT if no row was returned.  This preserves content-addressed
    /// semantics without a misleading DO UPDATE clause.
    ///
    /// # Errors
    ///
    /// Returns [`DbError::QueryFailed`] on database errors.
    async fn upsert(
        &self,
        conn: &mut PgConnection,
        new: &NewPromptVersion,
    ) -> Result<PromptVersion, DbError>;

    /// Finds a prompt version by its ID.
    ///
    /// # Errors
    ///
    /// Returns [`DbError::NotFound`] if no row matches.
    /// Returns [`DbError::QueryFailed`] on database errors.
    async fn find_by_id(
        &self,
        conn: &mut PgConnection,
        id: PromptVersionId,
    ) -> Result<PromptVersion, DbError>;

    /// Finds a prompt version by its stage and content hash.
    ///
    /// Returns `Ok(None)` when no match exists.
    ///
    /// # Errors
    ///
    /// Returns [`DbError::QueryFailed`] on database errors.
    async fn find_by_stage_and_hash(
        &self,
        conn: &mut PgConnection,
        stage: PromptStage,
        content_hash: &str,
    ) -> Result<Option<PromptVersion>, DbError>;
}

// ---------------------------------------------------------------------------
// Postgres implementation
// ---------------------------------------------------------------------------

/// Postgres implementation of [`PromptVersionRepository`].
///
/// A zero-sized type with no internal state.
pub struct PgPromptVersionRepository;

#[async_trait]
impl PromptVersionRepository for PgPromptVersionRepository {
    async fn upsert(
        &self,
        conn: &mut PgConnection,
        new: &NewPromptVersion,
    ) -> Result<PromptVersion, DbError> {
        let row = sqlx::query(
            "INSERT INTO prompt_versions (stage, content_hash, content) \
             VALUES ($1, $2, $3) \
             ON CONFLICT (stage, content_hash) DO NOTHING \
             RETURNING id, stage, content_hash, content, created_at",
        )
        .bind(new.stage.as_str())
        .bind(&new.content_hash)
        .bind(&new.content)
        .fetch_optional(&mut *conn)
        .await
        .map_err(|e| DbError::QueryFailed {
            context: "upserting prompt version".to_owned(),
            source: e,
        })?;

        if let Some(r) = row {
            return Ok(map_prompt_version_row(&r));
        }

        // Conflict path — content already exists for this stage.
        let r = sqlx::query(
            "SELECT id, stage, content_hash, content, created_at \
             FROM prompt_versions \
             WHERE stage = $1 AND content_hash = $2",
        )
        .bind(new.stage.as_str())
        .bind(&new.content_hash)
        .fetch_one(&mut *conn)
        .await
        .map_err(|e| DbError::QueryFailed {
            context: "finding existing prompt version after conflict".to_owned(),
            source: e,
        })?;

        Ok(map_prompt_version_row(&r))
    }

    async fn find_by_id(
        &self,
        conn: &mut PgConnection,
        id: PromptVersionId,
    ) -> Result<PromptVersion, DbError> {
        let row = sqlx::query(
            "SELECT id, stage, content_hash, content, created_at \
             FROM prompt_versions WHERE id = $1",
        )
        .bind(id.inner())
        .fetch_optional(&mut *conn)
        .await
        .map_err(|e| DbError::QueryFailed {
            context: format!("finding prompt version {id}"),
            source: e,
        })?
        .ok_or_else(|| DbError::NotFound {
            entity: "prompt_version",
            id: id.to_string(),
        })?;

        Ok(map_prompt_version_row(&row))
    }

    async fn find_by_stage_and_hash(
        &self,
        conn: &mut PgConnection,
        stage: PromptStage,
        content_hash: &str,
    ) -> Result<Option<PromptVersion>, DbError> {
        let row = sqlx::query(
            "SELECT id, stage, content_hash, content, created_at \
             FROM prompt_versions \
             WHERE stage = $1 AND content_hash = $2",
        )
        .bind(stage.as_str())
        .bind(content_hash)
        .fetch_optional(&mut *conn)
        .await
        .map_err(|e| DbError::QueryFailed {
            context: "finding prompt version by stage and hash".to_owned(),
            source: e,
        })?;

        Ok(row.as_ref().map(map_prompt_version_row))
    }
}

// ---------------------------------------------------------------------------
// Row mapping
// ---------------------------------------------------------------------------

/// Maps a raw `sqlx::Row` from a prompt version query into a
/// [`PromptVersion`].
fn map_prompt_version_row(r: &sqlx::postgres::PgRow) -> PromptVersion {
    PromptVersion::builder()
        .id(PromptVersionId::from(r.get::<uuid::Uuid, _>("id")))
        .stage(
            r.get::<String, _>("stage")
                .parse::<PromptStage>()
                .expect(UNKNOWN_PROMPT_STAGE_IN_DB),
        )
        .content_hash(r.get("content_hash"))
        .content(r.get("content"))
        .created_at(r.get("created_at"))
        .build()
}
