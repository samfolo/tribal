//! System fingerprint repository: trait definition and Postgres
//! implementation.
//!
//! System fingerprints are content-addressed and immutable. The upsert
//! operation either creates a new fingerprint or returns the existing row
//! when the `content_hash` already exists.

use async_trait::async_trait;
use sqlx::{PgConnection, Row};
use tribal_domain::{InferenceParameters, PromptVersionId, SystemFingerprint, SystemFingerprintId};
use typed_builder::TypedBuilder;

use super::common::columns::Columns;
use crate::DbError;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const COLUMNS: Columns = Columns(&[
    "id",
    "content_hash",
    "build_version",
    "extraction_system_prompt_version_id",
    "extraction_user_prompt_version_id",
    "triage_system_prompt_version_id",
    "triage_user_prompt_version_id",
    "relation_system_prompt_version_id",
    "relation_user_prompt_version_id",
    "extraction_inference_provider",
    "extraction_inference_model",
    "triage_inference_provider",
    "triage_inference_model",
    "relation_inference_provider",
    "relation_inference_model",
    "embedding_provider",
    "embedding_model",
    "inference_parameters",
    "created_at",
]);

// ---------------------------------------------------------------------------
// Input types
// ---------------------------------------------------------------------------

/// Input for upserting a system fingerprint.
///
/// Contains only caller-provided fields. Server-generated values
/// (`id`, `created_at`) are produced by Postgres via `DEFAULT`
/// clauses and returned via `RETURNING`.
#[derive(Debug, Clone, TypedBuilder)]
pub struct NewSystemFingerprint {
    /// Pre-computed SHA-256 content hash.
    pub content_hash: String,
    /// Build version from `TRIBAL_GIT_DESCRIBE`.
    pub build_version: String,

    // -- Prompt version IDs ---------------------------------------------------
    /// Extraction system prompt version.
    pub extraction_system_prompt_version_id: PromptVersionId,
    /// Extraction user prompt version.
    pub extraction_user_prompt_version_id: PromptVersionId,
    /// Triage system prompt version.
    pub triage_system_prompt_version_id: PromptVersionId,
    /// Triage user prompt version.
    pub triage_user_prompt_version_id: PromptVersionId,
    /// Relation system prompt version.
    pub relation_system_prompt_version_id: PromptVersionId,
    /// Relation user prompt version.
    pub relation_user_prompt_version_id: PromptVersionId,

    // -- Model identifiers ----------------------------------------------------
    /// Extraction inference provider name.
    pub extraction_inference_provider: String,
    /// Extraction inference model name.
    pub extraction_inference_model: String,
    /// Triage inference provider name.
    pub triage_inference_provider: String,
    /// Triage inference model name.
    pub triage_inference_model: String,
    /// Relation inference provider name.
    pub relation_inference_provider: String,
    /// Relation inference model name.
    pub relation_inference_model: String,
    /// Embedding provider name.
    pub embedding_provider: String,
    /// Embedding model name.
    pub embedding_model: String,

    // -- Inference parameters --------------------------------------------------
    /// Serialised inference parameters (JSONB).
    pub inference_parameters: serde_json::Value,
}

// ---------------------------------------------------------------------------
// Trait
// ---------------------------------------------------------------------------

/// Data access operations for system fingerprints.
///
/// All methods take `&mut PgConnection` as an explicit executor,
/// keeping the repository pool-agnostic. System fingerprints are
/// content-addressed and immutable — the upsert returns the existing
/// row when the content hash matches.
#[async_trait]
pub trait SystemFingerprintRepository {
    /// Inserts a new system fingerprint or returns the existing row when
    /// the `content_hash` already exists.
    ///
    /// Uses a two-step approach: INSERT ON CONFLICT DO NOTHING, then
    /// SELECT if no row was returned.
    ///
    /// # Errors
    ///
    /// Returns [`DbError::QueryFailed`] on database errors.
    async fn upsert(
        &self,
        conn: &mut PgConnection,
        new: &NewSystemFingerprint,
    ) -> Result<SystemFingerprint, DbError>;

    /// Finds a system fingerprint by its content hash.
    ///
    /// Returns `Ok(None)` when no match exists.
    ///
    /// # Errors
    ///
    /// Returns [`DbError::QueryFailed`] on database errors.
    async fn find_by_hash(
        &self,
        conn: &mut PgConnection,
        content_hash: &str,
    ) -> Result<Option<SystemFingerprint>, DbError>;
}

// ---------------------------------------------------------------------------
// Postgres implementation
// ---------------------------------------------------------------------------

/// Postgres implementation of [`SystemFingerprintRepository`].
///
/// A zero-sized type with no internal state.
pub struct PgSystemFingerprintRepository;

#[async_trait]
impl SystemFingerprintRepository for PgSystemFingerprintRepository {
    async fn upsert(
        &self,
        conn: &mut PgConnection,
        new: &NewSystemFingerprint,
    ) -> Result<SystemFingerprint, DbError> {
        let sql = format!(
            "INSERT INTO system_fingerprints (\
                 content_hash, build_version, \
                 extraction_system_prompt_version_id, extraction_user_prompt_version_id, \
                 triage_system_prompt_version_id, triage_user_prompt_version_id, \
                 relation_system_prompt_version_id, relation_user_prompt_version_id, \
                 extraction_inference_provider, extraction_inference_model, \
                 triage_inference_provider, triage_inference_model, \
                 relation_inference_provider, relation_inference_model, \
                 embedding_provider, embedding_model, \
                 inference_parameters\
             ) VALUES (\
                 $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17\
             ) \
             ON CONFLICT (content_hash) DO NOTHING \
             RETURNING {COLUMNS}",
        );

        let row = sqlx::query(&sql)
            .bind(&new.content_hash)
            .bind(&new.build_version)
            .bind(new.extraction_system_prompt_version_id.inner())
            .bind(new.extraction_user_prompt_version_id.inner())
            .bind(new.triage_system_prompt_version_id.inner())
            .bind(new.triage_user_prompt_version_id.inner())
            .bind(new.relation_system_prompt_version_id.inner())
            .bind(new.relation_user_prompt_version_id.inner())
            .bind(&new.extraction_inference_provider)
            .bind(&new.extraction_inference_model)
            .bind(&new.triage_inference_provider)
            .bind(&new.triage_inference_model)
            .bind(&new.relation_inference_provider)
            .bind(&new.relation_inference_model)
            .bind(&new.embedding_provider)
            .bind(&new.embedding_model)
            .bind(&new.inference_parameters)
            .fetch_optional(&mut *conn)
            .await
            .map_err(|e| DbError::QueryFailed {
                context: "upserting system fingerprint".to_owned(),
                source: e,
            })?;

        if let Some(r) = row {
            return Ok(map_system_fingerprint_row(&r));
        }

        // Conflict path — fingerprint already exists.
        let sql = format!("SELECT {COLUMNS} FROM system_fingerprints WHERE content_hash = $1",);

        let r = sqlx::query(&sql)
            .bind(&new.content_hash)
            .fetch_one(&mut *conn)
            .await
            .map_err(|e| DbError::QueryFailed {
                context: "finding existing system fingerprint after conflict".to_owned(),
                source: e,
            })?;

        Ok(map_system_fingerprint_row(&r))
    }

    async fn find_by_hash(
        &self,
        conn: &mut PgConnection,
        content_hash: &str,
    ) -> Result<Option<SystemFingerprint>, DbError> {
        let sql = format!("SELECT {COLUMNS} FROM system_fingerprints WHERE content_hash = $1",);

        let row = sqlx::query(&sql)
            .bind(content_hash)
            .fetch_optional(&mut *conn)
            .await
            .map_err(|e| DbError::QueryFailed {
                context: "finding system fingerprint by hash".to_owned(),
                source: e,
            })?;

        Ok(row.as_ref().map(map_system_fingerprint_row))
    }
}

// ---------------------------------------------------------------------------
// Row mapping
// ---------------------------------------------------------------------------

/// Maps a raw `sqlx::Row` from a system fingerprint query into a
/// [`SystemFingerprint`].
fn map_system_fingerprint_row(r: &sqlx::postgres::PgRow) -> SystemFingerprint {
    let params_value: serde_json::Value = r.get("inference_parameters");
    let inference_parameters: InferenceParameters =
        serde_json::from_value(params_value).unwrap_or_default();

    SystemFingerprint::builder()
        .id(SystemFingerprintId::from(r.get::<uuid::Uuid, _>("id")))
        .content_hash(r.get("content_hash"))
        .build_version(r.get("build_version"))
        .extraction_system_prompt_version_id(PromptVersionId::from(
            r.get::<uuid::Uuid, _>("extraction_system_prompt_version_id"),
        ))
        .extraction_user_prompt_version_id(PromptVersionId::from(
            r.get::<uuid::Uuid, _>("extraction_user_prompt_version_id"),
        ))
        .triage_system_prompt_version_id(PromptVersionId::from(
            r.get::<uuid::Uuid, _>("triage_system_prompt_version_id"),
        ))
        .triage_user_prompt_version_id(PromptVersionId::from(
            r.get::<uuid::Uuid, _>("triage_user_prompt_version_id"),
        ))
        .relation_system_prompt_version_id(PromptVersionId::from(
            r.get::<uuid::Uuid, _>("relation_system_prompt_version_id"),
        ))
        .relation_user_prompt_version_id(PromptVersionId::from(
            r.get::<uuid::Uuid, _>("relation_user_prompt_version_id"),
        ))
        .extraction_inference_provider(r.get("extraction_inference_provider"))
        .extraction_inference_model(r.get("extraction_inference_model"))
        .triage_inference_provider(r.get("triage_inference_provider"))
        .triage_inference_model(r.get("triage_inference_model"))
        .relation_inference_provider(r.get("relation_inference_provider"))
        .relation_inference_model(r.get("relation_inference_model"))
        .embedding_provider(r.get("embedding_provider"))
        .embedding_model(r.get("embedding_model"))
        .inference_parameters(inference_parameters)
        .created_at(r.get("created_at"))
        .build()
}
