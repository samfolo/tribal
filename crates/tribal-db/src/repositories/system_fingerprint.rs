//! System fingerprint repository: trait definition and Postgres
//! implementation.
//!
//! System fingerprints are content-addressed and immutable. The upsert
//! operation either creates a new fingerprint or returns the existing row
//! when the `content_hash` already exists.

use async_trait::async_trait;
use sqlx::{PgConnection, Row};
use tribal_domain::{PipelineParameters, SystemFingerprint, SystemFingerprintId};
use typed_builder::TypedBuilder;

use super::common::columns::Columns;
use crate::DbError;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// The column's CHECK forbids non-positive dimensions; a negative value
/// here is database corruption, not an expected state.
const NEGATIVE_DIMENSIONS_IN_DB: &str =
    "system_fingerprints.embedding_dimensions is negative despite its CHECK";

const COLUMNS: Columns = Columns(&[
    "id",
    "content_hash",
    "build_version",
    "extraction_binding_hash",
    "triage_binding_hash",
    "relation_binding_hash",
    "embedding_provider",
    "embedding_model",
    "embedding_dimensions",
    "pipeline_parameters",
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

    // -- Stage binding versions ------------------------------------------------
    /// The extraction stage's binding-version content address.
    pub extraction_binding_hash: String,
    /// The triage stage's binding-version content address.
    pub triage_binding_hash: String,
    /// The relation stage's binding-version content address.
    pub relation_binding_hash: String,

    // -- Embedding identity ------------------------------------------------
    /// Embedding provider name.
    pub embedding_provider: String,
    /// Embedding model name.
    pub embedding_model: String,
    /// Embedding vector dimensionality.
    pub embedding_dimensions: u32,

    // -- Pipeline parameters ----------------------------------------------
    /// Serialised pipeline parameters (JSONB).
    pub pipeline_parameters: serde_json::Value,
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
        let embedding_dimensions =
            i32::try_from(new.embedding_dimensions).map_err(|_| DbError::QueryFailed {
                context: format!(
                    "embedding dimensions {} exceed the column range",
                    new.embedding_dimensions
                ),
                source: sqlx::Error::Protocol("embedding_dimensions out of range".into()),
            })?;

        let sql = format!(
            "INSERT INTO system_fingerprints (\
                 content_hash, build_version, \
                 extraction_binding_hash, triage_binding_hash, relation_binding_hash, \
                 embedding_provider, embedding_model, embedding_dimensions, \
                 pipeline_parameters\
             ) VALUES (\
                 $1, $2, $3, $4, $5, $6, $7, $8, $9\
             ) \
             ON CONFLICT (content_hash) DO NOTHING \
             RETURNING {COLUMNS}",
        );

        let row = sqlx::query(&sql)
            .bind(&new.content_hash)
            .bind(&new.build_version)
            .bind(&new.extraction_binding_hash)
            .bind(&new.triage_binding_hash)
            .bind(&new.relation_binding_hash)
            .bind(&new.embedding_provider)
            .bind(&new.embedding_model)
            .bind(embedding_dimensions)
            .bind(&new.pipeline_parameters)
            .fetch_optional(&mut *conn)
            .await
            .map_err(|e| DbError::QueryFailed {
                context: "upserting system fingerprint".to_owned(),
                source: e,
            })?;

        if let Some(r) = row {
            return map_system_fingerprint_row(&r);
        }

        // Conflict path — fingerprint already exists.
        let sql = format!("SELECT {COLUMNS} FROM system_fingerprints WHERE content_hash = $1");

        let r = sqlx::query(&sql)
            .bind(&new.content_hash)
            .fetch_one(&mut *conn)
            .await
            .map_err(|e| DbError::QueryFailed {
                context: "finding existing system fingerprint after conflict".to_owned(),
                source: e,
            })?;

        map_system_fingerprint_row(&r)
    }

    async fn find_by_hash(
        &self,
        conn: &mut PgConnection,
        content_hash: &str,
    ) -> Result<Option<SystemFingerprint>, DbError> {
        let sql = format!("SELECT {COLUMNS} FROM system_fingerprints WHERE content_hash = $1");

        let row = sqlx::query(&sql)
            .bind(content_hash)
            .fetch_optional(&mut *conn)
            .await
            .map_err(|e| DbError::QueryFailed {
                context: "finding system fingerprint by hash".to_owned(),
                source: e,
            })?;

        row.as_ref().map(map_system_fingerprint_row).transpose()
    }
}

// ---------------------------------------------------------------------------
// Row mapping
// ---------------------------------------------------------------------------

/// Maps a raw `sqlx::Row` from a system fingerprint query into a
/// [`SystemFingerprint`].
fn map_system_fingerprint_row(r: &sqlx::postgres::PgRow) -> Result<SystemFingerprint, DbError> {
    let params_value: serde_json::Value = r.get("pipeline_parameters");
    let pipeline_parameters: PipelineParameters =
        serde_json::from_value(params_value).map_err(|e| DbError::QueryFailed {
            context: format!("deserialising pipeline_parameters for system fingerprint: {e}"),
            source: sqlx::Error::Decode(Box::new(e)),
        })?;

    let embedding_dimensions =
        u32::try_from(r.get::<i32, _>("embedding_dimensions")).expect(NEGATIVE_DIMENSIONS_IN_DB);

    Ok(SystemFingerprint::builder()
        .id(SystemFingerprintId::from(r.get::<uuid::Uuid, _>("id")))
        .content_hash(r.get("content_hash"))
        .build_version(r.get("build_version"))
        .extraction_binding_hash(r.get("extraction_binding_hash"))
        .triage_binding_hash(r.get("triage_binding_hash"))
        .relation_binding_hash(r.get("relation_binding_hash"))
        .embedding_provider(r.get("embedding_provider"))
        .embedding_model(r.get("embedding_model"))
        .embedding_dimensions(embedding_dimensions)
        .pipeline_parameters(pipeline_parameters)
        .created_at(r.get("created_at"))
        .build())
}
