//! Per-profile partial HNSW index operations for the embedding tables.
//!
//! Each embedding profile gets its own partial expression HNSW index, named by
//! epoch and built with `CREATE INDEX CONCURRENTLY` outside a transaction. The
//! three-way state check ([`IndexState`]) lets first-boot provisioning and the
//! reindex worker resume an interrupted build idempotently: absent builds,
//! `INVALID` (a crashed prior build) drops and rebuilds, valid skips. Raw
//! `sqlx::query()` is used because the DDL interpolates the profile UUID and
//! dimension as literals (a partial index predicate cannot be parameterised).

use async_trait::async_trait;
use sqlx::PgConnection;
use tribal_domain::EmbeddingProfileId;

use crate::DbError;

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/// The two tables that carry per-profile embedding indexes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EmbeddingTable {
    /// `embeddings` — knowledge-item vectors.
    Embeddings,
    /// `tag_embeddings` — tag-registry vectors.
    TagEmbeddings,
}

impl EmbeddingTable {
    /// The SQL table name. A fixed string, never interpolated from input.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Embeddings => "embeddings",
            Self::TagEmbeddings => "tag_embeddings",
        }
    }

    /// The partial HNSW index name for this table and profile epoch.
    #[must_use]
    pub fn hnsw_index_name(self, epoch: i64) -> String {
        format!("idx_{}_hnsw_epoch{epoch}", self.as_str())
    }
}

/// The catalogue state of a per-profile index, the input to the three-way
/// resume decision.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexState {
    /// No index of this name exists; build it.
    Absent,
    /// An index exists but is `INVALID` (a crashed concurrent build); drop and
    /// rebuild it.
    Invalid,
    /// A valid index of this name exists; the build phase already completed.
    Valid,
}

// ---------------------------------------------------------------------------
// Trait
// ---------------------------------------------------------------------------

/// Operations over the per-profile partial HNSW indexes.
///
/// All methods take `&mut PgConnection`; the concurrent build and drop must run
/// outside a transaction, which is the caller's responsibility.
#[async_trait]
pub trait EmbeddingIndexRepository {
    /// Returns the catalogue state of the named index.
    ///
    /// # Errors
    ///
    /// Returns [`DbError::QueryFailed`] on database errors.
    async fn index_state(
        &self,
        conn: &mut PgConnection,
        index_name: &str,
    ) -> Result<IndexState, DbError>;

    /// Ensures a valid per-profile partial HNSW index exists for the profile,
    /// resuming an interrupted build via the three-way state check.
    ///
    /// Must run outside a transaction (`CREATE INDEX CONCURRENTLY`).
    ///
    /// # Errors
    ///
    /// Returns [`DbError::QueryFailed`] on database errors.
    async fn ensure_partial_hnsw(
        &self,
        conn: &mut PgConnection,
        table: EmbeddingTable,
        epoch: i64,
        dimensions: u32,
        profile_id: EmbeddingProfileId,
    ) -> Result<(), DbError>;

    /// Drops a per-profile partial HNSW index if it exists, used when pruning a
    /// retired profile's tombstone.
    ///
    /// Must run outside a transaction (`DROP INDEX CONCURRENTLY`).
    ///
    /// # Errors
    ///
    /// Returns [`DbError::QueryFailed`] on database errors.
    async fn drop_partial_hnsw(
        &self,
        conn: &mut PgConnection,
        table: EmbeddingTable,
        epoch: i64,
    ) -> Result<(), DbError>;
}

// ---------------------------------------------------------------------------
// Postgres implementation
// ---------------------------------------------------------------------------

/// Postgres implementation of [`EmbeddingIndexRepository`].
///
/// A zero-sized type with no internal state.
pub struct PgEmbeddingIndexRepository;

#[async_trait]
impl EmbeddingIndexRepository for PgEmbeddingIndexRepository {
    async fn index_state(
        &self,
        conn: &mut PgConnection,
        index_name: &str,
    ) -> Result<IndexState, DbError> {
        let state: String = sqlx::query_scalar(
            "SELECT CASE \
                 WHEN to_regclass($1) IS NULL THEN 'absent' \
                 WHEN (SELECT indisvalid FROM pg_index WHERE indexrelid = to_regclass($1)) \
                     THEN 'valid' \
                 ELSE 'invalid' \
             END",
        )
        .bind(index_name)
        .fetch_one(&mut *conn)
        .await
        .map_err(|e| DbError::QueryFailed {
            context: format!("reading index state for {index_name}"),
            source: e,
        })?;

        Ok(match state.as_str() {
            "absent" => IndexState::Absent,
            "valid" => IndexState::Valid,
            _ => IndexState::Invalid,
        })
    }

    async fn ensure_partial_hnsw(
        &self,
        conn: &mut PgConnection,
        table: EmbeddingTable,
        epoch: i64,
        dimensions: u32,
        profile_id: EmbeddingProfileId,
    ) -> Result<(), DbError> {
        let index_name = table.hnsw_index_name(epoch);

        match self.index_state(conn, &index_name).await? {
            IndexState::Valid => return Ok(()),
            IndexState::Invalid => {
                drop_index_concurrently(conn, &index_name).await?;
            }
            IndexState::Absent => {}
        }

        // The profile UUID is rendered from a typed value, never untrusted text,
        // and the table/index names come from the fixed EmbeddingTable strings.
        let create = format!(
            "CREATE INDEX CONCURRENTLY {index_name} \
             ON {table} USING hnsw ((embedding::halfvec({dimensions})) halfvec_cosine_ops) \
             WHERE embedding_profile_id = '{profile}'::uuid",
            table = table.as_str(),
            profile = profile_id.inner(),
        );

        sqlx::query(&create)
            .execute(&mut *conn)
            .await
            .map_err(|e| DbError::QueryFailed {
                context: format!("building partial HNSW index {index_name}"),
                source: e,
            })?;

        Ok(())
    }

    async fn drop_partial_hnsw(
        &self,
        conn: &mut PgConnection,
        table: EmbeddingTable,
        epoch: i64,
    ) -> Result<(), DbError> {
        drop_index_concurrently(conn, &table.hnsw_index_name(epoch)).await
    }
}

async fn drop_index_concurrently(conn: &mut PgConnection, index_name: &str) -> Result<(), DbError> {
    sqlx::query(&format!("DROP INDEX CONCURRENTLY IF EXISTS {index_name}"))
        .execute(&mut *conn)
        .await
        .map_err(|e| DbError::QueryFailed {
            context: format!("dropping index {index_name}"),
            source: e,
        })?;
    Ok(())
}
