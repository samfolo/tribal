//! Tag registry repository: trait definition and Postgres implementation.
//!
//! The tag registry is a global (not per-project) catalogue of canonical
//! tags.  Upsert semantics use `INSERT ... ON CONFLICT DO NOTHING` for
//! idempotent writes.

use async_trait::async_trait;
use sqlx::{PgConnection, Row};
use tribal_domain::TagRegistryEntry;

use super::common::columns::Columns;
use crate::DbError;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const COLUMNS: Columns = Columns(&["tag", "first_seen_at", "last_seen_at", "usage_count"]);

// ---------------------------------------------------------------------------
// Trait
// ---------------------------------------------------------------------------

/// Data access operations for the global tag registry.
///
/// All methods take `&mut PgConnection` as an explicit executor,
/// keeping the repository pool-agnostic.
#[async_trait]
pub trait TagRegistryRepository {
    /// Inserts a tag into the registry if it does not already exist and
    /// returns the entry.
    ///
    /// Uses `INSERT ... ON CONFLICT DO NOTHING` followed by a `SELECT`
    /// so that both new and pre-existing tags are returned.
    ///
    /// # Errors
    ///
    /// Returns [`DbError::QueryFailed`] on database errors.
    async fn upsert(&self, conn: &mut PgConnection, tag: &str)
    -> Result<TagRegistryEntry, DbError>;

    /// Inserts multiple tags into the registry idempotently and returns
    /// all corresponding entries ordered by `tag`.
    ///
    /// Both newly inserted and pre-existing tags are returned.
    /// An empty input slice returns an empty `Vec` without issuing a query.
    ///
    /// # Errors
    ///
    /// Returns [`DbError::QueryFailed`] on database errors.
    async fn batch_upsert(
        &self,
        conn: &mut PgConnection,
        tags: &[String],
    ) -> Result<Vec<TagRegistryEntry>, DbError>;

    /// Returns every entry in the tag registry ordered by `tag`.
    ///
    /// # Errors
    ///
    /// Returns [`DbError::QueryFailed`] on database errors.
    async fn find_all(&self, conn: &mut PgConnection) -> Result<Vec<TagRegistryEntry>, DbError>;
}

// ---------------------------------------------------------------------------
// Postgres implementation
// ---------------------------------------------------------------------------

/// Postgres implementation of [`TagRegistryRepository`].
///
/// A zero-sized type with no internal state.
pub struct PgTagRegistryRepository;

#[async_trait]
impl TagRegistryRepository for PgTagRegistryRepository {
    async fn upsert(
        &self,
        conn: &mut PgConnection,
        tag: &str,
    ) -> Result<TagRegistryEntry, DbError> {
        sqlx::query(
            "INSERT INTO tag_registry (tag) VALUES ($1) \
             ON CONFLICT (tag) DO NOTHING",
        )
        .bind(tag)
        .execute(&mut *conn)
        .await
        .map_err(|e| DbError::QueryFailed {
            context: format!("upserting tag {tag:?}"),
            source: e,
        })?;

        let sql = format!("SELECT {COLUMNS} FROM tag_registry WHERE tag = $1");

        let row = sqlx::query(&sql)
            .bind(tag)
            .fetch_one(&mut *conn)
            .await
            .map_err(|e| DbError::QueryFailed {
                context: format!("selecting tag {tag:?} after upsert"),
                source: e,
            })?;

        Ok(map_tag_registry_entry_row(&row))
    }

    async fn batch_upsert(
        &self,
        conn: &mut PgConnection,
        tags: &[String],
    ) -> Result<Vec<TagRegistryEntry>, DbError> {
        if tags.is_empty() {
            return Ok(Vec::new());
        }

        // Two statements rather than a CTE: in Postgres, a data-modifying
        // CTE and its outer SELECT share the same snapshot, so the SELECT
        // cannot see rows the CTE just inserted.
        sqlx::query(
            "INSERT INTO tag_registry (tag) \
             SELECT * FROM UNNEST($1::text[]) \
             ON CONFLICT (tag) DO NOTHING",
        )
        .bind(tags)
        .execute(&mut *conn)
        .await
        .map_err(|e| DbError::QueryFailed {
            context: format!("batch upserting {} tags", tags.len()),
            source: e,
        })?;

        let sql = format!("SELECT {COLUMNS} FROM tag_registry WHERE tag = ANY($1) ORDER BY tag");

        let rows = sqlx::query(&sql)
            .bind(tags)
            .fetch_all(&mut *conn)
            .await
            .map_err(|e| DbError::QueryFailed {
                context: format!("selecting {} tags after batch upsert", tags.len()),
                source: e,
            })?;

        Ok(rows.iter().map(map_tag_registry_entry_row).collect())
    }

    async fn find_all(&self, conn: &mut PgConnection) -> Result<Vec<TagRegistryEntry>, DbError> {
        let sql = format!("SELECT {COLUMNS} FROM tag_registry ORDER BY tag");

        let rows =
            sqlx::query(&sql)
                .fetch_all(&mut *conn)
                .await
                .map_err(|e| DbError::QueryFailed {
                    context: "listing all tags".to_owned(),
                    source: e,
                })?;

        Ok(rows.iter().map(map_tag_registry_entry_row).collect())
    }
}

// ---------------------------------------------------------------------------
// Row mapping
// ---------------------------------------------------------------------------

/// Maps a raw `sqlx::Row` from a tag registry query into a [`TagRegistryEntry`].
fn map_tag_registry_entry_row(r: &sqlx::postgres::PgRow) -> TagRegistryEntry {
    TagRegistryEntry::builder()
        .tag(r.get("tag"))
        .first_seen_at(r.get("first_seen_at"))
        .build()
}
