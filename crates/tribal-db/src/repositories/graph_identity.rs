//! Database-owned graph identity.

use async_trait::async_trait;
use sqlx::PgConnection;
use tribal_domain::GraphId;

use crate::DbError;

#[async_trait]
pub trait GraphIdentityRepository {
    /// Reads the database's one immutable graph identity.
    ///
    /// # Errors
    ///
    /// Returns [`DbError::GraphIdentityMissing`] when the row is absent —
    /// an uninitialised or behind database — and [`DbError::QueryFailed`]
    /// on database errors.
    async fn get(&self, conn: &mut PgConnection) -> Result<GraphId, DbError>;
}

/// Postgres implementation of [`GraphIdentityRepository`].
pub struct PgGraphIdentityRepository;

#[async_trait]
impl GraphIdentityRepository for PgGraphIdentityRepository {
    async fn get(&self, conn: &mut PgConnection) -> Result<GraphId, DbError> {
        let row = sqlx::query!("SELECT graph_id FROM graph_identity")
            .fetch_optional(&mut *conn)
            .await
            .map_err(|source| DbError::QueryFailed {
                context: "reading graph identity".to_owned(),
                source,
            })?;
        row.map(|row| GraphId::from(row.graph_id))
            .ok_or(DbError::GraphIdentityMissing)
    }
}
