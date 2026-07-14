//! Namespace-bound default credential mapping.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use sqlx::PgConnection;
use tribal_domain::{AuthTokenId, CredentialGenerationId};

use crate::DbError;

/// Durable join between one configuration authority and its default token.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocalDefaultCredential {
    pub authority_namespace: String,
    pub generation_id: CredentialGenerationId,
    pub token_id: AuthTokenId,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[async_trait]
pub trait LocalDefaultCredentialRepository {
    /// Finds the mapping for an authority namespace.
    async fn find(
        &self,
        conn: &mut PgConnection,
        authority_namespace: &str,
    ) -> Result<Option<LocalDefaultCredential>, DbError>;

    /// Inserts or replaces the mapping held under the caller's transaction lock.
    async fn replace(
        &self,
        conn: &mut PgConnection,
        authority_namespace: &str,
        generation_id: CredentialGenerationId,
        token_id: AuthTokenId,
    ) -> Result<LocalDefaultCredential, DbError>;

    /// Deletes only the named namespace's mapping.
    async fn delete(
        &self,
        conn: &mut PgConnection,
        authority_namespace: &str,
    ) -> Result<bool, DbError>;
}

/// Postgres implementation of [`LocalDefaultCredentialRepository`].
pub struct PgLocalDefaultCredentialRepository;

#[async_trait]
impl LocalDefaultCredentialRepository for PgLocalDefaultCredentialRepository {
    async fn find(
        &self,
        conn: &mut PgConnection,
        authority_namespace: &str,
    ) -> Result<Option<LocalDefaultCredential>, DbError> {
        let row = sqlx::query!(
            r#"
            SELECT authority_namespace, generation_id, token_id, created_at, updated_at
            FROM local_default_credentials
            WHERE authority_namespace = $1
            "#,
            authority_namespace,
        )
        .fetch_optional(&mut *conn)
        .await
        .map_err(|source| DbError::QueryFailed {
            context: "finding local default credential".to_owned(),
            source,
        })?;
        Ok(row.map(|row| LocalDefaultCredential {
            authority_namespace: row.authority_namespace,
            generation_id: CredentialGenerationId::from(row.generation_id),
            token_id: AuthTokenId::from(row.token_id),
            created_at: row.created_at,
            updated_at: row.updated_at,
        }))
    }

    async fn replace(
        &self,
        conn: &mut PgConnection,
        authority_namespace: &str,
        generation_id: CredentialGenerationId,
        token_id: AuthTokenId,
    ) -> Result<LocalDefaultCredential, DbError> {
        let row = sqlx::query!(
            r#"
            INSERT INTO local_default_credentials
                (authority_namespace, generation_id, token_id)
            VALUES ($1, $2, $3)
            ON CONFLICT (authority_namespace) DO UPDATE
            SET generation_id = EXCLUDED.generation_id,
                token_id = EXCLUDED.token_id,
                updated_at = now()
            RETURNING authority_namespace, generation_id, token_id, created_at, updated_at
            "#,
            authority_namespace,
            generation_id.inner(),
            token_id.inner(),
        )
        .fetch_one(&mut *conn)
        .await
        .map_err(|source| DbError::QueryFailed {
            context: "replacing local default credential".to_owned(),
            source,
        })?;
        Ok(LocalDefaultCredential {
            authority_namespace: row.authority_namespace,
            generation_id: CredentialGenerationId::from(row.generation_id),
            token_id: AuthTokenId::from(row.token_id),
            created_at: row.created_at,
            updated_at: row.updated_at,
        })
    }

    async fn delete(
        &self,
        conn: &mut PgConnection,
        authority_namespace: &str,
    ) -> Result<bool, DbError> {
        let result = sqlx::query!(
            "DELETE FROM local_default_credentials WHERE authority_namespace = $1",
            authority_namespace,
        )
        .execute(&mut *conn)
        .await
        .map_err(|source| DbError::QueryFailed {
            context: "deleting local default credential".to_owned(),
            source,
        })?;
        Ok(result.rows_affected() == 1)
    }
}
