//! Auth token repository: trait definition and Postgres implementation.
//!
//! Auth tokens use SHA-256 hashes for storage and lookup.  Revocation
//! is terminal — `revoked_at` is set once and never cleared.  The
//! verification logic (checking expiry, revocation, and resolving the
//! principal) belongs to auth middleware, not to this repository.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use sqlx::{PgConnection, Row};
use tribal_domain::{AuthToken, AuthTokenId, PrincipalId};
use typed_builder::TypedBuilder;

use super::common::columns::columns;
use crate::DbError;

// ---------------------------------------------------------------------------
// Input types
// ---------------------------------------------------------------------------

/// Input for creating a new auth token record.
///
/// Contains only caller-provided fields.  Server-generated values
/// (`id`, `created_at`) are produced by Postgres via `DEFAULT`
/// clauses and returned via `RETURNING`.
#[derive(Debug, TypedBuilder)]
pub struct NewAuthToken {
    /// SHA-256 hex-encoded token hash (64 chars, lowercase).
    pub token_hash: String,
    /// The principal this token authenticates.
    pub principal_id: PrincipalId,
    /// When this token expires.
    pub expires_at: DateTime<Utc>,
}

// ---------------------------------------------------------------------------
// Trait
// ---------------------------------------------------------------------------

/// Data access operations for auth tokens.
///
/// All methods take `&mut PgConnection` as an explicit executor,
/// keeping the repository pool-agnostic.
#[async_trait]
pub trait AuthTokenRepository {
    /// Inserts a new auth token and returns the populated domain type.
    ///
    /// # Errors
    ///
    /// Returns [`DbError::UniqueViolation`] if the token hash already exists.
    /// Returns [`DbError::QueryFailed`] on other database errors.
    async fn insert(
        &self,
        conn: &mut PgConnection,
        new: &NewAuthToken,
    ) -> Result<AuthToken, DbError>;

    /// Finds an auth token by its hash.
    ///
    /// Returns `Ok(None)` when no token matches — a missing token is
    /// a normal authentication outcome (invalid token), not an error.
    ///
    /// # Errors
    ///
    /// Returns [`DbError::QueryFailed`] on database errors.
    async fn find_by_hash(
        &self,
        conn: &mut PgConnection,
        token_hash: &str,
    ) -> Result<Option<AuthToken>, DbError>;

    /// Finds all tokens for a principal, ordered by `created_at DESC`.
    ///
    /// Returns an empty vec when no tokens exist.
    ///
    /// # Errors
    ///
    /// Returns [`DbError::QueryFailed`] on database errors.
    async fn find_by_principal_id(
        &self,
        conn: &mut PgConnection,
        principal_id: PrincipalId,
    ) -> Result<Vec<AuthToken>, DbError>;

    /// Revokes a token by setting `revoked_at`.
    ///
    /// Idempotent: revoking an already-revoked token returns it with
    /// the original `revoked_at` unchanged.
    ///
    /// # Errors
    ///
    /// Returns [`DbError::NotFound`] if the token ID does not exist.
    /// Returns [`DbError::QueryFailed`] on database errors.
    async fn revoke(
        &self,
        conn: &mut PgConnection,
        id: AuthTokenId,
        revoked_at: DateTime<Utc>,
    ) -> Result<AuthToken, DbError>;
}

// ---------------------------------------------------------------------------
// Postgres implementation
// ---------------------------------------------------------------------------

/// Postgres implementation of [`AuthTokenRepository`].
///
/// A zero-sized type with no internal state.
pub struct PgAuthTokenRepository;

const COLUMNS: &[&str] = &[
    "id",
    "token_hash",
    "principal_id",
    "expires_at",
    "created_at",
    "revoked_at",
];

#[async_trait]
impl AuthTokenRepository for PgAuthTokenRepository {
    async fn insert(
        &self,
        conn: &mut PgConnection,
        new: &NewAuthToken,
    ) -> Result<AuthToken, DbError> {
        let sql = format!(
            "INSERT INTO auth_tokens (token_hash, principal_id, expires_at) \
             VALUES ($1, $2, $3) \
             RETURNING {columns}",
            columns = columns(COLUMNS),
        );

        let result = sqlx::query(&sql)
            .bind(&new.token_hash)
            .bind(new.principal_id.inner())
            .bind(new.expires_at)
            .fetch_one(&mut *conn)
            .await;

        match result {
            Ok(row) => Ok(map_auth_token_row(&row)),
            Err(e) => {
                if let Some(uv) = super::common::constraint::try_into_unique_violation(&e) {
                    Err(uv)
                } else {
                    Err(DbError::QueryFailed {
                        context: "inserting auth token".to_owned(),
                        source: e,
                    })
                }
            }
        }
    }

    async fn find_by_hash(
        &self,
        conn: &mut PgConnection,
        token_hash: &str,
    ) -> Result<Option<AuthToken>, DbError> {
        let sql = format!(
            "SELECT {columns} FROM auth_tokens WHERE token_hash = $1",
            columns = columns(COLUMNS),
        );

        let row = sqlx::query(&sql)
            .bind(token_hash)
            .fetch_optional(&mut *conn)
            .await
            .map_err(|e| DbError::QueryFailed {
                context: "finding auth token by hash".to_owned(),
                source: e,
            })?;

        Ok(row.as_ref().map(map_auth_token_row))
    }

    async fn find_by_principal_id(
        &self,
        conn: &mut PgConnection,
        principal_id: PrincipalId,
    ) -> Result<Vec<AuthToken>, DbError> {
        let sql = format!(
            "SELECT {columns} FROM auth_tokens \
             WHERE principal_id = $1 \
             ORDER BY created_at DESC",
            columns = columns(COLUMNS),
        );

        let rows = sqlx::query(&sql)
            .bind(principal_id.inner())
            .fetch_all(&mut *conn)
            .await
            .map_err(|e| DbError::QueryFailed {
                context: format!("finding auth tokens for principal {principal_id}"),
                source: e,
            })?;

        Ok(rows.iter().map(map_auth_token_row).collect())
    }

    async fn revoke(
        &self,
        conn: &mut PgConnection,
        id: AuthTokenId,
        revoked_at: DateTime<Utc>,
    ) -> Result<AuthToken, DbError> {
        // Conditional update — only sets revoked_at when not already revoked.
        sqlx::query(
            "UPDATE auth_tokens SET revoked_at = $2 \
             WHERE id = $1 AND revoked_at IS NULL",
        )
        .bind(id.inner())
        .bind(revoked_at)
        .execute(&mut *conn)
        .await
        .map_err(|e| DbError::QueryFailed {
            context: format!("revoking auth token {id}"),
            source: e,
        })?;

        // Fetch current state regardless of whether the update affected rows.
        let sql = format!(
            "SELECT {columns} FROM auth_tokens WHERE id = $1",
            columns = columns(COLUMNS),
        );

        let row = sqlx::query(&sql)
            .bind(id.inner())
            .fetch_optional(&mut *conn)
            .await
            .map_err(|e| DbError::QueryFailed {
                context: format!("finding auth token after revoke {id}"),
                source: e,
            })?
            .ok_or_else(|| DbError::NotFound {
                entity: "auth_token",
                id: id.to_string(),
            })?;

        Ok(map_auth_token_row(&row))
    }
}

// ---------------------------------------------------------------------------
// Row mapping
// ---------------------------------------------------------------------------

/// Maps a raw `sqlx::Row` from an auth token query into an
/// [`AuthToken`].
fn map_auth_token_row(r: &sqlx::postgres::PgRow) -> AuthToken {
    AuthToken::builder()
        .id(AuthTokenId::from(r.get::<uuid::Uuid, _>("id")))
        .token_hash(r.get("token_hash"))
        .principal_id(PrincipalId::from(r.get::<uuid::Uuid, _>("principal_id")))
        .expires_at(r.get("expires_at"))
        .created_at(r.get("created_at"))
        .revoked_at(r.get("revoked_at"))
        .build()
}
