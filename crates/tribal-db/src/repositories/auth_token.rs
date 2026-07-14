//! Auth token repository: trait definition and Postgres implementation.
//!
//! Auth tokens use SHA-256 hashes for storage and lookup.  Revocation
//! is terminal — `revoked_at` is set once and never cleared.  The
//! verification logic (checking expiry, revocation, and resolving the
//! principal) belongs to auth middleware, not to this repository.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use sqlx::{PgConnection, Row};
use tribal_domain::{AuthToken, AuthTokenId, PrincipalId, Scope};
use typed_builder::TypedBuilder;

use super::common::columns::Columns;
use crate::DbError;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const COLUMNS: Columns = Columns(&[
    "id",
    "token_hash",
    "principal_id",
    "scopes",
    "audience",
    "expires_at",
    "created_at",
    "revoked_at",
]);

// ---------------------------------------------------------------------------
// Input types
// ---------------------------------------------------------------------------

/// Input for creating a new auth token record.
///
/// Contains only caller-provided fields. Server-generated values
/// (`id`, `created_at`) are produced by Postgres via `DEFAULT`
/// clauses and returned via `RETURNING`.
#[derive(Debug, Clone, TypedBuilder)]
pub struct NewAuthToken {
    /// SHA-256 hex-encoded token hash (64 chars, lowercase).
    pub token_hash: String,
    /// The principal this token authenticates.
    pub principal_id: PrincipalId,
    /// Permission scopes granted to this token.
    pub scopes: Vec<Scope>,
    /// Canonical resource URL this token is audience-bound to per RFC 8707.
    pub audience: String,
    /// When this token expires.
    pub expires_at: DateTime<Utc>,
}

/// Stable keyset position for token inventory.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AuthTokenPageKey {
    pub created_at: DateTime<Utc>,
    pub id: AuthTokenId,
}

/// Token row joined to the principal key rendered to operators.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthTokenInventoryRow {
    pub token: AuthToken,
    pub principal: String,
}

#[derive(sqlx::FromRow)]
struct AuthTokenInventoryDbRow {
    id: uuid::Uuid,
    token_hash: String,
    principal_id: uuid::Uuid,
    scopes: Vec<String>,
    audience: String,
    expires_at: DateTime<Utc>,
    created_at: DateTime<Utc>,
    revoked_at: Option<DateTime<Utc>>,
    principal_key: String,
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

    /// Finds a token by its stable id.
    async fn find_by_id(
        &self,
        conn: &mut PgConnection,
        id: AuthTokenId,
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
    /// Returns `(token, true)` when this call performed the revocation, or
    /// `(token, false)` when the token was already revoked (idempotent).
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
    ) -> Result<(AuthToken, bool), DbError>;

    /// Returns all auth tokens, ordered by `created_at DESC`.
    ///
    /// Returns an empty vec when no tokens exist.
    ///
    /// # Errors
    ///
    /// Returns [`DbError::QueryFailed`] on database errors.
    async fn find_all(&self, conn: &mut PgConnection) -> Result<Vec<AuthToken>, DbError>;

    /// Batch-revokes all active tokens, optionally filtered by principal.
    ///
    /// Only tokens that are neither revoked nor expired are affected.
    /// Returns the count of newly-revoked tokens.
    ///
    /// # Errors
    ///
    /// Returns [`DbError::QueryFailed`] on database errors.
    async fn revoke_all(
        &self,
        conn: &mut PgConnection,
        principal_id: Option<PrincipalId>,
        revoked_at: DateTime<Utc>,
    ) -> Result<u64, DbError>;

    /// Captures the newest token visible at the start of an inventory walk.
    async fn page_high_water(
        &self,
        conn: &mut PgConnection,
    ) -> Result<Option<AuthTokenPageKey>, DbError>;

    /// Lists one descending token keyset window bounded by a high water.
    async fn list_page(
        &self,
        conn: &mut PgConnection,
        high_water: AuthTokenPageKey,
        after: Option<AuthTokenPageKey>,
        limit: u16,
    ) -> Result<Vec<AuthTokenInventoryRow>, DbError>;
}

// ---------------------------------------------------------------------------
// Postgres implementation
// ---------------------------------------------------------------------------

/// Postgres implementation of [`AuthTokenRepository`].
///
/// A zero-sized type with no internal state.
pub struct PgAuthTokenRepository;

#[async_trait]
impl AuthTokenRepository for PgAuthTokenRepository {
    async fn insert(
        &self,
        conn: &mut PgConnection,
        new: &NewAuthToken,
    ) -> Result<AuthToken, DbError> {
        let sql = format!(
            "INSERT INTO auth_tokens (token_hash, principal_id, scopes, audience, expires_at) \
             VALUES ($1, $2, $3, $4, $5) \
             RETURNING {COLUMNS}",
        );

        let scope_strings: Vec<&str> = new.scopes.iter().map(Scope::as_str).collect();

        let result = sqlx::query(&sql)
            .bind(&new.token_hash)
            .bind(new.principal_id.inner())
            .bind(&scope_strings)
            .bind(&new.audience)
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
        let sql = format!("SELECT {COLUMNS} FROM auth_tokens WHERE token_hash = $1");

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

    async fn find_by_id(
        &self,
        conn: &mut PgConnection,
        id: AuthTokenId,
    ) -> Result<Option<AuthToken>, DbError> {
        let sql = format!("SELECT {COLUMNS} FROM auth_tokens WHERE id = $1");
        let row = sqlx::query(&sql)
            .bind(id.inner())
            .fetch_optional(&mut *conn)
            .await
            .map_err(|source| DbError::QueryFailed {
                context: format!("finding auth token by id {id}"),
                source,
            })?;
        Ok(row.as_ref().map(map_auth_token_row))
    }

    async fn find_by_principal_id(
        &self,
        conn: &mut PgConnection,
        principal_id: PrincipalId,
    ) -> Result<Vec<AuthToken>, DbError> {
        let sql = format!(
            "SELECT {COLUMNS} FROM auth_tokens \
             WHERE principal_id = $1 \
             ORDER BY created_at DESC, id",
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
    ) -> Result<(AuthToken, bool), DbError> {
        // Conditional update — only sets revoked_at when not already revoked.
        let affected = sqlx::query(
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
        })?
        .rows_affected();

        // Fetch current state regardless of whether the update affected rows.
        let sql = format!("SELECT {COLUMNS} FROM auth_tokens WHERE id = $1");

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

        Ok((map_auth_token_row(&row), affected > 0))
    }

    async fn find_all(&self, conn: &mut PgConnection) -> Result<Vec<AuthToken>, DbError> {
        let sql = format!("SELECT {COLUMNS} FROM auth_tokens ORDER BY created_at DESC, id");

        let rows =
            sqlx::query(&sql)
                .fetch_all(&mut *conn)
                .await
                .map_err(|e| DbError::QueryFailed {
                    context: "listing all auth tokens".to_owned(),
                    source: e,
                })?;

        Ok(rows.iter().map(map_auth_token_row).collect())
    }

    async fn revoke_all(
        &self,
        conn: &mut PgConnection,
        principal_id: Option<PrincipalId>,
        revoked_at: DateTime<Utc>,
    ) -> Result<u64, DbError> {
        let result = if let Some(pid) = principal_id {
            sqlx::query(
                "UPDATE auth_tokens SET revoked_at = $1 \
                 WHERE revoked_at IS NULL AND expires_at >= $1 AND principal_id = $2",
            )
            .bind(revoked_at)
            .bind(pid.inner())
            .execute(&mut *conn)
            .await
            .map_err(|e| DbError::QueryFailed {
                context: format!("revoking all active auth tokens for principal {pid}"),
                source: e,
            })?
        } else {
            sqlx::query(
                "UPDATE auth_tokens SET revoked_at = $1 \
                 WHERE revoked_at IS NULL AND expires_at >= $1",
            )
            .bind(revoked_at)
            .execute(&mut *conn)
            .await
            .map_err(|e| DbError::QueryFailed {
                context: "revoking all active auth tokens".to_owned(),
                source: e,
            })?
        };

        Ok(result.rows_affected())
    }

    async fn page_high_water(
        &self,
        conn: &mut PgConnection,
    ) -> Result<Option<AuthTokenPageKey>, DbError> {
        let row = sqlx::query!(
            r#"
            SELECT created_at, id
            FROM auth_tokens
            ORDER BY created_at DESC, id DESC
            LIMIT 1
            "#
        )
        .fetch_optional(&mut *conn)
        .await
        .map_err(|source| DbError::QueryFailed {
            context: "capturing token inventory high water".to_owned(),
            source,
        })?;
        Ok(row.map(|row| AuthTokenPageKey {
            created_at: row.created_at,
            id: AuthTokenId::from(row.id),
        }))
    }

    async fn list_page(
        &self,
        conn: &mut PgConnection,
        high_water: AuthTokenPageKey,
        after: Option<AuthTokenPageKey>,
        limit: u16,
    ) -> Result<Vec<AuthTokenInventoryRow>, DbError> {
        let rows = match after {
            Some(after) => {
                sqlx::query_as!(
                    AuthTokenInventoryDbRow,
                    r#"
                    SELECT t.id, t.token_hash, t.principal_id, t.scopes,
                           t.audience, t.expires_at, t.created_at, t.revoked_at,
                           p.principal_key
                    FROM auth_tokens t
                    JOIN principals p ON p.id = t.principal_id
                    WHERE (t.created_at, t.id) <= ($1, $2)
                      AND (t.created_at, t.id) < ($3, $4)
                    ORDER BY t.created_at DESC, t.id DESC
                    LIMIT $5
                    "#,
                    high_water.created_at,
                    high_water.id.inner(),
                    after.created_at,
                    after.id.inner(),
                    i64::from(limit),
                )
                .fetch_all(&mut *conn)
                .await
            }
            None => {
                sqlx::query_as!(
                    AuthTokenInventoryDbRow,
                    r#"
                    SELECT t.id, t.token_hash, t.principal_id, t.scopes,
                           t.audience, t.expires_at, t.created_at, t.revoked_at,
                           p.principal_key
                    FROM auth_tokens t
                    JOIN principals p ON p.id = t.principal_id
                    WHERE (t.created_at, t.id) <= ($1, $2)
                    ORDER BY t.created_at DESC, t.id DESC
                    LIMIT $3
                    "#,
                    high_water.created_at,
                    high_water.id.inner(),
                    i64::from(limit),
                )
                .fetch_all(&mut *conn)
                .await
            }
        }
        .map_err(|source| DbError::QueryFailed {
            context: "listing a token inventory page".to_owned(),
            source,
        })?;
        Ok(rows.into_iter().map(map_inventory_row).collect())
    }
}

// ---------------------------------------------------------------------------
// Row mapping
// ---------------------------------------------------------------------------

const EXPECT_VALID_SCOPE_IN_DB: &str = "invariant: database contains valid scopes";

/// Maps a raw `sqlx::Row` from an auth token query into an
/// [`AuthToken`].
///
/// # Panics
///
/// Panics if a scope value stored in the database fails to parse. The
/// application only writes validated scopes, so invalid values indicate
/// data corruption.
fn map_auth_token_row(r: &sqlx::postgres::PgRow) -> AuthToken {
    let scope_strings: Vec<String> = r.get("scopes");
    let scopes = scope_strings
        .iter()
        .map(|s| Scope::parse(s).unwrap_or_else(|_| panic!("{EXPECT_VALID_SCOPE_IN_DB}: {s:?}")))
        .collect();

    AuthToken::builder()
        .id(AuthTokenId::from(r.get::<uuid::Uuid, _>("id")))
        .token_hash(r.get("token_hash"))
        .principal_id(PrincipalId::from(r.get::<uuid::Uuid, _>("principal_id")))
        .scopes(scopes)
        .audience(r.get("audience"))
        .expires_at(r.get("expires_at"))
        .created_at(r.get("created_at"))
        .revoked_at(r.get("revoked_at"))
        .build()
}

fn map_inventory_row(row: AuthTokenInventoryDbRow) -> AuthTokenInventoryRow {
    let scopes = row
        .scopes
        .into_iter()
        .map(|scope| {
            Scope::parse(&scope).unwrap_or_else(|_| panic!("{EXPECT_VALID_SCOPE_IN_DB}: {scope:?}"))
        })
        .collect();
    AuthTokenInventoryRow {
        token: AuthToken::builder()
            .id(AuthTokenId::from(row.id))
            .token_hash(row.token_hash)
            .principal_id(PrincipalId::from(row.principal_id))
            .scopes(scopes)
            .audience(row.audience)
            .expires_at(row.expires_at)
            .created_at(row.created_at)
            .revoked_at(row.revoked_at)
            .build(),
        principal: row.principal_key,
    }
}
