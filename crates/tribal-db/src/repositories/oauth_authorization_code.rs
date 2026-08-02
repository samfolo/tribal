//! OAuth authorisation-code repository: trait definition and Postgres impl.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use sqlx::{PgConnection, Row};
use tribal_domain::{OauthAuthorizationCode, PrincipalId};
use typed_builder::TypedBuilder;

use super::common::columns::Columns;
use crate::DbError;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const COLUMNS: Columns = Columns(&[
    "code_hash",
    "client_id",
    "redirect_uri",
    "code_challenge",
    "scope",
    "resource",
    "principal_id",
    "issued_at",
    "expires_at",
    "consumed_at",
]);

// ---------------------------------------------------------------------------
// Input
// ---------------------------------------------------------------------------

/// Input for inserting a new authorisation code.
#[derive(Debug, Clone, TypedBuilder)]
pub struct NewOauthAuthorizationCode {
    /// SHA-256 hex digest of the raw code.
    pub code_hash: String,
    /// Client identifier.
    pub client_id: String,
    /// Redirect URI.
    pub redirect_uri: String,
    /// S256 code challenge.
    pub code_challenge: String,
    /// Optional declared scope.
    #[builder(default)]
    pub scope: Option<String>,
    /// Optional declared resource indicator (RFC 8707).
    #[builder(default)]
    pub resource: Option<String>,
    /// Principal identifier.
    pub principal_id: PrincipalId,
    /// Expiry timestamp.
    pub expires_at: DateTime<Utc>,
}

// ---------------------------------------------------------------------------
// Trait
// ---------------------------------------------------------------------------

/// Data access operations for OAuth authorisation codes.
#[async_trait]
pub trait OauthAuthorizationCodeRepository {
    /// Inserts a new code record.
    ///
    /// # Errors
    ///
    /// Returns [`DbError::UniqueViolation`] on hash collision.
    /// Returns [`DbError::QueryFailed`] on other failures.
    async fn insert(
        &self,
        conn: &mut PgConnection,
        new: &NewOauthAuthorizationCode,
    ) -> Result<OauthAuthorizationCode, DbError>;

    /// Atomically marks a code as consumed and returns it. Returns
    /// `Ok(None)` when the code does not exist, is expired, or was
    /// already consumed; the same `None` value covers replay attempts
    /// and missing codes so the application response is uniform per
    /// RFC 6749 the custody contract.2 (`invalid_grant`).
    ///
    /// # Errors
    ///
    /// Returns [`DbError::QueryFailed`] on database errors.
    async fn consume_by_hash(
        &self,
        conn: &mut PgConnection,
        code_hash: &str,
        now: DateTime<Utc>,
    ) -> Result<Option<OauthAuthorizationCode>, DbError>;
}

// ---------------------------------------------------------------------------
// Postgres implementation
// ---------------------------------------------------------------------------

/// Postgres implementation of [`OauthAuthorizationCodeRepository`].
pub struct PgOauthAuthorizationCodeRepository;

#[async_trait]
impl OauthAuthorizationCodeRepository for PgOauthAuthorizationCodeRepository {
    async fn insert(
        &self,
        conn: &mut PgConnection,
        new: &NewOauthAuthorizationCode,
    ) -> Result<OauthAuthorizationCode, DbError> {
        let sql = format!(
            "INSERT INTO oauth_authorization_codes ( \
                code_hash, client_id, redirect_uri, code_challenge, \
                code_challenge_method, scope, resource, principal_id, expires_at \
             ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9) \
             RETURNING {COLUMNS}",
        );

        let result = sqlx::query(&sql)
            .bind(&new.code_hash)
            .bind(&new.client_id)
            .bind(&new.redirect_uri)
            .bind(&new.code_challenge)
            .bind(OauthAuthorizationCode::CODE_CHALLENGE_METHOD_S256)
            .bind(&new.scope)
            .bind(&new.resource)
            .bind(new.principal_id.inner())
            .bind(new.expires_at)
            .fetch_one(&mut *conn)
            .await;

        match result {
            Ok(row) => Ok(map_code_row(&row)),
            Err(e) => match super::common::constraint::try_into_unique_violation(&e) {
                Some(uv) => Err(uv),
                None => Err(DbError::QueryFailed {
                    context: "inserting oauth authorization code".to_owned(),
                    source: e,
                }),
            },
        }
    }

    async fn consume_by_hash(
        &self,
        conn: &mut PgConnection,
        code_hash: &str,
        now: DateTime<Utc>,
    ) -> Result<Option<OauthAuthorizationCode>, DbError> {
        let sql = format!(
            "UPDATE oauth_authorization_codes \
             SET consumed_at = $2 \
             WHERE code_hash = $1 AND consumed_at IS NULL AND expires_at > $2 \
             RETURNING {COLUMNS}",
        );

        let row = sqlx::query(&sql)
            .bind(code_hash)
            .bind(now)
            .fetch_optional(&mut *conn)
            .await
            .map_err(|e| DbError::QueryFailed {
                context: "consuming oauth authorization code".to_owned(),
                source: e,
            })?;

        Ok(row.as_ref().map(map_code_row))
    }
}

// ---------------------------------------------------------------------------
// Row mapping
// ---------------------------------------------------------------------------

fn map_code_row(r: &sqlx::postgres::PgRow) -> OauthAuthorizationCode {
    OauthAuthorizationCode::builder()
        .code_hash(r.get("code_hash"))
        .client_id(r.get("client_id"))
        .redirect_uri(r.get("redirect_uri"))
        .code_challenge(r.get("code_challenge"))
        .scope(r.get("scope"))
        .resource(r.get("resource"))
        .principal_id(PrincipalId::from(r.get::<uuid::Uuid, _>("principal_id")))
        .issued_at(r.get("issued_at"))
        .expires_at(r.get("expires_at"))
        .consumed_at(r.get("consumed_at"))
        .build()
}
