//! OAuth client repository: trait definition and Postgres implementation.
//!
//! Stores the records produced by the dynamic client registration
//! `/register` endpoint.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use sqlx::{PgConnection, Row};
use tribal_domain::{ApplicationType, OauthClient, TokenEndpointAuthMethod};
use typed_builder::TypedBuilder;

use super::common::columns::Columns;
use crate::DbError;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const COLUMNS: Columns = Columns(&[
    "client_id",
    "client_secret_hash",
    "client_name",
    "redirect_uris",
    "grant_types",
    "response_types",
    "token_endpoint_auth_method",
    "scope",
    "application_type",
    "created_at",
    "client_secret_expires_at",
]);

// ---------------------------------------------------------------------------
// Input
// ---------------------------------------------------------------------------

/// Input for inserting a new OAuth client record.
#[derive(Debug, Clone, TypedBuilder)]
pub struct NewOauthClient {
    /// Opaque client identifier returned at registration.
    pub client_id: String,
    /// SHA-256 hex digest of the issued secret; `None` for public clients.
    #[builder(default)]
    pub client_secret_hash: Option<String>,
    /// Human-readable client name.
    #[builder(default)]
    pub client_name: Option<String>,
    /// Registered redirect URIs.
    pub redirect_uris: Vec<String>,
    /// Grant types declared at registration.
    pub grant_types: Vec<String>,
    /// Response types declared at registration.
    pub response_types: Vec<String>,
    /// Token endpoint auth method.
    pub token_endpoint_auth_method: TokenEndpointAuthMethod,
    /// Optional declared scope.
    #[builder(default)]
    pub scope: Option<String>,
    /// Optional declared application type.
    #[builder(default)]
    pub application_type: Option<ApplicationType>,
    /// Client secret expiry timestamp, when applicable.
    #[builder(default)]
    pub client_secret_expires_at: Option<DateTime<Utc>>,
}

// ---------------------------------------------------------------------------
// Trait
// ---------------------------------------------------------------------------

/// Data access operations for OAuth clients.
#[async_trait]
pub trait OauthClientRepository {
    /// Inserts a new client record.
    ///
    /// # Errors
    ///
    /// Returns [`DbError::UniqueViolation`] when `client_id` already
    /// exists. Returns [`DbError::QueryFailed`] on other failures.
    async fn insert(
        &self,
        conn: &mut PgConnection,
        new: &NewOauthClient,
    ) -> Result<OauthClient, DbError>;

    /// Finds a client by identifier.
    ///
    /// # Errors
    ///
    /// Returns [`DbError::QueryFailed`] on database errors.
    async fn find_by_id(
        &self,
        conn: &mut PgConnection,
        client_id: &str,
    ) -> Result<Option<OauthClient>, DbError>;
}

// ---------------------------------------------------------------------------
// Postgres implementation
// ---------------------------------------------------------------------------

/// Postgres implementation of [`OauthClientRepository`].
pub struct PgOauthClientRepository;

#[async_trait]
impl OauthClientRepository for PgOauthClientRepository {
    async fn insert(
        &self,
        conn: &mut PgConnection,
        new: &NewOauthClient,
    ) -> Result<OauthClient, DbError> {
        let sql = format!(
            "INSERT INTO oauth_clients ( \
                client_id, client_secret_hash, client_name, redirect_uris, \
                grant_types, response_types, token_endpoint_auth_method, \
                scope, application_type, client_secret_expires_at \
             ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10) \
             RETURNING {COLUMNS}",
        );

        let result = sqlx::query(&sql)
            .bind(&new.client_id)
            .bind(&new.client_secret_hash)
            .bind(&new.client_name)
            .bind(&new.redirect_uris)
            .bind(&new.grant_types)
            .bind(&new.response_types)
            .bind(new.token_endpoint_auth_method.as_str())
            .bind(&new.scope)
            .bind(new.application_type.map(ApplicationType::as_str))
            .bind(new.client_secret_expires_at)
            .fetch_one(&mut *conn)
            .await;

        match result {
            Ok(row) => Ok(map_oauth_client_row(&row)),
            Err(e) => match super::common::constraint::try_into_unique_violation(&e) {
                Some(uv) => Err(uv),
                None => Err(DbError::QueryFailed {
                    context: "inserting oauth client".to_owned(),
                    source: e,
                }),
            },
        }
    }

    async fn find_by_id(
        &self,
        conn: &mut PgConnection,
        client_id: &str,
    ) -> Result<Option<OauthClient>, DbError> {
        let sql = format!("SELECT {COLUMNS} FROM oauth_clients WHERE client_id = $1");

        let row = sqlx::query(&sql)
            .bind(client_id)
            .fetch_optional(&mut *conn)
            .await
            .map_err(|e| DbError::QueryFailed {
                context: format!("finding oauth client by id '{client_id}'"),
                source: e,
            })?;

        Ok(row.as_ref().map(map_oauth_client_row))
    }
}

// ---------------------------------------------------------------------------
// Row mapping
// ---------------------------------------------------------------------------

const EXPECT_VALID_AUTH_METHOD: &str = "invariant: database contains valid auth method";
const EXPECT_VALID_APPLICATION_TYPE: &str = "invariant: database contains valid application type";

fn map_oauth_client_row(r: &sqlx::postgres::PgRow) -> OauthClient {
    let auth_method_str: String = r.get("token_endpoint_auth_method");
    let auth_method = TokenEndpointAuthMethod::parse(&auth_method_str)
        .unwrap_or_else(|| panic!("{EXPECT_VALID_AUTH_METHOD}: {auth_method_str:?}"));

    let application_type: Option<ApplicationType> =
        r.get::<Option<String>, _>("application_type").map(|s| {
            ApplicationType::parse(&s)
                .unwrap_or_else(|| panic!("{EXPECT_VALID_APPLICATION_TYPE}: {s:?}"))
        });

    OauthClient::builder()
        .client_id(r.get("client_id"))
        .client_secret_hash(r.get("client_secret_hash"))
        .client_name(r.get("client_name"))
        .redirect_uris(r.get("redirect_uris"))
        .grant_types(r.get("grant_types"))
        .response_types(r.get("response_types"))
        .token_endpoint_auth_method(auth_method)
        .scope(r.get("scope"))
        .application_type(application_type)
        .created_at(r.get("created_at"))
        .client_secret_expires_at(r.get("client_secret_expires_at"))
        .build()
}
