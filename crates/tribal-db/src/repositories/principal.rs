//! Principal repository: trait definition and Postgres implementation.
//!
//! Principals are user or agent identities attributed to every write
//! operation.  The repository provides insert and lookup operations.
//! `find_by_key` returns `Option<Principal>` because absence is a valid
//! outcome in find-or-create flows.

use async_trait::async_trait;
use sqlx::PgConnection;
use tribal_domain::{Principal, PrincipalId};
use typed_builder::TypedBuilder;

use crate::DbError;

/// Input for creating a new principal.
///
/// Contains only caller-provided fields.  Server-generated values
/// (`id`, `created_at`) are produced by Postgres via `DEFAULT`
/// clauses and returned via `RETURNING *`.
#[derive(Debug, TypedBuilder)]
pub struct NewPrincipal {
    /// Human-readable key (e.g. `"user:sam"`, `"principal:local"`).
    pub principal_key: String,
    /// Optional display name.
    #[builder(default)]
    pub display_name: Option<String>,
}

/// Data access operations for principals.
///
/// All methods take `&mut PgConnection` as an explicit executor,
/// keeping the repository pool-agnostic.
#[async_trait]
pub trait PrincipalRepository {
    /// Inserts a new principal and returns the fully populated domain type.
    ///
    /// # Errors
    ///
    /// Returns [`DbError::UniqueViolation`] if a principal with the same
    /// `principal_key` already exists.  Returns [`DbError::QueryFailed`]
    /// on other database errors.
    async fn insert(
        &self,
        conn: &mut PgConnection,
        new_principal: &NewPrincipal,
    ) -> Result<Principal, DbError>;

    /// Finds a principal by its ID.
    ///
    /// # Errors
    ///
    /// Returns [`DbError::NotFound`] if no principal with the given ID
    /// exists.  Returns [`DbError::QueryFailed`] on database errors.
    async fn find_by_id(
        &self,
        conn: &mut PgConnection,
        id: PrincipalId,
    ) -> Result<Principal, DbError>;

    /// Finds a principal by its key (e.g. `"user:sam"`).
    ///
    /// Returns `None` if no matching principal exists.  Absence is a
    /// valid outcome (used in find-or-create flows), not an error.
    ///
    /// # Errors
    ///
    /// Returns [`DbError::QueryFailed`] on database errors.
    async fn find_by_key(
        &self,
        conn: &mut PgConnection,
        principal_key: &str,
    ) -> Result<Option<Principal>, DbError>;
}

/// Postgres implementation of [`PrincipalRepository`].
///
/// A zero-sized type with no internal state.
pub struct PgPrincipalRepository;

#[async_trait]
impl PrincipalRepository for PgPrincipalRepository {
    async fn insert(
        &self,
        conn: &mut PgConnection,
        new_principal: &NewPrincipal,
    ) -> Result<Principal, DbError> {
        let row = sqlx::query!(
            r#"
            INSERT INTO principals (principal_key, display_name)
            VALUES ($1, $2)
            RETURNING *
            "#,
            new_principal.principal_key,
            new_principal.display_name,
        )
        .fetch_one(&mut *conn)
        .await;

        match row {
            Ok(r) => Ok(Principal::builder()
                .id(PrincipalId::from(r.id))
                .principal_key(r.principal_key)
                .display_name(r.display_name)
                .created_at(r.created_at)
                .build()),
            Err(e) => {
                if let Some(uv) = super::try_into_unique_violation(&e) {
                    Err(uv)
                } else {
                    Err(DbError::QueryFailed {
                        context: "inserting principal".to_owned(),
                        source: e,
                    })
                }
            }
        }
    }

    async fn find_by_id(
        &self,
        conn: &mut PgConnection,
        id: PrincipalId,
    ) -> Result<Principal, DbError> {
        let r = sqlx::query!(r#"SELECT * FROM principals WHERE id = $1"#, id.inner())
            .fetch_optional(&mut *conn)
            .await
            .map_err(|e| DbError::QueryFailed {
                context: format!("finding principal by id {id}"),
                source: e,
            })?
            .ok_or_else(|| DbError::NotFound {
                entity: "principal",
                id: id.to_string(),
            })?;

        Ok(Principal::builder()
            .id(PrincipalId::from(r.id))
            .principal_key(r.principal_key)
            .display_name(r.display_name)
            .created_at(r.created_at)
            .build())
    }

    async fn find_by_key(
        &self,
        conn: &mut PgConnection,
        principal_key: &str,
    ) -> Result<Option<Principal>, DbError> {
        let r = sqlx::query!(
            r#"SELECT * FROM principals WHERE principal_key = $1"#,
            principal_key,
        )
        .fetch_optional(&mut *conn)
        .await
        .map_err(|e| DbError::QueryFailed {
            context: format!("finding principal by key '{principal_key}'"),
            source: e,
        })?;

        Ok(r.map(|r| {
            Principal::builder()
                .id(PrincipalId::from(r.id))
                .principal_key(r.principal_key)
                .display_name(r.display_name)
                .created_at(r.created_at)
                .build()
        }))
    }
}
