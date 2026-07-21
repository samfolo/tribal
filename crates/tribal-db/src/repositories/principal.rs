//! Principal repository: trait definition and Postgres implementation.
//!
//! Principals are user or agent identities attributed to every write
//! operation.  The repository provides insert and lookup operations.
//! `find_by_key` returns `Option<Principal>` because absence is a valid
//! outcome in find-or-create flows.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use sqlx::PgConnection;
use tribal_domain::{PlatformBinding, Principal, PrincipalId};
use typed_builder::TypedBuilder;

use crate::DbError;

/// Input for creating a new principal.
///
/// Contains only caller-provided fields.  Server-generated values
/// (`id`, `created_at`) are produced by Postgres via `DEFAULT`
/// clauses and returned via `RETURNING *`.
#[derive(Debug, Clone, TypedBuilder)]
pub struct NewPrincipal {
    /// Human-readable key (e.g. `"user:sam"`, `"principal:local"`).
    pub principal_key: String,
    /// Optional display name. Must be `None` when `platform_binding` is set —
    /// a platform-bound principal derives its name and stores none (enforced by
    /// a CHECK on `principals`).
    #[builder(default)]
    pub display_name: Option<String>,
    /// The platform `(user, account)` to bind, or `None` for a local-only
    /// principal.
    #[builder(default)]
    pub platform_binding: Option<PlatformBinding>,
}

/// Database-observed result of ensuring a local principal.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EnsurePrincipalOutcome {
    /// This call inserted the principal.
    Inserted(Principal),
    /// The principal already existed.
    Existing(Principal),
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

    /// Ensures a local principal exists without aborting a racing transaction.
    ///
    /// # Errors
    ///
    /// Returns [`DbError::NotFound`] if a conflicting write leaves no matching
    /// principal and [`DbError::QueryFailed`] on database errors.
    async fn ensure_local_by_key(
        &self,
        conn: &mut PgConnection,
        principal_key: &str,
    ) -> Result<EnsurePrincipalOutcome, DbError>;

    /// Finds the principal bound to a platform `(user, account)`, creating it
    /// if none exists, and returns it. Keyed on the binding, so two users in
    /// one account resolve to two distinct principals and a re-resolve returns
    /// the same one. Idempotent under concurrency through the binding's unique
    /// index — a racing second caller reads the existing row rather than
    /// minting a second principal.
    ///
    /// # Errors
    ///
    /// Returns [`DbError::QueryFailed`] on database errors.
    async fn find_or_create_platform_bound(
        &self,
        conn: &mut PgConnection,
        binding: &PlatformBinding,
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

    /// Finds multiple principals by their IDs.
    ///
    /// Returns the principals that were found, in no guaranteed order.
    /// Missing IDs are silently omitted from the result (not an error).
    ///
    /// # Errors
    ///
    /// Returns [`DbError::QueryFailed`] on database errors.
    async fn find_by_ids(
        &self,
        conn: &mut PgConnection,
        ids: &[PrincipalId],
    ) -> Result<Vec<Principal>, DbError>;
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
        let account_reference = new_principal
            .platform_binding
            .as_ref()
            .map(PlatformBinding::account_reference);
        let platform_user_id = new_principal
            .platform_binding
            .as_ref()
            .map(PlatformBinding::platform_user_id);

        let row = sqlx::query!(
            r#"
            INSERT INTO principals (principal_key, display_name, account_reference, platform_user_id)
            VALUES ($1, $2, $3, $4)
            RETURNING *
            "#,
            new_principal.principal_key,
            new_principal.display_name,
            account_reference,
            platform_user_id,
        )
        .fetch_one(&mut *conn)
        .await;

        match row {
            Ok(r) => Ok(build_principal(
                r.id,
                r.principal_key,
                r.display_name,
                r.account_reference,
                r.platform_user_id,
                r.created_at,
            )),
            Err(e) => {
                if let Some(uv) = super::common::constraint::try_into_unique_violation(&e) {
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

    async fn ensure_local_by_key(
        &self,
        conn: &mut PgConnection,
        principal_key: &str,
    ) -> Result<EnsurePrincipalOutcome, DbError> {
        let inserted = sqlx::query!(
            r#"
            INSERT INTO principals (principal_key)
            VALUES ($1)
            ON CONFLICT DO NOTHING
            RETURNING *
            "#,
            principal_key,
        )
        .fetch_optional(&mut *conn)
        .await
        .map_err(|source| DbError::QueryFailed {
            context: format!("ensuring local principal '{principal_key}'"),
            source,
        })?;

        if let Some(row) = inserted {
            return Ok(EnsurePrincipalOutcome::Inserted(build_principal(
                row.id,
                row.principal_key,
                row.display_name,
                row.account_reference,
                row.platform_user_id,
                row.created_at,
            )));
        }

        self.find_by_key(conn, principal_key)
            .await?
            .map(EnsurePrincipalOutcome::Existing)
            .ok_or_else(|| DbError::NotFound {
                entity: "principal",
                id: principal_key.to_owned(),
            })
    }

    async fn find_or_create_platform_bound(
        &self,
        conn: &mut PgConnection,
        binding: &PlatformBinding,
    ) -> Result<Principal, DbError> {
        // A stable key derived one-to-one from the binding. The account
        // reference is length-prefixed so two distinct bindings whose opaque
        // tokens straddle the delimiter cannot collide onto one key.
        let principal_key = format!(
            "platform:{}:{}/{}",
            binding.account_reference().len(),
            binding.account_reference(),
            binding.platform_user_id(),
        );

        // A bound row hits two unique indexes at once — the total principal_key
        // and the partial binding index — so a single-target ON CONFLICT would
        // let a racer trip the other and error. Targetless DO NOTHING suppresses
        // a conflict on either without error: it inserts when new, and returns
        // no row when a concurrent resolve already created it.
        let inserted = sqlx::query!(
            r#"
            INSERT INTO principals (principal_key, platform_user_id, account_reference)
            VALUES ($1, $2, $3)
            ON CONFLICT DO NOTHING
            RETURNING *
            "#,
            principal_key,
            binding.platform_user_id(),
            binding.account_reference(),
        )
        .fetch_optional(&mut *conn)
        .await
        .map_err(|e| DbError::QueryFailed {
            context: format!(
                "creating platform-bound principal for user {}",
                binding.platform_user_id(),
            ),
            source: e,
        })?;

        if let Some(r) = inserted {
            return Ok(build_principal(
                r.id,
                r.principal_key,
                r.display_name,
                r.account_reference,
                r.platform_user_id,
                r.created_at,
            ));
        }

        // The conflict means the row already exists; read it back by binding.
        let r = sqlx::query!(
            r#"
            SELECT * FROM principals
            WHERE platform_user_id = $1 AND account_reference = $2
            "#,
            binding.platform_user_id(),
            binding.account_reference(),
        )
        .fetch_one(&mut *conn)
        .await
        .map_err(|e| DbError::QueryFailed {
            context: format!(
                "reading platform-bound principal for user {}",
                binding.platform_user_id(),
            ),
            source: e,
        })?;

        Ok(build_principal(
            r.id,
            r.principal_key,
            r.display_name,
            r.account_reference,
            r.platform_user_id,
            r.created_at,
        ))
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

        Ok(build_principal(
            r.id,
            r.principal_key,
            r.display_name,
            r.account_reference,
            r.platform_user_id,
            r.created_at,
        ))
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
            build_principal(
                r.id,
                r.principal_key,
                r.display_name,
                r.account_reference,
                r.platform_user_id,
                r.created_at,
            )
        }))
    }

    async fn find_by_ids(
        &self,
        conn: &mut PgConnection,
        ids: &[PrincipalId],
    ) -> Result<Vec<Principal>, DbError> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }

        let raw_ids: Vec<uuid::Uuid> = ids.iter().map(|id| *id.inner()).collect();

        let rows = sqlx::query!(r#"SELECT * FROM principals WHERE id = ANY($1)"#, &raw_ids)
            .fetch_all(&mut *conn)
            .await
            .map_err(|e| DbError::QueryFailed {
                context: format!("finding principals by {} ids", ids.len()),
                source: e,
            })?;

        Ok(rows
            .into_iter()
            .map(|r| {
                build_principal(
                    r.id,
                    r.principal_key,
                    r.display_name,
                    r.account_reference,
                    r.platform_user_id,
                    r.created_at,
                )
            })
            .collect())
    }
}

// ---------------------------------------------------------------------------
// Row mapping
// ---------------------------------------------------------------------------

/// Builds a [`Principal`] from its stored columns, folding the paired linkage
/// columns into an [`Option<PlatformBinding>`].
fn build_principal(
    id: uuid::Uuid,
    principal_key: String,
    display_name: Option<String>,
    account_reference: Option<String>,
    platform_user_id: Option<String>,
    created_at: DateTime<Utc>,
) -> Principal {
    Principal::builder()
        .id(PrincipalId::from(id))
        .principal_key(principal_key)
        .display_name(display_name)
        .platform_binding(platform_binding_from_columns(
            account_reference,
            platform_user_id,
        ))
        .created_at(created_at)
        .build()
}

/// Folds the two nullable linkage columns into a binding. The paired-null
/// CHECK on `principals` guarantees they are both set or both null, so a
/// half-set pair is a schema violation, not a representable state.
fn platform_binding_from_columns(
    account_reference: Option<String>,
    platform_user_id: Option<String>,
) -> Option<PlatformBinding> {
    match (account_reference, platform_user_id) {
        (Some(account_reference), Some(platform_user_id)) => {
            Some(PlatformBinding::new(account_reference, platform_user_id))
        }
        (None, None) => None,
        _ => panic!("half-bound principal in database — the paired-null CHECK is missing"),
    }
}
