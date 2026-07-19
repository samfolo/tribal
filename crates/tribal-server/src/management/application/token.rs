//! Revision-bound token administration and bounded inventory.

use std::str::FromStr as _;

use chrono::{TimeDelta, Utc};
use tribal_auth::issue_token_with_record;
use tribal_config::MAX_TTL_HOURS;
use tribal_db::{
    AuthTokenInventoryRow, AuthTokenPageKey, AuthTokenRepository, DbError, PgAuthTokenRepository,
    PgPrincipalRepository, PrincipalRepository,
};
use tribal_domain::{
    AuthToken, AuthTokenId, LOCAL_PRINCIPAL_KEY, full_access_scopes, is_mintable_scope,
};
use tribal_wire::management::{
    AdministrationFailure, BootstrapTokenPolicy, ConfigRevision, CredentialPersistenceResult,
    InventoryItemRef, IssuedBearerToken, ManagementError, ManagementResponseError, Revisioned,
    RuntimeIdentity, TokenCreateOutcome, TokenCreateRequest, TokenCreateResult, TokenInventory,
    TokenListRequest, TokenPage, TokenRevokeAllOutcome, TokenRevokeAllRequest,
    TokenRevokeAllResult, TokenRevokeOutcome, TokenRevokeRequest, TokenRevokeResult, TokenState,
    TokenSummary,
};

use super::{
    super::lifecycle::{LifecycleController, RuntimeCredentialTarget},
    credential::{
        CredentialCoordinator, CredentialCoordinatorError, PersistedIssuance,
        PersistedIssuanceOrigin,
    },
    database::{DatabaseAccess, DatabaseAccessError, find_or_create_principal},
    operation::OperationContext,
    pagination::{
        INVENTORY_RESULT_BUDGET, InventoryCursor, InventoryCursorError, InventoryMethod,
        InventoryPosition,
    },
};

#[derive(Debug, thiserror::Error)]
pub(super) enum TokenAdministrationError {
    #[error(transparent)]
    Session(#[from] DatabaseAccessError),
    #[error("token issuance was refused")]
    Issuance,
    #[error("token repository failed: {source}")]
    Repository {
        #[source]
        source: DbError,
    },
    #[error("persisted credential failed: {source}")]
    Credential {
        #[source]
        source: CredentialCoordinatorError,
    },
    #[error(transparent)]
    Cursor(#[from] InventoryCursorError),
    #[error("token cursor contains an invalid position")]
    CursorPosition,
    #[error("token inventory item exceeds the response budget")]
    ItemTooLarge(AuthTokenId),
}

#[derive(Clone)]
pub(super) struct TokenAdministration {
    database: DatabaseAccess,
    credentials: CredentialCoordinator,
}

pub(super) struct BootstrapTokenReceipt {
    pub(super) raw: String,
    pub(super) summary: TokenSummary,
    pub(super) origin: PersistedIssuanceOrigin,
}

/// Validated token inputs retained across the bootstrap effect chain.
pub(super) struct PreparedBootstrapToken {
    ensure: bool,
    principal: String,
    scopes: Vec<tribal_domain::Scope>,
    audience: String,
    now: chrono::DateTime<Utc>,
    expires_at: chrono::DateTime<Utc>,
}

impl TokenAdministration {
    pub(super) fn new(database: DatabaseAccess, credentials: CredentialCoordinator) -> Self {
        Self {
            database,
            credentials,
        }
    }

    pub(super) async fn create(
        &self,
        operation: &OperationContext,
        request: TokenCreateRequest,
    ) -> Result<TokenCreateResult, TokenAdministrationError> {
        self.create_with(operation, request, None).await
    }

    /// Issues a bearer bound to the expected runtime: the lifecycle
    /// owner resolves the audience-bearing target, issuance uses that
    /// exact resource, and the post-commit revalidation decides release.
    /// Every other outcome — a changed target, a lost runtime, an
    /// indeterminate read — fails closed: the bearer zeroizes on drop
    /// and the committed token is revoked under a fresh operation, so
    /// the caller's own deadline cannot defeat the compensation.
    pub(super) async fn create_bound_to_runtime(
        &self,
        lifecycle: &LifecycleController,
        operation: &OperationContext,
        request: TokenCreateRequest,
        expected: RuntimeIdentity,
    ) -> Result<TokenCreateResult, ManagementResponseError> {
        if request.persist_as_default {
            return Err(administration_error(
                "a runtime-targeted token cannot persist as the default credential",
                AdministrationFailure::TokenIssuanceRefused,
            ));
        }
        let target = credential_target(lifecycle, operation, expected.clone())
            .await?
            .ok_or_else(credential_target_conflict)?;
        let result = self
            .create_for_runtime(operation, request, target.canonical_resource.clone())
            .await
            .map_err(public_error)?;
        let revalidated = credential_target(lifecycle, operation, expected).await;
        if matches!(&revalidated, Ok(Some(current)) if *current == target) {
            return Ok(result);
        }
        let issued = result.value.summary.id;
        drop(result);
        let compensation = OperationContext::new(tokio_util::sync::CancellationToken::new());
        self.revoke_issued(&compensation, issued)
            .await
            .map_err(public_error)?;
        Err(credential_target_conflict())
    }

    /// Issues against the lifecycle-resolved runtime audience: the same
    /// revision gate as [`Self::create`], with the explicit resource
    /// replacing the session-config derivation.
    pub(super) async fn create_for_runtime(
        &self,
        operation: &OperationContext,
        request: TokenCreateRequest,
        audience: String,
    ) -> Result<TokenCreateResult, TokenAdministrationError> {
        self.create_with(operation, request, Some(audience)).await
    }

    async fn create_with(
        &self,
        operation: &OperationContext,
        request: TokenCreateRequest,
        explicit_audience: Option<String>,
    ) -> Result<TokenCreateResult, TokenAdministrationError> {
        let session = self
            .database
            .mutation_session(operation, &request.expected_revision)
            .await?;
        let principal = request
            .principal
            .unwrap_or_else(|| LOCAL_PRINCIPAL_KEY.to_owned());
        let scopes = normalise_scopes(request.scopes)?;
        let expires_at =
            compute_expires_at(request.ttl_hours, session.config.auth.token_ttl_hours)?;
        let audience = match explicit_audience {
            Some(audience) => audience,
            None => {
                crate::startup::resolve_oauth_runtime(&session.config)
                    .map_err(|_| TokenAdministrationError::Issuance)?
                    .canonical_resource
            }
        };
        let revision = session.revision.clone();
        session.checkpoint()?;
        let (issued, credential) = if request.persist_as_default {
            (
                self.credentials
                    .issue_persisted(session, principal, scopes, audience, expires_at)
                    .await
                    .map_err(|source| TokenAdministrationError::Credential { source })?,
                CredentialPersistenceResult::Persisted,
            )
        } else {
            let (transaction, issued, principal) = session
                .operation()
                .cancel_safe(async {
                    let mut transaction = session.pool.begin().await.map_err(connection_error)?;
                    let principal = find_or_create_principal(&mut transaction, &principal)
                        .await
                        .map_err(repository)?;
                    let issued = issue_token_with_record(
                        &mut transaction,
                        &PgAuthTokenRepository,
                        principal.id(),
                        scopes,
                        audience,
                        expires_at,
                    )
                    .await
                    .map_err(repository)?;
                    let principal = principal.principal_key().to_owned();
                    Ok::<_, TokenAdministrationError>((transaction, issued, principal))
                })
                .await
                .map_err(DatabaseAccessError::from)??;
            session.checkpoint()?;
            transaction.commit().await.map_err(connection_error)?;
            (
                PersistedIssuance {
                    raw: issued.raw,
                    token: issued.token,
                    principal,
                    origin: PersistedIssuanceOrigin::Issued,
                },
                CredentialPersistenceResult::NotRequested,
            )
        };
        Ok(Revisioned {
            config_revision: revision,
            value: TokenCreateOutcome {
                token: IssuedBearerToken::new(issued.raw),
                summary: summary(&issued.token, issued.principal),
                credential,
            },
        })
    }

    pub(super) async fn preflight_bootstrap(
        &self,
        operation: &OperationContext,
        expected_revision: &ConfigRevision,
        policy: BootstrapTokenPolicy,
    ) -> Result<PreparedBootstrapToken, TokenAdministrationError> {
        let snapshot = self
            .database
            .config_snapshot(operation, Some(expected_revision))
            .await?;
        let (ensure, principal, ttl_hours, scopes) = match policy {
            BootstrapTokenPolicy::EnsureLocalCredential {
                principal,
                ttl_hours,
                scopes,
            } => (true, principal, ttl_hours, scopes),
            BootstrapTokenPolicy::Create {
                principal,
                ttl_hours,
                scopes,
            } => (false, principal, ttl_hours, scopes),
        };
        let principal = principal.unwrap_or_else(|| LOCAL_PRINCIPAL_KEY.to_owned());
        let scopes = normalise_scopes(scopes)?;
        let now = Utc::now();
        let expires_at =
            compute_expires_at_at(ttl_hours, snapshot.config.auth.token_ttl_hours, now)?;
        let audience = crate::startup::resolve_oauth_runtime(&snapshot.config)
            .map_err(|_| TokenAdministrationError::Issuance)?
            .canonical_resource;
        Ok(PreparedBootstrapToken {
            ensure,
            principal,
            scopes,
            audience,
            now,
            expires_at,
        })
    }

    pub(super) async fn provision_bootstrap(
        &self,
        operation: &OperationContext,
        expected_revision: ConfigRevision,
        prepared: PreparedBootstrapToken,
    ) -> Result<Revisioned<BootstrapTokenReceipt>, TokenAdministrationError> {
        let session = self
            .database
            .mutation_session(operation, &expected_revision)
            .await?;
        let PreparedBootstrapToken {
            ensure,
            principal,
            scopes,
            audience,
            now,
            expires_at,
        } = prepared;
        let revision = session.revision.clone();
        session.checkpoint()?;
        let issued = if ensure {
            self.credentials
                .ensure_persisted(session, principal, scopes, audience, now, expires_at)
                .await
        } else {
            self.credentials
                .issue_persisted(session, principal, scopes, audience, expires_at)
                .await
        }
        .map_err(|source| TokenAdministrationError::Credential { source })?;
        Ok(Revisioned {
            config_revision: revision,
            value: BootstrapTokenReceipt {
                summary: summary(&issued.token, issued.principal),
                raw: issued.raw,
                origin: issued.origin,
            },
        })
    }

    /// Unconditionally revokes a token this application just issued —
    /// the post-commit revalidation's damage-control path. No revision
    /// gate: the revocation must land even though the world moved.
    pub(super) async fn revoke_issued(
        &self,
        operation: &OperationContext,
        id: AuthTokenId,
    ) -> Result<(), TokenAdministrationError> {
        let session = self.database.read_session(operation).await?;
        let transaction = operation
            .cancel_safe(async {
                let mut transaction = session.pool.begin().await.map_err(connection_error)?;
                PgAuthTokenRepository
                    .revoke(&mut transaction, id, Utc::now())
                    .await
                    .map_err(repository)?;
                Ok::<_, TokenAdministrationError>(transaction)
            })
            .await
            .map_err(DatabaseAccessError::from)??;
        transaction.commit().await.map_err(connection_error)?;
        Ok(())
    }

    pub(super) async fn revoke(
        &self,
        operation: &OperationContext,
        request: TokenRevokeRequest,
    ) -> Result<TokenRevokeResult, TokenAdministrationError> {
        let session = self
            .database
            .mutation_session(operation, &request.expected_revision)
            .await?;
        let (transaction, outcome) = operation
            .cancel_safe(async {
                let mut transaction = session.pool.begin().await.map_err(connection_error)?;
                let Some(existing) = PgAuthTokenRepository
                    .find_by_id(&mut transaction, request.id)
                    .await
                    .map_err(repository)?
                else {
                    return Ok::<_, TokenAdministrationError>((
                        transaction,
                        TokenRevokeOutcome::NotFound { id: request.id },
                    ));
                };
                let principal = PgPrincipalRepository
                    .find_by_id(&mut transaction, existing.principal_id())
                    .await
                    .map_err(repository)?;
                let (token, revoked) = PgAuthTokenRepository
                    .revoke(&mut transaction, request.id, Utc::now())
                    .await
                    .map_err(repository)?;
                let token = summary(&token, principal.principal_key().to_owned());
                let outcome = if revoked {
                    TokenRevokeOutcome::Revoked { token }
                } else {
                    TokenRevokeOutcome::AlreadyRevoked { token }
                };
                Ok((transaction, outcome))
            })
            .await
            .map_err(DatabaseAccessError::from)??;
        session.checkpoint()?;
        transaction.commit().await.map_err(connection_error)?;
        Ok(session.revisioned(outcome))
    }

    pub(super) async fn revoke_all(
        &self,
        operation: &OperationContext,
        request: TokenRevokeAllRequest,
    ) -> Result<TokenRevokeAllResult, TokenAdministrationError> {
        let session = self
            .database
            .mutation_session(operation, &request.expected_revision)
            .await?;
        let (transaction, outcome) = operation
            .cancel_safe(async {
                let mut transaction = session.pool.begin().await.map_err(connection_error)?;
                let principal_id = if let Some(principal) = request.principal {
                    let Some(principal) = PgPrincipalRepository
                        .find_by_key(&mut transaction, &principal)
                        .await
                        .map_err(repository)?
                    else {
                        return Ok::<_, TokenAdministrationError>((
                            transaction,
                            TokenRevokeAllOutcome { revoked: 0 },
                        ));
                    };
                    Some(principal.id())
                } else {
                    None
                };
                let revoked = PgAuthTokenRepository
                    .revoke_all(&mut transaction, principal_id, Utc::now())
                    .await
                    .map_err(repository)?;
                Ok((transaction, TokenRevokeAllOutcome { revoked }))
            })
            .await
            .map_err(DatabaseAccessError::from)??;
        session.checkpoint()?;
        transaction.commit().await.map_err(connection_error)?;
        Ok(session.revisioned(outcome))
    }

    pub(super) async fn list(
        &self,
        operation: &OperationContext,
        request: TokenListRequest,
    ) -> Result<TokenInventory, TokenAdministrationError> {
        let session = self.database.read_session(operation).await?;
        let cursor = request
            .page
            .after
            .as_ref()
            .map(|cursor| {
                InventoryCursor::decode(cursor, InventoryMethod::TokenList, &session.revision)
            })
            .transpose()?;
        let mut connection = session.pool.acquire().await.map_err(connection_error)?;
        let (high_water, after) = if let Some(cursor) = cursor {
            (
                token_key(&cursor.high_water)?,
                Some(token_key(&cursor.after)?),
            )
        } else {
            let Some(high_water) = PgAuthTokenRepository
                .page_high_water(&mut connection)
                .await
                .map_err(repository)?
            else {
                return Ok(session.revisioned(TokenPage {
                    items: Vec::new(),
                    next: None,
                }));
            };
            (high_water, None)
        };
        let fetch_limit = request.page.size.get().saturating_add(1);
        let rows = PgAuthTokenRepository
            .list_page(&mut connection, high_water, after, fetch_limit)
            .await
            .map_err(repository)?;
        bounded_page(&session, high_water, &rows, request.page.size.get())
    }
}

fn normalise_scopes(
    mut scopes: Vec<tribal_domain::Scope>,
) -> Result<Vec<tribal_domain::Scope>, TokenAdministrationError> {
    if scopes.is_empty() {
        scopes = full_access_scopes();
    }
    if scopes.iter().any(|scope| !is_mintable_scope(scope)) {
        return Err(TokenAdministrationError::Issuance);
    }
    scopes.sort_by(|left, right| left.as_str().cmp(right.as_str()));
    scopes.dedup();
    Ok(scopes)
}

fn compute_expires_at(
    requested_hours: Option<u64>,
    configured_hours: u64,
) -> Result<chrono::DateTime<Utc>, TokenAdministrationError> {
    compute_expires_at_at(requested_hours, configured_hours, Utc::now())
}

fn compute_expires_at_at(
    requested_hours: Option<u64>,
    configured_hours: u64,
    now: chrono::DateTime<Utc>,
) -> Result<chrono::DateTime<Utc>, TokenAdministrationError> {
    let hours = requested_hours.unwrap_or(configured_hours);
    if hours == 0 || hours > MAX_TTL_HOURS {
        return Err(TokenAdministrationError::Issuance);
    }
    let hours = i64::try_from(hours).map_err(|_| TokenAdministrationError::Issuance)?;
    let lifetime = TimeDelta::try_hours(hours).ok_or(TokenAdministrationError::Issuance)?;
    now.checked_add_signed(lifetime)
        .ok_or(TokenAdministrationError::Issuance)
}

fn bounded_page(
    session: &super::database::DatabaseSession,
    high_water: AuthTokenPageKey,
    rows: &[AuthTokenInventoryRow],
    requested: u16,
) -> Result<TokenInventory, TokenAdministrationError> {
    let requested = usize::from(requested);
    let mut items = Vec::with_capacity(rows.len().min(requested));
    let mut next = None;
    for (index, row) in rows.iter().enumerate() {
        if index == requested {
            next = items
                .last()
                .map(|item| token_cursor(&session.revision, high_water, item).encode())
                .transpose()?;
            break;
        }
        let candidate_item = summary(&row.token, row.principal.clone());
        let has_more = index + 1 < rows.len();
        let candidate_next = if has_more {
            Some(token_cursor(&session.revision, high_water, &candidate_item).encode()?)
        } else {
            None
        };
        items.push(candidate_item);
        let candidate = session.revisioned(TokenPage {
            items: items.clone(),
            next: candidate_next.clone(),
        });
        if serde_json::to_vec(&candidate)
            .map_err(|source| InventoryCursorError::Encoding { source })?
            .len()
            > INVENTORY_RESULT_BUDGET
        {
            let oversized = items.pop().ok_or(InventoryCursorError::InternalInvariant)?;
            if items.is_empty() {
                return Err(TokenAdministrationError::ItemTooLarge(oversized.id));
            }
            next = items
                .last()
                .map(|item| token_cursor(&session.revision, high_water, item).encode())
                .transpose()?;
            break;
        }
        next = candidate_next;
    }
    Ok(session.revisioned(TokenPage { items, next }))
}

fn summary(token: &AuthToken, principal: String) -> TokenSummary {
    let now = Utc::now();
    let state = if let Some(revoked_at) = token.revoked_at() {
        TokenState::Revoked { revoked_at }
    } else if token.expires_at() <= now {
        TokenState::Expired
    } else {
        TokenState::Active
    };
    TokenSummary {
        id: token.id(),
        principal,
        scopes: token.scopes().to_vec(),
        audience: token.audience().to_owned(),
        state,
        created_at: token.created_at(),
        expires_at: token.expires_at(),
    }
}

fn token_key(position: &InventoryPosition) -> Result<AuthTokenPageKey, TokenAdministrationError> {
    Ok(AuthTokenPageKey {
        created_at: position.created_at,
        id: AuthTokenId::from_str(&position.id)
            .map_err(|_| TokenAdministrationError::CursorPosition)?,
    })
}

fn token_cursor(
    revision: &tribal_wire::management::ConfigRevision,
    high_water: AuthTokenPageKey,
    after: &TokenSummary,
) -> InventoryCursor {
    InventoryCursor::new(
        InventoryMethod::TokenList,
        revision.clone(),
        InventoryPosition {
            created_at: high_water.created_at,
            id: high_water.id.to_string(),
        },
        InventoryPosition {
            created_at: after.created_at,
            id: after.id.to_string(),
        },
    )
}

pub(super) fn public_error(error: TokenAdministrationError) -> ManagementResponseError {
    match error {
        TokenAdministrationError::Session(DatabaseAccessError::Operation(failure))
        | TokenAdministrationError::Credential {
            source: CredentialCoordinatorError::Operation(failure),
        } => super::operation::public_error(failure),
        TokenAdministrationError::Session(DatabaseAccessError::RevisionConflict {
            expected,
            actual,
        })
        | TokenAdministrationError::Cursor(InventoryCursorError::Stale { expected, actual }) => {
            ManagementResponseError {
                message: "configuration changed before token administration".to_owned(),
                error: ManagementError::ConfigConflict { expected, actual },
            }
        }
        TokenAdministrationError::ItemTooLarge(id) => administration_error(
            "token inventory item exceeds the response budget",
            AdministrationFailure::InventoryItemTooLarge {
                item: InventoryItemRef::Token(id),
            },
        ),
        TokenAdministrationError::Cursor(
            InventoryCursorError::Malformed | InventoryCursorError::WrongMethod,
        )
        | TokenAdministrationError::CursorPosition => ManagementResponseError {
            message: "token inventory cursor is invalid".to_owned(),
            error: ManagementError::ConfigurationInvalid { fields: Vec::new() },
        },
        error @ TokenAdministrationError::Cursor(
            InventoryCursorError::Encoding { .. } | InventoryCursorError::InternalInvariant,
        ) => super::private_failure(
            &error,
            ManagementResponseError {
                message: "token inventory could not be encoded".to_owned(),
                error: ManagementError::InternalInvariant,
            },
        ),
        ref error @ TokenAdministrationError::Credential { ref source } => {
            let failure = if matches!(source, CredentialCoordinatorError::Unavailable) {
                AdministrationFailure::PersistedCredentialUnavailable
            } else {
                AdministrationFailure::PersistedCredentialRecoveryFailed
            };
            super::private_administration_error(
                error,
                "persisted credential administration failed",
                failure,
            )
        }
        TokenAdministrationError::Issuance => administration_error(
            "token issuance was refused",
            AdministrationFailure::TokenIssuanceRefused,
        ),
        TokenAdministrationError::Session(DatabaseAccessError::Configuration(error)) => {
            super::super::configuration::management_error(error)
        }
        error @ (TokenAdministrationError::Session(DatabaseAccessError::Connection { .. })
        | TokenAdministrationError::Repository { .. }) => super::private_administration_error(
            &error,
            "token database is unavailable",
            AdministrationFailure::DatabaseUnavailable,
        ),
    }
}

fn connection_error(source: sqlx::Error) -> TokenAdministrationError {
    repository(DbError::QueryFailed {
        context: "acquiring token administration connection".to_owned(),
        source,
    })
}

fn repository(source: DbError) -> TokenAdministrationError {
    TokenAdministrationError::Repository { source }
}

/// The lifecycle owner's answer for one targeted read; an absent owner
/// is the same internal fault every lifecycle read reports.
async fn credential_target(
    lifecycle: &LifecycleController,
    operation: &OperationContext,
    expected: RuntimeIdentity,
) -> Result<Option<RuntimeCredentialTarget>, ManagementResponseError> {
    lifecycle
        .credential_target_for(operation, expected)
        .await
        .map_err(super::operation::public_error)?
        .ok_or_else(|| ManagementResponseError {
            message: "lifecycle owner is unavailable".to_owned(),
            error: ManagementError::InternalInvariant,
        })
}

/// The targeted-issuance refusal: the expected runtime is not the
/// attached, settled data plane — or stopped being it inside the
/// issuance window.
pub(super) fn credential_target_conflict() -> ManagementResponseError {
    administration_error(
        "credential target changed or is unavailable",
        AdministrationFailure::CredentialTargetConflict,
    )
}

fn administration_error(message: &str, failure: AdministrationFailure) -> ManagementResponseError {
    ManagementResponseError {
        message: message.to_owned(),
        error: ManagementError::Administration { failure },
    }
}

#[cfg(test)]
mod tests {
    use tribal_wire::management::{
        ConfigDigest, ConfigFilePath, ConfigRevision, PageRequest, PageSize,
    };

    use super::*;
    use crate::management::{
        application::credential::CredentialCoordinator,
        authority::ConfigAuthorityNamespace,
        configuration::ConfigAuthority,
        lifecycle::{ScriptedCredentialAnswer, controller_answering_credential_targets},
        worker,
    };

    fn a_runtime_identity() -> RuntimeIdentity {
        RuntimeIdentity {
            instance_id: "expected-runtime".to_owned(),
            pid: 4242,
            binary_version: "0.0.0".to_owned(),
            config_path: ConfigFilePath {
                path: "/tmp/tribal.yaml".to_owned(),
            },
        }
    }

    fn a_credential_target(resource: &str) -> RuntimeCredentialTarget {
        RuntimeCredentialTarget {
            runtime: a_runtime_identity(),
            canonical_resource: resource.to_owned(),
        }
    }

    fn a_bound_request(revision: ConfigRevision, persist_as_default: bool) -> TokenCreateRequest {
        TokenCreateRequest {
            expected_revision: revision,
            principal: None,
            ttl_hours: Some(2),
            scopes: Vec::new(),
            persist_as_default,
            expected_runtime: Some(a_runtime_identity()),
        }
    }

    fn is_credential_target_conflict(error: &ManagementResponseError) -> bool {
        matches!(
            error.error,
            ManagementError::Administration {
                failure: AdministrationFailure::CredentialTargetConflict,
            }
        )
    }

    fn config_worker(
        database_url: &str,
    ) -> (
        tempfile::TempDir,
        worker::ConfigWorkerClient,
        worker::ConfigWorkerRuntime,
    ) {
        let temp = tempfile::tempdir().expect("temporary config root");
        let path = temp.path().join("tribal.yaml");
        let config = tribal_config::TribalConfig::minimum_valid(database_url);
        std::fs::write(&path, serde_yaml::to_string(&config).unwrap()).unwrap();
        let (worker, runtime) = worker::spawn(ConfigAuthority::new(path)).unwrap();
        (temp, worker, runtime)
    }

    #[tokio::test]
    async fn test_create_list_and_stable_id_revocation_share_one_revision() {
        let database = tribal_test_utils::TestDb::new().await;
        let (_temp, worker, _worker_runtime) = config_worker(database.database_url());
        let revision = worker.resolved_snapshot().await.unwrap().revision;
        let (credentials, credential_runtime) = CredentialCoordinator::spawn(
            ConfigAuthorityNamespace::from_test("token-application"),
            tokio_util::sync::CancellationToken::new(),
        );
        let administration = TokenAdministration::new(DatabaseAccess::new(worker), credentials);
        let operation = OperationContext::new(tokio_util::sync::CancellationToken::new());

        let created = administration
            .create(
                &operation,
                TokenCreateRequest {
                    expected_revision: revision.clone(),
                    principal: None,
                    ttl_hours: Some(2),
                    scopes: Vec::new(),
                    persist_as_default: false,
                    expected_runtime: None,
                },
            )
            .await
            .expect("token creates");
        assert_eq!(created.config_revision, revision);
        assert_eq!(created.value.summary.principal, LOCAL_PRINCIPAL_KEY);
        assert_eq!(created.value.summary.scopes, full_access_scopes());
        assert!(!created.value.token.expose_secret().is_empty());

        let inventory = administration
            .list(
                &operation,
                TokenListRequest {
                    page: PageRequest {
                        size: PageSize::try_from(10).unwrap(),
                        after: None,
                    },
                },
            )
            .await
            .expect("tokens list");
        assert_eq!(inventory.config_revision, revision);
        assert_eq!(inventory.value.items.len(), 1);
        assert_eq!(inventory.value.items[0].id, created.value.summary.id);

        let revoked = administration
            .revoke(
                &operation,
                TokenRevokeRequest {
                    expected_revision: revision.clone(),
                    id: created.value.summary.id,
                },
            )
            .await
            .expect("token revokes");
        assert!(matches!(revoked.value, TokenRevokeOutcome::Revoked { .. }));
        let repeated = administration
            .revoke(
                &operation,
                TokenRevokeRequest {
                    expected_revision: revision,
                    id: created.value.summary.id,
                },
            )
            .await
            .expect("repeated revoke is typed");
        assert!(matches!(
            repeated.value,
            TokenRevokeOutcome::AlreadyRevoked { .. }
        ));

        let active = administration
            .create(
                &operation,
                TokenCreateRequest {
                    expected_revision: repeated.config_revision.clone(),
                    principal: None,
                    ttl_hours: Some(2),
                    scopes: Vec::new(),
                    persist_as_default: false,
                    expected_runtime: None,
                },
            )
            .await
            .expect("second token creates");
        let revoked_all = administration
            .revoke_all(
                &operation,
                TokenRevokeAllRequest {
                    expected_revision: repeated.config_revision.clone(),
                    principal: Some(LOCAL_PRINCIPAL_KEY.to_owned()),
                },
            )
            .await
            .expect("active principal tokens revoke");
        assert_eq!(revoked_all.value.revoked, 1);
        let inventory = administration
            .list(
                &operation,
                TokenListRequest {
                    page: PageRequest {
                        size: PageSize::try_from(10).unwrap(),
                        after: None,
                    },
                },
            )
            .await
            .expect("revoked tokens list");
        assert_eq!(inventory.value.items.len(), 2);
        assert!(
            inventory
                .value
                .items
                .iter()
                .all(|item| matches!(item.state, TokenState::Revoked { .. }))
        );
        assert!(
            inventory
                .value
                .items
                .iter()
                .any(|item| item.id == active.value.summary.id)
        );

        drop(administration);
        credential_runtime.join().await.unwrap();
    }

    #[tokio::test]
    async fn test_targeted_create_uses_the_explicit_audience_and_revoke_issued_skips_the_revision_gate()
     {
        let database = tribal_test_utils::TestDb::new().await;
        let (_temp, worker, _worker_runtime) = config_worker(database.database_url());
        let revision = worker.resolved_snapshot().await.unwrap().revision;
        let (credentials, credential_runtime) = CredentialCoordinator::spawn(
            ConfigAuthorityNamespace::from_test("token-runtime-target"),
            tokio_util::sync::CancellationToken::new(),
        );
        let administration = TokenAdministration::new(DatabaseAccess::new(worker), credentials);
        let operation = OperationContext::new(tokio_util::sync::CancellationToken::new());
        let audience = "http://127.0.0.1:9999/mcp";

        let created = administration
            .create_for_runtime(
                &operation,
                TokenCreateRequest {
                    expected_revision: revision.clone(),
                    principal: None,
                    ttl_hours: Some(2),
                    scopes: Vec::new(),
                    persist_as_default: false,
                    expected_runtime: None,
                },
                audience.to_owned(),
            )
            .await
            .expect("targeted token creates");
        assert_eq!(created.value.summary.audience, audience);

        administration
            .revoke_issued(&operation, created.value.summary.id)
            .await
            .expect("issued token revokes without a revision gate");
        let inventory = administration
            .list(
                &operation,
                TokenListRequest {
                    page: PageRequest {
                        size: PageSize::try_from(10).unwrap(),
                        after: None,
                    },
                },
            )
            .await
            .expect("tokens list");
        assert_eq!(inventory.value.items.len(), 1);
        assert!(matches!(
            inventory.value.items[0].state,
            TokenState::Revoked { .. }
        ));

        drop(administration);
        credential_runtime.join().await.unwrap();
    }

    #[tokio::test]
    async fn test_bound_create_releases_only_when_revalidation_matches_the_target() {
        let database = tribal_test_utils::TestDb::new().await;
        let (_temp, worker, _worker_runtime) = config_worker(database.database_url());
        let revision = worker.resolved_snapshot().await.unwrap().revision;
        let (credentials, credential_runtime) = CredentialCoordinator::spawn(
            ConfigAuthorityNamespace::from_test("token-bound-release"),
            tokio_util::sync::CancellationToken::new(),
        );
        let administration = TokenAdministration::new(DatabaseAccess::new(worker), credentials);
        let operation = OperationContext::new(tokio_util::sync::CancellationToken::new());
        let lifecycle = controller_answering_credential_targets(vec![
            ScriptedCredentialAnswer::Answer(Some(a_credential_target("https://runtime.test/mcp"))),
            ScriptedCredentialAnswer::Answer(Some(a_credential_target("https://runtime.test/mcp"))),
        ]);

        let created = administration
            .create_bound_to_runtime(
                &lifecycle,
                &operation,
                a_bound_request(revision, false),
                a_runtime_identity(),
            )
            .await
            .expect("bound token creates");

        assert_eq!(created.value.summary.audience, "https://runtime.test/mcp");
        drop(administration);
        credential_runtime.join().await.unwrap();
    }

    #[tokio::test]
    async fn test_bound_create_revokes_the_committed_token_when_the_target_changes_mid_window() {
        let database = tribal_test_utils::TestDb::new().await;
        let (_temp, worker, _worker_runtime) = config_worker(database.database_url());
        let revision = worker.resolved_snapshot().await.unwrap().revision;
        let (credentials, credential_runtime) = CredentialCoordinator::spawn(
            ConfigAuthorityNamespace::from_test("token-bound-changed"),
            tokio_util::sync::CancellationToken::new(),
        );
        let administration = TokenAdministration::new(DatabaseAccess::new(worker), credentials);
        let operation = OperationContext::new(tokio_util::sync::CancellationToken::new());
        let lifecycle = controller_answering_credential_targets(vec![
            ScriptedCredentialAnswer::Answer(Some(a_credential_target("https://runtime.test/mcp"))),
            ScriptedCredentialAnswer::Answer(Some(a_credential_target(
                "https://replacement.test/mcp",
            ))),
        ]);

        let refused = administration
            .create_bound_to_runtime(
                &lifecycle,
                &operation,
                a_bound_request(revision, false),
                a_runtime_identity(),
            )
            .await
            .expect_err("a changed target refuses release");

        assert!(is_credential_target_conflict(&refused));
        let inventory = administration
            .list(
                &operation,
                TokenListRequest {
                    page: PageRequest {
                        size: PageSize::try_from(10).unwrap(),
                        after: None,
                    },
                },
            )
            .await
            .expect("tokens list");
        assert_eq!(inventory.value.items.len(), 1);
        assert!(matches!(
            inventory.value.items[0].state,
            TokenState::Revoked { .. }
        ));
        drop(administration);
        credential_runtime.join().await.unwrap();
    }

    #[tokio::test]
    async fn test_bound_create_revokes_the_committed_token_when_revalidation_is_indeterminate() {
        let database = tribal_test_utils::TestDb::new().await;
        let (_temp, worker, _worker_runtime) = config_worker(database.database_url());
        let revision = worker.resolved_snapshot().await.unwrap().revision;
        let (credentials, credential_runtime) = CredentialCoordinator::spawn(
            ConfigAuthorityNamespace::from_test("token-bound-indeterminate"),
            tokio_util::sync::CancellationToken::new(),
        );
        let administration = TokenAdministration::new(DatabaseAccess::new(worker), credentials);
        let operation = OperationContext::new(tokio_util::sync::CancellationToken::new());
        let lifecycle = controller_answering_credential_targets(vec![
            ScriptedCredentialAnswer::Answer(Some(a_credential_target("https://runtime.test/mcp"))),
            ScriptedCredentialAnswer::Dropped,
        ]);

        let refused = administration
            .create_bound_to_runtime(
                &lifecycle,
                &operation,
                a_bound_request(revision, false),
                a_runtime_identity(),
            )
            .await
            .expect_err("an indeterminate revalidation refuses release");

        assert!(is_credential_target_conflict(&refused));
        let inventory = administration
            .list(
                &operation,
                TokenListRequest {
                    page: PageRequest {
                        size: PageSize::try_from(10).unwrap(),
                        after: None,
                    },
                },
            )
            .await
            .expect("tokens list");
        assert_eq!(inventory.value.items.len(), 1);
        assert!(matches!(
            inventory.value.items[0].state,
            TokenState::Revoked { .. }
        ));
        drop(administration);
        credential_runtime.join().await.unwrap();
    }

    #[tokio::test]
    async fn test_bound_create_refuses_a_persisting_request_before_any_issuance() {
        let database = tribal_test_utils::TestDb::new().await;
        let (_temp, worker, _worker_runtime) = config_worker(database.database_url());
        let revision = worker.resolved_snapshot().await.unwrap().revision;
        let (credentials, credential_runtime) = CredentialCoordinator::spawn(
            ConfigAuthorityNamespace::from_test("token-bound-persist"),
            tokio_util::sync::CancellationToken::new(),
        );
        let administration = TokenAdministration::new(DatabaseAccess::new(worker), credentials);
        let operation = OperationContext::new(tokio_util::sync::CancellationToken::new());
        let lifecycle = controller_answering_credential_targets(Vec::new());

        let refused = administration
            .create_bound_to_runtime(
                &lifecycle,
                &operation,
                a_bound_request(revision, true),
                a_runtime_identity(),
            )
            .await
            .expect_err("a persisting targeted request is refused");

        assert!(matches!(
            refused.error,
            ManagementError::Administration {
                failure: AdministrationFailure::TokenIssuanceRefused,
            }
        ));
        let inventory = administration
            .list(
                &operation,
                TokenListRequest {
                    page: PageRequest {
                        size: PageSize::try_from(10).unwrap(),
                        after: None,
                    },
                },
            )
            .await
            .expect("tokens list");
        assert!(inventory.value.items.is_empty());
        drop(administration);
        credential_runtime.join().await.unwrap();
    }

    #[tokio::test]
    async fn test_stale_mutation_is_refused_before_database_or_coordinator_use() {
        let (_temp, worker, _worker_runtime) =
            config_worker("postgres://user:pass@localhost:1/unreachable");
        let (credentials, credential_runtime) = CredentialCoordinator::spawn(
            ConfigAuthorityNamespace::from_test("stale-token-application"),
            tokio_util::sync::CancellationToken::new(),
        );
        let administration = TokenAdministration::new(DatabaseAccess::new(worker), credentials);
        let operation = OperationContext::new(tokio_util::sync::CancellationToken::new());

        let error = administration
            .create(
                &operation,
                TokenCreateRequest {
                    expected_revision: ConfigRevision::from_digest(&ConfigDigest::from_bytes(
                        b"stale",
                    )),
                    principal: None,
                    ttl_hours: None,
                    scopes: Vec::new(),
                    persist_as_default: true,
                    expected_runtime: None,
                },
            )
            .await
            .expect_err("stale revision is refused");

        assert!(matches!(
            error,
            TokenAdministrationError::Session(DatabaseAccessError::RevisionConflict { .. })
        ));
        drop(administration);
        credential_runtime.join().await.unwrap();
    }

    #[tokio::test]
    async fn test_unexpected_coordinator_exit_becomes_a_typed_persisted_credential_refusal() {
        let database = tribal_test_utils::TestDb::new().await;
        let (_temp, worker, _worker_runtime) = config_worker(database.database_url());
        let revision = worker.resolved_snapshot().await.unwrap().revision;
        let (credentials, runtime) = CredentialCoordinator::spawn(
            ConfigAuthorityNamespace::from_test("failed-token-application"),
            tokio_util::sync::CancellationToken::new(),
        );
        runtime.abort().await;
        let administration = TokenAdministration::new(DatabaseAccess::new(worker), credentials);
        let operation = OperationContext::new(tokio_util::sync::CancellationToken::new());

        let error = administration
            .create(
                &operation,
                TokenCreateRequest {
                    expected_revision: revision,
                    principal: None,
                    ttl_hours: Some(1),
                    scopes: Vec::new(),
                    persist_as_default: true,
                    expected_runtime: None,
                },
            )
            .await
            .expect_err("dead coordinator refuses persisted issuance");
        let public = public_error(error);

        assert!(matches!(
            public.error,
            ManagementError::Administration {
                failure: AdministrationFailure::PersistedCredentialUnavailable
            }
        ));
    }

    #[test]
    fn test_token_state_prefers_terminal_revocation_over_expiry() {
        let now = Utc::now();
        let token = AuthToken::builder()
            .id(AuthTokenId::new())
            .token_hash("0".repeat(64))
            .principal_id(tribal_domain::PrincipalId::new())
            .scopes(full_access_scopes())
            .audience("http://localhost/mcp".to_owned())
            .expires_at(now - chrono::Duration::hours(1))
            .created_at(now - chrono::Duration::hours(2))
            .revoked_at(Some(now))
            .build();

        assert!(matches!(
            summary(&token, LOCAL_PRINCIPAL_KEY.to_owned()).state,
            TokenState::Revoked { .. }
        ));
    }
}
