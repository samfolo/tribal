//! Namespace-bound credential durability and recovery.

#[cfg(unix)]
use std::os::unix::fs::PermissionsExt as _;
#[cfg(test)]
use std::sync::atomic::{AtomicUsize, Ordering};
use std::{
    fs::File,
    io,
    path::{Path, PathBuf},
    sync::Arc,
};

use serde::{Deserialize, Serialize};
use sqlx::Acquire as _;
use tokio::sync::{mpsc, oneshot, watch};
use tokio_util::sync::CancellationToken;
use tribal_auth::{IssuedAuthToken, issue_token_with_record};
use tribal_db::{
    AdvisoryLockRepository, AuthTokenRepository, DbError, LocalDefaultCredential,
    LocalDefaultCredentialRepository, PgAdvisoryLockRepository, PgAuthTokenRepository,
    PgLocalDefaultCredentialRepository, PgPrincipalRepository, PrincipalRepository,
};
use tribal_domain::{AuthTokenId, BearerToken, CredentialGenerationId, Scope};

use crate::management::{
    application::{
        database::{DatabaseSession, find_or_create_principal},
        operation::{OperationContext, OperationError},
    },
    authority::{AuthorityError, AuthorityLease, ConfigAuthorityNamespace, credential_paths},
};

const OWNER_FILE_MODE: u32 = 0o600;
const OWNER_DIRECTORY_MODE: u32 = 0o700;
const COORDINATOR_CAPACITY: usize = 1;
const CREDENTIAL_REUSE_WINDOW_MINUTES: i64 = 5;
const CREDENTIAL_STATEMENT_TIMEOUT_SECONDS: u64 = 5;
const RECONCILIATION_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(35);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum PersistedIssuanceOrigin {
    Existing,
    Issued,
}

/// Result retained inside the coordinator until one caller receives the secret.
pub(super) struct PersistedIssuance {
    pub(super) raw: String,
    pub(super) token: tribal_domain::AuthToken,
    pub(super) principal: String,
    pub(super) origin: PersistedIssuanceOrigin,
}

struct StagedIssuance {
    issuance: PersistedIssuance,
    generation_id: CredentialGenerationId,
}

struct PreparedIssuance {
    staged: StagedIssuance,
    envelope: PersistedCredentialEnvelope,
}

struct CredentialGrant {
    principal: String,
    scopes: Vec<Scope>,
    audience: String,
    expires_at: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, thiserror::Error)]
pub(super) enum CredentialCoordinatorError {
    #[error(transparent)]
    Operation(#[from] OperationError),
    #[error("credential coordinator is unavailable")]
    Unavailable,
    #[error("persisted credential reconciliation exceeded its terminal deadline")]
    ReconciliationTimedOut,
    #[error("credential database connection failed: {source}")]
    Connection {
        #[source]
        source: sqlx::Error,
    },
    #[error("credential database operation failed: {source}")]
    Database {
        #[source]
        source: DbError,
    },
    #[error(transparent)]
    Store(#[from] CredentialStoreError),
    #[error("credential store task failed: {source}")]
    StoreTask {
        #[source]
        source: tokio::task::JoinError,
    },
    #[error("generated bearer token failed validation")]
    GeneratedToken,
}

/// Secret-bearing direct-runtime result with no debug projection.
pub(crate) struct ResolvedPersistedBearer {
    pub(crate) token: BearerToken,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum PersistedCredentialReadError {
    #[error("configuration authority could not resolve persisted credentials: {source}")]
    Authority {
        #[source]
        source: AuthorityError,
    },
    #[error("namespaced persisted credential is invalid: {source}")]
    Namespaced {
        #[source]
        source: CredentialStoreError,
    },
    #[error("no namespaced persisted credential exists")]
    Missing,
}

/// Resolves the persisted bearer bound to a canonical config authority.
pub(crate) fn read_persisted_bearer(
    config_path: &Path,
) -> Result<ResolvedPersistedBearer, PersistedCredentialReadError> {
    let paths = AuthorityLease::paths_for(config_path)
        .map_err(|source| PersistedCredentialReadError::Authority { source })?;
    let store = CredentialStore {
        namespace: paths.namespace,
        stable_path: paths.stable_credential_path,
        pending_path: paths.pending_credential_path,
        #[cfg(test)]
        promotion_failures: AtomicUsize::new(0),
    };
    if let Some(envelope) = store
        .read_stable()
        .map_err(|source| PersistedCredentialReadError::Namespaced { source })?
    {
        let Auth::Bearer { token } = envelope.auth;
        return Ok(ResolvedPersistedBearer { token });
    }
    Err(PersistedCredentialReadError::Missing)
}

struct IssueCommand {
    session: DatabaseSession,
    grant: CredentialGrant,
    response: oneshot::Sender<Result<PersistedIssuance, CredentialCoordinatorError>>,
}

struct ExportCommand {
    session: DatabaseSession,
    audience: String,
    response: oneshot::Sender<Result<BearerToken, CredentialCoordinatorError>>,
}

struct EnsureCommand {
    session: DatabaseSession,
    grant: CredentialGrant,
    now: chrono::DateTime<chrono::Utc>,
    response: oneshot::Sender<Result<PersistedIssuance, CredentialCoordinatorError>>,
}

enum CredentialCommand {
    Issue(IssueCommand),
    Ensure(EnsureCommand),
    Export(ExportCommand),
}

/// Bounded secret-bearing client for one authority's credential task.
#[derive(Clone)]
pub(crate) struct CredentialCoordinator {
    sender: mpsc::Sender<CredentialCommand>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CredentialCoordinatorExit {
    Running,
    InputClosed,
}

/// Task ownership and terminal observation retained by `manage`.
pub(crate) struct CredentialCoordinatorRuntime {
    terminal: watch::Receiver<CredentialCoordinatorExit>,
    task: tokio::task::JoinHandle<()>,
}

impl CredentialCoordinator {
    pub(crate) fn spawn(
        namespace: ConfigAuthorityNamespace,
        shutdown: CancellationToken,
    ) -> (Self, CredentialCoordinatorRuntime) {
        let (sender, receiver) = mpsc::channel(COORDINATOR_CAPACITY);
        let (terminal_sender, terminal) = watch::channel(CredentialCoordinatorExit::Running);
        let task = tokio::spawn(run_coordinator(
            namespace.clone(),
            Arc::new(CredentialStore::new(namespace)),
            receiver,
            shutdown,
            terminal_sender,
        ));
        (
            Self { sender },
            CredentialCoordinatorRuntime { terminal, task },
        )
    }

    #[cfg(test)]
    pub(super) fn spawn_with_root(
        namespace: ConfigAuthorityNamespace,
        root: &Path,
        shutdown: CancellationToken,
    ) -> (Self, CredentialCoordinatorRuntime) {
        let (sender, receiver) = mpsc::channel(COORDINATOR_CAPACITY);
        let (terminal_sender, terminal) = watch::channel(CredentialCoordinatorExit::Running);
        let store = Arc::new(CredentialStore::with_root(namespace.clone(), root));
        let task = tokio::spawn(run_coordinator(
            namespace,
            store,
            receiver,
            shutdown,
            terminal_sender,
        ));
        (
            Self { sender },
            CredentialCoordinatorRuntime { terminal, task },
        )
    }

    pub(super) async fn issue_persisted(
        &self,
        session: DatabaseSession,
        principal: String,
        scopes: Vec<Scope>,
        audience: String,
        expires_at: chrono::DateTime<chrono::Utc>,
    ) -> Result<PersistedIssuance, CredentialCoordinatorError> {
        let operation = session.operation().clone();
        let (response, receiver) = oneshot::channel();
        self.submit(
            &operation,
            CredentialCommand::Issue(IssueCommand {
                session,
                grant: CredentialGrant {
                    principal,
                    scopes,
                    audience,
                    expires_at,
                },
                response,
            }),
            receiver,
        )
        .await
    }

    pub(super) async fn export_persisted(
        &self,
        session: DatabaseSession,
        audience: String,
    ) -> Result<BearerToken, CredentialCoordinatorError> {
        let operation = session.operation().clone();
        let (response, receiver) = oneshot::channel();
        self.submit(
            &operation,
            CredentialCommand::Export(ExportCommand {
                session,
                audience,
                response,
            }),
            receiver,
        )
        .await
    }

    pub(super) async fn ensure_persisted(
        &self,
        session: DatabaseSession,
        principal: String,
        scopes: Vec<Scope>,
        audience: String,
        now: chrono::DateTime<chrono::Utc>,
        expires_at: chrono::DateTime<chrono::Utc>,
    ) -> Result<PersistedIssuance, CredentialCoordinatorError> {
        let operation = session.operation().clone();
        let (response, receiver) = oneshot::channel();
        self.submit(
            &operation,
            CredentialCommand::Ensure(EnsureCommand {
                session,
                grant: CredentialGrant {
                    principal,
                    scopes,
                    audience,
                    expires_at,
                },
                now,
                response,
            }),
            receiver,
        )
        .await
    }

    async fn submit<T>(
        &self,
        operation: &OperationContext,
        command: CredentialCommand,
        receiver: oneshot::Receiver<Result<T, CredentialCoordinatorError>>,
    ) -> Result<T, CredentialCoordinatorError> {
        operation
            .cancel_safe(self.sender.send(command))
            .await?
            .map_err(|_| CredentialCoordinatorError::Unavailable)?;
        receiver
            .await
            .map_err(|_| CredentialCoordinatorError::Unavailable)?
    }
}

impl CredentialCoordinatorRuntime {
    pub(crate) fn terminal(&self) -> watch::Receiver<CredentialCoordinatorExit> {
        self.terminal.clone()
    }

    pub(crate) async fn join(self) -> Result<(), tokio::task::JoinError> {
        self.task.await
    }

    #[cfg(test)]
    pub(super) async fn abort(self) {
        self.task.abort();
        let _ = self.task.await;
    }
}

async fn run_coordinator(
    namespace: ConfigAuthorityNamespace,
    store: Arc<CredentialStore>,
    mut receiver: mpsc::Receiver<CredentialCommand>,
    shutdown: CancellationToken,
    terminal: watch::Sender<CredentialCoordinatorExit>,
) {
    loop {
        let command = tokio::select! {
            biased;
            () = shutdown.cancelled() => {
                receiver.close();
                break;
            }
            command = receiver.recv() => command,
        };
        let Some(command) = command else {
            break;
        };
        run_command(&namespace, &store, command).await;
    }
    while let Some(command) = receiver.recv().await {
        run_command(&namespace, &store, command).await;
    }
    let _ = terminal.send(CredentialCoordinatorExit::InputClosed);
}

async fn run_command(
    namespace: &ConfigAuthorityNamespace,
    store: &Arc<CredentialStore>,
    command: CredentialCommand,
) {
    match command {
        CredentialCommand::Issue(command) => {
            let result = issue_persisted(namespace, store, command).await;
            let _ = result.1.send(result.0);
        }
        CredentialCommand::Ensure(command) => {
            let result = ensure_persisted(namespace, store, command).await;
            let _ = result.1.send(result.0);
        }
        CredentialCommand::Export(command) => {
            let result = export_persisted(namespace, store, command).await;
            let _ = result.1.send(result.0);
        }
    }
}

async fn ensure_persisted(
    namespace: &ConfigAuthorityNamespace,
    store: &Arc<CredentialStore>,
    command: EnsureCommand,
) -> (
    Result<PersistedIssuance, CredentialCoordinatorError>,
    oneshot::Sender<Result<PersistedIssuance, CredentialCoordinatorError>>,
) {
    let EnsureCommand {
        session,
        grant,
        now,
        response,
    } = command;
    let result = ensure_persisted_inner(namespace, store, session, grant, now).await;
    (result, response)
}

async fn ensure_persisted_inner(
    namespace: &ConfigAuthorityNamespace,
    store: &Arc<CredentialStore>,
    session: DatabaseSession,
    grant: CredentialGrant,
    now: chrono::DateTime<chrono::Utc>,
) -> Result<PersistedIssuance, CredentialCoordinatorError> {
    let operation = session.operation().clone();
    let (mut transaction, previous) = begin_locked(&operation, namespace, &session).await?;
    let disposition = recover_before_commit(&operation, store, previous.clone()).await?;
    if matches!(
        disposition,
        RecoveryDisposition::Stable | RecoveryDisposition::PromotedPending
    ) {
        let envelope = read_stable_before_commit(&operation, store).await?;
        if let Some(existing) = operation
            .cancel_safe(reusable_issuance(
                &mut transaction,
                previous.as_ref(),
                envelope,
                &grant.principal,
                &grant.scopes,
                &grant.audience,
                now,
            ))
            .await??
        {
            return Ok(existing);
        }
    }
    replace_persisted(
        &operation,
        namespace,
        store,
        &session,
        transaction,
        previous,
        grant,
    )
    .await
}

async fn begin_locked(
    operation: &OperationContext,
    namespace: &ConfigAuthorityNamespace,
    session: &DatabaseSession,
) -> Result<
    (
        sqlx::Transaction<'static, sqlx::Postgres>,
        Option<LocalDefaultCredential>,
    ),
    CredentialCoordinatorError,
> {
    operation
        .cancel_safe(async {
            let mut transaction = session
                .pool
                .begin()
                .await
                .map_err(|source| CredentialCoordinatorError::Connection { source })?;
            set_statement_timeout(&mut transaction).await?;
            PgAdvisoryLockRepository
                .acquire_credential_replacement_xact(&mut transaction, namespace.as_str())
                .await
                .map_err(database_error)?;
            let previous = PgLocalDefaultCredentialRepository
                .find(&mut transaction, namespace.as_str())
                .await
                .map_err(database_error)?;
            Ok((transaction, previous))
        })
        .await?
}

async fn recover_before_commit(
    operation: &OperationContext,
    store: &Arc<CredentialStore>,
    previous: Option<LocalDefaultCredential>,
) -> Result<RecoveryDisposition, CredentialCoordinatorError> {
    operation.checkpoint()?;
    let disposition = store.recover_async(previous).await?;
    operation.checkpoint()?;
    Ok(disposition)
}

async fn read_stable_before_commit(
    operation: &OperationContext,
    store: &Arc<CredentialStore>,
) -> Result<Option<PersistedCredentialEnvelope>, CredentialCoordinatorError> {
    operation.checkpoint()?;
    let envelope = store.read_stable_async().await?;
    operation.checkpoint()?;
    Ok(envelope)
}

async fn reusable_issuance(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    mapping: Option<&LocalDefaultCredential>,
    envelope: Option<PersistedCredentialEnvelope>,
    principal_key: &str,
    scopes: &[Scope],
    audience: &str,
    now: chrono::DateTime<chrono::Utc>,
) -> Result<Option<PersistedIssuance>, CredentialCoordinatorError> {
    let Some(mapping) = mapping else {
        return Ok(None);
    };
    let Some(envelope) = envelope.filter(|envelope| mapping_matches(mapping, envelope)) else {
        return Ok(None);
    };
    let Some(token) = PgAuthTokenRepository
        .find_by_id(transaction, envelope.token_id)
        .await
        .map_err(database_error)?
    else {
        return Ok(None);
    };
    let principal = PgPrincipalRepository
        .find_by_id(transaction, token.principal_id())
        .await
        .map_err(database_error)?;
    let reuse_after =
        now.checked_add_signed(chrono::Duration::minutes(CREDENTIAL_REUSE_WINDOW_MINUTES));
    let Auth::Bearer { token: bearer } = envelope.auth;
    let reusable = token.revoked_at().is_none()
        && reuse_after.is_some_and(|reuse_after| token.expires_at() > reuse_after)
        && token.audience() == audience
        && token.scopes() == scopes
        && principal.principal_key() == principal_key
        && tribal_common::sha256_hex(bearer.as_str()) == token.token_hash();
    Ok(reusable.then(|| PersistedIssuance {
        raw: bearer.as_str().to_owned(),
        token,
        principal: principal.principal_key().to_owned(),
        origin: PersistedIssuanceOrigin::Existing,
    }))
}

async fn export_persisted(
    namespace: &ConfigAuthorityNamespace,
    store: &Arc<CredentialStore>,
    command: ExportCommand,
) -> (
    Result<BearerToken, CredentialCoordinatorError>,
    oneshot::Sender<Result<BearerToken, CredentialCoordinatorError>>,
) {
    let ExportCommand {
        session,
        audience,
        response,
    } = command;
    let result = export_persisted_inner(namespace, store, session, &audience).await;
    (result, response)
}

async fn export_persisted_inner(
    namespace: &ConfigAuthorityNamespace,
    store: &Arc<CredentialStore>,
    session: DatabaseSession,
    audience: &str,
) -> Result<BearerToken, CredentialCoordinatorError> {
    let operation = session.operation().clone();
    let (mut transaction, mapping) = begin_locked(&operation, namespace, &session).await?;
    let disposition = recover_before_commit(&operation, store, mapping.clone()).await?;
    if !matches!(
        disposition,
        RecoveryDisposition::Stable | RecoveryDisposition::PromotedPending
    ) {
        return Err(CredentialCoordinatorError::Unavailable);
    }
    let envelope = read_stable_before_commit(&operation, store)
        .await?
        .filter(|envelope| {
            mapping
                .as_ref()
                .is_some_and(|mapping| mapping_matches(mapping, envelope))
        })
        .ok_or(CredentialCoordinatorError::Unavailable)?;
    let token = operation
        .cancel_safe(async {
            PgAuthTokenRepository
                .find_by_id(&mut transaction, envelope.token_id)
                .await
                .map_err(database_error)
        })
        .await??
        .filter(|token| {
            token.revoked_at().is_none()
                && token.expires_at() >= chrono::Utc::now()
                && token.audience() == audience
        })
        .ok_or(CredentialCoordinatorError::Unavailable)?;
    let Auth::Bearer { token: bearer } = envelope.auth;
    if tribal_common::sha256_hex(bearer.as_str()) != token.token_hash() {
        return Err(CredentialCoordinatorError::Unavailable);
    }
    Ok(bearer)
}

async fn issue_persisted(
    namespace: &ConfigAuthorityNamespace,
    store: &Arc<CredentialStore>,
    command: IssueCommand,
) -> (
    Result<PersistedIssuance, CredentialCoordinatorError>,
    oneshot::Sender<Result<PersistedIssuance, CredentialCoordinatorError>>,
) {
    let IssueCommand {
        session,
        grant,
        response,
    } = command;
    let result = issue_persisted_inner(namespace, store, session, grant).await;
    (result, response)
}

async fn issue_persisted_inner(
    namespace: &ConfigAuthorityNamespace,
    store: &Arc<CredentialStore>,
    session: DatabaseSession,
    grant: CredentialGrant,
) -> Result<PersistedIssuance, CredentialCoordinatorError> {
    let operation = session.operation().clone();
    let (transaction, previous) = begin_locked(&operation, namespace, &session).await?;
    let _ = recover_before_commit(&operation, store, previous.clone()).await?;
    replace_persisted(
        &operation,
        namespace,
        store,
        &session,
        transaction,
        previous,
        grant,
    )
    .await
}

async fn replace_persisted(
    operation: &OperationContext,
    namespace: &ConfigAuthorityNamespace,
    store: &Arc<CredentialStore>,
    session: &DatabaseSession,
    mut transaction: sqlx::Transaction<'static, sqlx::Postgres>,
    previous: Option<LocalDefaultCredential>,
    grant: CredentialGrant,
) -> Result<PersistedIssuance, CredentialCoordinatorError> {
    let staged = stage_replacement(
        operation,
        namespace,
        store,
        &mut transaction,
        previous,
        grant,
    )
    .await?;
    let commit = transaction.commit().await;
    finish_replacement(namespace, store, session, staged, commit).await
}

async fn stage_replacement(
    operation: &OperationContext,
    namespace: &ConfigAuthorityNamespace,
    store: &Arc<CredentialStore>,
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    previous: Option<LocalDefaultCredential>,
    grant: CredentialGrant,
) -> Result<StagedIssuance, CredentialCoordinatorError> {
    let prepared = operation
        .cancel_safe(prepare_replacement(namespace, transaction, grant))
        .await??;
    operation.checkpoint()?;
    let result: Result<_, CredentialCoordinatorError> = async {
        store.stage_async(prepared.envelope.clone()).await?;
        operation
            .cancel_safe(finalise_replacement(
                namespace,
                transaction,
                previous,
                &prepared,
            ))
            .await??;
        operation.checkpoint()?;
        Ok(prepared.staged)
    }
    .await;
    if result.is_err() {
        store.discard_pending_async().await?;
    }
    result
}

async fn prepare_replacement(
    namespace: &ConfigAuthorityNamespace,
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    grant: CredentialGrant,
) -> Result<PreparedIssuance, CredentialCoordinatorError> {
    let principal = find_or_create_principal(transaction, &grant.principal)
        .await
        .map_err(database_error)?;
    let IssuedAuthToken { raw, token } = issue_token_with_record(
        transaction,
        &PgAuthTokenRepository,
        principal.id(),
        grant.scopes,
        grant.audience,
        grant.expires_at,
    )
    .await
    .map_err(database_error)?;
    let bearer = raw
        .parse::<BearerToken>()
        .map_err(|_| CredentialCoordinatorError::GeneratedToken)?;
    let envelope = PersistedCredentialEnvelope {
        namespace: namespace.clone(),
        generation_id: CredentialGenerationId::new(),
        token_id: token.id(),
        auth: Auth::Bearer { token: bearer },
    };
    let generation_id = envelope.generation_id;
    Ok(PreparedIssuance {
        envelope,
        staged: StagedIssuance {
            generation_id,
            issuance: PersistedIssuance {
                raw,
                token,
                principal: principal.principal_key().to_owned(),
                origin: PersistedIssuanceOrigin::Issued,
            },
        },
    })
}

async fn finalise_replacement(
    namespace: &ConfigAuthorityNamespace,
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    previous: Option<LocalDefaultCredential>,
    prepared: &PreparedIssuance,
) -> Result<(), CredentialCoordinatorError> {
    if let Some(previous) = previous
        && previous.token_id != prepared.staged.issuance.token.id()
    {
        PgAuthTokenRepository
            .revoke(transaction, previous.token_id, chrono::Utc::now())
            .await
            .map_err(database_error)?;
    }
    PgLocalDefaultCredentialRepository
        .replace(
            transaction,
            namespace.as_str(),
            prepared.envelope.generation_id,
            prepared.envelope.token_id,
        )
        .await
        .map_err(database_error)?;
    Ok(())
}

async fn finish_replacement(
    namespace: &ConfigAuthorityNamespace,
    store: &Arc<CredentialStore>,
    session: &DatabaseSession,
    staged: StagedIssuance,
    commit: Result<(), sqlx::Error>,
) -> Result<PersistedIssuance, CredentialCoordinatorError> {
    let failure = match commit {
        Ok(()) => match store.promote_pending_async().await {
            Ok(()) => return Ok(staged.issuance),
            Err(failure) => failure,
        },
        Err(source) => CredentialCoordinatorError::Connection { source },
    };
    tokio::time::timeout(
        RECONCILIATION_TIMEOUT,
        reconcile_replacement(namespace, store, session, staged, failure),
    )
    .await
    .map_err(|_| CredentialCoordinatorError::ReconciliationTimedOut)?
}

async fn reconcile_replacement(
    namespace: &ConfigAuthorityNamespace,
    store: &Arc<CredentialStore>,
    session: &DatabaseSession,
    staged: StagedIssuance,
    failure: CredentialCoordinatorError,
) -> Result<PersistedIssuance, CredentialCoordinatorError> {
    let mut connection = session
        .pool
        .acquire()
        .await
        .map_err(|source| CredentialCoordinatorError::Connection { source })?;
    let mut transaction = connection
        .begin()
        .await
        .map_err(|source| CredentialCoordinatorError::Connection { source })?;
    set_statement_timeout(&mut transaction).await?;
    PgAdvisoryLockRepository
        .acquire_credential_replacement_xact(&mut transaction, namespace.as_str())
        .await
        .map_err(database_error)?;
    let mapping = PgLocalDefaultCredentialRepository
        .find(&mut transaction, namespace.as_str())
        .await
        .map_err(database_error)?;
    let disposition = store.recover_async(mapping.clone()).await?;
    let authoritative = mapping.as_ref().is_some_and(|mapping| {
        mapping.authority_namespace == namespace.as_str()
            && mapping.generation_id == staged.generation_id
            && mapping.token_id == staged.issuance.token.id()
    });
    let stable = store.read_stable_async().await?.is_some_and(|envelope| {
        envelope.generation_id == staged.generation_id
            && envelope.token_id == staged.issuance.token.id()
    });
    let token = PgAuthTokenRepository
        .find_by_id(&mut transaction, staged.issuance.token.id())
        .await
        .map_err(database_error)?;
    transaction
        .commit()
        .await
        .map_err(|source| CredentialCoordinatorError::Connection { source })?;
    let token_matches = token.is_some_and(|token| {
        token.revoked_at().is_none()
            && token.token_hash() == tribal_common::sha256_hex(&staged.issuance.raw)
    });
    if authoritative
        && stable
        && token_matches
        && matches!(
            disposition,
            RecoveryDisposition::Stable | RecoveryDisposition::PromotedPending
        )
    {
        Ok(staged.issuance)
    } else {
        Err(failure)
    }
}

async fn set_statement_timeout(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<(), CredentialCoordinatorError> {
    sqlx::query(&format!(
        "SET LOCAL statement_timeout = '{CREDENTIAL_STATEMENT_TIMEOUT_SECONDS}s'"
    ))
    .execute(&mut **transaction)
    .await
    .map(|_| ())
    .map_err(|source| CredentialCoordinatorError::Connection { source })
}

fn database_error(source: DbError) -> CredentialCoordinatorError {
    CredentialCoordinatorError::Database { source }
}

/// Secret-bearing authentication material retained inside the manager.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "data", rename_all = "snake_case")]
pub(super) enum Auth {
    Bearer { token: BearerToken },
}

impl std::fmt::Debug for Auth {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Bearer { .. } => formatter
                .debug_struct("Bearer")
                .field("token", &"<redacted>")
                .finish(),
        }
    }
}

/// File envelope joined to the database mapping by three durable identities.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct PersistedCredentialEnvelope {
    pub(super) namespace: ConfigAuthorityNamespace,
    pub(super) generation_id: CredentialGenerationId,
    pub(super) token_id: AuthTokenId,
    pub(super) auth: Auth,
}

impl std::fmt::Debug for PersistedCredentialEnvelope {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PersistedCredentialEnvelope")
            .field("namespace", &self.namespace)
            .field("generation_id", &self.generation_id)
            .field("token_id", &self.token_id)
            .field("auth", &"<redacted>")
            .finish()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum RecoveryDisposition {
    Empty,
    Stable,
    PromotedPending,
    ReplaceMapped { token_id: AuthTokenId },
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum CredentialStoreError {
    #[error("credential envelope path has no parent: '{}'", path.display())]
    ParentlessPath { path: PathBuf },
    #[error("credential envelope I/O failed at '{}': {source}", path.display())]
    Io {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
    #[error("credential envelope encoding failed: {source}")]
    Encoding {
        #[source]
        source: serde_json::Error,
    },
    #[error("credential envelope belongs to another authority namespace")]
    NamespaceMismatch,
}

/// Owner-only pending/stable envelope store for one configuration authority.
pub(super) struct CredentialStore {
    namespace: ConfigAuthorityNamespace,
    stable_path: PathBuf,
    pending_path: PathBuf,
    #[cfg(test)]
    promotion_failures: AtomicUsize,
}

impl CredentialStore {
    pub(super) fn new(namespace: ConfigAuthorityNamespace) -> Self {
        let (stable_path, pending_path) = credential_paths(&namespace);
        Self {
            namespace,
            stable_path,
            pending_path,
            #[cfg(test)]
            promotion_failures: AtomicUsize::new(0),
        }
    }

    #[cfg(test)]
    fn with_root(namespace: ConfigAuthorityNamespace, root: &Path) -> Self {
        let directory = root.join("tribal/credentials");
        Self {
            stable_path: directory.join(format!("{namespace}.json")),
            pending_path: directory.join(format!("{namespace}.pending")),
            namespace,
            promotion_failures: AtomicUsize::new(0),
        }
    }

    async fn stage_async(
        self: &Arc<Self>,
        envelope: PersistedCredentialEnvelope,
    ) -> Result<(), CredentialCoordinatorError> {
        let store = Arc::clone(self);
        run_store_task(move || store.stage(&envelope)).await
    }

    async fn promote_pending_async(self: &Arc<Self>) -> Result<(), CredentialCoordinatorError> {
        let store = Arc::clone(self);
        run_store_task(move || store.promote_pending()).await
    }

    async fn discard_pending_async(self: &Arc<Self>) -> Result<(), CredentialCoordinatorError> {
        let store = Arc::clone(self);
        run_store_task(move || Self::remove_if_exists(&store.pending_path)).await
    }

    async fn recover_async(
        self: &Arc<Self>,
        mapping: Option<LocalDefaultCredential>,
    ) -> Result<RecoveryDisposition, CredentialCoordinatorError> {
        let store = Arc::clone(self);
        run_store_task(move || store.recover(mapping.as_ref())).await
    }

    async fn read_stable_async(
        self: &Arc<Self>,
    ) -> Result<Option<PersistedCredentialEnvelope>, CredentialCoordinatorError> {
        let store = Arc::clone(self);
        run_store_task(move || store.read_stable()).await
    }

    pub(super) fn stage(
        &self,
        envelope: &PersistedCredentialEnvelope,
    ) -> Result<(), CredentialStoreError> {
        self.validate_namespace(envelope)?;
        let parent = parent_directory(&self.pending_path)?;
        std::fs::create_dir_all(parent).map_err(|source| file_error(parent, source))?;
        #[cfg(unix)]
        {
            std::fs::set_permissions(
                parent,
                std::fs::Permissions::from_mode(OWNER_DIRECTORY_MODE),
            )
            .map_err(|source| file_error(parent, source))?;
        }
        let bytes = serde_json::to_vec(envelope)
            .map_err(|source| CredentialStoreError::Encoding { source })?;
        tribal_config::write_atomically(&self.pending_path, &bytes, Some(OWNER_FILE_MODE))
            .map_err(|source| file_error(&self.pending_path, source))
    }

    pub(super) fn promote_pending(&self) -> Result<(), CredentialStoreError> {
        #[cfg(test)]
        if self
            .promotion_failures
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |failures| {
                failures.checked_sub(1)
            })
            .is_ok()
        {
            return Err(file_error(
                &self.pending_path,
                io::Error::other("injected pending promotion failure"),
            ));
        }
        let parent = parent_directory(&self.stable_path)?;
        std::fs::rename(&self.pending_path, &self.stable_path)
            .map_err(|source| file_error(&self.pending_path, source))?;
        File::open(parent)
            .and_then(|directory| directory.sync_all())
            .map_err(|source| file_error(parent, source))
    }

    #[cfg(test)]
    fn fail_next_promotion(&self) {
        self.promotion_failures.fetch_add(1, Ordering::Relaxed);
    }

    pub(super) fn recover(
        &self,
        mapping: Option<&LocalDefaultCredential>,
    ) -> Result<RecoveryDisposition, CredentialStoreError> {
        let stable = self.read(&self.stable_path)?;
        let pending = self.read(&self.pending_path)?;
        let stable_matches = mapping
            .zip(stable.as_ref())
            .is_some_and(|(mapping, envelope)| mapping_matches(mapping, envelope));
        let pending_matches = mapping
            .zip(pending.as_ref())
            .is_some_and(|(mapping, envelope)| mapping_matches(mapping, envelope));

        if stable_matches {
            Self::remove_if_exists(&self.pending_path)?;
            return Ok(RecoveryDisposition::Stable);
        }
        if pending_matches {
            self.promote_pending()?;
            return Ok(RecoveryDisposition::PromotedPending);
        }
        Self::remove_if_exists(&self.pending_path)?;
        if let Some(mapping) = mapping {
            return Ok(RecoveryDisposition::ReplaceMapped {
                token_id: mapping.token_id,
            });
        }
        Self::remove_if_exists(&self.stable_path)?;
        Ok(RecoveryDisposition::Empty)
    }

    pub(super) fn read_stable(
        &self,
    ) -> Result<Option<PersistedCredentialEnvelope>, CredentialStoreError> {
        self.read(&self.stable_path)
    }

    fn read(
        &self,
        path: &Path,
    ) -> Result<Option<PersistedCredentialEnvelope>, CredentialStoreError> {
        let bytes = match std::fs::read(path) {
            Ok(bytes) => bytes,
            Err(source) if source.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(source) => return Err(file_error(path, source)),
        };
        let envelope = serde_json::from_slice(&bytes)
            .map_err(|source| CredentialStoreError::Encoding { source })?;
        self.validate_namespace(&envelope)?;
        Ok(Some(envelope))
    }

    fn validate_namespace(
        &self,
        envelope: &PersistedCredentialEnvelope,
    ) -> Result<(), CredentialStoreError> {
        if envelope.namespace == self.namespace {
            Ok(())
        } else {
            Err(CredentialStoreError::NamespaceMismatch)
        }
    }

    fn remove_if_exists(path: &Path) -> Result<(), CredentialStoreError> {
        match std::fs::remove_file(path) {
            Ok(()) => Ok(()),
            Err(source) if source.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(source) => Err(file_error(path, source)),
        }
    }
}

async fn run_store_task<T>(
    task: impl FnOnce() -> Result<T, CredentialStoreError> + Send + 'static,
) -> Result<T, CredentialCoordinatorError>
where
    T: Send + 'static,
{
    tokio::task::spawn_blocking(task)
        .await
        .map_err(|source| CredentialCoordinatorError::StoreTask { source })?
        .map_err(CredentialCoordinatorError::Store)
}

fn parent_directory(path: &Path) -> Result<&Path, CredentialStoreError> {
    path.parent()
        .ok_or_else(|| CredentialStoreError::ParentlessPath {
            path: path.to_path_buf(),
        })
}

fn mapping_matches(
    mapping: &LocalDefaultCredential,
    envelope: &PersistedCredentialEnvelope,
) -> bool {
    mapping.authority_namespace == envelope.namespace.as_str()
        && mapping.generation_id == envelope.generation_id
        && mapping.token_id == envelope.token_id
}

fn file_error(path: &Path, source: io::Error) -> CredentialStoreError {
    CredentialStoreError::Io {
        path: path.to_owned(),
        source,
    }
}

#[cfg(test)]
mod recovery {
    use std::sync::{Arc, Mutex};

    use chrono::Utc;
    use sqlx::postgres::PgPoolOptions;
    use tokio::sync::Notify;
    use tribal_db::{
        AuthTokenRepository as _, LocalDefaultCredentialRepository as _, PgAuthTokenRepository,
        PgLocalDefaultCredentialRepository,
    };
    use tribal_domain::full_access_scopes;
    use tribal_wire::management::{ConfigDigest, ConfigRevision};

    use super::*;

    fn namespace(value: &str) -> ConfigAuthorityNamespace {
        ConfigAuthorityNamespace::from_test(value)
    }

    fn envelope(namespace: &ConfigAuthorityNamespace) -> PersistedCredentialEnvelope {
        PersistedCredentialEnvelope {
            namespace: namespace.clone(),
            generation_id: CredentialGenerationId::new(),
            token_id: AuthTokenId::new(),
            auth: Auth::Bearer {
                token: "secret-token".parse().expect("token parses"),
            },
        }
    }

    #[tokio::test]
    async fn test_close_ends_admission_with_live_client_handles() {
        let root = tempfile::tempdir().expect("temporary credential root");
        let shutdown = CancellationToken::new();
        let (coordinator, runtime) = CredentialCoordinator::spawn_with_root(
            namespace("close"),
            root.path(),
            shutdown.clone(),
        );
        let mut terminal = runtime.terminal();

        shutdown.cancel();
        terminal.changed().await.expect("terminal state changes");

        assert_eq!(
            *terminal.borrow_and_update(),
            CredentialCoordinatorExit::InputClosed
        );
        assert!(coordinator.sender.is_closed());
        runtime.join().await.expect("coordinator joins");
    }

    fn session(database: &tribal_test_utils::TestDb, revision: &ConfigRevision) -> DatabaseSession {
        DatabaseSession::for_test(
            revision.clone(),
            Arc::new(tribal_config::TribalConfig::minimum_valid(
                database.database_url(),
            )),
            database.pool().clone(),
        )
    }

    fn mapping(envelope: &PersistedCredentialEnvelope) -> LocalDefaultCredential {
        LocalDefaultCredential {
            authority_namespace: envelope.namespace.as_str().to_owned(),
            generation_id: envelope.generation_id,
            token_id: envelope.token_id,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        }
    }

    #[test]
    fn test_pre_commit_pending_loses_to_matching_stable_mapping() {
        let root = tempfile::tempdir().expect("temporary credential root");
        let namespace = namespace("0123456789abcdef01234567");
        let store = CredentialStore::with_root(namespace.clone(), root.path());
        let stable = envelope(&namespace);
        store.stage(&stable).expect("stable stages");
        store.promote_pending().expect("stable promotes");
        let pending = envelope(&namespace);
        store.stage(&pending).expect("replacement stages");

        assert_eq!(
            store.recover(Some(&mapping(&stable))).expect("recovery"),
            RecoveryDisposition::Stable
        );
        assert_eq!(store.read_stable().expect("stable reads"), Some(stable));
        assert!(!store.pending_path.exists());
    }

    #[test]
    fn test_committed_pending_is_promoted_after_lost_ack_or_pre_rename_crash() {
        let root = tempfile::tempdir().expect("temporary credential root");
        let namespace = namespace("0123456789abcdef01234567");
        let store = CredentialStore::with_root(namespace.clone(), root.path());
        let committed = envelope(&namespace);
        store.stage(&committed).expect("pending stages");

        assert_eq!(
            store.recover(Some(&mapping(&committed))).expect("recovery"),
            RecoveryDisposition::PromotedPending
        );
        assert_eq!(store.read_stable().expect("stable reads"), Some(committed));
        assert!(!store.pending_path.exists());
    }

    #[tokio::test]
    async fn test_committed_replacement_reconciles_after_lost_ack() {
        let database = tribal_test_utils::TestDb::new().await;
        let root = tempfile::tempdir().expect("temporary credential root");
        let namespace = namespace("1123456789abcdef01234567");
        let store = Arc::new(CredentialStore::with_root(namespace.clone(), root.path()));
        let revision = ConfigRevision::from_digest(&ConfigDigest::from_bytes(b"lost-ack"));
        let session = session(&database, &revision);
        let mut connection = session.pool.acquire().await.expect("connection");
        let mut transaction = connection.begin().await.expect("transaction");
        PgAdvisoryLockRepository
            .acquire_credential_replacement_xact(&mut transaction, namespace.as_str())
            .await
            .expect("credential lock");
        let staged = stage_replacement(
            session.operation(),
            &namespace,
            &store,
            &mut transaction,
            None,
            CredentialGrant {
                principal: tribal_domain::LOCAL_PRINCIPAL_KEY.to_owned(),
                scopes: full_access_scopes(),
                audience: "http://localhost/mcp".to_owned(),
                expires_at: Utc::now() + chrono::Duration::hours(1),
            },
        )
        .await
        .expect("replacement stages");
        let token_id = staged.issuance.token.id();
        transaction.commit().await.expect("server commits");
        drop(connection);

        let issued = finish_replacement(
            &namespace,
            &store,
            &session,
            staged,
            Err(sqlx::Error::Io(io::Error::other(
                "commit acknowledgement lost",
            ))),
        )
        .await
        .expect("lost acknowledgement reconciles");

        assert_eq!(issued.token.id(), token_id);
        let mapping = PgLocalDefaultCredentialRepository
            .find(
                &mut database.pool().acquire().await.unwrap(),
                namespace.as_str(),
            )
            .await
            .unwrap()
            .expect("mapping exists");
        assert_eq!(mapping.token_id, token_id);
        assert_eq!(
            store
                .read_stable()
                .unwrap()
                .expect("stable exists")
                .token_id,
            token_id
        );
        assert!(!store.pending_path.exists());
    }

    #[tokio::test]
    async fn test_committed_replacement_reconciles_after_promotion_failure() {
        let database = tribal_test_utils::TestDb::new().await;
        let root = tempfile::tempdir().expect("temporary credential root");
        let namespace = namespace("2123456789abcdef01234567");
        let store = Arc::new(CredentialStore::with_root(namespace.clone(), root.path()));
        let revision = ConfigRevision::from_digest(&ConfigDigest::from_bytes(b"promotion"));
        store.fail_next_promotion();

        let issued = issue_persisted_inner(
            &namespace,
            &store,
            session(&database, &revision),
            CredentialGrant {
                principal: tribal_domain::LOCAL_PRINCIPAL_KEY.to_owned(),
                scopes: full_access_scopes(),
                audience: "http://localhost/mcp".to_owned(),
                expires_at: Utc::now() + chrono::Duration::hours(1),
            },
        )
        .await
        .expect("promotion failure reconciles");

        let mapping = PgLocalDefaultCredentialRepository
            .find(
                &mut database.pool().acquire().await.unwrap(),
                namespace.as_str(),
            )
            .await
            .unwrap()
            .expect("mapping exists");
        assert_eq!(mapping.token_id, issued.token.id());
        assert_eq!(
            store
                .read_stable()
                .unwrap()
                .expect("stable exists")
                .token_id,
            issued.token.id()
        );
        assert!(!store.pending_path.exists());
    }

    #[tokio::test]
    async fn test_shutdown_before_transaction_admission_leaves_no_credential_effect() {
        let database = tribal_test_utils::TestDb::new().await;
        let root = tempfile::tempdir().expect("temporary credential root");
        let namespace = namespace("3123456789abcdef01234567");
        let store = Arc::new(CredentialStore::with_root(namespace.clone(), root.path()));
        let entered = Arc::new(Mutex::new(None::<oneshot::Sender<()>>));
        let release = Arc::new(Notify::new());
        let pool = PgPoolOptions::new()
            .max_connections(1)
            .before_acquire({
                let entered = Arc::clone(&entered);
                let release = Arc::clone(&release);
                move |_, _| {
                    let entered = Arc::clone(&entered);
                    let release = Arc::clone(&release);
                    Box::pin(async move {
                        let signal = entered.lock().expect("admission signal lock").take();
                        if let Some(signal) = signal {
                            let _ = signal.send(());
                            release.notified().await;
                        }
                        Ok(true)
                    })
                }
            })
            .connect(database.database_url())
            .await
            .expect("credential pool connects");
        drop(pool.acquire().await.expect("idle connection primes"));
        let (admitted, admission) = oneshot::channel();
        *entered.lock().expect("admission signal lock") = Some(admitted);
        let shutdown = CancellationToken::new();
        let revision = ConfigRevision::from_digest(&ConfigDigest::from_bytes(b"shutdown"));
        let session = DatabaseSession::for_test_with_operation(
            revision,
            Arc::new(tribal_config::TribalConfig::minimum_valid(
                database.database_url(),
            )),
            pool,
            OperationContext::new(shutdown.clone()),
        );
        let task = tokio::spawn({
            let namespace = namespace.clone();
            let store = Arc::clone(&store);
            async move {
                issue_persisted_inner(
                    &namespace,
                    &store,
                    session,
                    CredentialGrant {
                        principal: tribal_domain::LOCAL_PRINCIPAL_KEY.to_owned(),
                        scopes: full_access_scopes(),
                        audience: "http://localhost/mcp".to_owned(),
                        expires_at: Utc::now() + chrono::Duration::hours(1),
                    },
                )
                .await
            }
        });

        admission.await.expect("transaction acquisition begins");
        shutdown.cancel();
        release.notify_one();
        let Err(error) = task.await.expect("credential task joins") else {
            panic!("shutdown must refuse uncommitted issuance");
        };

        assert!(matches!(
            error,
            CredentialCoordinatorError::Operation(OperationError::ManagerShuttingDown)
        ));
        let tokens = PgAuthTokenRepository
            .find_all(&mut database.pool().acquire().await.expect("token connection"))
            .await
            .expect("tokens read");
        assert!(tokens.is_empty());
        assert!(
            PgLocalDefaultCredentialRepository
                .find(
                    &mut database.pool().acquire().await.expect("mapping connection"),
                    namespace.as_str(),
                )
                .await
                .expect("mapping reads")
                .is_none()
        );
        assert!(!store.pending_path.exists());
        assert!(!store.stable_path.exists());
    }

    #[test]
    fn test_distinct_namespaces_never_promote_or_remove_each_others_files() {
        let root = tempfile::tempdir().expect("temporary credential root");
        let first_namespace = namespace("0123456789abcdef01234567");
        let second_namespace = namespace("fedcba9876543210fedcba98");
        let first = CredentialStore::with_root(first_namespace.clone(), root.path());
        let second = CredentialStore::with_root(second_namespace.clone(), root.path());
        let first_envelope = envelope(&first_namespace);
        let second_envelope = envelope(&second_namespace);
        first.stage(&first_envelope).expect("first stages");
        second.stage(&second_envelope).expect("second stages");

        first
            .recover(Some(&mapping(&first_envelope)))
            .expect("first recovers");

        assert_eq!(
            second.read(&second.pending_path).expect("second reads"),
            Some(second_envelope)
        );
    }

    #[test]
    fn test_envelope_debug_never_exports_the_bearer() {
        let envelope = envelope(&namespace("0123456789abcdef01234567"));
        let debug = format!("{envelope:?}");
        assert!(debug.contains("<redacted>"));
        assert!(!debug.contains("secret-token"));
    }

    #[test]
    fn test_concurrent_recovery_converges_on_one_stable_generation() {
        let root = tempfile::tempdir().expect("temporary credential root");
        let namespace = namespace("0123456789abcdef01234567");
        let store = std::sync::Arc::new(CredentialStore::with_root(namespace.clone(), root.path()));
        let stable = envelope(&namespace);
        store.stage(&stable).expect("stable stages");
        store.promote_pending().expect("stable promotes");
        let mapping = std::sync::Arc::new(mapping(&stable));
        let observers: Vec<_> = (0..2)
            .map(|_| {
                let store = std::sync::Arc::clone(&store);
                let mapping = std::sync::Arc::clone(&mapping);
                std::thread::spawn(move || store.recover(Some(&mapping)))
            })
            .collect();

        for observer in observers {
            assert_eq!(
                observer
                    .join()
                    .expect("recovery thread joins")
                    .expect("recovery"),
                RecoveryDisposition::Stable
            );
        }
        assert_eq!(store.read_stable().expect("stable reads"), Some(stable));
    }

    #[test]
    fn test_mismatched_namespace_fails_before_pending_file_creation() {
        let root = tempfile::tempdir().expect("temporary credential root");
        let current_namespace = namespace("0123456789abcdef01234567");
        let store = CredentialStore::with_root(current_namespace, root.path());
        let foreign = envelope(&namespace("fedcba9876543210fedcba98"));

        assert!(matches!(
            store.stage(&foreign),
            Err(CredentialStoreError::NamespaceMismatch)
        ));
        assert!(!store.pending_path.exists());
    }

    #[cfg(unix)]
    #[test]
    fn test_staged_envelope_and_directory_are_owner_only() {
        use std::os::unix::fs::PermissionsExt as _;

        let root = tempfile::tempdir().expect("temporary credential root");
        let namespace = namespace("0123456789abcdef01234567");
        let store = CredentialStore::with_root(namespace.clone(), root.path());
        store.stage(&envelope(&namespace)).expect("envelope stages");

        let directory_mode = std::fs::metadata(
            store
                .pending_path
                .parent()
                .expect("credential path has a parent"),
        )
        .expect("directory metadata")
        .permissions()
        .mode()
            & 0o777;
        let file_mode = std::fs::metadata(&store.pending_path)
            .expect("file metadata")
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(directory_mode, OWNER_DIRECTORY_MODE);
        assert_eq!(file_mode, OWNER_FILE_MODE);
    }

    #[tokio::test]
    async fn test_concurrent_persisted_issuance_converges_and_revokes_the_displaced_generation() {
        let database = tribal_test_utils::TestDb::new().await;
        let root = tempfile::tempdir().expect("temporary credential root");
        let namespace = namespace("0123456789abcdef01234567");
        let (coordinator, runtime) = CredentialCoordinator::spawn_with_root(
            namespace.clone(),
            root.path(),
            CancellationToken::new(),
        );
        let revision = ConfigRevision::from_digest(&ConfigDigest::from_bytes(b"credential-test"));
        let expires_at = Utc::now() + chrono::Duration::hours(1);

        let first = coordinator.issue_persisted(
            session(&database, &revision),
            tribal_domain::LOCAL_PRINCIPAL_KEY.to_owned(),
            full_access_scopes(),
            "http://localhost/mcp".to_owned(),
            expires_at,
        );
        let second = coordinator.issue_persisted(
            session(&database, &revision),
            tribal_domain::LOCAL_PRINCIPAL_KEY.to_owned(),
            full_access_scopes(),
            "http://localhost/mcp".to_owned(),
            expires_at,
        );
        let (first, second) = tokio::join!(first, second);
        let first = first.expect("first issuance completes");
        let second = second.expect("second issuance completes");
        assert_ne!(first.token.id(), second.token.id());
        assert_ne!(first.raw, second.raw);

        let mut connection = database.pool().acquire().await.unwrap();
        let mapping = PgLocalDefaultCredentialRepository
            .find(&mut connection, namespace.as_str())
            .await
            .unwrap()
            .expect("mapping exists");
        let first_row = PgAuthTokenRepository
            .find_by_id(&mut connection, first.token.id())
            .await
            .unwrap()
            .unwrap();
        let second_row = PgAuthTokenRepository
            .find_by_id(&mut connection, second.token.id())
            .await
            .unwrap()
            .unwrap();
        let mapped_is_first = mapping.token_id == first.token.id();
        assert!(mapped_is_first || mapping.token_id == second.token.id());
        assert_eq!(first_row.revoked_at().is_none(), mapped_is_first);
        assert_eq!(second_row.revoked_at().is_none(), !mapped_is_first);
        let stable = CredentialStore::with_root(namespace, root.path())
            .read_stable()
            .unwrap()
            .expect("stable envelope exists");
        assert_eq!(stable.token_id, mapping.token_id);

        let mut terminal = runtime.terminal();
        drop(coordinator);
        runtime.join().await.unwrap();
        terminal.changed().await.unwrap();
        assert_eq!(*terminal.borrow(), CredentialCoordinatorExit::InputClosed);
    }

    #[tokio::test]
    async fn test_ensure_reuses_only_an_exact_live_grant() {
        let database = tribal_test_utils::TestDb::new().await;
        let root = tempfile::tempdir().expect("temporary credential root");
        let namespace = namespace("abcdef0123456789abcdef01");
        let (coordinator, runtime) = CredentialCoordinator::spawn_with_root(
            namespace,
            root.path(),
            CancellationToken::new(),
        );
        let revision = ConfigRevision::from_digest(&ConfigDigest::from_bytes(b"ensure-test"));
        let now = Utc::now();
        let scopes = full_access_scopes();

        let first = coordinator
            .ensure_persisted(
                session(&database, &revision),
                tribal_domain::LOCAL_PRINCIPAL_KEY.to_owned(),
                scopes.clone(),
                "http://localhost/mcp".to_owned(),
                now,
                now + chrono::Duration::hours(1),
            )
            .await
            .expect("first ensure issues");
        assert_eq!(first.origin, PersistedIssuanceOrigin::Issued);
        let repeated = coordinator
            .ensure_persisted(
                session(&database, &revision),
                tribal_domain::LOCAL_PRINCIPAL_KEY.to_owned(),
                scopes.clone(),
                "http://localhost/mcp".to_owned(),
                now,
                now + chrono::Duration::hours(8),
            )
            .await
            .expect("exact ensure reuses");
        assert_eq!(repeated.origin, PersistedIssuanceOrigin::Existing);
        assert_eq!(repeated.token.id(), first.token.id());

        let mut narrower = scopes;
        narrower.pop();
        let replaced = coordinator
            .ensure_persisted(
                session(&database, &revision),
                tribal_domain::LOCAL_PRINCIPAL_KEY.to_owned(),
                narrower,
                "http://localhost/mcp".to_owned(),
                now,
                now + chrono::Duration::hours(1),
            )
            .await
            .expect("scope mismatch replaces");
        assert_eq!(replaced.origin, PersistedIssuanceOrigin::Issued);
        assert_ne!(replaced.token.id(), first.token.id());
        let old = PgAuthTokenRepository
            .find_by_id(
                &mut database.pool().acquire().await.unwrap(),
                first.token.id(),
            )
            .await
            .unwrap()
            .unwrap();
        assert!(old.revoked_at().is_some());

        drop(coordinator);
        runtime.join().await.unwrap();
    }
}
