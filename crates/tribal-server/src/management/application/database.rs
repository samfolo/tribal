//! Revision-bound database sessions for management capabilities.
use std::{future::Future, pin::Pin, sync::Arc};

use sqlx::PgPool;
use tribal_config::TribalConfig;
use tribal_db::{
    DbError, MigrationHeadStatus, MigrationRepository, NewPrincipal, PgMigrationRepository,
    PgPrincipalRepository, PrincipalRepository,
};
use tribal_domain::{LOCAL_PRINCIPAL_KEY, Principal};
use tribal_wire::management::{
    ConfigRevision, DatabaseInitialiseOutcome, DatabaseInitialiseRequest, DatabaseInitialiseResult,
    Revisioned,
};

use super::{
    super::{
        configuration::{ConfigAuthorityError, ResolvedConfigSnapshot},
        worker::ConfigWorkerClient,
    },
    operation::{OperationContext, OperationError},
};
use crate::startup::run_migrations;

const POOL_NAME: &str = "management";
const DEFAULT_DATABASE_URL: &str = "postgresql://tribal:tribal@localhost:5432/tribal";
pub(crate) const DATABASE_COMMAND_DEFAULTS: [(&str, &str); 1] =
    [("database.url", DEFAULT_DATABASE_URL)];
pub(crate) const COMMAND_POOL_MAX_CONNECTIONS: u32 = 1;
pub(crate) const COMMAND_STATEMENT_TIMEOUT_MS: u64 = 30_000;

type PoolFuture = Pin<Box<dyn Future<Output = Result<PgPool, tribal_db::DbError>> + Send>>;
type PoolFactory = Arc<dyn Fn(Arc<TribalConfig>) -> PoolFuture + Send + Sync>;

/// Manager-owned access to a database selected by a proven configuration.
#[derive(Clone)]
pub(crate) struct DatabaseAccess {
    config: ConfigWorkerClient,
    pool_factory: PoolFactory,
}

impl std::fmt::Debug for DatabaseAccess {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DatabaseAccess")
            .field("config", &self.config)
            .field("pool_factory", &"<database connector>")
            .finish()
    }
}

/// One pool and configuration view selected by the same durable revision.
#[derive(Debug, Clone)]
pub(crate) struct DatabaseSession {
    pub(crate) revision: ConfigRevision,
    pub(crate) config: Arc<TribalConfig>,
    pub(crate) pool: PgPool,
    operation: OperationContext,
}

impl DatabaseSession {
    #[cfg(test)]
    pub(super) fn for_test(
        revision: ConfigRevision,
        config: Arc<TribalConfig>,
        pool: PgPool,
    ) -> Self {
        Self {
            revision,
            config,
            pool,
            operation: OperationContext::new(tokio_util::sync::CancellationToken::new()),
        }
    }

    pub(crate) fn revisioned<T>(&self, value: T) -> Revisioned<T> {
        Revisioned {
            config_revision: self.revision.clone(),
            value,
        }
    }

    pub(super) fn checkpoint(&self) -> Result<(), DatabaseAccessError> {
        self.operation
            .checkpoint()
            .map_err(DatabaseAccessError::from)
    }

    pub(in crate::management) fn operation(&self) -> &OperationContext {
        &self.operation
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum DatabaseAccessError {
    #[error(transparent)]
    Operation(#[from] OperationError),
    #[error(transparent)]
    Configuration(#[from] ConfigAuthorityError),
    #[error("configuration revision conflict")]
    RevisionConflict {
        expected: ConfigRevision,
        actual: ConfigRevision,
    },
    #[error("database connection is unavailable: {source}")]
    Connection {
        #[source]
        source: tribal_db::DbError,
    },
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum DatabaseInitialiseError {
    #[error(transparent)]
    Session(#[from] DatabaseAccessError),
    #[error("database migration state is unavailable: {source}")]
    MigrationState {
        #[source]
        source: tribal_db::DbError,
    },
    #[error("database migration connection is unavailable: {source}")]
    MigrationConnection {
        #[source]
        source: sqlx::Error,
    },
    #[error("database migration failed: {source}")]
    Migration {
        #[source]
        source: crate::error::AppError,
    },
    #[error("local principal initialisation failed: {source}")]
    Principal {
        #[source]
        source: DbError,
    },
    #[error("compiled database migration catalogue is empty")]
    EmptyMigrationCatalogue,
}

impl DatabaseAccess {
    pub(crate) fn new(config: ConfigWorkerClient) -> Self {
        Self::with_pool_factory(
            config,
            Arc::new(|config| {
                Box::pin(async move {
                    tribal_db::create_pool(
                        &config.database,
                        POOL_NAME,
                        COMMAND_POOL_MAX_CONNECTIONS,
                        COMMAND_STATEMENT_TIMEOUT_MS,
                    )
                    .await
                })
            }),
        )
    }

    fn with_pool_factory(config: ConfigWorkerClient, pool_factory: PoolFactory) -> Self {
        Self {
            config,
            pool_factory,
        }
    }

    pub(crate) async fn session(
        &self,
        operation: &OperationContext,
        expected_revision: Option<&ConfigRevision>,
    ) -> Result<DatabaseSession, DatabaseAccessError> {
        let ResolvedConfigSnapshot { config, revision } =
            self.config_snapshot(operation, expected_revision).await?;
        let pool = operation
            .cancel_safe((self.pool_factory)(Arc::clone(&config)))
            .await?
            .map_err(|source| DatabaseAccessError::Connection { source })?;
        Ok(DatabaseSession {
            revision,
            config,
            pool,
            operation: operation.clone(),
        })
    }

    /// Resolves configuration without opening the operation's database pool.
    pub(super) async fn config_snapshot(
        &self,
        operation: &OperationContext,
        expected_revision: Option<&ConfigRevision>,
    ) -> Result<ResolvedConfigSnapshot, DatabaseAccessError> {
        let snapshot = operation
            .cancel_safe(self.config.resolved_snapshot())
            .await??;
        if let Some(expected) = expected_revision
            && expected != &snapshot.revision
        {
            return Err(DatabaseAccessError::RevisionConflict {
                expected: expected.clone(),
                actual: snapshot.revision,
            });
        }
        Ok(snapshot)
    }

    pub(crate) async fn read_session(
        &self,
        operation: &OperationContext,
    ) -> Result<DatabaseSession, DatabaseAccessError> {
        self.session(operation, None).await
    }

    pub(crate) async fn mutation_session(
        &self,
        operation: &OperationContext,
        expected_revision: &ConfigRevision,
    ) -> Result<DatabaseSession, DatabaseAccessError> {
        self.session(operation, Some(expected_revision)).await
    }

    pub(crate) async fn initialise(
        &self,
        operation: &OperationContext,
        request: DatabaseInitialiseRequest,
    ) -> Result<DatabaseInitialiseResult, DatabaseInitialiseError> {
        let session = self
            .mutation_session(operation, &request.expected_revision)
            .await?;
        let expected_head = tribal_db::MIGRATOR
            .iter()
            .last()
            .ok_or(DatabaseInitialiseError::EmptyMigrationCatalogue)?
            .version;
        let migration_state = operation
            .cancel_safe(async {
                let mut connection =
                    session.pool.acquire().await.map_err(|source| {
                        DatabaseInitialiseError::MigrationConnection { source }
                    })?;
                PgMigrationRepository
                    .current_head_matches(&mut connection, expected_head)
                    .await
                    .map_err(|source| DatabaseInitialiseError::MigrationState { source })
            })
            .await
            .map_err(DatabaseAccessError::from)??;

        operation
            .cancel_safe(run_migrations(&session.pool))
            .await
            .map_err(DatabaseAccessError::from)?
            .map_err(|source| DatabaseInitialiseError::Migration { source })?;

        let (transaction, principal_existed) = operation
            .cancel_safe(async {
                let mut transaction =
                    session.pool.begin().await.map_err(|source| {
                        DatabaseInitialiseError::MigrationConnection { source }
                    })?;
                let principal_existed = tribal_db::PgPrincipalRepository
                    .find_by_key(&mut transaction, LOCAL_PRINCIPAL_KEY)
                    .await
                    .map_err(|source| DatabaseInitialiseError::Principal { source })?
                    .is_some();
                find_or_create_principal(&mut transaction, LOCAL_PRINCIPAL_KEY)
                    .await
                    .map_err(|source| DatabaseInitialiseError::Principal { source })?;
                Ok::<_, DatabaseInitialiseError>((transaction, principal_existed))
            })
            .await
            .map_err(DatabaseAccessError::from)??;
        session.checkpoint()?;
        transaction
            .commit()
            .await
            .map_err(|source| DatabaseInitialiseError::MigrationConnection { source })?;

        let outcome =
            if matches!(migration_state, MigrationHeadStatus::Matches) && principal_existed {
                DatabaseInitialiseOutcome::AlreadyInitialised
            } else {
                DatabaseInitialiseOutcome::Initialised
            };
        Ok(session.revisioned(outcome).into())
    }
}

pub(super) async fn find_or_create_principal(
    connection: &mut sqlx::PgConnection,
    principal_key: &str,
) -> Result<Principal, DbError> {
    if let Some(principal) = PgPrincipalRepository
        .find_by_key(&mut *connection, principal_key)
        .await?
    {
        return Ok(principal);
    }

    let new = NewPrincipal::builder()
        .principal_key(principal_key.to_owned())
        .build();
    match PgPrincipalRepository.insert(&mut *connection, &new).await {
        Ok(principal) => Ok(principal),
        Err(DbError::UniqueViolation { .. }) => PgPrincipalRepository
            .find_by_key(connection, principal_key)
            .await?
            .ok_or_else(|| DbError::NotFound {
                entity: "principal",
                id: principal_key.to_owned(),
            }),
        Err(source) => Err(source),
    }
}

#[cfg(test)]
mod revision_session {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;
    use crate::management::{configuration::ConfigAuthority, worker};

    fn operation() -> OperationContext {
        OperationContext::new(tokio_util::sync::CancellationToken::new())
    }

    fn config_worker(
        database_url: &str,
    ) -> (
        tempfile::TempDir,
        std::path::PathBuf,
        ConfigWorkerClient,
        worker::ConfigWorkerRuntime,
    ) {
        let temp = tempfile::tempdir().expect("temporary config root");
        let path = temp.path().join("tribal.yaml");
        let config = TribalConfig::minimum_valid(database_url);
        std::fs::write(&path, serde_yaml::to_string(&config).unwrap()).unwrap();
        let (worker, runtime) = worker::spawn(ConfigAuthority::new(path.clone())).unwrap();
        (temp, path, worker, runtime)
    }

    fn lazy_pool() -> PgPool {
        sqlx::postgres::PgPoolOptions::new()
            .connect_lazy("postgres://user:pass@localhost:5432/tribal")
            .unwrap()
    }

    #[tokio::test]
    async fn test_mutation_refuses_stale_revision_before_connection() {
        let (_temp, _path, worker, _runtime) =
            config_worker("postgres://user:pass@localhost:5432/first");
        let connections = Arc::new(AtomicUsize::new(0));
        let observed = Arc::clone(&connections);
        let access = DatabaseAccess::with_pool_factory(
            worker,
            Arc::new(move |_config| {
                observed.fetch_add(1, Ordering::SeqCst);
                Box::pin(async { Ok(lazy_pool()) })
            }),
        );
        let stale = ConfigRevision::from_digest(
            &tribal_wire::management::ConfigDigest::from_bytes(b"stale"),
        );

        let error = access
            .mutation_session(&operation(), &stale)
            .await
            .unwrap_err();

        assert!(matches!(
            error,
            DatabaseAccessError::RevisionConflict { .. }
        ));
        assert_eq!(connections.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn test_read_session_keeps_one_captured_snapshot_and_attributes_its_receipt() {
        let (_temp, path, worker, _runtime) =
            config_worker("postgres://user:pass@localhost:5432/first");
        let replacement_path = path.clone();
        let access = DatabaseAccess::with_pool_factory(
            worker.clone(),
            Arc::new(move |config| {
                assert!(config.database.url.ends_with("/first"));
                let replacement =
                    TribalConfig::minimum_valid("postgres://user:pass@localhost:5432/second");
                std::fs::write(
                    &replacement_path,
                    serde_yaml::to_string(&replacement).unwrap(),
                )
                .unwrap();
                Box::pin(async { Ok(lazy_pool()) })
            }),
        );

        let session = access.read_session(&operation()).await.unwrap();
        let receipt = session.revisioned("captured");
        let later = worker.resolved_snapshot().await.unwrap();

        assert!(session.config.database.url.ends_with("/first"));
        assert_eq!(receipt.config_revision, session.revision);
        assert_ne!(later.revision, session.revision);
        assert!(!session.pool.is_closed());
    }

    #[tokio::test]
    async fn test_initialise_ensures_local_principal_then_becomes_a_typed_noop() {
        let database = tribal_test_utils::TestDb::new().await;
        let (_temp, _path, worker, _runtime) = config_worker(database.database_url());
        let revision = worker.resolved_snapshot().await.unwrap().revision;
        let access = DatabaseAccess::new(worker);

        let first = access
            .initialise(
                &operation(),
                DatabaseInitialiseRequest {
                    expected_revision: revision.clone(),
                },
            )
            .await
            .unwrap();
        let repeated = access
            .initialise(
                &operation(),
                DatabaseInitialiseRequest {
                    expected_revision: revision.clone(),
                },
            )
            .await
            .unwrap();

        assert_eq!(first.config_revision, revision);
        assert_eq!(first.value, DatabaseInitialiseOutcome::Initialised);
        assert_eq!(
            repeated.value,
            DatabaseInitialiseOutcome::AlreadyInitialised
        );
    }
}
