//! Revision-bound database sessions for management capabilities.
use std::{future::Future, pin::Pin, sync::Arc};

use sqlx::{Connection as _, PgPool, postgres::PgConnectOptions};
use tribal_config::TribalConfig;
use tribal_db::{
    DbError, EnsurePrincipalOutcome, EnsureSystemOutcome, GraphIdentityRepository,
    MigrationHeadStatus, MigrationRepository, PgGraphIdentityRepository, PgMigrationRepository,
    PgPrincipalRepository, PgProjectRepository, PrincipalRepository, ProjectRepository,
};
use tribal_domain::{LOCAL_PRINCIPAL_KEY, Principal};
use tribal_wire::management::{
    ConfigRevision, DatabaseAdministrationTarget, DatabaseInitialiseOutcome,
    DatabaseInitialiseRequest, DatabaseInitialiseResult, DatabaseTargetFailure,
    DatabaseTargetFailureKind, DatabaseTargetInspectRequest, DatabaseTargetInspectResult,
    DatabaseTargetReceipt, DatabaseTargetState, FailurePresentation, Revisioned, SecretLiteral,
};

use super::{
    super::{
        configuration::{ConfigAuthorityError, ResolvedConfigSnapshot},
        product::database_endpoint,
        worker::{ConfigWorkerClient, ConfigWorkerRequestError},
    },
    operation::{OperationContext, OperationError},
};
use crate::{
    error::AppError,
    management::observed_at_unix_ms,
    startup::{MigrationRunOutcome, run_migrations},
};

const POOL_NAME: &str = "management";
const DEFAULT_DATABASE_URL: &str = "postgresql://tribal:tribal@localhost:5432/tribal";
/// The configuration key naming the database a transition is bound to.
pub(in crate::management) const DATABASE_URL_KEY: &str = "database.url";
pub(crate) const DATABASE_COMMAND_DEFAULTS: [(&str, &str); 1] =
    [(DATABASE_URL_KEY, DEFAULT_DATABASE_URL)];
pub(crate) const COMMAND_POOL_MAX_CONNECTIONS: u32 = 1;
pub(crate) const COMMAND_STATEMENT_TIMEOUT_MS: u64 = 30_000;
pub(super) const MIGRATION_TERMINAL_WINDOW: std::time::Duration =
    std::time::Duration::from_millis(COMMAND_STATEMENT_TIMEOUT_MS * 2);
/// One bound on any single reach for a candidate database — the inspection's
/// whole look, and each of a transition lease's operations. Generous against
/// a slow LAN, small against the 45 s operation window.
pub(in crate::management) const CANDIDATE_IO_WINDOW: std::time::Duration =
    std::time::Duration::from_secs(10);
const MUTATION_TERMINAL_WINDOW: std::time::Duration =
    std::time::Duration::from_millis(COMMAND_STATEMENT_TIMEOUT_MS);

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
        Self::for_test_with_operation(
            revision,
            config,
            pool,
            OperationContext::new(tokio_util::sync::CancellationToken::new()),
        )
    }

    #[cfg(test)]
    pub(super) fn for_test_with_operation(
        revision: ConfigRevision,
        config: Arc<TribalConfig>,
        pool: PgPool,
        operation: OperationContext,
    ) -> Self {
        Self {
            revision,
            config,
            pool,
            operation,
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
    #[error("database migration connection is unavailable: {source}")]
    MigrationConnection {
        #[source]
        source: sqlx::Error,
    },
    #[error("database migration failed: {source}")]
    Migration {
        #[source]
        source: AppError,
    },
    #[error("local principal initialisation failed: {source}")]
    Principal {
        #[source]
        source: DbError,
    },
    #[error("System project initialisation failed: {source}")]
    Project {
        #[source]
        source: DbError,
    },
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
        let snapshot = self
            .config
            .for_operation(operation)
            .resolved_snapshot()
            .await
            .map_err(config_worker_error)?;
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
        let (revision, pool) = match &request.target {
            DatabaseAdministrationTarget::Configured => {
                let session = self
                    .mutation_session(operation, &request.expected_revision)
                    .await?;
                (session.revision.clone(), session.pool.clone())
            }
            DatabaseAdministrationTarget::Candidate { url } => {
                let snapshot = self
                    .config_snapshot(operation, Some(&request.expected_revision))
                    .await?;
                let pool = operation
                    .cancel_safe(candidate_pool(url))
                    .await
                    .map_err(DatabaseAccessError::from)??;
                (snapshot.revision, pool)
            }
        };
        let revisioned = move |value| Revisioned {
            config_revision: revision,
            value,
        };
        let migration_outcome = operation
            .terminal(MIGRATION_TERMINAL_WINDOW, run_migrations(&pool))
            .await
            .map_err(DatabaseAccessError::from)?
            .map_err(|source| DatabaseInitialiseError::Migration { source })?;
        if migration_outcome == MigrationRunOutcome::GraphTransitionInProgress {
            return Ok(
                revisioned(DatabaseInitialiseOutcome::GraphTransitionInProgress {
                    transition_id: None,
                })
                .into(),
            );
        }

        let (transaction, principal_outcome, system_outcome) = operation
            .cancel_safe(async {
                let mut transaction = pool
                    .begin()
                    .await
                    .map_err(|source| DatabaseInitialiseError::MigrationConnection { source })?;
                let principal_outcome = PgPrincipalRepository
                    .ensure_local_by_key(&mut transaction, LOCAL_PRINCIPAL_KEY)
                    .await
                    .map_err(|source| DatabaseInitialiseError::Principal { source })?;
                let system_outcome = PgProjectRepository
                    .ensure_system(&mut transaction)
                    .await
                    .map_err(|source| DatabaseInitialiseError::Project { source })?;
                Ok::<_, DatabaseInitialiseError>((transaction, principal_outcome, system_outcome))
            })
            .await
            .map_err(DatabaseAccessError::from)??;
        operation
            .checkpoint()
            .map_err(DatabaseAccessError::from)
            .map_err(DatabaseInitialiseError::from)?;
        operation
            .terminal(MUTATION_TERMINAL_WINDOW, transaction.commit())
            .await
            .map_err(DatabaseAccessError::from)?
            .map_err(|source| DatabaseInitialiseError::MigrationConnection { source })?;

        let changed = matches!(migration_outcome, MigrationRunOutcome::Applied)
            || matches!(principal_outcome, EnsurePrincipalOutcome::Inserted(_))
            || matches!(system_outcome, EnsureSystemOutcome::Inserted(_));
        let outcome = if changed {
            DatabaseInitialiseOutcome::Initialised
        } else {
            DatabaseInitialiseOutcome::AlreadyInitialised
        };
        Ok(revisioned(outcome).into())
    }

    /// Inspects one candidate database read-only: reachability, schema state,
    /// and graph identity, without persisting or logging the secret and
    /// without touching the configured database.
    pub(crate) async fn inspect(
        &self,
        operation: &OperationContext,
        request: DatabaseTargetInspectRequest,
    ) -> Result<DatabaseTargetInspectResult, DatabaseAccessError> {
        let snapshot = self
            .config_snapshot(operation, Some(&request.expected_revision))
            .await?;
        let raw = request.candidate_url.expose_secret();
        let endpoint = database_endpoint(raw);
        let state = operation.cancel_safe(inspect_target_state(raw)).await?;
        Ok(Revisioned {
            config_revision: snapshot.revision,
            value: DatabaseTargetReceipt {
                endpoint,
                state,
                observed_at_unix_ms: observed_at_unix_ms(),
            },
        }
        .into())
    }
}

/// Opens a small one-off pool against the candidate for this request's
/// initialisation; the secret shapes the options and goes no further.
async fn candidate_pool(url: &SecretLiteral) -> Result<PgPool, DatabaseInitialiseError> {
    let options = parse_candidate_url(url.expose_secret()).map_err(|_| {
        DatabaseInitialiseError::MigrationConnection {
            source: sqlx::Error::Configuration("the candidate URL was refused".into()),
        }
    })?;
    sqlx::pool::PoolOptions::new()
        .max_connections(COMMAND_POOL_MAX_CONNECTIONS)
        .connect_with(options)
        .await
        .map_err(|source| DatabaseInitialiseError::MigrationConnection { source })
}

/// The connection parameters the pinned sqlx parser recognises by exact name;
/// it also accepts the `options[…]` family, which the check below admits
/// separately. Any other query key is refused before sqlx ever sees the URL:
/// the parser logs an unrecognised parameter's value at warn level, and a
/// candidate secret must never reach a log through that path.
const RECOGNISED_CANDIDATE_PARAMETERS: &[&str] = &[
    "sslmode",
    "ssl-mode",
    "sslrootcert",
    "ssl-root-cert",
    "ssl-ca",
    "sslcert",
    "ssl-cert",
    "sslkey",
    "ssl-key",
    "statement-cache-capacity",
    "host",
    "hostaddr",
    "port",
    "dbname",
    "user",
    "password",
    "application_name",
    "options",
];

/// Whether the pinned sqlx parser recognises this connection parameter: an
/// exact name, or a member of the `options[…]` family.
fn recognised_parameter(key: &str) -> bool {
    RECOGNISED_CANDIDATE_PARAMETERS.contains(&key)
        || (key.starts_with("options[") && key.ends_with(']'))
}

/// Parses a candidate URL without letting any part of it reach a log or a
/// receipt: the shape is proven with a silent parser, and a query key outside
/// the recognised set is refused with copy that renders no part of the URL.
pub(super) fn parse_candidate_url(raw: &str) -> Result<PgConnectOptions, DatabaseTargetFailure> {
    let parsed = url::Url::parse(raw).map_err(|_| {
        target_failure(
            DatabaseTargetFailureKind::InvalidUrl,
            "The URL is not a valid PostgreSQL connection string.",
            Some("Check the scheme, host, and database name."),
        )
    })?;
    // The offending key is not named: it is URL-derived text, and this copy
    // reaches receipts and diagnostics.
    if parsed
        .query_pairs()
        .any(|(key, _)| !recognised_parameter(&key))
    {
        return Err(target_failure(
            DatabaseTargetFailureKind::InvalidUrl,
            "The URL carries an unsupported connection parameter.",
            Some("Keep only the standard PostgreSQL connection parameters."),
        ));
    }
    raw.parse::<PgConnectOptions>().map_err(|_| {
        target_failure(
            DatabaseTargetFailureKind::InvalidUrl,
            "The URL is not a valid PostgreSQL connection string.",
            Some("Check the scheme, host, and database name."),
        )
    })
}

/// Connects to the candidate under one bounded window covering the connect,
/// the schema-head read, and the identity read. Every failure folds into a
/// typed, provider-neutral state whose copy carries no URL, host, or
/// credential.
pub(super) async fn inspect_target_state(raw: &str) -> DatabaseTargetState {
    let options = match parse_candidate_url(raw) {
        Ok(options) => options,
        Err(failure) => return DatabaseTargetState::Unavailable { failure },
    };
    let inspected = tokio::time::timeout(CANDIDATE_IO_WINDOW, async {
        let mut conn = match sqlx::PgConnection::connect_with(&options).await {
            Ok(conn) => conn,
            Err(error) => {
                return DatabaseTargetState::Unavailable {
                    failure: classify_connect_error(&error),
                };
            }
        };
        let state = schema_state(&mut conn).await;
        let _ = conn.close().await;
        state
    })
    .await;
    inspected.unwrap_or_else(|_elapsed| {
        unavailable(
            DatabaseTargetFailureKind::HostUnreachable,
            "The target did not answer within the inspection window.",
            Some("Check the host, port, and network path."),
        )
    })
}

/// Maps the migration head and graph identity onto the target state space.
async fn schema_state(conn: &mut sqlx::PgConnection) -> DatabaseTargetState {
    let Some(expected_head) = tribal_db::MIGRATOR.iter().last().map(|m| m.version) else {
        return unavailable(
            DatabaseTargetFailureKind::Unknown,
            "This binary carries no migration catalogue.",
            None,
        );
    };
    let Ok(observed) = PgMigrationRepository
        .current_head_matches(conn, expected_head)
        .await
    else {
        return unavailable(
            DatabaseTargetFailureKind::Unknown,
            "The schema state could not be read.",
            None,
        );
    };
    match observed {
        MigrationHeadStatus::MissingTable => DatabaseTargetState::Uninitialised,
        MigrationHeadStatus::Behind { found, .. } => DatabaseTargetState::Behind {
            pending_count: pending_after(found),
        },
        MigrationHeadStatus::Ahead { .. } => DatabaseTargetState::Ahead,
        MigrationHeadStatus::Matches => match PgGraphIdentityRepository.get(conn).await {
            Ok(graph_id) => DatabaseTargetState::Ready { graph_id },
            Err(DbError::GraphIdentityMissing) => unavailable(
                DatabaseTargetFailureKind::Unknown,
                "The database is migrated but carries no graph identity.",
                Some("Run database initialisation against this target."),
            ),
            Err(_) => unavailable(
                DatabaseTargetFailureKind::Unknown,
                "The graph identity could not be read.",
                None,
            ),
        },
    }
}

/// Counts the catalogue migrations above the found head.
fn pending_after(found: i64) -> u32 {
    let pending = tribal_db::MIGRATOR
        .iter()
        .filter(|migration| migration.version > found)
        .count();
    u32::try_from(pending).unwrap_or(u32::MAX)
}

/// Classifies a connection failure into the typed inspection kinds.
fn classify_connect_error(error: &sqlx::Error) -> DatabaseTargetFailure {
    match error {
        sqlx::Error::Tls(_) => target_failure(
            DatabaseTargetFailureKind::Tls,
            "TLS negotiation with the server failed.",
            Some("Check sslmode and the server certificate."),
        ),
        sqlx::Error::Io(_) => target_failure(
            DatabaseTargetFailureKind::HostUnreachable,
            "The host did not accept a connection.",
            Some("Check the host, port, and network path."),
        ),
        sqlx::Error::Protocol(_) => target_failure(
            DatabaseTargetFailureKind::IncompatibleServer,
            "The server did not speak the expected PostgreSQL protocol.",
            None,
        ),
        sqlx::Error::Configuration(_) => target_failure(
            DatabaseTargetFailureKind::InvalidUrl,
            "The connection options were refused before connecting.",
            Some("Check the URL's parameters."),
        ),
        sqlx::Error::Database(database) => match database.code().as_deref() {
            // 28xxx: invalid authorization specification.
            Some(code) if code.starts_with("28") => target_failure(
                DatabaseTargetFailureKind::Authentication,
                "The server refused the credentials.",
                Some("Check the username and password."),
            ),
            // 3D000: the named database does not exist.
            Some("3D000") => target_failure(
                DatabaseTargetFailureKind::DatabaseMissing,
                "The server has no database by that name.",
                Some("Create the database, or correct the name."),
            ),
            _ => target_failure(
                DatabaseTargetFailureKind::Unknown,
                "The server refused the connection.",
                None,
            ),
        },
        _ => target_failure(
            DatabaseTargetFailureKind::Unknown,
            "The connection failed.",
            None,
        ),
    }
}

fn target_failure(
    kind: DatabaseTargetFailureKind,
    message: &str,
    remediation: Option<&str>,
) -> DatabaseTargetFailure {
    DatabaseTargetFailure {
        kind,
        presentation: FailurePresentation {
            message: message.to_owned(),
            remediation: remediation.map(str::to_owned),
        },
    }
}

fn unavailable(
    kind: DatabaseTargetFailureKind,
    message: &str,
    remediation: Option<&str>,
) -> DatabaseTargetState {
    DatabaseTargetState::Unavailable {
        failure: target_failure(kind, message, remediation),
    }
}

fn config_worker_error(error: ConfigWorkerRequestError) -> DatabaseAccessError {
    match error {
        ConfigWorkerRequestError::Operation(error) => DatabaseAccessError::Operation(error),
        ConfigWorkerRequestError::Authority(error) => DatabaseAccessError::Configuration(error),
    }
}

pub(super) async fn find_or_create_principal(
    connection: &mut sqlx::PgConnection,
    principal_key: &str,
) -> Result<Principal, DbError> {
    match PgPrincipalRepository
        .ensure_local_by_key(connection, principal_key)
        .await?
    {
        EnsurePrincipalOutcome::Inserted(principal)
        | EnsurePrincipalOutcome::Existing(principal) => Ok(principal),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use tribal_test_utils::{TestDb, truncate_all_tables};

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
    async fn test_initialise_ensures_graph_defaults_then_becomes_a_typed_noop() {
        let database = tribal_test_utils::TestDb::new().await;
        let (_temp, _path, worker, _runtime) = config_worker(database.database_url());
        let revision = worker.resolved_snapshot().await.unwrap().revision;
        let access = DatabaseAccess::new(worker);

        let first = access
            .initialise(
                &operation(),
                DatabaseInitialiseRequest {
                    expected_revision: revision.clone(),
                    target: DatabaseAdministrationTarget::Configured,
                },
            )
            .await
            .unwrap();
        let repeated = access
            .initialise(
                &operation(),
                DatabaseInitialiseRequest {
                    expected_revision: revision.clone(),
                    target: DatabaseAdministrationTarget::Configured,
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

    #[tokio::test]
    async fn test_initialise_repairs_a_missing_system_project_at_current_head() {
        let database = tribal_test_utils::TestDb::new().await;
        let (_temp, _path, worker, _runtime) = config_worker(database.database_url());
        let revision = worker.resolved_snapshot().await.unwrap().revision;
        let access = DatabaseAccess::new(worker);
        access
            .initialise(
                &operation(),
                DatabaseInitialiseRequest {
                    expected_revision: revision.clone(),
                    target: DatabaseAdministrationTarget::Configured,
                },
            )
            .await
            .expect("establish graph defaults");
        let mut connection = database.raw_connection().await.expect("connect");
        truncate_all_tables(&mut connection).await;
        PgPrincipalRepository
            .ensure_local_by_key(&mut connection, LOCAL_PRINCIPAL_KEY)
            .await
            .expect("restore local principal");
        drop(connection);

        let repaired = access
            .initialise(
                &operation(),
                DatabaseInitialiseRequest {
                    expected_revision: revision.clone(),
                    target: DatabaseAdministrationTarget::Configured,
                },
            )
            .await
            .expect("repair System project");
        let repeated = access
            .initialise(
                &operation(),
                DatabaseInitialiseRequest {
                    expected_revision: revision,
                    target: DatabaseAdministrationTarget::Configured,
                },
            )
            .await
            .expect("repeat repair");

        assert_eq!(repaired.value, DatabaseInitialiseOutcome::Initialised);
        assert_eq!(
            repeated.value,
            DatabaseInitialiseOutcome::AlreadyInitialised
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_concurrent_initialise_calls_report_their_observed_mutations() {
        let database = tribal_test_utils::TestDb::new_unmigrated().await;
        let (_temp, _path, worker, _runtime) = config_worker(database.database_url());
        let revision = worker.resolved_snapshot().await.unwrap().revision;
        let first_access = DatabaseAccess::new(worker.clone());
        let second_access = DatabaseAccess::new(worker);
        let first_operation = operation();
        let second_operation = operation();
        let first_request = DatabaseInitialiseRequest {
            expected_revision: revision.clone(),
            target: DatabaseAdministrationTarget::Configured,
        };
        let second_request = DatabaseInitialiseRequest {
            expected_revision: revision,
            target: DatabaseAdministrationTarget::Configured,
        };

        let (first, second) = tokio::join!(
            first_access.initialise(&first_operation, first_request),
            second_access.initialise(&second_operation, second_request),
        );
        let mut outcomes = Vec::new();
        for result in [first, second] {
            match result {
                Ok(revisioned) => outcomes.push(revisioned.value),
                // The loser's bounded lock budget may expire while the
                // winner still runs the full catalogue; the documented
                // transient failure is a valid loser outcome.
                Err(DatabaseInitialiseError::Migration {
                    source: AppError::MigrationLockFailed { .. },
                }) => {}
                Err(other) => panic!("initialise failed: {other:?}"),
            }
        }

        assert!(outcomes.contains(&DatabaseInitialiseOutcome::Initialised));
        let mut connection = database.raw_connection().await.expect("connect");
        assert!(
            PgPrincipalRepository
                .find_by_key(&mut connection, LOCAL_PRINCIPAL_KEY)
                .await
                .expect("find local principal")
                .is_some()
        );
        PgProjectRepository
            .find_system(&mut connection)
            .await
            .expect("find System project");
    }

    // -----------------------------------------------------------------------
    // Target inspection
    // -----------------------------------------------------------------------

    #[test]
    fn test_classification_distinguishes_the_connect_failure_kinds() {
        let io = sqlx::Error::Io(std::io::Error::new(
            std::io::ErrorKind::ConnectionRefused,
            "refused",
        ));
        assert_eq!(
            classify_connect_error(&io).kind,
            DatabaseTargetFailureKind::HostUnreachable
        );

        let tls = sqlx::Error::Tls("handshake failed".into());
        assert_eq!(
            classify_connect_error(&tls).kind,
            DatabaseTargetFailureKind::Tls
        );

        let protocol = sqlx::Error::Protocol("not postgres".to_owned());
        assert_eq!(
            classify_connect_error(&protocol).kind,
            DatabaseTargetFailureKind::IncompatibleServer
        );

        let configuration = sqlx::Error::Configuration("bad options".into());
        assert_eq!(
            classify_connect_error(&configuration).kind,
            DatabaseTargetFailureKind::InvalidUrl
        );

        for failure in [io, tls, protocol, configuration].map(|e| classify_connect_error(&e)) {
            assert!(
                !failure.presentation.message.contains("postgres://"),
                "presentation copy carries no URL",
            );
        }
    }

    #[test]
    fn test_candidate_urls_with_unrecognised_parameters_are_refused_unrendered() {
        let refused = parse_candidate_url(
            "postgres://user:pass@localhost:5432/db?credential=super-secret-value",
        )
        .expect_err("an unrecognised parameter is refused");
        assert_eq!(refused.kind, DatabaseTargetFailureKind::InvalidUrl);
        let rendered = format!("{refused:?}");
        for material in [
            "super-secret-value",
            "credential",
            "user",
            "pass",
            "localhost",
        ] {
            assert!(
                !rendered.contains(material),
                "no URL-derived material reaches the refusal: {rendered}",
            );
        }

        parse_candidate_url("postgres://user:pass@localhost:5432/db?sslmode=require")
            .expect("recognised parameters pass");
        parse_candidate_url("postgres://user:pass@localhost:5432/db?options[search_path]=tribal")
            .expect("the options family passes");
    }

    #[test]
    fn test_pending_counts_catalogue_entries_above_the_found_head() {
        assert!(
            pending_after(0) >= 1,
            "an empty history is behind by the whole catalogue"
        );
        let head = tribal_db::MIGRATOR
            .iter()
            .last()
            .expect("catalogue")
            .version;
        assert_eq!(pending_after(head), 0);
    }

    #[tokio::test]
    async fn test_inspect_distinguishes_the_reachable_target_states() {
        let ctx = TestDb::new().await;

        // Ready: migrated with a seeded identity.
        let ready = inspect_target_state(ctx.database_url()).await;
        assert!(
            matches!(ready, DatabaseTargetState::Ready { .. }),
            "a migrated database is ready, got {ready:?}",
        );

        // Authentication: a wrong password against the same live server.
        let bad_credentials = swap_password(ctx.database_url());
        let denied = inspect_target_state(&bad_credentials).await;
        assert!(
            matches!(
                denied,
                DatabaseTargetState::Unavailable {
                    failure: DatabaseTargetFailure {
                        kind: DatabaseTargetFailureKind::Authentication,
                        ..
                    }
                }
            ),
            "wrong credentials classify as authentication, got {denied:?}",
        );

        // DatabaseMissing: a name the server does not have.
        let missing = swap_database(ctx.database_url(), "absent_database_name");
        let absent = inspect_target_state(&missing).await;
        assert!(
            matches!(
                absent,
                DatabaseTargetState::Unavailable {
                    failure: DatabaseTargetFailure {
                        kind: DatabaseTargetFailureKind::DatabaseMissing,
                        ..
                    }
                }
            ),
            "a missing database classifies as such, got {absent:?}",
        );

        // InvalidUrl: not a connection string at all.
        let invalid = inspect_target_state("not a url").await;
        assert!(matches!(
            invalid,
            DatabaseTargetState::Unavailable {
                failure: DatabaseTargetFailure {
                    kind: DatabaseTargetFailureKind::InvalidUrl,
                    ..
                }
            }
        ),);
    }

    #[tokio::test]
    async fn test_inspect_distinguishes_the_schema_states() {
        // Uninitialised: no migrations table at all.
        let bare = TestDb::new_unmigrated().await;
        let uninitialised = inspect_target_state(bare.database_url()).await;
        assert!(
            matches!(uninitialised, DatabaseTargetState::Uninitialised),
            "an unmigrated database is uninitialised, got {uninitialised:?}",
        );

        // Behind: migrated to a prior head only.
        let behind_db = TestDb::new_unmigrated().await;
        let prior_head = tribal_db::MIGRATOR
            .iter()
            .rev()
            .nth(1)
            .expect("at least two migrations")
            .version;
        let prior = sqlx::migrate::Migrator {
            migrations: std::borrow::Cow::Owned(
                tribal_db::MIGRATOR
                    .iter()
                    .filter(|migration| migration.version <= prior_head)
                    .cloned()
                    .collect(),
            ),
            ..sqlx::migrate::Migrator::DEFAULT
        };
        prior
            .run(behind_db.pool())
            .await
            .expect("migrate to the prior head");
        let behind = inspect_target_state(behind_db.database_url()).await;
        assert!(
            matches!(behind, DatabaseTargetState::Behind { pending_count: 1 }),
            "a prior-head database is behind by one, got {behind:?}",
        );

        // Ahead: a synthetic head past the binary's catalogue.
        let ahead_db = TestDb::new().await;
        let mut conn = ahead_db.raw_connection().await.expect("connection");
        let head = tribal_db::MIGRATOR
            .iter()
            .last()
            .expect("catalogue")
            .version;
        PgMigrationRepository
            .insert_synthetic_row_for_test(&mut conn, head + 1)
            .await
            .expect("stage an ahead head");
        let ahead = inspect_target_state(ahead_db.database_url()).await;
        assert!(
            matches!(ahead, DatabaseTargetState::Ahead),
            "a database past the catalogue is ahead, got {ahead:?}",
        );
    }

    #[tokio::test]
    async fn test_initialise_applies_to_a_candidate_target_and_converges() {
        let configured = TestDb::new().await;
        let candidate = TestDb::new_unmigrated().await;
        let (_temp, _path, worker, _runtime) = config_worker(configured.database_url());
        let revision = worker.resolved_snapshot().await.unwrap().revision;
        let access = DatabaseAccess::new(worker);
        let candidate_request = |revision| DatabaseInitialiseRequest {
            expected_revision: revision,
            target: DatabaseAdministrationTarget::Candidate {
                url: SecretLiteral::try_from(candidate.database_url().to_owned())
                    .expect("a valid candidate url"),
            },
        };

        let stale = ConfigRevision::from_digest(
            &tribal_wire::management::ConfigDigest::from_bytes(b"stale"),
        );
        let refused = access
            .initialise(&operation(), candidate_request(stale))
            .await
            .unwrap_err();
        assert!(matches!(
            refused,
            DatabaseInitialiseError::Session(DatabaseAccessError::RevisionConflict { .. })
        ));

        let first = access
            .initialise(&operation(), candidate_request(revision.clone()))
            .await
            .unwrap();
        assert_eq!(first.value, DatabaseInitialiseOutcome::Initialised);

        let ready = inspect_target_state(candidate.database_url()).await;
        assert!(
            matches!(ready, DatabaseTargetState::Ready { .. }),
            "an initialised candidate is ready with an identity, got {ready:?}",
        );

        let repeated = access
            .initialise(&operation(), candidate_request(revision))
            .await
            .unwrap();
        assert_eq!(
            repeated.value,
            DatabaseInitialiseOutcome::AlreadyInitialised,
            "candidate initialisation converges under repeats",
        );
    }

    fn swap_password(url: &str) -> String {
        let mut parsed = url::Url::parse(url).expect("test database url");
        parsed
            .set_password(Some("wrong-password"))
            .expect("set password");
        parsed.to_string()
    }

    fn swap_database(url: &str, database: &str) -> String {
        let mut parsed = url::Url::parse(url).expect("test database url");
        parsed.set_path(&format!("/{database}"));
        parsed.to_string()
    }
}
