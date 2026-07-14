//! Revision-bound database sessions for management capabilities.
#![allow(
    dead_code,
    reason = "database calls are required to cross this revision-bound seam"
)]

use std::{future::Future, pin::Pin, sync::Arc};

use sqlx::PgPool;
use tribal_config::TribalConfig;
use tribal_wire::management::{ConfigRevision, Revisioned};

use super::super::{
    configuration::{ConfigAuthorityError, ResolvedConfigSnapshot},
    worker::ConfigWorkerClient,
};
use crate::commands::common::{COMMAND_POOL_MAX_CONNECTIONS, COMMAND_STATEMENT_TIMEOUT_MS};

const POOL_NAME: &str = "management";

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
#[derive(Debug)]
pub(crate) struct DatabaseSession {
    pub(crate) revision: ConfigRevision,
    pub(crate) config: Arc<TribalConfig>,
    pub(crate) pool: PgPool,
}

impl DatabaseSession {
    pub(crate) fn revisioned<T>(&self, value: T) -> Revisioned<T> {
        Revisioned {
            config_revision: self.revision.clone(),
            value,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum DatabaseAccessError {
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
        expected_revision: Option<&ConfigRevision>,
    ) -> Result<DatabaseSession, DatabaseAccessError> {
        let ResolvedConfigSnapshot { config, revision } = self.config.resolved_snapshot().await?;
        if let Some(expected) = expected_revision
            && expected != &revision
        {
            return Err(DatabaseAccessError::RevisionConflict {
                expected: expected.clone(),
                actual: revision,
            });
        }
        let pool = (self.pool_factory)(Arc::clone(&config))
            .await
            .map_err(|source| DatabaseAccessError::Connection { source })?;
        Ok(DatabaseSession {
            revision,
            config,
            pool,
        })
    }

    pub(crate) async fn read_session(&self) -> Result<DatabaseSession, DatabaseAccessError> {
        self.session(None).await
    }

    pub(crate) async fn mutation_session(
        &self,
        expected_revision: &ConfigRevision,
    ) -> Result<DatabaseSession, DatabaseAccessError> {
        self.session(Some(expected_revision)).await
    }
}

#[cfg(test)]
mod revision_session {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;
    use crate::management::{configuration::ConfigAuthority, worker};

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

        let error = access.mutation_session(&stale).await.unwrap_err();

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

        let session = access.read_session().await.unwrap();
        let receipt = session.revisioned("captured");
        let later = worker.resolved_snapshot().await.unwrap();

        assert!(session.config.database.url.ends_with("/first"));
        assert_eq!(receipt.config_revision, session.revision);
        assert_ne!(later.revision, session.revision);
        assert!(!session.pool.is_closed());
    }
}
