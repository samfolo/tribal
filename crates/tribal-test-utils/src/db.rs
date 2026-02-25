//! Database test infrastructure: container lifecycle, connection management,
//! and per-test transaction isolation.
//!
//! [`TestContext`] manages a pgvector container (via testcontainers) and a
//! connection pool.  It is initialised once per test binary via
//! [`test_context`] and shared across all tests in that binary.
//!
//! [`TestTransaction`] wraps a raw [`PgConnection`] with a manual `BEGIN`.
//! When dropped, the connection is closed synchronously (TCP socket close),
//! and Postgres automatically rolls back the uncommitted transaction.
//!
//! # Why raw connections instead of pooled?
//!
//! Both `Transaction::drop` and `PoolConnection::drop` in sqlx 0.8 use
//! `tokio::spawn` internally.  Under `#[tokio::test]`'s default
//! `current_thread` runtime, each test creates its own runtime that shuts
//! down when the test future completes.  Spawned cleanup tasks are
//! cancelled before they can return connections to the pool, eventually
//! exhausting the pool's semaphore and causing `PoolTimedOut`.
//!
//! Raw `PgConnection` has no async `Drop` — it simply closes the TCP
//! socket synchronously.  Each test gets its own connection, and
//! Postgres rolls back the open transaction when the socket closes.

use std::{
    ops::{Deref, DerefMut},
    time::Duration,
};

use sqlx::{Connection, Executor, PgConnection, PgPool, postgres::PgPoolOptions};
use testcontainers::{
    ContainerAsync, GenericImage, ImageExt,
    core::{IntoContainerPort, WaitFor, wait::LogWaitStrategy},
    runners::AsyncRunner,
};

use crate::TestDbError;

/// Shared test database context: a pgvector container and connection pool.
///
/// Created once per test binary via [`test_context`]. The container is
/// started with the `ankane/pgvector` image, migrations are run, and a
/// connection pool is established.
///
/// Individual tests call [`begin_test`](TestContext::begin_test) to obtain
/// a [`TestTransaction`] that rolls back on drop.
///
/// # Container lifetime
///
/// The container stays alive for the lifetime of the `TestContext`. Since
/// `TestContext` is stored in a `static` [`OnceCell`](tokio::sync::OnceCell),
/// the container lives for the entire test binary execution. Testcontainers
/// removes the container when the process exits.
pub struct TestContext {
    /// The connection pool connected to the test container.
    pool: PgPool,
    /// Connection URL for creating raw (non-pooled) connections.
    database_url: String,
    /// The running container handle. Held to keep the container alive;
    /// dropped automatically when the process exits.
    _container: ContainerAsync<GenericImage>,
}

impl TestContext {
    /// Creates a new test context: starts a pgvector container, runs all
    /// migrations, and establishes a connection pool.
    ///
    /// # Errors
    ///
    /// Returns [`TestDbError`] if the container fails to start, host/port
    /// resolution fails, migrations fail, or the connection pool cannot
    /// be created.
    pub async fn new() -> Result<Self, TestDbError> {
        // During startup, "database system is ready to accept connections"
        // appears twice — once for the init-phase Unix socket and once
        // for the final TCP listener. Waiting for the second occurrence
        // ensures the TCP socket is ready. We check both stdout and
        // stderr because the Docker entrypoint may route Postgres log
        // output to either stream.
        let ready_condition = WaitFor::log(
            LogWaitStrategy::stdout_or_stderr("database system is ready to accept connections")
                .with_times(2),
        );

        let container = GenericImage::new("ankane/pgvector", "latest")
            .with_exposed_port(5432.tcp())
            .with_wait_for(ready_condition)
            .with_env_var("POSTGRES_DB", "tribal_test")
            .with_env_var("POSTGRES_USER", "tribal")
            .with_env_var("POSTGRES_PASSWORD", "tribal")
            .start()
            .await
            .map_err(|source| TestDbError::ContainerStart {
                context: "starting ankane/pgvector container".to_owned(),
                source,
            })?;

        let host = container
            .get_host()
            .await
            .map_err(|source| TestDbError::ContainerStart {
                context: "resolving container host".to_owned(),
                source,
            })?;

        let host_port = container.get_host_port_ipv4(5432).await.map_err(|source| {
            TestDbError::PortMapping {
                container_port: 5432,
                source,
            }
        })?;

        let database_url = format!("postgres://tribal:tribal@{host}:{host_port}/tribal_test");

        let pool = PgPoolOptions::new()
            .max_connections(5)
            .acquire_timeout(Duration::from_secs(5))
            .connect(&database_url)
            .await
            .map_err(|source| TestDbError::PoolCreation {
                context: format!("connecting to test database at {host}:{host_port}"),
                source,
            })?;

        tribal_db::MIGRATOR.run(&pool).await?;

        tracing::info!(
            %host,
            host_port,
            "test database ready: container started, migrations applied",
        );

        Ok(Self {
            pool,
            database_url,
            _container: container,
        })
    }

    /// Returns a reference to the connection pool.
    ///
    /// Prefer [`begin_test`](Self::begin_test) for individual tests to
    /// ensure isolation via transaction rollback.
    #[must_use]
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// Opens a raw (non-pooled) connection to the test database.
    ///
    /// Unlike pool connections, raw `PgConnection` drops synchronously
    /// (TCP socket close), avoiding the `PoolConnection::drop` spawn
    /// issue that leaks connections under `#[tokio::test]`.
    ///
    /// Statements execute with auto-commit (no wrapping transaction).
    /// Use this when tests need committed data visible to other pool
    /// connections (e.g. worker integration tests).
    ///
    /// # Errors
    ///
    /// Returns [`TestDbError::ConnectionFailed`] if the connection
    /// cannot be established.
    pub async fn raw_connection(&self) -> Result<PgConnection, TestDbError> {
        PgConnection::connect(&self.database_url)
            .await
            .map_err(|source| TestDbError::ConnectionFailed { source })
    }

    /// Creates an independent connection pool to the test database.
    ///
    /// Each call creates a fresh pool with new TCP connections, isolated
    /// from the shared [`pool`](Self::pool).  Use this in tests whose
    /// code-under-test holds pool connections across spawned tasks —
    /// a per-test pool avoids cross-test connection leaks caused by
    /// `PoolConnection::drop` under `#[tokio::test]`'s `current_thread`
    /// runtime.
    ///
    /// # Errors
    ///
    /// Returns [`TestDbError::PoolCreation`] if the pool cannot connect.
    pub async fn create_pool(&self) -> Result<PgPool, TestDbError> {
        PgPoolOptions::new()
            .max_connections(5)
            .acquire_timeout(Duration::from_secs(5))
            .connect(&self.database_url)
            .await
            .map_err(|source| TestDbError::PoolCreation {
                context: "creating per-test pool".into(),
                source,
            })
    }

    /// Begins a new test transaction.
    ///
    /// Opens a raw connection to the test database and sends `BEGIN`.
    /// When the returned [`TestTransaction`] is dropped, the TCP socket
    /// closes and Postgres automatically rolls back the transaction.
    ///
    /// # Errors
    ///
    /// Returns [`TestDbError::TransactionBegin`] if the connection cannot
    /// be established or the `BEGIN` statement fails.
    pub async fn begin_test(&self) -> Result<TestTransaction, TestDbError> {
        let mut conn = PgConnection::connect(&self.database_url)
            .await
            .map_err(|source| TestDbError::TransactionBegin { source })?;

        conn.execute("BEGIN")
            .await
            .map_err(|source| TestDbError::TransactionBegin { source })?;

        Ok(TestTransaction { conn })
    }
}

/// A raw database connection with an open `BEGIN`, rolled back on drop.
///
/// Wraps a [`PgConnection`] and implements [`DerefMut`] to expose the
/// underlying connection as an executor.  When dropped, the TCP socket
/// closes synchronously and Postgres rolls back the open transaction.
///
/// # Usage
///
/// ```rust,no_run
/// # async fn example(ctx: &tribal_test_utils::TestContext) {
/// let mut txn = ctx.begin_test().await.expect("should begin transaction");
///
/// // Use &mut *txn as an executor for sqlx queries:
/// sqlx::query("INSERT INTO principals (principal_key) VALUES ($1)")
///     .bind("test-agent")
///     .execute(&mut *txn)
///     .await
///     .expect("insert should succeed");
///
/// // Connection closed on drop; Postgres rolls back the transaction.
/// # }
/// ```
pub struct TestTransaction {
    /// The underlying raw connection (with an open `BEGIN`).
    conn: PgConnection,
}

impl Deref for TestTransaction {
    type Target = PgConnection;

    fn deref(&self) -> &Self::Target {
        &self.conn
    }
}

impl DerefMut for TestTransaction {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.conn
    }
}

/// Serialisation lock for tests that commit data to the shared database.
///
/// [`TestTransaction`]-based tests are isolated via rollback and can run
/// in parallel.  Tests that commit data (e.g. worker integration tests
/// whose spawned tasks acquire their own pool connections) share mutable
/// state in the `tasks` table and must run serially.
///
/// Hold the returned guard for the entire test.  Uses
/// [`tokio::sync::Mutex`] so the guard is `Send` and can be held
/// across `.await` points without triggering `clippy::await_holding_lock`.
pub async fn serial_lock() -> tokio::sync::MutexGuard<'static, ()> {
    static LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());
    LOCK.lock().await
}

/// Global test context, initialised once per test binary.
static CONTEXT: tokio::sync::OnceCell<TestContext> = tokio::sync::OnceCell::const_new();

/// Returns the shared test context, initialising it on first call.
///
/// The first invocation starts a pgvector container, runs migrations, and
/// creates a connection pool. Subsequent calls return the same context
/// immediately.
///
/// # Panics
///
/// Panics if the test context cannot be created (container start failure,
/// migration failure, or pool creation failure). This is intentional — a
/// test infrastructure failure should abort the test binary with a clear
/// diagnostic rather than propagating a `Result` through every test
/// function.
pub async fn test_context() -> &'static TestContext {
    CONTEXT
        .get_or_init(|| async {
            TestContext::new()
                .await
                .expect("failed to initialise test context")
        })
        .await
}
