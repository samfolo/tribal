//! Database test infrastructure: container lifecycle, pool management,
//! and per-test transaction isolation.
//!
//! [`TestContext`] manages a pgvector container (via testcontainers) and a
//! connection pool. It is initialised once per test binary via
//! [`test_context`] and shared across all tests in that binary.
//!
//! [`TestTransaction`] wraps a pooled connection with a manual `BEGIN`.
//! The pool's `before_acquire` hook sends `ROLLBACK` before each reuse,
//! providing complete test isolation without schema separation.
//!
//! # Why not `sqlx::Transaction`?
//!
//! `Transaction::drop` spawns an async rollback task via `tokio::spawn`.
//! Under `#[tokio::test]`'s default `current_thread` runtime, the runtime
//! shuts down before the task completes, leaking the pool connection.
//! By using a raw `PoolConnection` with manual `BEGIN`, we ensure
//! `PoolConnection::drop` (which is synchronous) always returns the
//! connection to the pool.  The deferred `ROLLBACK` in `before_acquire`
//! cleans up dirty state before the next test reuses the connection.

use std::{
    ops::{Deref, DerefMut},
    time::Duration,
};

use sqlx::{
    Executor, PgConnection, PgPool, Postgres, pool::PoolConnection, postgres::PgPoolOptions,
};
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
            .before_acquire(|conn, _meta| {
                Box::pin(async move {
                    conn.execute("ROLLBACK").await.ok();
                    Ok(true)
                })
            })
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

    /// Begins a new test transaction.
    ///
    /// Acquires a pooled connection and sends `BEGIN` manually.  When the
    /// returned [`TestTransaction`] is dropped, `PoolConnection::drop`
    /// returns the connection synchronously.  The pool's `before_acquire`
    /// hook sends `ROLLBACK` before the next test reuses the connection.
    ///
    /// # Errors
    ///
    /// Returns [`TestDbError::TransactionBegin`] if the connection cannot
    /// be acquired or the `BEGIN` statement fails.
    pub async fn begin_test(&self) -> Result<TestTransaction, TestDbError> {
        let mut conn = self
            .pool
            .acquire()
            .await
            .map_err(|source| TestDbError::TransactionBegin { source })?;

        conn.execute("BEGIN")
            .await
            .map_err(|source| TestDbError::TransactionBegin { source })?;

        Ok(TestTransaction { conn })
    }
}

/// A pooled connection with an open `BEGIN` that is rolled back on reuse.
///
/// Wraps a [`PoolConnection<Postgres>`] and implements [`DerefMut`] to
/// expose the underlying [`PgConnection`] as an executor.  When dropped,
/// `PoolConnection::drop` returns the connection to the pool synchronously
/// (no async spawn).  The pool's `before_acquire` hook sends `ROLLBACK`
/// before the next test reuses the connection.
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
/// // Connection returned to pool on drop; ROLLBACK runs on next acquire.
/// # }
/// ```
pub struct TestTransaction {
    /// The underlying pooled connection (with an open `BEGIN`).
    conn: PoolConnection<Postgres>,
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
