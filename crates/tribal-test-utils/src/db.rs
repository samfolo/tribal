//! Database test infrastructure: template-clone-per-test isolation.
//!
//! Every test runs against its own physical Postgres database, cloned from a
//! single migrated *template* via `CREATE DATABASE … TEMPLATE` (a ~10–20ms
//! copy). Each test owns its database, so committed data is isolated with
//! nothing to serialise.
//!
//! # Two runner modes, one code path
//!
//! - **External** — `DATABASE_URL` is set (the `just test` wrapper, which runs
//!   the suite under `cargo nextest`). It points at a running server whose
//!   template was built once up front; each [`TestDb`] clones a fresh database
//!   from it.
//! - **Embedded** — `DATABASE_URL` is unset (plain `cargo test`). A pgvector
//!   container is started once per process via testcontainers and the template
//!   is built in-process; each [`TestDb`] clones a fresh database from it.
//!
//! Either way every test owns its database. Tests that mutate process-global
//! state — environment variables or the working directory — serialise on
//! [`env_lock`], since that state is shared across all threads in a process.
//!
//! [`TestTransaction`] uses a raw [`PgConnection`] rather than a pooled one:
//! pooled- and transaction-connection drops in sqlx spawn async cleanup, which
//! is cancelled when a `#[tokio::test]` `current_thread` runtime shuts down —
//! eventually exhausting the pool (`PoolTimedOut`). A raw connection closes its
//! socket synchronously and Postgres rolls back the open transaction.

use std::{
    ops::{Deref, DerefMut},
    time::Duration,
};

use sqlx::{
    Connection, Executor, PgConnection, PgPool, migrate::Migrator, postgres::PgPoolOptions,
};
use testcontainers::{
    ContainerAsync, GenericImage, ImageExt,
    core::{IntoContainerPort, WaitFor, wait::LogWaitStrategy},
    runners::AsyncRunner,
};
use url::Url;
use uuid::Uuid;

use crate::TestDbError;

/// Postgres connection ceiling for the embedded-mode container.
///
/// A parallel run opens many pools against one server at once, so the stock
/// ceiling of 100 is a source of `PoolTimedOut` flakes under load. The
/// container is ephemeral, so the headroom costs nothing past the run. The
/// `just test` wrapper and the `db-up` dev container carry the same ceiling;
/// keep the three in step.
const TEST_DB_MAX_CONNECTIONS: u32 = 500;

/// Name of the migrated template database that every test clones from.
const TEMPLATE_DB_NAME: &str = "tribal_template";

/// The Postgres *maintenance* database. `CREATE`/`DROP DATABASE` cannot run
/// while connected to the database being created or dropped, so all such
/// statements are issued from a connection to this database.
const MAINTENANCE_DB_NAME: &str = "postgres";

/// Advisory-lock key serialising template construction across processes.
///
/// Under nextest many test processes may race to build the template on first
/// use; this session-level advisory lock ensures exactly one builds it while
/// the rest wait and then observe that it already exists. The value is
/// arbitrary but must be stable across processes.
const TEMPLATE_BUILD_LOCK_KEY: i64 = 8_241_980_123;

/// Returns `url` with its database (the path segment) replaced by `database`,
/// preserving scheme, credentials, host, port, and query parameters.
///
/// # Panics
///
/// Panics if `url` cannot be parsed. The inputs are either URLs this module
/// constructs or the `DATABASE_URL` the run is configured with, so a parse
/// failure is a setup error that should abort the test loudly.
fn replace_database(url: &str, database: &str) -> String {
    let mut parsed = Url::parse(url).expect("connection URL must be a valid URL");
    parsed.set_path(&format!("/{database}"));
    parsed.into()
}

// ---------------------------------------------------------------------------
// Base server resolution (external DATABASE_URL or embedded container)
// ---------------------------------------------------------------------------

/// An embedded pgvector server, started once per process in the absence of an
/// external `DATABASE_URL`. The container handle is held to keep it alive;
/// testcontainers removes it when the process exits.
struct EmbeddedServer {
    /// Connection URL to the maintenance database on the embedded server.
    base_url: String,
    /// The running container handle, kept alive for the process lifetime.
    _container: ContainerAsync<GenericImage>,
}

/// The embedded server, lazily started once per process.
static EMBEDDED_SERVER: tokio::sync::OnceCell<EmbeddedServer> = tokio::sync::OnceCell::const_new();

/// Marks the template as built for this process (built at most once).
static TEMPLATE_READY: tokio::sync::OnceCell<()> = tokio::sync::OnceCell::const_new();

/// Starts the embedded pgvector container and returns its maintenance URL.
async fn start_embedded_server() -> Result<EmbeddedServer, TestDbError> {
    // During startup, "database system is ready to accept connections"
    // appears twice — once for the init-phase Unix socket and once for the
    // final TCP listener. Waiting for the second occurrence ensures the TCP
    // socket is ready. Both streams are checked because the entrypoint may
    // route Postgres log output to either.
    let ready_condition = WaitFor::log(
        LogWaitStrategy::stdout_or_stderr("database system is ready to accept connections")
            .with_times(2),
    );

    let container = GenericImage::new("pgvector/pgvector", "0.8.2-pg17")
        .with_exposed_port(5432.tcp())
        .with_wait_for(ready_condition)
        .with_env_var("POSTGRES_DB", MAINTENANCE_DB_NAME)
        .with_env_var("POSTGRES_USER", "tribal")
        .with_env_var("POSTGRES_PASSWORD", "tribal")
        // A leading `-c` makes the entrypoint launch Postgres with these
        // settings (it prepends `postgres` to a dash-led command). `fsync` and
        // friends are disabled because the data is ephemeral and this speeds
        // up template cloning.
        .with_cmd([
            "-c".to_owned(),
            format!("max_connections={TEST_DB_MAX_CONNECTIONS}"),
            "-c".to_owned(),
            "fsync=off".to_owned(),
            "-c".to_owned(),
            "full_page_writes=off".to_owned(),
            "-c".to_owned(),
            "synchronous_commit=off".to_owned(),
        ])
        .start()
        .await
        .map_err(|source| TestDbError::ContainerStart {
            context: "starting pgvector/pgvector container".to_owned(),
            source,
        })?;

    let host = container
        .get_host()
        .await
        .map_err(|source| TestDbError::ContainerStart {
            context: "resolving container host".to_owned(),
            source,
        })?;
    let host_port =
        container
            .get_host_port_ipv4(5432)
            .await
            .map_err(|source| TestDbError::PortMapping {
                container_port: 5432,
                source,
            })?;

    let base_url = format!("postgres://tribal:tribal@{host}:{host_port}/{MAINTENANCE_DB_NAME}");

    tracing::info!(%host, host_port, "embedded test server ready");

    Ok(EmbeddedServer {
        base_url,
        _container: container,
    })
}

/// Resolves the base (maintenance-database) URL for this process: the external
/// `DATABASE_URL` if set, otherwise a lazily-started embedded container.
async fn base_url() -> Result<String, TestDbError> {
    if let Ok(url) = std::env::var("DATABASE_URL") {
        return Ok(url);
    }
    let server = EMBEDDED_SERVER
        .get_or_try_init(start_embedded_server)
        .await?;
    Ok(server.base_url.clone())
}

// ---------------------------------------------------------------------------
// Template construction
// ---------------------------------------------------------------------------

/// Builds the migrated template database once on `base_url`'s server.
///
/// Connects to the maintenance database, takes a session-level advisory lock
/// (so concurrent processes don't race), and — if the template does not
/// already exist — creates it, runs `migrator` against it, and freezes it
/// (`IS_TEMPLATE true`, `ALLOW_CONNECTIONS false`) so it can be used as a
/// `CREATE DATABASE … TEMPLATE` source. Idempotent: if the template already
/// exists it is left untouched. `migrator` supplies the schema migrations.
///
/// # Errors
///
/// Returns [`TestDbError`] if the maintenance connection fails, the advisory
/// lock or any DDL statement fails, or migrations fail.
pub async fn build_test_template(base_url: &str, migrator: &Migrator) -> Result<(), TestDbError> {
    let admin_url = replace_database(base_url, MAINTENANCE_DB_NAME);
    let mut admin = PgConnection::connect(&admin_url)
        .await
        .map_err(|source| TestDbError::ConnectionFailed { source })?;

    sqlx::query("SELECT pg_advisory_lock($1)")
        .bind(TEMPLATE_BUILD_LOCK_KEY)
        .execute(&mut admin)
        .await
        .map_err(|source| TestDbError::Provision {
            context: "acquiring template build advisory lock".to_owned(),
            source,
        })?;

    // Build under the lock; release it afterwards on every path. `admin` is
    // owned throughout, so DDL uses the simple query protocol (a raw `&str`),
    // which Postgres requires for `CREATE DATABASE` / `ALTER DATABASE`.
    let result = async {
        let exists: bool =
            sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname = $1)")
                .bind(TEMPLATE_DB_NAME)
                .fetch_one(&mut admin)
                .await
                .map_err(|source| TestDbError::Provision {
                    context: "checking for existing template database".to_owned(),
                    source,
                })?;
        if exists {
            return Ok(());
        }

        admin
            .execute(format!("CREATE DATABASE \"{TEMPLATE_DB_NAME}\"").as_str())
            .await
            .map_err(|source| TestDbError::Provision {
                context: "creating template database".to_owned(),
                source,
            })?;

        // Run migrations against the freshly-created template, then drop the
        // pool so no connection lingers when we freeze it.
        let template_url = replace_database(base_url, TEMPLATE_DB_NAME);
        let pool = PgPoolOptions::new()
            .max_connections(1)
            .acquire_timeout(Duration::from_secs(10))
            .connect(&template_url)
            .await
            .map_err(|source| TestDbError::PoolCreation {
                context: "connecting to template database for migration".to_owned(),
                source,
            })?;
        migrator.run(&pool).await?;
        pool.close().await;

        // Terminate any straggling backends on the template before freezing so
        // the first `CREATE DATABASE … TEMPLATE` cannot fail on a lingering
        // connection.
        let _ = sqlx::query(
            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = $1",
        )
        .bind(TEMPLATE_DB_NAME)
        .execute(&mut admin)
        .await;

        admin
            .execute(
                format!(
                    "ALTER DATABASE \"{TEMPLATE_DB_NAME}\" \
                     WITH IS_TEMPLATE true ALLOW_CONNECTIONS false"
                )
                .as_str(),
            )
            .await
            .map_err(|source| TestDbError::Provision {
                context: "freezing template database".to_owned(),
                source,
            })?;

        tracing::info!(template = TEMPLATE_DB_NAME, "test template database built");
        Ok(())
    }
    .await;

    // A failure to unlock is non-fatal (the session ends with the connection),
    // so it does not mask the build result.
    let _ = sqlx::query("SELECT pg_advisory_unlock($1)")
        .bind(TEMPLATE_BUILD_LOCK_KEY)
        .execute(&mut admin)
        .await;

    result
}

/// Ensures the template exists for this process, building it at most once.
async fn ensure_template(base_url: &str) -> Result<(), TestDbError> {
    TEMPLATE_READY
        .get_or_try_init(|| build_test_template(base_url, &tribal_db::MIGRATOR))
        .await?;
    Ok(())
}

// ---------------------------------------------------------------------------
// TestDb — one cloned database per instance
// ---------------------------------------------------------------------------

/// An isolated test database, cloned from the migrated template.
///
/// Construction clones a fresh `t_<uuid>` database via `CREATE DATABASE …
/// TEMPLATE`. On drop the database is dropped with `WITH (FORCE)` so churn does
/// not accumulate within a long-lived server.
///
/// Each [`TestDb`] owns its database, so committed data is isolated without any
/// transaction or lock. Use [`pool`](Self::pool) for the shared pool,
/// [`create_pool`](Self::create_pool) for an independent pool,
/// [`raw_connection`](Self::raw_connection) for a non-pooled connection, or
/// [`begin`](Self::begin) for a rolled-back transaction.
pub struct TestDb {
    /// Connection pool to this test's cloned database.
    pool: PgPool,
    /// Connection URL to this test's cloned database.
    database_url: String,
    /// Maintenance-database URL used to drop this database on cleanup.
    admin_url: String,
    /// The cloned database's name (`t_<uuid>`).
    db_name: String,
}

impl TestDb {
    /// Provisions a fresh isolated database, building the template on first use.
    ///
    /// # Panics
    ///
    /// Panics if the database cannot be provisioned. A test-infrastructure
    /// failure should abort the test with a clear diagnostic rather than
    /// propagating a `Result` through every test.
    pub async fn new() -> Self {
        Self::try_new()
            .await
            .expect("failed to provision test database")
    }

    /// Provisions a fresh isolated database, returning errors instead of
    /// panicking.
    ///
    /// # Errors
    ///
    /// Returns [`TestDbError`] if the base server is unreachable, the template
    /// cannot be built, the clone cannot be created, or the pool cannot connect.
    pub async fn try_new() -> Result<Self, TestDbError> {
        let base = base_url().await?;
        ensure_template(&base).await?;

        let admin_url = replace_database(&base, MAINTENANCE_DB_NAME);
        let db_name = format!("t_{}", Uuid::new_v4().simple());

        let mut admin = PgConnection::connect(&admin_url)
            .await
            .map_err(|source| TestDbError::ConnectionFailed { source })?;
        admin
            .execute(
                format!("CREATE DATABASE \"{db_name}\" TEMPLATE \"{TEMPLATE_DB_NAME}\"").as_str(),
            )
            .await
            .map_err(|source| TestDbError::Provision {
                context: format!("cloning test database {db_name}"),
                source,
            })?;
        // Close the maintenance connection promptly; cleanup opens its own.
        let _ = admin.close().await;

        let database_url = replace_database(&base, &db_name);
        let pool = PgPoolOptions::new()
            .max_connections(5)
            .acquire_timeout(Duration::from_secs(5))
            .connect(&database_url)
            .await
            .map_err(|source| TestDbError::PoolCreation {
                context: format!("connecting to cloned database {db_name}"),
                source,
            })?;

        Ok(Self {
            pool,
            database_url,
            admin_url,
            db_name,
        })
    }

    /// Returns the connection URL for this test's database.
    ///
    /// Thread this into `TribalConfig.database.url` for server/e2e tests so the
    /// application under test points at this test's own database.
    pub fn database_url(&self) -> &str {
        &self.database_url
    }

    /// Returns a reference to this test's connection pool.
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// Opens a raw (non-pooled) connection to this test's database.
    ///
    /// Statements execute with auto-commit. Raw `PgConnection` drops
    /// synchronously, avoiding the pooled-connection `Drop` spawn issue under
    /// `#[tokio::test]`.
    ///
    /// # Errors
    ///
    /// Returns [`TestDbError::ConnectionFailed`] if the connection cannot be
    /// established.
    pub async fn raw_connection(&self) -> Result<PgConnection, TestDbError> {
        PgConnection::connect(&self.database_url)
            .await
            .map_err(|source| TestDbError::ConnectionFailed { source })
    }

    /// Creates an independent connection pool to this test's database.
    ///
    /// Each call creates a fresh pool. Use this where the code under test holds
    /// pool connections across spawned tasks.
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
                context: "creating per-test pool".to_owned(),
                source,
            })
    }

    /// Begins a transaction on a raw connection, rolled back on drop.
    ///
    /// Opens a raw connection and sends `BEGIN`. When the returned
    /// [`TestTransaction`] drops, the socket closes and Postgres rolls back.
    ///
    /// # Errors
    ///
    /// Returns [`TestDbError::TransactionBegin`] if the connection or `BEGIN`
    /// fails.
    pub async fn begin(&self) -> Result<TestTransaction, TestDbError> {
        let mut conn = PgConnection::connect(&self.database_url)
            .await
            .map_err(|source| TestDbError::TransactionBegin { source })?;
        conn.execute("BEGIN")
            .await
            .map_err(|source| TestDbError::TransactionBegin { source })?;
        Ok(TestTransaction { conn })
    }
}

impl Drop for TestDb {
    fn drop(&mut self) {
        // `Drop` is synchronous and may run on a `current_thread` runtime that
        // is shutting down, where spawning is unreliable. Run the drop on a
        // dedicated thread with its own runtime so it always completes.
        let admin_url = self.admin_url.clone();
        let db_name = self.db_name.clone();
        let _ = std::thread::spawn(move || {
            let Ok(rt) = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
            else {
                return;
            };
            rt.block_on(async {
                if let Ok(mut admin) = PgConnection::connect(&admin_url).await {
                    let _ = admin
                        .execute(
                            format!("DROP DATABASE IF EXISTS \"{db_name}\" WITH (FORCE)").as_str(),
                        )
                        .await;
                    let _ = admin.close().await;
                }
            });
        })
        .join();
    }
}

/// A raw database connection with an open `BEGIN`, rolled back on drop.
///
/// Implements [`DerefMut`] to expose the underlying connection as an executor.
/// When dropped, the TCP socket closes synchronously and Postgres rolls back
/// the open transaction.
///
/// # Usage
///
/// ```rust,no_run
/// # async fn example(db: &tribal_test_utils::TestDb) {
/// let mut txn = db.begin().await.expect("should begin transaction");
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

/// Returns a lazy `PgPool` that never connects to a real database.
///
/// Suitable for tests that construct a type requiring a `PgPool` but never
/// exercise a code path that acquires a connection.
///
/// # Panics
///
/// Panics if the dummy connection string cannot be parsed.
pub fn lazy_pool() -> PgPool {
    PgPool::connect_lazy("postgres://localhost/dummy").expect("lazy pool URL must parse")
}

/// Serialises tests that mutate process-global state — environment variables
/// or the current working directory.
///
/// Database state is isolated per test by [`TestDb`], but environment variables
/// and the process working directory are shared by every thread in a process.
/// Tests that mutate them (via env/cwd guards) acquire this lock so they do not
/// observe one another's mutations. Under `cargo nextest` (process-per-test)
/// the lock is uncontended; under `cargo test` (threads in one process) it
/// serialises those tests.
///
/// Acquire it as the first line of the test and hold the guard for the whole
/// test. Uses [`tokio::sync::Mutex`] so the guard is `Send` across `.await`.
pub async fn env_lock() -> tokio::sync::MutexGuard<'static, ()> {
    static LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());
    LOCK.lock().await
}

#[cfg(test)]
mod tests {
    use super::replace_database;

    #[test]
    fn replace_database_swaps_the_path_segment() {
        assert_eq!(
            replace_database("postgres://tribal:tribal@localhost:55432/postgres", "t_abc"),
            "postgres://tribal:tribal@localhost:55432/t_abc",
        );
    }

    #[test]
    fn replace_database_preserves_query_parameters() {
        assert_eq!(
            replace_database(
                "postgres://u:p@host:5432/olddb?sslmode=disable&application_name=x",
                "newdb",
            ),
            "postgres://u:p@host:5432/newdb?sslmode=disable&application_name=x",
        );
    }

    #[test]
    fn replace_database_handles_authority_without_path() {
        assert_eq!(
            replace_database("postgres://u:p@host:5432", "newdb"),
            "postgres://u:p@host:5432/newdb",
        );
    }
}
