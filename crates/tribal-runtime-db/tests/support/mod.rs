//! Isolated runtime-database provisioning for tests.
//!
//! Each call carves a fresh database migrated to head, so a concurrency test
//! runs against real Postgres with no cross-test state. Two modes: an external
//! `DATABASE_URL` server (the `just test` wrapper) or a lazily-started embedded
//! container (plain `cargo test`).

use sqlx::{Connection, Executor, PgConnection, PgPool, postgres::PgPoolOptions};
use testcontainers::{
    ContainerAsync, GenericImage, ImageExt,
    core::{IntoContainerPort, WaitFor, wait::LogWaitStrategy},
    runners::AsyncRunner,
};
use url::Url;
use uuid::Uuid;

/// A provisioned, migrated runtime database for one test. Dropping it returns
/// the connections; an embedded container is torn down with the process.
pub struct RuntimeTestDb {
    pool: PgPool,
    _container: Option<ContainerAsync<GenericImage>>,
}

impl RuntimeTestDb {
    /// The pool connected to this test's isolated database.
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }
}

/// Provisions a fresh runtime database migrated to head.
pub async fn provision() -> RuntimeTestDb {
    let (base_url, container) = base_server().await;
    let db_name = format!("runtime_test_{}", Uuid::new_v4().simple());

    let mut admin = PgConnection::connect(&base_url)
        .await
        .expect("connect to the maintenance database");
    admin
        .execute(format!("CREATE DATABASE \"{db_name}\"").as_str())
        .await
        .expect("create the test database");
    admin
        .close()
        .await
        .expect("close the maintenance connection");

    let db_url = replace_database(&base_url, &db_name);
    let pool = PgPoolOptions::new()
        .max_connections(64)
        .connect(&db_url)
        .await
        .expect("connect to the test database");
    tribal_runtime_db::MIGRATOR
        .run(&pool)
        .await
        .expect("migrate the runtime database to head");

    RuntimeTestDb {
        pool,
        _container: container,
    }
}

/// Resolves the maintenance-database URL: the external `DATABASE_URL` if set,
/// otherwise a freshly started embedded container.
async fn base_server() -> (String, Option<ContainerAsync<GenericImage>>) {
    if let Ok(url) = std::env::var("DATABASE_URL") {
        return (url, None);
    }

    let ready = WaitFor::log(
        LogWaitStrategy::stdout_or_stderr("database system is ready to accept connections")
            .with_times(2),
    );
    let container = GenericImage::new("pgvector/pgvector", "0.8.2-pg17")
        .with_exposed_port(5432.tcp())
        .with_wait_for(ready)
        .with_env_var("POSTGRES_DB", "postgres")
        .with_env_var("POSTGRES_USER", "tribal")
        .with_env_var("POSTGRES_PASSWORD", "tribal")
        .start()
        .await
        .expect("start the embedded postgres container");
    let host = container.get_host().await.expect("resolve container host");
    let port = container
        .get_host_port_ipv4(5432)
        .await
        .expect("map container port");

    (
        format!("postgres://tribal:tribal@{host}:{port}/postgres"),
        Some(container),
    )
}

/// Returns `base_url` with its database path replaced by `db_name`, preserving
/// host, credentials, and query parameters.
fn replace_database(base_url: &str, db_name: &str) -> String {
    let mut url = Url::parse(base_url).expect("base url parses");
    url.set_path(db_name);
    url.into()
}
