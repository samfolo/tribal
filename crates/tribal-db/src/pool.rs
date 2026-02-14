//! Connection pool creation for the database layer.
//!
//! The startup sequence calls [`create_pool`] twice — once for the MCP
//! read-path pool and once for the worker write-path pool — each with its
//! own max-connections and statement-timeout settings.

use std::time::Duration;

use sqlx::postgres::PgPoolOptions;
use sqlx::{Executor, PgPool};
use tribal_domain::DatabaseConfig;

use crate::DbError;

/// Creates a PostgreSQL connection pool with the given configuration.
///
/// The pool is configured with:
/// - `max_connections` from the caller (differs between MCP and worker pools)
/// - `acquire_timeout` from [`DatabaseConfig::acquire_timeout_seconds`]
/// - Per-connection `statement_timeout` set via an `after_connect` callback
///
/// The `pool_name` parameter is used for tracing instrumentation and is the
/// value carried by [`DbError::PoolExhausted`] if the pool runs out of
/// connections at runtime.
///
/// # Errors
///
/// Returns [`DbError::QueryFailed`] if the initial connection to the
/// database fails.
pub async fn create_pool(
    config: &DatabaseConfig,
    pool_name: &'static str,
    max_connections: u32,
    statement_timeout_seconds: u64,
) -> Result<PgPool, DbError> {
    let pool = PgPoolOptions::new()
        .max_connections(max_connections)
        .acquire_timeout(Duration::from_secs(config.acquire_timeout_seconds))
        .after_connect(move |conn, _meta| {
            Box::pin(async move {
                let timeout_ms = statement_timeout_seconds.saturating_mul(1000);
                conn.execute(format!("SET statement_timeout = {timeout_ms}").as_str())
                    .await?;
                Ok(())
            })
        })
        .connect(&config.url)
        .await
        .map_err(|source| DbError::QueryFailed {
            context: format!("connecting to pool '{pool_name}'"),
            source,
        })?;

    tracing::info!(
        pool_name,
        max_connections,
        statement_timeout_seconds,
        acquire_timeout_seconds = config.acquire_timeout_seconds,
        "database pool created",
    );

    Ok(pool)
}
