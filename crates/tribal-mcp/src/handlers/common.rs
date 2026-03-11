//! Shared utilities for MCP tool handlers.

use rmcp::model::CallToolResult;
use sqlx::PgPool;
use tribal_db::DbError;

use crate::{
    error::{IntoCallToolResult, IntoMcpError},
    server_handler::POOL_NAME,
};

// ---------------------------------------------------------------------------
// Pool helpers
// ---------------------------------------------------------------------------

/// Acquires a connection from the pool, mapping errors to `CallToolResult`.
///
/// Pool exhaustion and unexpected acquisition failures are converted into
/// application-level error results rather than protocol errors.
pub(crate) async fn acquire_connection(
    pool: &PgPool,
) -> Result<sqlx::pool::PoolConnection<sqlx::Postgres>, CallToolResult> {
    pool.acquire()
        .await
        .map_err(|e| map_pool_error(e, "acquiring connection from pool"))
}

/// Begins a transaction from the pool, mapping errors to `CallToolResult`.
///
/// Used by write-path handlers that require transactional semantics.
pub(crate) async fn begin_transaction(
    pool: &PgPool,
) -> Result<sqlx::Transaction<'static, sqlx::Postgres>, CallToolResult> {
    pool.begin()
        .await
        .map_err(|e| map_pool_error(e, "beginning transaction"))
}

/// Maps a pool acquisition or transaction-begin error to a `CallToolResult`.
fn map_pool_error(err: sqlx::Error, context: &str) -> CallToolResult {
    let db_err = match err {
        sqlx::Error::PoolTimedOut => DbError::PoolExhausted {
            pool_name: POOL_NAME,
        },
        other => DbError::QueryFailed {
            context: context.into(),
            source: other,
        },
    };
    db_err.into_mcp_error().into_call_tool_result()
}
