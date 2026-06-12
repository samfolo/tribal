//! Transaction brackets shared by the runtime's guarded writes: the
//! begin/commit error wrapping every multi-statement transition uses.

use sqlx::{Connection, PgConnection};

use crate::AgentRuntimeError;

/// Begins a transaction, wrapping the failure with the runtime's
/// context vocabulary.
pub(crate) async fn begin<'c>(
    conn: &'c mut PgConnection,
    context: &str,
) -> Result<sqlx::Transaction<'c, sqlx::Postgres>, AgentRuntimeError> {
    conn.begin().await.map_err(|source| {
        AgentRuntimeError::database(
            context,
            tribal_db::DbError::QueryFailed {
                context: context.to_owned(),
                source,
            },
        )
    })
}

/// Commits a transaction, wrapping the failure with the runtime's
/// context vocabulary.
pub(crate) async fn commit(
    txn: sqlx::Transaction<'_, sqlx::Postgres>,
    context: &str,
) -> Result<(), AgentRuntimeError> {
    txn.commit().await.map_err(|source| {
        AgentRuntimeError::database(
            context,
            tribal_db::DbError::QueryFailed {
                context: context.to_owned(),
                source,
            },
        )
    })
}
