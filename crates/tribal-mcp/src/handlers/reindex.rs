//! Handlers for the reindex operator tools.

use rmcp::{
    model::{CallToolResult, ErrorData as McpError},
    service::{RequestContext, RoleServer},
};
use tracing::Instrument;
use tribal_db::{
    DbError, EmbeddingProfileRepository, PgEmbeddingProfileRepository, PgReindexRunRepository,
    ReindexRunRepository,
};
use tribal_domain::{ReindexRunId, ReindexRunState, span_attrs};

use super::common::begin_transaction;
use crate::{
    error::{IntoCallToolResult, IntoMcpError, McpToolError},
    mapping::McpReindexCancelResponse,
    server_handler::TribalServerHandler,
};

/// The error message stamped on a run aborted by an operator cancel.
const CANCEL_REASON: &str = "cancelled by operator";

/// The outcome of a cancel request.
enum ReindexCancelOutcome {
    /// A live run was transitioned to aborted and its building profile failed.
    Cancelled(ReindexRunId),
    /// No live run existed, or it reached a terminal state before the cancel
    /// could claim it.
    NoLiveRun,
}

/// Errors from the reindex cancel service function.
#[derive(Debug, thiserror::Error)]
enum ReindexCancelError {
    #[error(transparent)]
    Db(#[from] DbError),
}

impl IntoMcpError for ReindexCancelError {
    fn into_mcp_error(self) -> McpToolError {
        match self {
            Self::Db(e) => e.into_mcp_error(),
        }
    }
}

impl TribalServerHandler {
    /// Handles the `tribal_reindex_cancel` tool call: aborts the live reindex
    /// run, if any. Gated by the `tribal.embedding:execute` scope at dispatch.
    pub(crate) async fn handle_reindex_cancel(
        &self,
        _params: serde_json::Value,
        context: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, McpError> {
        let principal = self.resolve_principal(&context)?;
        let span = tracing::info_span!(
            parent: None,
            "tribal.reindex_cancel",
            { span_attrs::PRINCIPAL_KEY } = principal.principal_key(),
            { span_attrs::TRANSPORT } = self.transport_name,
        );
        self.apply_reindex_cancel().instrument(span).await
    }

    /// Core cancel logic, separated from the tool adapter so it can be tested
    /// without a `Peer<RoleServer>`.
    async fn apply_reindex_cancel(&self) -> Result<CallToolResult, McpError> {
        let mut tx = match begin_transaction(
            &self.state.pool_worker,
            self.config.pool_name,
            &self.state.metrics,
        )
        .await
        {
            Ok(tx) => tx,
            Err(result) => return Ok(result),
        };

        let outcome = match execute_reindex_cancel(&mut tx).await {
            Ok(outcome) => outcome,
            Err(e) => return Ok(e.into_mcp_error().into_call_tool_result()),
        };

        if let Err(e) = tx.commit().await {
            let db_err = DbError::QueryFailed {
                context: "committing reindex cancel".to_owned(),
                source: e,
            };
            return Ok(db_err.into_mcp_error().into_call_tool_result());
        }

        let response = match outcome {
            ReindexCancelOutcome::Cancelled(id) => McpReindexCancelResponse {
                cancelled: true,
                run_id: Some(id.to_string()),
            },
            ReindexCancelOutcome::NoLiveRun => McpReindexCancelResponse {
                cancelled: false,
                run_id: None,
            },
        };
        Ok(response.into_call_tool_result())
    }
}

/// Aborts the live reindex run within a single transaction.
///
/// The run transition is a compare-and-set on its current state, so a cutover
/// that completes the run between the read and the write wins the race: the
/// guard fails, nothing is cancelled, and the flip stands. The building profile
/// is only failed when the run transition succeeded.
async fn execute_reindex_cancel(
    conn: &mut sqlx::PgConnection,
) -> Result<ReindexCancelOutcome, ReindexCancelError> {
    let Some(run) = PgReindexRunRepository.find_live(conn).await? else {
        return Ok(ReindexCancelOutcome::NoLiveRun);
    };

    let aborted = PgReindexRunRepository
        .transition(
            conn,
            run.id(),
            run.state(),
            ReindexRunState::Aborted,
            Some(CANCEL_REASON),
        )
        .await?;
    if !aborted {
        return Ok(ReindexCancelOutcome::NoLiveRun);
    }

    PgEmbeddingProfileRepository
        .mark_failed(conn, run.target_profile_id())
        .await?;
    Ok(ReindexCancelOutcome::Cancelled(run.id()))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use tribal_db::{
        NewReindexRun, PgEmbeddingProfileRepository, PgPrincipalRepository, PgReindexRunRepository,
        PrincipalRepository,
    };
    use tribal_test_utils::{a_new_embedding_profile, a_new_principal, test_context};

    use super::*;

    #[tokio::test]
    async fn test_execute_reindex_cancel_aborts_the_live_run() {
        let ctx = test_context().await;
        let mut tx = ctx.begin_test().await.expect("begin_test");

        let principal = PgPrincipalRepository
            .insert(
                &mut tx,
                &a_new_principal()
                    .principal_key("user:reindex-cancel".to_owned())
                    .build(),
            )
            .await
            .expect("insert principal");
        let building = PgEmbeddingProfileRepository
            .insert(&mut tx, &a_new_embedding_profile().build())
            .await
            .expect("insert building profile");
        let run = PgReindexRunRepository
            .insert(
                &mut tx,
                &NewReindexRun::builder()
                    .target_profile_id(building.id())
                    .epoch(building.epoch())
                    .initiated_by_principal_id(principal.id())
                    .build(),
            )
            .await
            .expect("insert run");

        let outcome = execute_reindex_cancel(&mut tx).await.expect("cancel");
        assert!(
            matches!(outcome, ReindexCancelOutcome::Cancelled(id) if id == run.id()),
            "the live run is cancelled",
        );

        let cancelled = PgReindexRunRepository
            .find_by_id(&mut tx, run.id())
            .await
            .expect("find run")
            .expect("the run");
        assert_eq!(cancelled.state(), ReindexRunState::Aborted);
        assert!(
            PgReindexRunRepository
                .find_live(&mut tx)
                .await
                .expect("find_live")
                .is_none(),
            "no run is live after a cancel",
        );
    }

    #[tokio::test]
    async fn test_execute_reindex_cancel_reports_no_live_run() {
        let ctx = test_context().await;
        let mut tx = ctx.begin_test().await.expect("begin_test");

        let outcome = execute_reindex_cancel(&mut tx).await.expect("cancel");
        assert!(matches!(outcome, ReindexCancelOutcome::NoLiveRun));
    }
}
