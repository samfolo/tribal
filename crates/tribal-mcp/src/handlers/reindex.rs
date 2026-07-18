//! Handlers for the reindex operator tools.
//!
//! Each tool adapter resolves its principal, then delegates to the shared
//! [`tribal_worker`] reindex services so the MCP surface and the `tribal
//! reindex` CLI drive identical logic; the handler only maps outcomes and
//! errors onto the MCP wire shape.

use rmcp::{
    model::{CallToolResult, ErrorData as McpError},
    service::{RequestContext, RoleServer},
};
use tracing::Instrument;
use tribal_db::DbError;
use tribal_domain::{McpErrorCode, PrincipalId, span_attrs};
use tribal_worker::{
    ReindexCancelOutcome, ReindexOpError, ReindexRunRequest, drop_superseded_indexes,
    reindex_cancel, reindex_prune, reindex_run,
};

use super::common::begin_transaction;
use crate::{
    error::{IntoCallToolResult, IntoMcpError, McpToolError, invalid_argument},
    mapping::{
        McpReindexCancelResponse, McpReindexPruneResponse, McpReindexRequest, McpReindexResponse,
    },
    server_handler::TribalServerHandler,
};

impl IntoMcpError for ReindexOpError {
    fn into_mcp_error(self) -> McpToolError {
        let message = self.to_string();
        let code = match self {
            Self::Db(e) => return e.into_mcp_error(),
            Self::UnknownProvider(_) | Self::Url(_) | Self::Dimensions(_) => {
                McpErrorCode::InvalidArgument
            }
            Self::Provider(_) | Self::Probe(_) => McpErrorCode::FailedPrecondition,
        };
        McpToolError {
            code,
            message,
            details: serde_json::Value::Null,
        }
    }
}

impl TribalServerHandler {
    /// Handles the `tribal_reindex` tool call: resolves the named target,
    /// validates its credential and probes its drift signal, then creates a
    /// reindex run the worker drives. Gated by the `tribal.embedding:execute`
    /// scope at dispatch.
    pub(crate) async fn handle_reindex(
        &self,
        params: serde_json::Value,
        context: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, McpError> {
        let principal = self.resolve_principal(&context)?;
        let principal_id = principal.principal_id();
        let span = tracing::info_span!(
            parent: None,
            "tribal.reindex",
            { span_attrs::PRINCIPAL_KEY } = principal.principal_key(),
            { span_attrs::TRANSPORT } = self.transport_name(),
        );
        self.apply_reindex(params, principal_id)
            .instrument(span)
            .await
    }

    /// Parses the request and maps a reindex failure to an application error
    /// result; separated from the tool adapter for testing.
    async fn apply_reindex(
        &self,
        params: serde_json::Value,
        principal_id: PrincipalId,
    ) -> Result<CallToolResult, McpError> {
        let request: McpReindexRequest =
            serde_json::from_value(params).map_err(|e| invalid_argument(e.to_string()))?;
        match self.run_reindex(request, principal_id).await {
            Ok(result) => Ok(result),
            Err(e) => Ok(e.into_mcp_error().into_call_tool_result()),
        }
    }

    /// Delegates to [`tribal_worker::reindex_run`] and maps the resolved target
    /// and pre-flight estimate onto the MCP response shape.
    async fn run_reindex(
        &self,
        request: McpReindexRequest,
        principal_id: PrincipalId,
    ) -> Result<CallToolResult, ReindexOpError> {
        let req = ReindexRunRequest {
            provider: request.provider,
            model: request.model,
            dimensions: request.dimensions,
            base_url: request.base_url,
            dry_run: request.dry_run,
        };
        let outcome = reindex_run(
            &self.state.pool_worker,
            &self.state.gateway,
            &req,
            principal_id,
        )
        .await?;

        Ok(McpReindexResponse {
            outcome: outcome.resolution.label().to_owned(),
            run_id: outcome.resolution.run_id().map(|id| id.to_string()),
            provider: outcome.provider.as_str().to_owned(),
            model: outcome.model,
            dimensions: outcome.dimensions,
            base_url: outcome.normalised_base_url,
            estimated_items: outcome.estimated_items,
            estimated_tags: outcome.estimated_tags,
        }
        .into_call_tool_result())
    }

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
            { span_attrs::TRANSPORT } = self.transport_name(),
        );
        self.apply_reindex_cancel().instrument(span).await
    }

    /// Core cancel logic, separated from the tool adapter for testing.
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

        let outcome = match reindex_cancel(&mut tx).await {
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

    /// Handles the `tribal_reindex_prune` tool call: supersedes prunable
    /// profiles and deletes their embeddings. Gated by the
    /// `tribal.embedding:execute` scope at dispatch.
    pub(crate) async fn handle_reindex_prune(
        &self,
        _params: serde_json::Value,
        context: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, McpError> {
        let principal = self.resolve_principal(&context)?;
        let span = tracing::info_span!(
            parent: None,
            "tribal.reindex_prune",
            { span_attrs::PRINCIPAL_KEY } = principal.principal_key(),
            { span_attrs::TRANSPORT } = self.transport_name(),
        );
        self.apply_reindex_prune().instrument(span).await
    }

    /// Core prune logic, separated from the tool adapter for testing.
    async fn apply_reindex_prune(&self) -> Result<CallToolResult, McpError> {
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

        let outcome = match reindex_prune(&mut tx).await {
            Ok(outcome) => outcome,
            Err(e) => return Ok(e.into_mcp_error().into_call_tool_result()),
        };

        if let Err(e) = tx.commit().await {
            let db_err = DbError::QueryFailed {
                context: "committing reindex prune".to_owned(),
                source: e,
            };
            return Ok(db_err.into_mcp_error().into_call_tool_result());
        }

        // Reclaim the superseded profiles' partial indexes outside the
        // transaction (DROP INDEX CONCURRENTLY); the storage delete has already
        // committed, so this is best-effort.
        if !outcome.superseded_epochs.is_empty()
            && let Ok(mut conn) = self.state.pool_worker.acquire().await
        {
            drop_superseded_indexes(&mut conn, &outcome.superseded_epochs).await;
        }

        Ok(McpReindexPruneResponse {
            profiles_superseded: outcome.profiles_superseded,
            embeddings_deleted: outcome.embeddings_deleted,
            tag_embeddings_deleted: outcome.tag_embeddings_deleted,
        }
        .into_call_tool_result())
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use tribal_db::{
        EmbeddingProfileRepository, NewReindexRun, PgEmbeddingProfileRepository,
        PgPrincipalRepository, PgReindexRunRepository, PrincipalRepository, ReindexRunRepository,
    };
    use tribal_domain::ReindexRunState;
    use tribal_test_utils::{TestDb, a_new_embedding_profile, a_new_principal};

    use super::*;
    use crate::test_utils::{NO_STRUCTURED_CONTENT, TestHandler, first_text_content};

    #[tokio::test]
    async fn test_reindex_cancel_aborts_the_live_run() {
        let ctx = TestDb::new().await;
        let mut tx = ctx.begin().await.expect("begin");

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

        let outcome = reindex_cancel(&mut tx).await.expect("cancel");
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
    async fn test_reindex_cancel_reports_no_live_run() {
        let ctx = TestDb::new().await;
        let mut tx = ctx.begin().await.expect("begin");

        let outcome = reindex_cancel(&mut tx).await.expect("cancel");
        assert!(matches!(outcome, ReindexCancelOutcome::NoLiveRun));
    }

    #[tokio::test]
    async fn test_reindex_prune_supersedes_all_but_the_active() {
        let ctx = TestDb::new().await;
        let mut tx = ctx.begin().await.expect("begin");

        // An old complete profile, the active (highest-epoch complete), and a
        // failed one, inserted in ascending epoch order.
        let old = PgEmbeddingProfileRepository
            .insert(&mut tx, &a_new_embedding_profile().build())
            .await
            .expect("insert old");
        PgEmbeddingProfileRepository
            .mark_complete(&mut tx, old.id())
            .await
            .expect("complete old");
        let active = PgEmbeddingProfileRepository
            .insert(&mut tx, &a_new_embedding_profile().build())
            .await
            .expect("insert active");
        PgEmbeddingProfileRepository
            .mark_complete(&mut tx, active.id())
            .await
            .expect("complete active");
        let failed = PgEmbeddingProfileRepository
            .insert(&mut tx, &a_new_embedding_profile().build())
            .await
            .expect("insert failed");
        PgEmbeddingProfileRepository
            .mark_failed(&mut tx, failed.id())
            .await
            .expect("fail");

        let outcome = reindex_prune(&mut tx).await.expect("prune");
        assert_eq!(
            outcome.profiles_superseded, 2,
            "the old complete and the failed profiles are superseded, never the active",
        );

        let still_active = PgEmbeddingProfileRepository
            .find_active(&mut tx)
            .await
            .expect("find_active")
            .expect("an active profile");
        assert_eq!(
            still_active.id(),
            active.id(),
            "the active profile survives a prune",
        );
    }

    #[tokio::test]
    async fn test_apply_reindex_unknown_provider_is_an_application_error() {
        let handler = TestHandler::builder().build();

        let result = handler
            .apply_reindex(
                serde_json::json!({ "provider": "grpc", "model": "x" }),
                PrincipalId::new(),
            )
            .await
            .expect("no protocol error");

        assert_eq!(result.is_error, Some(true));
        assert!(
            result.structured_content.is_none(),
            "{NO_STRUCTURED_CONTENT}"
        );
        assert!(first_text_content(&result).contains("unknown embedding provider"));
    }

    #[tokio::test]
    async fn test_apply_reindex_missing_credential_is_a_failed_precondition() {
        let handler = TestHandler::builder().build();

        // OpenAI requires an API key; the default catalogue is empty, so the
        // target provider fails to build before any run is created.
        let result = handler
            .apply_reindex(
                serde_json::json!({
                    "provider": "openai",
                    "model": "text-embedding-3-small",
                    "dimensions": 1536,
                }),
                PrincipalId::new(),
            )
            .await
            .expect("no protocol error");

        assert_eq!(result.is_error, Some(true));
        assert!(
            result.structured_content.is_none(),
            "{NO_STRUCTURED_CONTENT}"
        );
        assert!(first_text_content(&result).contains("resolving the target provider"));
    }

    #[tokio::test]
    async fn test_apply_reindex_dry_run_estimates_without_creating_a_run() {
        let ctx = TestDb::new().await;
        let pool = ctx.create_pool().await.expect("pool");
        let handler = TestHandler::builder().pool(pool).build();

        // Ollama needs no credential, so a dry run resolves and estimates the
        // corpus (empty here) without a probe or a run.
        let result = handler
            .apply_reindex(
                serde_json::json!({
                    "provider": "ollama",
                    "model": "nomic-embed-text:v1.5",
                    "dimensions": 768,
                    "dry_run": true,
                }),
                PrincipalId::new(),
            )
            .await
            .expect("no protocol error");

        assert_eq!(result.is_error, Some(false));
        let structured = result.structured_content.expect("structured content");
        assert_eq!(structured["outcome"], "plan");
        assert_eq!(structured["run_id"], serde_json::Value::Null);
        assert_eq!(structured["dimensions"], 768);
        assert_eq!(structured["estimated_items"], 0);
        assert_eq!(structured["estimated_tags"], 0);
    }
}
