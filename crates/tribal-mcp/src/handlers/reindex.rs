//! Handlers for the reindex operator tools.

use rmcp::{
    model::{CallToolResult, ErrorData as McpError},
    service::{RequestContext, RoleServer},
};
use tracing::Instrument;
use tribal_db::{
    DbError, EmbeddingProfileRepository, EmbeddingRepository, PgEmbeddingProfileRepository,
    PgEmbeddingRepository, PgReindexRunRepository, PgTagEmbeddingRepository, ReindexRunRepository,
    TagEmbeddingRepository,
};
use tribal_domain::{
    DistanceMetric, EndpointUrlError, McpErrorCode, PrincipalId, ProviderKind, ReindexRunId,
    ReindexRunState, normalise_endpoint_url, span_attrs,
};
use tribal_inference::{DimensionResolutionError, InferenceError, resolve_dimensions};
use tribal_worker::{
    ReindexCreationOutcome, TargetProviderError, build_provider_for_identity, create_reindex_run,
    resolve_reindex_target,
};

use super::common::begin_transaction;
use crate::{
    error::{IntoCallToolResult, IntoMcpError, McpToolError, invalid_argument},
    mapping::{
        McpReindexCancelResponse, McpReindexPruneResponse, McpReindexRequest, McpReindexResponse,
    },
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

/// Errors from the reindex create service function.
#[derive(Debug, thiserror::Error)]
enum ReindexError {
    /// The provider string did not name a known embedding provider.
    #[error("unknown embedding provider: {0}")]
    UnknownProvider(String),
    /// The target endpoint URL is malformed.
    #[error(transparent)]
    Url(#[from] EndpointUrlError),
    /// The target dimension could not be resolved for the model.
    #[error(transparent)]
    Dimensions(#[from] DimensionResolutionError),
    /// The target provider could not be built (a missing credential, an
    /// unsupported provider kind).
    #[error("resolving the target provider: {0}")]
    Provider(TargetProviderError),
    /// The drift-signal probe against the target provider failed.
    #[error("probing the target provider: {0}")]
    Probe(InferenceError),
    #[error(transparent)]
    Db(#[from] DbError),
}

impl IntoMcpError for ReindexError {
    fn into_mcp_error(self) -> McpToolError {
        match self {
            Self::Db(e) => e.into_mcp_error(),
            other => {
                let code = match &other {
                    Self::UnknownProvider(_) | Self::Url(_) | Self::Dimensions(_) => {
                        McpErrorCode::InvalidArgument
                    }
                    Self::Provider(_) | Self::Probe(_) => McpErrorCode::FailedPrecondition,
                    Self::Db(_) => McpErrorCode::Internal,
                };
                McpToolError {
                    code,
                    message: other.to_string(),
                    details: serde_json::Value::Null,
                }
            }
        }
    }
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
            { span_attrs::TRANSPORT } = self.transport_name,
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

    /// Resolves the target identity, validates the credential, probes the drift
    /// signal, and creates the run. The provider build and probe precede the
    /// transaction, since no provider call may span the single-flight lock; the
    /// run is then created atomically.
    async fn run_reindex(
        &self,
        request: McpReindexRequest,
        principal_id: PrincipalId,
    ) -> Result<CallToolResult, ReindexError> {
        let provider_kind = request
            .provider
            .parse::<ProviderKind>()
            .map_err(|_| ReindexError::UnknownProvider(request.provider.clone()))?;
        let base_url = request
            .base_url
            .clone()
            .unwrap_or_else(|| provider_kind.default_base_url().to_owned());
        let normalised_base_url = normalise_endpoint_url(&base_url)?;
        let dimensions = resolve_dimensions(provider_kind, &request.model, request.dimensions)?;

        let (provider, _semaphore) = build_provider_for_identity(
            &self.state.provider_registry,
            &self.state.credentials,
            provider_kind,
            &normalised_base_url,
            &request.model,
            dimensions,
        )
        .map_err(ReindexError::Provider)?;
        let target = resolve_reindex_target(
            provider.as_ref(),
            provider_kind,
            normalised_base_url.clone(),
            request.model.clone(),
            dimensions,
            DistanceMetric::Cosine,
        )
        .await
        .map_err(ReindexError::Probe)?;

        let mut tx = self.state.pool_worker.begin().await.map_err(|e| {
            ReindexError::Db(DbError::QueryFailed {
                context: "beginning the reindex transaction".to_owned(),
                source: e,
            })
        })?;
        let outcome = create_reindex_run(&mut tx, &target, principal_id).await?;
        tx.commit().await.map_err(|e| {
            ReindexError::Db(DbError::QueryFailed {
                context: "committing the reindex".to_owned(),
                source: e,
            })
        })?;

        let (outcome_label, run_id) = match outcome {
            ReindexCreationOutcome::Created(run) => ("created", Some(run.id().to_string())),
            ReindexCreationOutcome::Unchanged(_) => ("unchanged", None),
            ReindexCreationOutcome::AlreadyLive(run) => {
                ("already_live", Some(run.id().to_string()))
            }
            ReindexCreationOutcome::LockContended => ("lock_contended", None),
        };
        Ok(McpReindexResponse {
            outcome: outcome_label.to_owned(),
            run_id,
            provider: provider_kind.as_str().to_owned(),
            model: request.model,
            dimensions,
            base_url: normalised_base_url,
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
            { span_attrs::TRANSPORT } = self.transport_name,
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

        let outcome = match execute_reindex_prune(&mut tx).await {
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

        Ok(McpReindexPruneResponse {
            profiles_superseded: outcome.profiles_superseded,
            embeddings_deleted: outcome.embeddings_deleted,
            tag_embeddings_deleted: outcome.tag_embeddings_deleted,
        }
        .into_call_tool_result())
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

/// The counts a prune reclaimed.
struct ReindexPruneOutcome {
    profiles_superseded: u64,
    embeddings_deleted: u64,
    tag_embeddings_deleted: u64,
}

/// Errors from the reindex prune service function.
#[derive(Debug, thiserror::Error)]
enum ReindexPruneError {
    #[error(transparent)]
    Db(#[from] DbError),
}

impl IntoMcpError for ReindexPruneError {
    fn into_mcp_error(self) -> McpToolError {
        match self {
            Self::Db(e) => e.into_mcp_error(),
        }
    }
}

/// Supersedes every prunable profile and deletes their embeddings within a
/// single transaction. Supersede precedes delete, so the delete's join sees the
/// freshly-superseded profiles; the active profile and its rows are untouched.
async fn execute_reindex_prune(
    conn: &mut sqlx::PgConnection,
) -> Result<ReindexPruneOutcome, ReindexPruneError> {
    let profiles_superseded = PgEmbeddingProfileRepository
        .supersede_prunable(conn)
        .await?;
    let embeddings_deleted = PgEmbeddingRepository.delete_superseded(conn).await?;
    let tag_embeddings_deleted = PgTagEmbeddingRepository.delete_superseded(conn).await?;
    Ok(ReindexPruneOutcome {
        profiles_superseded,
        embeddings_deleted,
        tag_embeddings_deleted,
    })
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
    use crate::test_utils::TestHandler;

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

    #[tokio::test]
    async fn test_execute_reindex_prune_supersedes_all_but_the_active() {
        let ctx = test_context().await;
        let mut tx = ctx.begin_test().await.expect("begin_test");

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

        let outcome = execute_reindex_prune(&mut tx).await.expect("prune");
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
        let structured = result.structured_content.expect("structured content");
        assert_eq!(structured["code"], "invalid_argument");
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
        let structured = result.structured_content.expect("structured content");
        assert_eq!(structured["code"], "failed_precondition");
    }
}
