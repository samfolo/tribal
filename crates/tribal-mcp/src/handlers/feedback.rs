//! Handler for `tribal_feedback` — retrieval session quality rating.

use std::{str::FromStr, sync::Arc};

use rmcp::{
    model::{CallToolResult, ErrorData as McpError},
    service::{RequestContext, RoleServer},
};
use sqlx::PgConnection;
use tracing::Instrument;
use tribal_db::{DbError, EmbeddingProfileRepository, PgEmbeddingProfileRepository};
use tribal_domain::{
    EmbeddingProfileId, FeedbackRating, InferenceParameters, KnowledgeItemId, McpErrorCode,
    PrincipalId, span_attrs, TaskType,
};

use super::common::begin_transaction;
use crate::{
    error::{IntoCallToolResult, IntoMcpError, McpToolError, invalid_argument},
    fingerprint::{FingerprintError, PipelineProviderIdentities, compute_and_upsert_fingerprint},
    mapping::{McpFeedbackRequest, McpFeedbackResponse},
    server_handler::{ActivePromptVersions, ConnectionRepositories, TribalServerHandler},
};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const EMPTY_QUERY_TEXT: &str = "query_text must not be empty";
const EMPTY_RETURNED_ITEMS: &str = "returned_item_ids must contain at least one item";
const INVALID_RATING: &str = "rating must be \"positive\" or \"negative\"";

// ---------------------------------------------------------------------------
// Service types
// ---------------------------------------------------------------------------

/// Domain-level parameters for the feedback service function.
struct FeedbackParams {
    trace_id: String,
    query_text: String,
    returned_item_ids: Vec<KnowledgeItemId>,
    explored_anchor_ids: Vec<KnowledgeItemId>,
    principal_id: PrincipalId,
    rating: FeedbackRating,
    notes: Option<String>,
    /// The profile that produced the rated results, echoed by the client from
    /// the discover response. `None` when the client did not carry it back.
    embedding_profile_id: Option<EmbeddingProfileId>,
    active_prompts: ActivePromptVersions,
    build_version: Arc<str>,
    provider_identities: PipelineProviderIdentities,
    inference_parameters: InferenceParameters,
}

/// Errors that can occur during feedback execution.
#[derive(Debug, thiserror::Error)]
enum FeedbackError {
    #[error(transparent)]
    Db(#[from] DbError),

    #[error(transparent)]
    Fingerprint(#[from] FingerprintError),
}

impl IntoMcpError for FeedbackError {
    fn into_mcp_error(self) -> McpToolError {
        match self {
            Self::Db(e) => e.into_mcp_error(),
            Self::Fingerprint(e) => e.into_mcp_error(),
        }
    }
}

// ---------------------------------------------------------------------------
// Handler
// ---------------------------------------------------------------------------

impl TribalServerHandler {
    /// Handles the `tribal_feedback` tool call.
    pub(crate) async fn handle_feedback(
        &self,
        params: serde_json::Value,
        context: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, McpError> {
        let principal = self.resolve_principal(&context)?;
        let span = tracing::info_span!(
            parent: None,
            "tribal.feedback",
            { span_attrs::PRINCIPAL_KEY } = principal.principal_key(),
            { span_attrs::TRANSPORT } = self.transport_name,
            { span_attrs::PROJECT_ID } = tracing::field::Empty,
        );

        // Attach this span to the retrieval session trace so the
        // feedback action appears alongside the discover/explore calls
        // it rates.
        if let Some(trace_id) = params.get("trace_id").and_then(|v| v.as_str()) {
            let _ = tribal_telemetry::parent_span_from_trace_id(&span, trace_id);
        }

        self.apply_feedback(params, principal.principal_id())
            .instrument(span)
            .await
    }

    /// Core logic for `tribal_feedback`, separated from the outer handler
    /// so it can be tested without a `Peer<RoleServer>`.
    async fn apply_feedback(
        &self,
        params: serde_json::Value,
        principal_id: PrincipalId,
    ) -> Result<CallToolResult, McpError> {
        if let Some(project) = &self.session.read().await.project {
            tracing::Span::current()
                .record(span_attrs::PROJECT_ID, tracing::field::display(&project.id));
        }

        let request: McpFeedbackRequest =
            serde_json::from_value(params).map_err(|e| invalid_argument(e.to_string()))?;

        // -- Validate trace_id ------------------------------------------------

        if !tribal_telemetry::is_valid_trace_id(&request.trace_id) {
            return Ok(McpToolError {
                code: McpErrorCode::InvalidArgument,
                message: tribal_telemetry::INVALID_TRACE_ID.into(),
                details: serde_json::json!({}),
            }
            .into_call_tool_result());
        }

        // -- Validate query_text ----------------------------------------------

        if request.query_text.is_empty() {
            return Ok(McpToolError {
                code: McpErrorCode::InvalidArgument,
                message: EMPTY_QUERY_TEXT.into(),
                details: serde_json::json!({}),
            }
            .into_call_tool_result());
        }

        // -- Validate returned_item_ids ---------------------------------------

        if request.returned_item_ids.is_empty() {
            return Ok(McpToolError {
                code: McpErrorCode::InvalidArgument,
                message: EMPTY_RETURNED_ITEMS.into(),
                details: serde_json::json!({}),
            }
            .into_call_tool_result());
        }

        let mut returned_item_ids = Vec::with_capacity(request.returned_item_ids.len());
        for raw in &request.returned_item_ids {
            match KnowledgeItemId::from_str(raw) {
                Ok(id) => returned_item_ids.push(id),
                Err(e) => return Ok(e.into_mcp_error().into_call_tool_result()),
            }
        }

        // -- Validate explored_anchor_ids (optional) --------------------------

        let explored_anchor_ids = if let Some(ref anchors) = request.explored_anchor_ids {
            let mut parsed = Vec::with_capacity(anchors.len());
            for raw in anchors {
                match KnowledgeItemId::from_str(raw) {
                    Ok(id) => parsed.push(id),
                    Err(e) => return Ok(e.into_mcp_error().into_call_tool_result()),
                }
            }
            parsed
        } else {
            Vec::new()
        };

        // -- Validate rating --------------------------------------------------

        let Ok(rating) = FeedbackRating::from_str(&request.rating) else {
            return Ok(McpToolError {
                code: McpErrorCode::InvalidArgument,
                message: INVALID_RATING.into(),
                details: serde_json::json!({}),
            }
            .into_call_tool_result());
        };

        // -- Validate embedding_profile_id (optional) -------------------------

        let embedding_profile_id = match &request.embedding_profile_id {
            Some(raw) => match EmbeddingProfileId::from_str(raw) {
                Ok(id) => Some(id),
                Err(e) => return Ok(e.into_mcp_error().into_call_tool_result()),
            },
            None => None,
        };

        // -- Build params and execute -----------------------------------------
        // The embedding lineage (model and profile id) is resolved inside
        // `execute_feedback`: the client-supplied producing profile when given
        // and still resolvable, otherwise the active profile.

        let active_prompts = self.state.active_prompt_versions.read().await.clone();

        let provider_identities = PipelineProviderIdentities {
            extraction: self.state.facade.completion_identity(TaskType::Extraction).clone(),
            triage: self.state.facade.completion_identity(TaskType::Triage).clone(),
            relation: self.state.facade.completion_identity(TaskType::Relation).clone(),
            embedding: self.state.embedding_identity.clone(),
        };

        let feedback_params = FeedbackParams {
            trace_id: request.trace_id,
            query_text: request.query_text,
            returned_item_ids,
            explored_anchor_ids,
            principal_id,
            rating,
            notes: request.notes,
            embedding_profile_id,
            active_prompts,
            build_version: Arc::clone(&self.state.build_version),
            provider_identities,
            inference_parameters: self.state.inference_parameters.clone(),
        };

        let mut tx = match begin_transaction(
            &self.state.pool_mcp,
            self.config.pool_name,
            &self.state.metrics,
        )
        .await
        {
            Ok(tx) => tx,
            Err(call_result) => return Ok(call_result),
        };

        let feedback = match execute_feedback(&mut tx, &self.repositories, feedback_params).await {
            Ok(f) => f,
            Err(e) => return Ok(e.into_mcp_error().into_call_tool_result()),
        };

        if let Err(e) = tx.commit().await {
            let db_err = DbError::QueryFailed {
                context: "committing feedback transaction".into(),
                source: e,
            };
            return Ok(db_err.into_mcp_error().into_call_tool_result());
        }

        Ok(McpFeedbackResponse::from(&feedback).into_call_tool_result())
    }
}

// ---------------------------------------------------------------------------
// Service function
// ---------------------------------------------------------------------------

/// Executes the feedback operation: builds the new feedback record
/// and inserts it.
///
/// All inputs and outputs are domain types — no MCP types cross this
/// boundary.
async fn execute_feedback(
    conn: &mut PgConnection,
    repositories: &ConnectionRepositories,
    params: FeedbackParams,
) -> Result<tribal_domain::RetrievalFeedback, FeedbackError> {
    // -- System fingerprint ---------------------------------------------------

    let fingerprint_hash = compute_and_upsert_fingerprint(
        conn,
        repositories,
        &params.active_prompts,
        &params.provider_identities,
        &params.build_version,
        &params.inference_parameters,
    )
    .await?;

    // -- Embedding lineage ----------------------------------------------------
    // The model and profile id record the profile that produced the rated
    // results: the client-supplied producing profile when it is given and still
    // resolves, otherwise the active profile. A producing profile that no longer
    // resolves (pruned, or a stale id) falls back to active, as does the absence
    // of any id. Provisioning completes a genesis profile before serving, so an
    // absent active profile is a consistency fault.

    let producing_profile = match params.embedding_profile_id {
        Some(id) => PgEmbeddingProfileRepository.find_by_id(conn, id).await?,
        None => None,
    };

    let profile = match producing_profile {
        Some(profile) => profile,
        None => PgEmbeddingProfileRepository
            .find_active(conn)
            .await?
            .ok_or(DbError::NotFound {
                entity: "embedding_profile",
                id: "active".to_owned(),
            })?,
    };

    // -- Feedback record ------------------------------------------------------

    let new_feedback = tribal_db::NewRetrievalFeedback::builder()
        .trace_id(params.trace_id.to_ascii_lowercase())
        .query_text(params.query_text)
        .embedding_model(profile.model().to_owned())
        .embedding_profile_id(profile.id())
        .returned_item_ids(params.returned_item_ids)
        .explored_anchor_ids(params.explored_anchor_ids)
        .system_fingerprint_hash(fingerprint_hash)
        .principal_id(params.principal_id)
        .rating(params.rating)
        .notes(params.notes)
        .build();

    let feedback = repositories
        .retrieval_feedback
        .insert(conn, &new_feedback)
        .await?;

    Ok(feedback)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use rmcp::model::ErrorCode;
    use tokio::sync::RwLock;
    use tribal_domain::{
        FeedbackRating, InferenceParameters, KnowledgeItemId, PrincipalId, ProjectId,
    };
    use tribal_test_utils::{
        MockPromptVersionRepository, MockRetrievalFeedbackRepository, TestContext,
        a_new_embedding_profile, a_prompt_version, a_retrieval_feedback, ensure_genesis_profile,
        test_context,
    };

    use super::*;
    use crate::test_utils::{
        NO_STRUCTURED_CONTENT, TestHandler, configure_fingerprint_mocks, first_text_content,
        test_active_prompt_versions, test_provider_identities, test_repositories,
    };

    // -- Constants ---------------------------------------------------------

    const NO_PROTOCOL_ERROR: &str = "should not return a protocol error";

    // -- Helpers -----------------------------------------------------------

    fn repos_for_feedback(feedback: tribal_domain::RetrievalFeedback) -> ConnectionRepositories {
        let mut repos = test_repositories();
        repos.retrieval_feedback = Arc::new(
            MockRetrievalFeedbackRepository::builder()
                .on_insert(feedback, None)
                .build(),
        );
        repos
    }

    async fn call_execute(
        repos: &ConnectionRepositories,
        params: FeedbackParams,
    ) -> Result<tribal_domain::RetrievalFeedback, FeedbackError> {
        let ctx = test_context().await;
        let mut tx = ctx.begin_test().await.expect("begin");
        // `execute_feedback` reads the active profile for the embedding lineage.
        ensure_genesis_profile(&mut tx, "nomic-embed-text:v1.5", 768).await;
        execute_feedback(&mut tx, repos, params).await
    }

    fn default_params() -> FeedbackParams {
        FeedbackParams {
            trace_id: "00000000000000000000000000000001".into(),
            query_text: "auth patterns".into(),
            returned_item_ids: vec![KnowledgeItemId::new()],
            explored_anchor_ids: Vec::new(),
            principal_id: PrincipalId::new(),
            rating: FeedbackRating::Positive,
            notes: None,
            embedding_profile_id: None,
            active_prompts: test_active_prompt_versions(),
            build_version: Arc::from("test-build"),
            provider_identities: test_provider_identities(),
            inference_parameters: InferenceParameters::default(),
        }
    }

    // -- Adapter: validation -----------------------------------------------

    #[tokio::test]
    async fn test_apply_feedback_malformed_json_returns_protocol_error() {
        let handler = TestHandler::builder().build();

        let err = handler
            .apply_feedback(serde_json::json!({"trace_id": 123}), PrincipalId::new())
            .await
            .expect_err("should return Err(McpError) for malformed params");

        assert_eq!(err.code, ErrorCode::INVALID_PARAMS);
    }

    #[tokio::test]
    async fn test_apply_feedback_empty_trace_id_returns_application_error() {
        let handler = TestHandler::builder().build();

        let ki_id = KnowledgeItemId::new().to_string();
        let result = handler
            .apply_feedback(
                serde_json::json!({
                    "trace_id": "",
                    "query_text": "auth patterns",
                    "returned_item_ids": [ki_id],
                    "rating": "positive",
                }),
                PrincipalId::new(),
            )
            .await
            .expect(NO_PROTOCOL_ERROR);

        assert_eq!(result.is_error, Some(true));
        assert!(
            result.structured_content.is_none(),
            "{NO_STRUCTURED_CONTENT}"
        );
        assert!(first_text_content(&result).contains(tribal_telemetry::INVALID_TRACE_ID));
    }

    /// Verifies that an uppercase `trace_id` is normalised to lowercase
    /// before reaching the storage path. The `when_insert` predicate
    /// asserts the mock receives the lowercased form.
    #[tokio::test]
    async fn test_execute_feedback_stores_lowercase_trace_id() {
        let feedback = a_retrieval_feedback().build();

        let params = FeedbackParams {
            trace_id: "4BF92F3577B34DA6A3CE929D0E0E4736".into(),
            ..default_params()
        };

        let mut repos = test_repositories();
        repos.retrieval_feedback = Arc::new(
            MockRetrievalFeedbackRepository::builder()
                .when_insert(|new_fb| new_fb.trace_id == "4bf92f3577b34da6a3ce929d0e0e4736")
                .respond_with(feedback, None)
                .build(),
        );
        configure_fingerprint_mocks(&mut repos, &params.active_prompts);

        call_execute(&repos, params).await.expect("should succeed");
    }

    // -- Service: embedding lineage ----------------------------------------

    /// With no `embedding_profile_id` supplied, the lineage records the active
    /// profile (the existing fallback behaviour).
    #[tokio::test]
    async fn test_execute_feedback_records_active_profile_when_id_absent() {
        let feedback = a_retrieval_feedback().build();

        let ctx = test_context().await;
        let mut tx = ctx.begin_test().await.expect("begin");
        ensure_genesis_profile(&mut tx, "active-model:v1", 768).await;
        // Bind the expectation to the profile the handler resolves as active in
        // this transaction, rather than assuming the seeded genesis is the
        // highest-epoch profile in a database other tests also write to.
        let active = PgEmbeddingProfileRepository
            .find_active(&mut tx)
            .await
            .expect("find active")
            .expect("an active profile");
        let active_id = active.id();
        let active_model = active.model().to_owned();

        let mut repos = test_repositories();
        repos.retrieval_feedback = Arc::new(
            MockRetrievalFeedbackRepository::builder()
                .when_insert(move |new_fb| {
                    new_fb.embedding_profile_id == active_id
                        && new_fb.embedding_model == active_model
                })
                .respond_with(feedback, None)
                .build(),
        );
        // `default_params` carries no `embedding_profile_id`, so the lineage
        // resolves through the active-profile fallback.
        let params = default_params();
        configure_fingerprint_mocks(&mut repos, &params.active_prompts);

        execute_feedback(&mut tx, &repos, params)
            .await
            .expect("should record the active profile");
    }

    /// When the client carries back the producing `embedding_profile_id`, the
    /// lineage records that profile's id and model, not the active one. The
    /// genesis profile is seeded active, then a second profile is inserted so it
    /// becomes the new active; the (now non-active) genesis is passed through.
    #[tokio::test]
    async fn test_execute_feedback_records_producing_profile_when_id_present() {
        let feedback = a_retrieval_feedback().build();

        let ctx = test_context().await;
        let mut tx = ctx.begin_test().await.expect("begin");

        // Genesis is active first; capture its id and model.
        let producing = ensure_genesis_profile(&mut tx, "producing-model:v1", 768).await;
        let producing_id = producing.id();

        // Insert a second complete profile via the repository. Its higher epoch
        // makes it the new active, so `producing` is no longer active.
        let second = PgEmbeddingProfileRepository
            .insert(
                &mut tx,
                &a_new_embedding_profile()
                    .model("active-model:v2".to_owned())
                    .build(),
            )
            .await
            .expect("insert second profile");
        PgEmbeddingProfileRepository
            .mark_complete(&mut tx, second.id())
            .await
            .expect("complete second profile");

        // Sanity: the active profile is now the second one, not the producer.
        let active = PgEmbeddingProfileRepository
            .find_active(&mut tx)
            .await
            .expect("find active")
            .expect("an active profile");
        assert_ne!(active.id(), producing_id);

        let mut repos = test_repositories();
        repos.retrieval_feedback = Arc::new(
            MockRetrievalFeedbackRepository::builder()
                .when_insert(move |new_fb| {
                    new_fb.embedding_profile_id == producing_id
                        && new_fb.embedding_model == "producing-model:v1"
                })
                .respond_with(feedback, None)
                .build(),
        );
        let params = FeedbackParams {
            embedding_profile_id: Some(producing_id),
            ..default_params()
        };
        configure_fingerprint_mocks(&mut repos, &params.active_prompts);

        execute_feedback(&mut tx, &repos, params)
            .await
            .expect("should record the producing profile");
    }

    #[tokio::test]
    async fn test_apply_feedback_non_hex_trace_id_returns_application_error() {
        let handler = TestHandler::builder().build();

        let ki_id = KnowledgeItemId::new().to_string();
        let result = handler
            .apply_feedback(
                serde_json::json!({
                    "trace_id": "my-trace-42",
                    "query_text": "auth patterns",
                    "returned_item_ids": [ki_id],
                    "rating": "positive",
                }),
                PrincipalId::new(),
            )
            .await
            .expect(NO_PROTOCOL_ERROR);

        assert_eq!(result.is_error, Some(true));
        assert!(
            result.structured_content.is_none(),
            "{NO_STRUCTURED_CONTENT}"
        );
        assert!(first_text_content(&result).contains(tribal_telemetry::INVALID_TRACE_ID));
    }

    #[tokio::test]
    async fn test_apply_feedback_empty_query_text_returns_application_error() {
        let handler = TestHandler::builder().build();

        let ki_id = KnowledgeItemId::new().to_string();
        let result = handler
            .apply_feedback(
                serde_json::json!({
                    "trace_id": "00000000000000000000000000000001",
                    "query_text": "",
                    "returned_item_ids": [ki_id],
                    "rating": "positive",
                }),
                PrincipalId::new(),
            )
            .await
            .expect(NO_PROTOCOL_ERROR);

        assert_eq!(result.is_error, Some(true));
        assert!(
            result.structured_content.is_none(),
            "{NO_STRUCTURED_CONTENT}"
        );
        assert!(first_text_content(&result).contains(EMPTY_QUERY_TEXT));
    }

    #[tokio::test]
    async fn test_apply_feedback_empty_returned_item_ids_returns_application_error() {
        let handler = TestHandler::builder().build();

        let result = handler
            .apply_feedback(
                serde_json::json!({
                    "trace_id": "00000000000000000000000000000001",
                    "query_text": "auth patterns",
                    "returned_item_ids": [],
                    "rating": "positive",
                }),
                PrincipalId::new(),
            )
            .await
            .expect(NO_PROTOCOL_ERROR);

        assert_eq!(result.is_error, Some(true));
        assert!(
            result.structured_content.is_none(),
            "{NO_STRUCTURED_CONTENT}"
        );
        assert!(first_text_content(&result).contains(EMPTY_RETURNED_ITEMS));
    }

    #[tokio::test]
    async fn test_apply_feedback_invalid_returned_item_id_prefix_returns_application_error() {
        let handler = TestHandler::builder().build();

        let wrong_prefix_id = ProjectId::new().to_string();
        let result = handler
            .apply_feedback(
                serde_json::json!({
                    "trace_id": "00000000000000000000000000000001",
                    "query_text": "auth patterns",
                    "returned_item_ids": [wrong_prefix_id],
                    "rating": "positive",
                }),
                PrincipalId::new(),
            )
            .await
            .expect(NO_PROTOCOL_ERROR);

        assert_eq!(result.is_error, Some(true));
        assert!(
            result.structured_content.is_none(),
            "{NO_STRUCTURED_CONTENT}"
        );
        assert!(first_text_content(&result).contains("expected prefix"));
    }

    #[tokio::test]
    async fn test_apply_feedback_invalid_explored_anchor_id_prefix_returns_application_error() {
        let handler = TestHandler::builder().build();

        let ki_id = KnowledgeItemId::new().to_string();
        let wrong_prefix_id = ProjectId::new().to_string();
        let result = handler
            .apply_feedback(
                serde_json::json!({
                    "trace_id": "00000000000000000000000000000001",
                    "query_text": "auth patterns",
                    "returned_item_ids": [ki_id],
                    "explored_anchor_ids": [wrong_prefix_id],
                    "rating": "positive",
                }),
                PrincipalId::new(),
            )
            .await
            .expect(NO_PROTOCOL_ERROR);

        assert_eq!(result.is_error, Some(true));
        assert!(
            result.structured_content.is_none(),
            "{NO_STRUCTURED_CONTENT}"
        );
        assert!(first_text_content(&result).contains("expected prefix"));
    }

    #[tokio::test]
    async fn test_apply_feedback_invalid_rating_returns_application_error() {
        let handler = TestHandler::builder().build();

        let ki_id = KnowledgeItemId::new().to_string();
        let result = handler
            .apply_feedback(
                serde_json::json!({
                    "trace_id": "00000000000000000000000000000001",
                    "query_text": "auth patterns",
                    "returned_item_ids": [ki_id],
                    "rating": "neutral",
                }),
                PrincipalId::new(),
            )
            .await
            .expect(NO_PROTOCOL_ERROR);

        assert_eq!(result.is_error, Some(true));
        assert!(
            result.structured_content.is_none(),
            "{NO_STRUCTURED_CONTENT}"
        );
        assert!(first_text_content(&result).contains(INVALID_RATING));
    }

    #[tokio::test]
    async fn test_apply_feedback_malformed_embedding_profile_id_returns_application_error() {
        let handler = TestHandler::builder().build();

        let ki_id = KnowledgeItemId::new().to_string();
        let result = handler
            .apply_feedback(
                serde_json::json!({
                    "trace_id": "00000000000000000000000000000001",
                    "query_text": "auth patterns",
                    "returned_item_ids": [ki_id],
                    "rating": "positive",
                    "embedding_profile_id": "not-a-profile-id",
                }),
                PrincipalId::new(),
            )
            .await
            .expect(NO_PROTOCOL_ERROR);

        assert_eq!(result.is_error, Some(true));
        assert!(
            result.structured_content.is_none(),
            "{NO_STRUCTURED_CONTENT}"
        );
        assert!(first_text_content(&result).contains("expected prefix"));
    }

    /// `lazy_pool` cannot open connections, so the call fails at the
    /// transaction-begin phase. We assert the error message is a downstream
    /// pool/connection failure rather than any input-validation message, which
    /// confirms validation passed and the error originates from the pool.
    #[tokio::test]
    async fn test_apply_feedback_lazy_pool_fails_after_validation() {
        let handler = TestHandler::builder().build();

        let ki_id = KnowledgeItemId::new().to_string();
        let result = handler
            .apply_feedback(
                serde_json::json!({
                    "trace_id": "00000000000000000000000000000001",
                    "query_text": "auth patterns",
                    "returned_item_ids": [ki_id],
                    "rating": "positive",
                }),
                PrincipalId::new(),
            )
            .await
            .expect(NO_PROTOCOL_ERROR);

        assert_eq!(result.is_error, Some(true));
        assert!(
            result.structured_content.is_none(),
            "{NO_STRUCTURED_CONTENT}"
        );

        let message = first_text_content(&result);
        // The error must come from the pool layer, not input validation: the
        // transaction-begin failure surfaces as either a pool-exhaustion or a
        // query-failed message, never one of the validation messages.
        assert!(
            message.contains("pool") || message.contains("query failed"),
            "error should originate from the pool, not input validation: {message}",
        );
        assert!(!message.contains(EMPTY_QUERY_TEXT));
        assert!(!message.contains(EMPTY_RETURNED_ITEMS));
        assert!(!message.contains(INVALID_RATING));
    }

    #[tokio::test]
    async fn test_apply_feedback_optional_fields_omitted_succeeds() {
        let prin_id = PrincipalId::new();
        let feedback = a_retrieval_feedback().principal_id(prin_id).build();
        let active_prompts = test_active_prompt_versions();

        let mut repos = repos_for_feedback(feedback);
        configure_fingerprint_mocks(&mut repos, &active_prompts);

        // The feedback path reads the active profile for the embedding lineage,
        // so this test commits a genesis profile. A dedicated database isolates
        // it, keeping that committed profile out of the parallel suite's
        // global-state tests (prune, feedback provenance).
        let ctx = TestContext::new().await.expect("dedicated test database");
        let pool = ctx.pool().clone();
        let mut conn = ctx.raw_connection().await.expect("conn");
        ensure_genesis_profile(&mut conn, "nomic-embed-text:v1.5", 768).await;
        drop(conn);
        let handler = TestHandler::builder()
            .pool(pool)
            .repositories(repos)
            .active_prompt_versions(Arc::new(RwLock::new(active_prompts)))
            .build();

        let ki_id = KnowledgeItemId::new().to_string();
        let result = handler
            .apply_feedback(
                serde_json::json!({
                    "trace_id": "00000000000000000000000000000001",
                    "query_text": "auth patterns",
                    "returned_item_ids": [ki_id],
                    "rating": "positive",
                }),
                prin_id,
            )
            .await
            .expect(NO_PROTOCOL_ERROR);

        assert_eq!(result.is_error, Some(false));
    }

    // -- Service: happy path -----------------------------------------------

    #[tokio::test]
    async fn test_execute_feedback_happy_path() {
        let prin_id = PrincipalId::new();
        let feedback = a_retrieval_feedback().principal_id(prin_id).build();
        let expected_id = feedback.id();
        let expected_rating = feedback.rating();

        let params = FeedbackParams {
            principal_id: prin_id,
            ..default_params()
        };

        let mut repos = test_repositories();
        repos.retrieval_feedback = Arc::new(
            MockRetrievalFeedbackRepository::builder()
                .when_insert(move |new_fb| new_fb.principal_id == prin_id)
                .respond_with(feedback, None)
                .build(),
        );
        configure_fingerprint_mocks(&mut repos, &params.active_prompts);

        let result = call_execute(&repos, params).await.expect("should succeed");

        assert_eq!(result.id(), expected_id);
        assert_eq!(result.rating(), expected_rating);
        assert!(result.explored_anchor_ids().is_empty());
        assert!(result.notes().is_none());
        let hash = result.system_fingerprint_hash();
        assert_eq!(hash.len(), 64);
        assert!(hash.chars().all(|c| c.is_ascii_hexdigit()));
    }

    // -- Service: error paths ----------------------------------------------

    #[tokio::test]
    async fn test_execute_feedback_db_error_on_insert() {
        let params = default_params();

        let mut repos = test_repositories();
        repos.retrieval_feedback = Arc::new(
            MockRetrievalFeedbackRepository::builder()
                .on_insert_error(
                    || DbError::QueryFailed {
                        context: "insert failed".into(),
                        source: sqlx::Error::RowNotFound,
                    },
                    None,
                )
                .build(),
        );
        configure_fingerprint_mocks(&mut repos, &params.active_prompts);

        let err = call_execute(&repos, params).await.expect_err("should fail");

        assert!(matches!(
            err,
            FeedbackError::Db(DbError::QueryFailed { .. })
        ));
    }

    #[tokio::test]
    async fn test_execute_feedback_missing_prompt_versions_returns_error() {
        let mut repos = test_repositories();
        repos.prompt_version = Arc::new(
            MockPromptVersionRepository::builder()
                .on_find_by_ids(vec![], None)
                .build(),
        );

        let params = default_params();
        let err = call_execute(&repos, params).await.expect_err("should fail");

        assert!(matches!(
            err,
            FeedbackError::Fingerprint(FingerprintError::MissingPromptVersions { .. })
        ));
    }

    /// When `find_by_ids` returns fewer than 6 prompt versions (some
    /// present, some missing), the fingerprint computation still fails.
    #[tokio::test]
    async fn test_execute_feedback_partial_prompt_versions_returns_error() {
        let prompts = test_active_prompt_versions();
        let ids = prompts.version_ids();

        // Return versions for only the first 3 of 6 required IDs.
        let partial: Vec<_> = ids[..3]
            .iter()
            .map(|&id| a_prompt_version().id(id).build())
            .collect();

        let mut repos = test_repositories();
        repos.prompt_version = Arc::new(
            MockPromptVersionRepository::builder()
                .on_find_by_ids(partial, None)
                .build(),
        );

        let params = FeedbackParams {
            active_prompts: prompts,
            ..default_params()
        };
        let err = call_execute(&repos, params).await.expect_err("should fail");

        assert!(matches!(
            err,
            FeedbackError::Fingerprint(FingerprintError::MissingPromptVersions { .. })
        ));
    }
}
