//! Handler for `tribal_feedback` — retrieval session quality rating.

use std::str::FromStr;

use rmcp::{
    model::{CallToolResult, ErrorData as McpError},
    service::{RequestContext, RoleServer},
};
use sqlx::PgConnection;
use tracing::Instrument;
use tribal_db::DbError;
use tribal_domain::{FeedbackRating, KnowledgeItemId, McpErrorCode, PrincipalId, span_attrs};

use super::common::acquire_connection;
use crate::{
    error::{IntoCallToolResult, IntoMcpError, McpToolError, invalid_argument},
    mapping::{McpFeedbackRequest, McpFeedbackResponse},
    server_handler::{ConnectionRepositories, TribalServerHandler},
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
    embedding_model: String,
    returned_item_ids: Vec<KnowledgeItemId>,
    explored_anchor_ids: Vec<KnowledgeItemId>,
    principal_id: PrincipalId,
    rating: FeedbackRating,
    notes: Option<String>,
}

/// Errors that can occur during feedback execution.
#[derive(Debug, thiserror::Error)]
enum FeedbackError {
    #[error(transparent)]
    Db(#[from] DbError),
}

impl IntoMcpError for FeedbackError {
    fn into_mcp_error(self) -> McpToolError {
        match self {
            Self::Db(e) => e.into_mcp_error(),
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
            { span_attrs::TRANSPORT } = self.transport_name.as_str(),
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

        // -- Embedding model --------------------------------------------------

        let embedding_model = self.state.embedding_provider.identity().model.clone();

        // -- Build params and execute -----------------------------------------

        let feedback_params = FeedbackParams {
            trace_id: request.trace_id,
            query_text: request.query_text,
            embedding_model,
            returned_item_ids,
            explored_anchor_ids,
            principal_id,
            rating,
            notes: request.notes,
        };

        let mut conn = match acquire_connection(
            &self.state.pool_mcp,
            self.config.pool_name,
            &self.state.metrics,
        )
        .await
        {
            Ok(conn) => conn,
            Err(call_result) => return Ok(call_result),
        };

        let feedback = match execute_feedback(&mut conn, &self.repositories, feedback_params).await
        {
            Ok(f) => f,
            Err(e) => return Ok(e.into_mcp_error().into_call_tool_result()),
        };

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
    let new_feedback = tribal_db::NewRetrievalFeedback::builder()
        .trace_id(params.trace_id)
        .query_text(params.query_text)
        .embedding_model(params.embedding_model)
        .returned_item_ids(params.returned_item_ids)
        .explored_anchor_ids(params.explored_anchor_ids)
        .principal_id(params.principal_id)
        .rating(params.rating)
        .notes(params.notes)
        .policy_version(None)
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
    use tribal_domain::{FeedbackRating, KnowledgeItemId, PrincipalId, ProjectId};
    use tribal_test_utils::{MockRetrievalFeedbackRepository, a_retrieval_feedback, test_context};

    use super::*;
    use crate::test_utils::{TestHandler, test_repositories};

    // -- Constants ---------------------------------------------------------

    const STRUCTURED_CONTENT: &str = "structured_content must be present";
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
        execute_feedback(&mut tx, repos, params).await
    }

    fn default_params() -> FeedbackParams {
        FeedbackParams {
            trace_id: "00000000000000000000000000000001".into(),
            query_text: "auth patterns".into(),
            embedding_model: "mock-model".into(),
            returned_item_ids: vec![KnowledgeItemId::new()],
            explored_anchor_ids: Vec::new(),
            principal_id: PrincipalId::new(),
            rating: FeedbackRating::Positive,
            notes: None,
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
        let structured = result.structured_content.expect(STRUCTURED_CONTENT);
        assert_eq!(structured["code"], "invalid_argument");
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
        let structured = result.structured_content.expect(STRUCTURED_CONTENT);
        assert_eq!(structured["code"], "invalid_argument");
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
        let structured = result.structured_content.expect(STRUCTURED_CONTENT);
        assert_eq!(structured["code"], "invalid_argument");
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
        let structured = result.structured_content.expect(STRUCTURED_CONTENT);
        assert_eq!(structured["code"], "invalid_argument");
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
        let structured = result.structured_content.expect(STRUCTURED_CONTENT);
        assert_eq!(structured["code"], "invalid_argument");
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
        let structured = result.structured_content.expect(STRUCTURED_CONTENT);
        assert_eq!(structured["code"], "invalid_argument");
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
        let structured = result.structured_content.expect(STRUCTURED_CONTENT);
        assert_eq!(structured["code"], "invalid_argument");
    }

    /// `lazy_pool` cannot open connections, so the call fails at the
    /// pool acquisition phase. We assert `is_error` to confirm validation
    /// passed and the error originates from the pool, not from input
    /// validation.
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
        let structured = result.structured_content.expect(STRUCTURED_CONTENT);
        assert_ne!(
            structured["code"], "invalid_argument",
            "error should originate from pool, not input validation",
        );
    }

    #[tokio::test]
    async fn test_apply_feedback_optional_fields_omitted_succeeds() {
        let prin_id = PrincipalId::new();
        let feedback = a_retrieval_feedback().principal_id(prin_id).build();
        let repos = repos_for_feedback(feedback);

        let ctx = test_context().await;
        let pool = ctx.create_pool().await.expect("pool");
        let handler = TestHandler::builder()
            .pool(pool)
            .repositories(repos)
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

        let mut repos = test_repositories();
        repos.retrieval_feedback = Arc::new(
            MockRetrievalFeedbackRepository::builder()
                .when_insert(move |new_fb| new_fb.principal_id == prin_id)
                .respond_with(feedback, None)
                .build(),
        );

        let params = FeedbackParams {
            principal_id: prin_id,
            ..default_params()
        };
        let result = call_execute(&repos, params).await.expect("should succeed");

        assert_eq!(result.id(), expected_id);
        assert_eq!(result.rating(), expected_rating);
        assert!(result.explored_anchor_ids().is_empty());
        assert!(result.notes().is_none());
        assert!(result.policy_version().is_none());
    }

    // -- Service: error paths ----------------------------------------------

    #[tokio::test]
    async fn test_execute_feedback_db_error_on_insert() {
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

        let params = default_params();
        let err = call_execute(&repos, params).await.expect_err("should fail");

        assert!(matches!(
            err,
            FeedbackError::Db(DbError::QueryFailed { .. })
        ));
    }
}
