//! Handler for `tribal_feedback` — retrieval session quality rating.

use std::str::FromStr;
use std::sync::Arc;

use rmcp::{
    model::{CallToolResult, ErrorData as McpError},
    service::{RequestContext, RoleServer},
};
use sqlx::{PgConnection, PgPool};
use tokio::sync::RwLock;
use tribal_db::DbError;
use tribal_domain::{FeedbackRating, KnowledgeItemId, McpErrorCode};
use tribal_inference::EmbeddingProvider;

use super::common::acquire_connection;
use crate::{
    error::{IntoCallToolResult, IntoMcpError, McpToolError, invalid_argument},
    mapping::{McpRetrievalFeedbackRequest, McpRetrievalFeedbackResponse},
    server_handler::{ConnectionRepositories, TribalServerHandler},
    session::SessionContext,
};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const MAX_TRACE_ID_LENGTH: usize = 256;

const EMPTY_TRACE_ID: &str = "trace_id must not be empty";
const TRACE_ID_TOO_LONG: &str = "trace_id exceeds maximum length of 256 characters";
const EMPTY_QUERY_TEXT: &str = "query_text must not be empty";
const EMPTY_RETURNED_ITEMS: &str = "returned_item_ids must contain at least one item";
const INVALID_RATING: &str = "rating must be \"positive\" or \"negative\"";

// ---------------------------------------------------------------------------
// Service types
// ---------------------------------------------------------------------------

/// Domain-level parameters for the retrieval feedback service function.
struct RetrievalFeedbackParams {
    trace_id: String,
    query_text: String,
    embedding_model: String,
    returned_item_ids: Vec<KnowledgeItemId>,
    explored_anchor_ids: Vec<KnowledgeItemId>,
    principal_key: String,
    rating: FeedbackRating,
    notes: Option<String>,
}

/// Errors that can occur during retrieval feedback execution.
#[derive(Debug, thiserror::Error)]
enum RetrievalFeedbackError {
    #[error(transparent)]
    Db(#[from] DbError),

    #[error("principal not found for key: {principal_key}")]
    PrincipalNotFound { principal_key: String },
}

impl IntoMcpError for RetrievalFeedbackError {
    fn into_mcp_error(self) -> McpToolError {
        match self {
            Self::Db(e) => e.into_mcp_error(),
            Self::PrincipalNotFound { principal_key } => McpToolError {
                code: McpErrorCode::FailedPrecondition,
                message: format!(
                    "session principal_key \"{principal_key}\" does not resolve to a known principal"
                ),
                details: serde_json::json!({}),
            },
        }
    }
}

// ---------------------------------------------------------------------------
// Handler
// ---------------------------------------------------------------------------

impl TribalServerHandler {
    /// Handles the `tribal_feedback` tool call.
    pub(crate) async fn handle_retrieval_feedback(
        &self,
        params: serde_json::Value,
        _context: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, McpError> {
        Self::apply_retrieval_feedback(
            &self.pool,
            &self.repositories,
            &self.session,
            &self.embedding_provider,
            params,
        )
        .await
    }

    /// Core logic for `tribal_feedback`, separated from the outer handler
    /// so it can be tested without a `Peer<RoleServer>`.
    ///
    /// Parses the request, validates inputs, reads session state (principal
    /// key), resolves the embedding model, then acquires a connection and
    /// delegates to [`execute_retrieval_feedback`] for domain logic. Domain
    /// errors are returned as error `CallToolResult` values via `IntoMcpError`
    /// / `IntoCallToolResult`. Only protocol-level errors (malformed JSON)
    /// return `Err(McpError)`.
    async fn apply_retrieval_feedback(
        pool: &PgPool,
        repositories: &ConnectionRepositories,
        session: &RwLock<SessionContext>,
        embedding_provider: &Arc<dyn EmbeddingProvider>,
        params: serde_json::Value,
    ) -> Result<CallToolResult, McpError> {
        let request: McpRetrievalFeedbackRequest =
            serde_json::from_value(params).map_err(|e| invalid_argument(e.to_string()))?;

        // -- Validate trace_id ------------------------------------------------

        if request.trace_id.is_empty() {
            return Ok(McpToolError {
                code: McpErrorCode::InvalidArgument,
                message: EMPTY_TRACE_ID.into(),
                details: serde_json::json!({}),
            }
            .into_call_tool_result());
        }
        if request.trace_id.len() > MAX_TRACE_ID_LENGTH {
            return Ok(McpToolError {
                code: McpErrorCode::InvalidArgument,
                message: TRACE_ID_TOO_LONG.into(),
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

        let rating = match FeedbackRating::from_str(&request.rating) {
            Ok(r) => r,
            Err(_) => {
                return Ok(McpToolError {
                    code: McpErrorCode::InvalidArgument,
                    message: INVALID_RATING.into(),
                    details: serde_json::json!({}),
                }
                .into_call_tool_result());
            }
        };

        // -- Session state ----------------------------------------------------

        let principal_key = {
            let guard = session.read().await;
            guard.principal_key.clone()
        };

        // -- Embedding model --------------------------------------------------

        let embedding_model = embedding_provider.identity().model.clone();

        // -- Build params and execute -----------------------------------------

        let feedback_params = RetrievalFeedbackParams {
            trace_id: request.trace_id,
            query_text: request.query_text,
            embedding_model,
            returned_item_ids,
            explored_anchor_ids,
            principal_key,
            rating,
            notes: request.notes,
        };

        let mut conn = match acquire_connection(pool).await {
            Ok(conn) => conn,
            Err(call_result) => return Ok(call_result),
        };

        let feedback =
            match execute_retrieval_feedback(&mut conn, repositories, feedback_params).await {
                Ok(f) => f,
                Err(e) => return Ok(e.into_mcp_error().into_call_tool_result()),
            };

        Ok(McpRetrievalFeedbackResponse::from(&feedback).into_call_tool_result())
    }
}

// ---------------------------------------------------------------------------
// Service function
// ---------------------------------------------------------------------------

/// Executes the retrieval feedback operation: resolves the principal,
/// builds the new feedback record, and inserts it.
///
/// All inputs and outputs are domain types — no MCP types cross this
/// boundary.
async fn execute_retrieval_feedback(
    conn: &mut PgConnection,
    repositories: &ConnectionRepositories,
    params: RetrievalFeedbackParams,
) -> Result<tribal_domain::RetrievalFeedback, RetrievalFeedbackError> {
    let principal = repositories
        .principal
        .find_by_key(conn, &params.principal_key)
        .await?
        .ok_or_else(|| RetrievalFeedbackError::PrincipalNotFound {
            principal_key: params.principal_key.clone(),
        })?;

    let new_feedback = tribal_db::NewRetrievalFeedback::builder()
        .trace_id(params.trace_id)
        .query_text(params.query_text)
        .embedding_model(params.embedding_model)
        .returned_item_ids(params.returned_item_ids)
        .explored_anchor_ids(params.explored_anchor_ids)
        .principal_id(principal.id())
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
    use tribal_domain::{KnowledgeItemId, PrincipalId, ProjectId};
    use tribal_test_utils::{
        MockEmbeddingProvider, MockPrincipalRepository, MockRetrievalFeedbackRepository,
        a_principal, a_retrieval_feedback, lazy_pool, test_context,
    };

    use super::*;
    use crate::{session::SessionContext, test_utils::test_repositories};

    // -- Constants ---------------------------------------------------------

    const STRUCTURED_CONTENT: &str = "structured_content must be present";
    const NO_PROTOCOL_ERROR: &str = "should not return a protocol error";

    // -- Helpers -----------------------------------------------------------

    fn test_session() -> Arc<RwLock<SessionContext>> {
        Arc::new(RwLock::new(SessionContext::new(None, "user:test".into())))
    }

    fn test_embedding_provider() -> Arc<dyn EmbeddingProvider> {
        Arc::new(MockEmbeddingProvider::builder().build())
    }

    fn valid_retrieval_feedback_json() -> serde_json::Value {
        let ki_id = KnowledgeItemId::new().to_string();
        serde_json::json!({
            "trace_id": "00000000000000000000000000000001",
            "query_text": "auth patterns",
            "returned_item_ids": [ki_id],
            "rating": "positive",
        })
    }

    fn repos_for_retrieval_feedback(
        principal: tribal_domain::Principal,
        feedback: tribal_domain::RetrievalFeedback,
    ) -> ConnectionRepositories {
        let mut repos = test_repositories();
        repos.principal = Arc::new(
            MockPrincipalRepository::builder()
                .on_find_by_key(Some(principal), None)
                .build(),
        );
        repos.retrieval_feedback = Arc::new(
            MockRetrievalFeedbackRepository::builder()
                .on_insert(feedback, None)
                .build(),
        );
        repos
    }

    async fn call_execute(
        repos: &ConnectionRepositories,
        params: RetrievalFeedbackParams,
    ) -> Result<tribal_domain::RetrievalFeedback, RetrievalFeedbackError> {
        let ctx = test_context().await;
        let mut tx = ctx.begin_test().await.expect("begin");
        execute_retrieval_feedback(&mut tx, repos, params).await
    }

    fn default_params() -> RetrievalFeedbackParams {
        RetrievalFeedbackParams {
            trace_id: "00000000000000000000000000000001".into(),
            query_text: "auth patterns".into(),
            embedding_model: "mock-model".into(),
            returned_item_ids: vec![KnowledgeItemId::new()],
            explored_anchor_ids: Vec::new(),
            principal_key: "user:test".into(),
            rating: FeedbackRating::Positive,
            notes: None,
        }
    }

    // -- Adapter: validation -----------------------------------------------

    #[tokio::test]
    async fn test_apply_retrieval_feedback_malformed_json_returns_protocol_error() {
        let pool = lazy_pool();
        let repos = test_repositories();
        let session = test_session();
        let provider = test_embedding_provider();

        let err = TribalServerHandler::apply_retrieval_feedback(
            &pool,
            &repos,
            &session,
            &provider,
            serde_json::json!({"trace_id": 123}),
        )
        .await
        .expect_err("should return Err(McpError) for malformed params");

        assert_eq!(err.code, ErrorCode::INVALID_PARAMS);
    }

    #[tokio::test]
    async fn test_apply_retrieval_feedback_empty_trace_id_returns_application_error() {
        let pool = lazy_pool();
        let repos = test_repositories();
        let session = test_session();
        let provider = test_embedding_provider();

        let mut json = valid_retrieval_feedback_json();
        json["trace_id"] = serde_json::json!("");

        let result = TribalServerHandler::apply_retrieval_feedback(
            &pool, &repos, &session, &provider, json,
        )
        .await
        .expect(NO_PROTOCOL_ERROR);

        assert_eq!(result.is_error, Some(true));
        let structured = result.structured_content.expect(STRUCTURED_CONTENT);
        assert_eq!(structured["code"], "invalid_argument");
    }

    #[tokio::test]
    async fn test_apply_retrieval_feedback_trace_id_too_long_returns_application_error() {
        let pool = lazy_pool();
        let repos = test_repositories();
        let session = test_session();
        let provider = test_embedding_provider();

        let mut json = valid_retrieval_feedback_json();
        json["trace_id"] = serde_json::json!("x".repeat(257));

        let result = TribalServerHandler::apply_retrieval_feedback(
            &pool, &repos, &session, &provider, json,
        )
        .await
        .expect(NO_PROTOCOL_ERROR);

        assert_eq!(result.is_error, Some(true));
        let structured = result.structured_content.expect(STRUCTURED_CONTENT);
        assert_eq!(structured["code"], "invalid_argument");
    }

    #[tokio::test]
    async fn test_apply_retrieval_feedback_empty_query_text_returns_application_error() {
        let pool = lazy_pool();
        let repos = test_repositories();
        let session = test_session();
        let provider = test_embedding_provider();

        let mut json = valid_retrieval_feedback_json();
        json["query_text"] = serde_json::json!("");

        let result = TribalServerHandler::apply_retrieval_feedback(
            &pool, &repos, &session, &provider, json,
        )
        .await
        .expect(NO_PROTOCOL_ERROR);

        assert_eq!(result.is_error, Some(true));
        let structured = result.structured_content.expect(STRUCTURED_CONTENT);
        assert_eq!(structured["code"], "invalid_argument");
    }

    #[tokio::test]
    async fn test_apply_retrieval_feedback_empty_returned_item_ids_returns_application_error() {
        let pool = lazy_pool();
        let repos = test_repositories();
        let session = test_session();
        let provider = test_embedding_provider();

        let mut json = valid_retrieval_feedback_json();
        json["returned_item_ids"] = serde_json::json!([]);

        let result = TribalServerHandler::apply_retrieval_feedback(
            &pool, &repos, &session, &provider, json,
        )
        .await
        .expect(NO_PROTOCOL_ERROR);

        assert_eq!(result.is_error, Some(true));
        let structured = result.structured_content.expect(STRUCTURED_CONTENT);
        assert_eq!(structured["code"], "invalid_argument");
    }

    #[tokio::test]
    async fn test_apply_retrieval_feedback_invalid_returned_item_id_prefix_returns_application_error()
    {
        let pool = lazy_pool();
        let repos = test_repositories();
        let session = test_session();
        let provider = test_embedding_provider();

        let wrong_prefix_id = ProjectId::new().to_string();
        let mut json = valid_retrieval_feedback_json();
        json["returned_item_ids"] = serde_json::json!([wrong_prefix_id]);

        let result = TribalServerHandler::apply_retrieval_feedback(
            &pool, &repos, &session, &provider, json,
        )
        .await
        .expect(NO_PROTOCOL_ERROR);

        assert_eq!(result.is_error, Some(true));
        let structured = result.structured_content.expect(STRUCTURED_CONTENT);
        assert_eq!(structured["code"], "invalid_argument");
    }

    #[tokio::test]
    async fn test_apply_retrieval_feedback_invalid_explored_anchor_id_prefix_returns_application_error()
    {
        let pool = lazy_pool();
        let repos = test_repositories();
        let session = test_session();
        let provider = test_embedding_provider();

        let wrong_prefix_id = ProjectId::new().to_string();
        let mut json = valid_retrieval_feedback_json();
        json["explored_anchor_ids"] = serde_json::json!([wrong_prefix_id]);

        let result = TribalServerHandler::apply_retrieval_feedback(
            &pool, &repos, &session, &provider, json,
        )
        .await
        .expect(NO_PROTOCOL_ERROR);

        assert_eq!(result.is_error, Some(true));
        let structured = result.structured_content.expect(STRUCTURED_CONTENT);
        assert_eq!(structured["code"], "invalid_argument");
    }

    #[tokio::test]
    async fn test_apply_retrieval_feedback_invalid_rating_returns_application_error() {
        let pool = lazy_pool();
        let repos = test_repositories();
        let session = test_session();
        let provider = test_embedding_provider();

        let mut json = valid_retrieval_feedback_json();
        json["rating"] = serde_json::json!("neutral");

        let result = TribalServerHandler::apply_retrieval_feedback(
            &pool, &repos, &session, &provider, json,
        )
        .await
        .expect(NO_PROTOCOL_ERROR);

        assert_eq!(result.is_error, Some(true));
        let structured = result.structured_content.expect(STRUCTURED_CONTENT);
        assert_eq!(structured["code"], "invalid_argument");
    }

    #[tokio::test]
    async fn test_apply_retrieval_feedback_pool_exhaustion_returns_application_error() {
        let pool = lazy_pool();
        let repos = test_repositories();
        let session = test_session();
        let provider = test_embedding_provider();

        let json = valid_retrieval_feedback_json();

        let result = TribalServerHandler::apply_retrieval_feedback(
            &pool, &repos, &session, &provider, json,
        )
        .await
        .expect(NO_PROTOCOL_ERROR);

        assert_eq!(result.is_error, Some(true));
        let structured = result.structured_content.expect(STRUCTURED_CONTENT);
        assert_eq!(structured["code"], "resource_exhausted");
    }

    #[tokio::test]
    async fn test_apply_retrieval_feedback_optional_fields_omitted_succeeds() {
        let prin_id = PrincipalId::new();
        let principal = a_principal().id(prin_id).build();
        let feedback = a_retrieval_feedback().principal_id(prin_id).build();
        let repos = repos_for_retrieval_feedback(principal, feedback);

        let pool = test_context().await.pool().clone();
        let session = test_session();
        let provider = test_embedding_provider();

        let json = valid_retrieval_feedback_json();

        let result = TribalServerHandler::apply_retrieval_feedback(
            &pool, &repos, &session, &provider, json,
        )
        .await
        .expect(NO_PROTOCOL_ERROR);

        assert_eq!(result.is_error, Some(false));
    }

    // -- Service: happy path -----------------------------------------------

    #[tokio::test]
    async fn test_execute_retrieval_feedback_happy_path() {
        let prin_id = PrincipalId::new();
        let principal = a_principal().id(prin_id).build();
        let feedback = a_retrieval_feedback().principal_id(prin_id).build();
        let expected_id = feedback.id();

        let repos = repos_for_retrieval_feedback(principal, feedback);

        let params = RetrievalFeedbackParams {
            principal_key: "user:test".into(),
            ..default_params()
        };

        let result = call_execute(&repos, params).await.expect("should succeed");
        assert_eq!(result.id(), expected_id);
    }

    // -- Service: error paths ----------------------------------------------

    #[tokio::test]
    async fn test_execute_retrieval_feedback_principal_not_found() {
        let mut repos = test_repositories();
        repos.principal = Arc::new(
            MockPrincipalRepository::builder()
                .on_find_by_key(None, None)
                .build(),
        );

        let params = RetrievalFeedbackParams {
            principal_key: "user:unknown".into(),
            ..default_params()
        };

        let err = call_execute(&repos, params)
            .await
            .expect_err("should fail");

        assert!(
            matches!(err, RetrievalFeedbackError::PrincipalNotFound { principal_key } if principal_key == "user:unknown")
        );
    }

    #[tokio::test]
    async fn test_execute_retrieval_feedback_db_error_on_insert() {
        let prin_id = PrincipalId::new();
        let principal = a_principal().id(prin_id).build();

        let mut repos = test_repositories();
        repos.principal = Arc::new(
            MockPrincipalRepository::builder()
                .on_find_by_key(Some(principal), None)
                .build(),
        );
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
        let err = call_execute(&repos, params)
            .await
            .expect_err("should fail");

        assert!(matches!(
            err,
            RetrievalFeedbackError::Db(DbError::QueryFailed { .. })
        ));
    }
}
