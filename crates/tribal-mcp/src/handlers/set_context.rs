use std::str::FromStr;

use rmcp::{
    model::{CallToolResult, ErrorData as McpError},
    service::{RequestContext, RoleServer},
};
use sqlx::PgPool;
use tokio::sync::RwLock;
use tribal_db::DbError;
use tribal_domain::ProjectId;

use crate::{
    error::{IntoCallToolResult, IntoMcpError, invalid_argument},
    mapping::{McpSetContextRequest, McpSetContextResponse},
    server_handler::{ConnectionRepositories, POOL_NAME, TribalServerHandler},
    session::{SessionContext, SessionProject, notify_session_updated},
};

impl TribalServerHandler {
    /// Handles the `tribal_set_context` tool call.
    ///
    /// Delegates to [`apply_set_context`] for the core logic, then sends a
    /// resource-updated notification if the session was mutated.
    pub(crate) async fn handle_set_context(
        &self,
        params: serde_json::Value,
        context: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, McpError> {
        let result =
            Self::apply_set_context(&self.pool, &self.repositories, &self.session, params).await?;

        if result.is_error != Some(true) {
            notify_session_updated(&self.session, &context.peer).await;
        }

        Ok(result)
    }

    /// Core logic for `tribal_set_context`, separated from the outer handler
    /// so it can be tested without a `Peer<RoleServer>`.
    ///
    /// Parses the request, validates and resolves a project ID (if supplied),
    /// then applies partial updates to the session. Returns the full
    /// post-mutation session context as a `CallToolResult`.
    async fn apply_set_context(
        pool: &PgPool,
        repositories: &ConnectionRepositories,
        session: &RwLock<SessionContext>,
        params: serde_json::Value,
    ) -> Result<CallToolResult, McpError> {
        let request: McpSetContextRequest =
            serde_json::from_value(params).map_err(|e| invalid_argument(e.to_string()))?;

        let resolved_project = if let Some(ref raw_id) = request.project_id {
            let proj_id = match ProjectId::from_str(raw_id) {
                Ok(id) => id,
                Err(e) => return Ok(e.into_mcp_error().into_call_tool_result()),
            };

            let Ok(mut conn) = pool.acquire().await else {
                return Ok(DbError::PoolExhausted {
                    pool_name: POOL_NAME,
                }
                .into_mcp_error()
                .into_call_tool_result());
            };

            let project = match repositories.project.find_by_id(&mut conn, proj_id).await {
                Ok(p) => p,
                Err(e) => return Ok(e.into_mcp_error().into_call_tool_result()),
            };

            Some(SessionProject {
                id: project.id(),
                name: project.name().to_owned(),
                git_remote: project.git_remote().to_owned(),
            })
        } else {
            None
        };

        let response = {
            let mut ctx = session.write().await;

            if let Some(project) = resolved_project {
                ctx.project = Some(project);
            }
            if let Some(model) = request.model {
                ctx.actor.model = Some(model);
            }
            if let Some(provider) = request.provider {
                ctx.actor.provider = Some(provider);
            }

            McpSetContextResponse::from(&*ctx)
        };

        Ok(response.into_call_tool_result())
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use rmcp::model::ErrorCode;
    use tokio::sync::RwLock;
    use tribal_domain::{KnowledgeItemId, ProjectId};
    use tribal_test_utils::{
        ExhaustBehaviour, MockProjectRepository, a_not_found, a_project, test_context,
    };

    use crate::{
        server_handler::{ConnectionRepositories, TribalServerHandler},
        session::SessionContext,
        test_utils::test_repositories,
    };

    // -- Constants ---------------------------------------------------------

    const STRUCTURED_CONTENT: &str = "structured_content must be present";
    const NO_PROTOCOL_ERROR: &str = "should not return a protocol error";

    // -- Helpers -----------------------------------------------------------

    fn repositories_with_project_mock(mock: MockProjectRepository) -> ConnectionRepositories {
        let mut repos = test_repositories();
        repos.project = Arc::new(mock);
        repos
    }

    // -- Happy path -------------------------------------------------------

    #[tokio::test]
    async fn test_empty_request_returns_unchanged_session() {
        let ctx = test_context().await;
        let session = Arc::new(RwLock::new(SessionContext::new(None, "user:test".into())));

        let result = TribalServerHandler::apply_set_context(
            ctx.pool(),
            &test_repositories(),
            &session,
            serde_json::json!({}),
        )
        .await
        .expect(NO_PROTOCOL_ERROR);

        assert_eq!(result.is_error, Some(false));

        let structured = result.structured_content.expect(STRUCTURED_CONTENT);
        assert_eq!(structured["principal_key"], "user:test");
        assert!(structured["project"].is_null());
        assert!(structured["actor"]["model"].is_null());
        assert!(structured["actor"]["provider"].is_null());
    }

    #[tokio::test]
    async fn test_set_model_updates_actor() {
        let ctx = test_context().await;
        let session = Arc::new(RwLock::new(SessionContext::new(None, "user:test".into())));

        let result = TribalServerHandler::apply_set_context(
            ctx.pool(),
            &test_repositories(),
            &session,
            serde_json::json!({ "model": "claude-opus-4-6" }),
        )
        .await
        .expect(NO_PROTOCOL_ERROR);

        assert_eq!(result.is_error, Some(false));

        let structured = result.structured_content.expect(STRUCTURED_CONTENT);
        assert_eq!(structured["actor"]["model"], "claude-opus-4-6");
        assert!(structured["actor"]["provider"].is_null());

        let guard = session.read().await;
        assert_eq!(guard.actor.model.as_deref(), Some("claude-opus-4-6"));
    }

    #[tokio::test]
    async fn test_set_provider_updates_actor() {
        let ctx = test_context().await;
        let session = Arc::new(RwLock::new(SessionContext::new(None, "user:test".into())));

        let result = TribalServerHandler::apply_set_context(
            ctx.pool(),
            &test_repositories(),
            &session,
            serde_json::json!({ "provider": "anthropic" }),
        )
        .await
        .expect(NO_PROTOCOL_ERROR);

        assert_eq!(result.is_error, Some(false));

        let structured = result.structured_content.expect(STRUCTURED_CONTENT);
        assert_eq!(structured["actor"]["provider"], "anthropic");

        let guard = session.read().await;
        assert_eq!(guard.actor.provider.as_deref(), Some("anthropic"));
    }

    #[tokio::test]
    async fn test_set_model_and_provider() {
        let ctx = test_context().await;
        let session = Arc::new(RwLock::new(SessionContext::new(None, "user:test".into())));

        let result = TribalServerHandler::apply_set_context(
            ctx.pool(),
            &test_repositories(),
            &session,
            serde_json::json!({
                "model": "claude-opus-4-6",
                "provider": "anthropic",
            }),
        )
        .await
        .expect(NO_PROTOCOL_ERROR);

        assert_eq!(result.is_error, Some(false));

        let structured = result.structured_content.expect(STRUCTURED_CONTENT);
        assert_eq!(structured["actor"]["model"], "claude-opus-4-6");
        assert_eq!(structured["actor"]["provider"], "anthropic");
    }

    #[tokio::test]
    async fn test_set_project_id_with_valid_project() {
        let ctx = test_context().await;
        let project = a_project().build();
        let proj_id_str = project.id().to_string();

        let mock = MockProjectRepository::builder()
            .on_find_by_id(project.clone(), None)
            .build();
        let repos = repositories_with_project_mock(mock);
        let session = Arc::new(RwLock::new(SessionContext::new(None, "user:test".into())));

        let result = TribalServerHandler::apply_set_context(
            ctx.pool(),
            &repos,
            &session,
            serde_json::json!({ "project_id": proj_id_str }),
        )
        .await
        .expect(NO_PROTOCOL_ERROR);

        assert_eq!(result.is_error, Some(false));

        let structured = result.structured_content.expect(STRUCTURED_CONTENT);
        assert_eq!(structured["project"]["id"], proj_id_str);
        assert_eq!(structured["project"]["name"], project.name());
        assert_eq!(structured["project"]["git_remote"], project.git_remote());

        let guard = session.read().await;
        let session_project = guard.project.as_ref().expect("project should be set");
        assert_eq!(session_project.id, project.id());
        assert_eq!(session_project.name, project.name());
    }

    #[tokio::test]
    async fn test_set_all_fields() {
        let ctx = test_context().await;
        let project = a_project().build();
        let proj_id_str = project.id().to_string();

        let mock = MockProjectRepository::builder()
            .on_find_by_id(project.clone(), None)
            .build();
        let repos = repositories_with_project_mock(mock);
        let session = Arc::new(RwLock::new(SessionContext::new(None, "user:test".into())));

        let result = TribalServerHandler::apply_set_context(
            ctx.pool(),
            &repos,
            &session,
            serde_json::json!({
                "project_id": proj_id_str,
                "model": "claude-opus-4-6",
                "provider": "anthropic",
            }),
        )
        .await
        .expect(NO_PROTOCOL_ERROR);

        assert_eq!(result.is_error, Some(false));

        let structured = result.structured_content.expect(STRUCTURED_CONTENT);
        assert_eq!(structured["project"]["id"], proj_id_str);
        assert_eq!(structured["actor"]["model"], "claude-opus-4-6");
        assert_eq!(structured["actor"]["provider"], "anthropic");
    }

    #[tokio::test]
    async fn test_partial_updates_are_additive() {
        let ctx = test_context().await;
        let session = Arc::new(RwLock::new(SessionContext::new(None, "user:test".into())));
        let repos = test_repositories();

        TribalServerHandler::apply_set_context(
            ctx.pool(),
            &repos,
            &session,
            serde_json::json!({ "model": "claude-opus-4-6" }),
        )
        .await
        .expect(NO_PROTOCOL_ERROR);

        let result = TribalServerHandler::apply_set_context(
            ctx.pool(),
            &repos,
            &session,
            serde_json::json!({ "provider": "anthropic" }),
        )
        .await
        .expect(NO_PROTOCOL_ERROR);

        let structured = result.structured_content.expect(STRUCTURED_CONTENT);
        assert_eq!(structured["actor"]["model"], "claude-opus-4-6");
        assert_eq!(structured["actor"]["provider"], "anthropic");
    }

    #[tokio::test]
    async fn test_idempotent_same_values() {
        let ctx = test_context().await;
        let session = Arc::new(RwLock::new(SessionContext::new(None, "user:test".into())));
        let repos = test_repositories();
        let params = serde_json::json!({ "model": "claude-opus-4-6" });

        let result1 =
            TribalServerHandler::apply_set_context(ctx.pool(), &repos, &session, params.clone())
                .await
                .expect(NO_PROTOCOL_ERROR);

        let result2 = TribalServerHandler::apply_set_context(ctx.pool(), &repos, &session, params)
            .await
            .expect(NO_PROTOCOL_ERROR);

        let s1 = result1.structured_content.expect(STRUCTURED_CONTENT);
        let s2 = result2.structured_content.expect(STRUCTURED_CONTENT);
        assert_eq!(s1, s2);
    }

    // -- Error paths -------------------------------------------------------

    #[tokio::test]
    async fn test_invalid_project_id_prefix() {
        let ctx = test_context().await;
        let session = Arc::new(RwLock::new(SessionContext::new(None, "user:test".into())));
        let wrong_type_id = KnowledgeItemId::new().to_string();

        let result = TribalServerHandler::apply_set_context(
            ctx.pool(),
            &test_repositories(),
            &session,
            serde_json::json!({ "project_id": wrong_type_id }),
        )
        .await
        .expect("should return Ok with error result, not Err");

        assert_eq!(result.is_error, Some(true));

        let structured = result.structured_content.expect(STRUCTURED_CONTENT);
        assert_eq!(structured["code"], "invalid_argument");
    }

    #[tokio::test]
    async fn test_invalid_project_id_uuid() {
        let ctx = test_context().await;
        let session = Arc::new(RwLock::new(SessionContext::new(None, "user:test".into())));

        let result = TribalServerHandler::apply_set_context(
            ctx.pool(),
            &test_repositories(),
            &session,
            serde_json::json!({ "project_id": "proj_not-a-uuid" }),
        )
        .await
        .expect("should return Ok with error result, not Err");

        assert_eq!(result.is_error, Some(true));

        let structured = result.structured_content.expect(STRUCTURED_CONTENT);
        assert_eq!(structured["code"], "invalid_argument");
    }

    #[tokio::test]
    async fn test_nonexistent_project_id() {
        let ctx = test_context().await;
        let proj_id = ProjectId::new();

        let mock = MockProjectRepository::builder()
            .on_find_by_id_exhaust(ExhaustBehaviour::Error(a_not_found(
                "project",
                proj_id.to_string(),
            )))
            .build();
        let repos = repositories_with_project_mock(mock);
        let session = Arc::new(RwLock::new(SessionContext::new(None, "user:test".into())));

        let result = TribalServerHandler::apply_set_context(
            ctx.pool(),
            &repos,
            &session,
            serde_json::json!({ "project_id": proj_id.to_string() }),
        )
        .await
        .expect("should return Ok with error result, not Err");

        assert_eq!(result.is_error, Some(true));

        let structured = result.structured_content.expect(STRUCTURED_CONTENT);
        assert_eq!(structured["code"], "not_found");
    }

    #[tokio::test]
    async fn test_malformed_json_params() {
        let ctx = test_context().await;
        let session = Arc::new(RwLock::new(SessionContext::new(None, "user:test".into())));

        let err = TribalServerHandler::apply_set_context(
            ctx.pool(),
            &test_repositories(),
            &session,
            serde_json::json!({ "project_id": 123 }),
        )
        .await
        .expect_err("should return Err(McpError) for malformed params");

        assert_eq!(err.code, ErrorCode::INVALID_PARAMS);
    }

    // -- Response shape ----------------------------------------------------

    #[tokio::test]
    async fn test_response_always_has_principal_key_and_actor() {
        let ctx = test_context().await;
        let session = Arc::new(RwLock::new(SessionContext::new(None, "user:test".into())));

        let result = TribalServerHandler::apply_set_context(
            ctx.pool(),
            &test_repositories(),
            &session,
            serde_json::json!({}),
        )
        .await
        .expect(NO_PROTOCOL_ERROR);

        let structured = result.structured_content.expect(STRUCTURED_CONTENT);
        assert_eq!(structured["principal_key"], "user:test");
        assert!(structured["actor"].is_object());
        assert!(structured["actor"].get("client_name").is_some());
        assert!(structured["actor"].get("client_version").is_some());
        assert!(structured["actor"].get("model").is_some());
        assert!(structured["actor"].get("provider").is_some());
    }

    #[tokio::test]
    async fn test_response_project_null_when_unset() {
        let ctx = test_context().await;
        let session = Arc::new(RwLock::new(SessionContext::new(None, "user:test".into())));

        let result = TribalServerHandler::apply_set_context(
            ctx.pool(),
            &test_repositories(),
            &session,
            serde_json::json!({}),
        )
        .await
        .expect(NO_PROTOCOL_ERROR);

        let structured = result.structured_content.expect(STRUCTURED_CONTENT);
        assert!(
            structured.get("project").is_some(),
            "project key must be present"
        );
        assert!(
            structured["project"].is_null(),
            "project value must be null"
        );
    }
}
