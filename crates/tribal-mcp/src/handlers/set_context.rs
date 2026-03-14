use std::str::FromStr;

use rmcp::{
    model::{CallToolResult, ErrorData as McpError},
    service::{RequestContext, RoleServer},
};
use sqlx::PgConnection;
use tribal_db::DbError;
use tribal_domain::ProjectId;

use super::common::acquire_connection;
use crate::{
    error::{IntoCallToolResult, IntoMcpError, invalid_argument},
    mapping::{McpSetContextRequest, McpSetContextResponse},
    server_handler::{ConnectionRepositories, TribalServerHandler},
    session::{SessionProject, notify_session_updated},
};

impl TribalServerHandler {
    /// Handles the `tribal_set_context` tool call.
    ///
    /// Delegates to [`apply_set_context`] for the core logic, then sends a
    /// resource-updated notification only when the session was actually
    /// mutated (i.e. at least one field changed value).
    pub(crate) async fn handle_set_context(
        &self,
        params: serde_json::Value,
        context: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, McpError> {
        let (result, mutated) = self.apply_set_context(params).await?;

        if mutated {
            notify_session_updated(&self.session, &context.peer).await;
        }

        Ok(result)
    }

    /// Core logic for `tribal_set_context`, separated from the outer handler
    /// so it can be tested without a `Peer<RoleServer>`.
    ///
    /// Parses the request, validates and resolves a project ID (if supplied),
    /// then applies partial updates to the session. Returns `(result, mutated)`
    /// where `mutated` is `true` only when at least one session field was
    /// replaced with a different value.
    ///
    /// Domain errors (invalid ID, not-found, pool exhaustion) are returned as
    /// error `CallToolResult` values via `IntoMcpError` / `IntoCallToolResult`.
    /// Only protocol-level errors (malformed JSON) return `Err(McpError)`.
    async fn apply_set_context(
        &self,
        params: serde_json::Value,
    ) -> Result<(CallToolResult, bool), McpError> {
        let request: McpSetContextRequest =
            serde_json::from_value(params).map_err(|e| invalid_argument(e.to_string()))?;

        if request == McpSetContextRequest::default() {
            let response = McpSetContextResponse::from(&*self.session.read().await);
            return Ok((response.into_call_tool_result(), false));
        }

        let resolved_project = if let Some(ref raw_id) = request.project_id {
            let proj_id = match ProjectId::from_str(raw_id) {
                Ok(id) => id,
                Err(e) => {
                    return Ok((e.into_mcp_error().into_call_tool_result(), false));
                }
            };

            let mut conn = match acquire_connection(&self.pool).await {
                Ok(c) => c,
                Err(call_result) => return Ok((call_result, false)),
            };

            match resolve_project(&mut conn, &self.repositories, proj_id).await {
                Ok(p) => Some(p),
                Err(e) => return Ok((e.into_mcp_error().into_call_tool_result(), false)),
            }
        } else {
            None
        };

        let mut ctx = self.session.write().await;
        let mut changed = false;

        if let Some(project) = resolved_project {
            let same = ctx.project.as_ref() == Some(&project);
            ctx.project = Some(project);
            if !same {
                changed = true;
            }
        }
        if let Some(ref model) = request.model
            && ctx.actor.model.as_ref() != Some(model)
        {
            ctx.actor.model = Some(model.clone());
            changed = true;
        }
        if let Some(ref provider) = request.provider
            && ctx.actor.provider.as_ref() != Some(provider)
        {
            ctx.actor.provider = Some(provider.clone());
            changed = true;
        }

        let mut response = McpSetContextResponse::from(&*ctx);
        response.mutated = changed;
        Ok((response.into_call_tool_result(), changed))
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Resolves a `ProjectId` into a `SessionProject` by looking it up in the
/// repository.
async fn resolve_project(
    conn: &mut PgConnection,
    repositories: &ConnectionRepositories,
    proj_id: ProjectId,
) -> Result<SessionProject, DbError> {
    let project = repositories.project.find_by_id(conn, proj_id).await?;
    Ok(SessionProject {
        id: project.id(),
        name: project.name().to_owned(),
        git_remote: project.git_remote().to_owned(),
    })
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use rmcp::model::ErrorCode;
    use tokio::sync::RwLock;
    use tribal_db::DbError;
    use tribal_domain::{KnowledgeItemId, ProjectId, PromptVersionId};
    use tribal_test_utils::{
        ExhaustBehaviour, MockEmbeddingProvider, MockProjectRepository, a_not_found, a_project,
        lazy_pool, test_context,
    };

    use super::resolve_project;
    use crate::{
        config::HandlerConfig,
        server_handler::{ActivePromptVersions, ConnectionRepositories, TribalServerHandler},
        session::{SessionContext, SessionProject},
        test_utils::test_repositories,
    };

    // -- Constants ---------------------------------------------------------

    const STRUCTURED_CONTENT: &str = "structured_content must be present";
    const NO_PROTOCOL_ERROR: &str = "should not return a protocol error";

    // -- Helpers -----------------------------------------------------------

    fn test_prompt_versions() -> Arc<RwLock<ActivePromptVersions>> {
        Arc::new(RwLock::new(ActivePromptVersions {
            extraction_system_prompt_version_id: PromptVersionId::new(),
            extraction_user_prompt_version_id: PromptVersionId::new(),
            triage_system_prompt_version_id: PromptVersionId::new(),
            triage_user_prompt_version_id: PromptVersionId::new(),
            relation_system_prompt_version_id: PromptVersionId::new(),
            relation_user_prompt_version_id: PromptVersionId::new(),
        }))
    }

    fn test_handler() -> TribalServerHandler {
        test_handler_with_repos(test_repositories())
    }

    fn test_handler_with_repos(repos: ConnectionRepositories) -> TribalServerHandler {
        TribalServerHandler::new(
            lazy_pool(),
            repos,
            Arc::new(MockEmbeddingProvider::builder().build()),
            test_prompt_versions(),
            SessionContext::new(None, "user:test".into()),
            HandlerConfig::default(),
        )
    }

    fn test_handler_with_pool_and_repos(
        pool: sqlx::PgPool,
        repos: ConnectionRepositories,
    ) -> TribalServerHandler {
        TribalServerHandler::new(
            pool,
            repos,
            Arc::new(MockEmbeddingProvider::builder().build()),
            test_prompt_versions(),
            SessionContext::new(None, "user:test".into()),
            HandlerConfig::default(),
        )
    }

    fn repositories_with_project_mock(mock: MockProjectRepository) -> ConnectionRepositories {
        let mut repos = test_repositories();
        repos.project = Arc::new(mock);
        repos
    }

    async fn call_resolve_project(
        repos: &ConnectionRepositories,
        proj_id: ProjectId,
    ) -> Result<SessionProject, DbError> {
        let ctx = test_context().await;
        let mut tx = ctx.begin_test().await.expect("begin");
        resolve_project(&mut tx, repos, proj_id).await
    }

    // -- Service: project resolution --------------------------------------

    #[tokio::test]
    async fn test_resolve_project_returns_session_project() {
        let project = a_project().build();
        let mock = MockProjectRepository::builder()
            .on_find_by_id(project.clone(), None)
            .build();
        let repos = repositories_with_project_mock(mock);

        let result = call_resolve_project(&repos, project.id()).await.unwrap();

        assert_eq!(result.id, project.id());
        assert_eq!(result.name, project.name());
        assert_eq!(result.git_remote, project.git_remote());
    }

    #[tokio::test]
    async fn test_resolve_project_not_found() {
        let proj_id = ProjectId::new();
        let mock = MockProjectRepository::builder()
            .on_find_by_id_exhaust(ExhaustBehaviour::Error(a_not_found(
                "project",
                proj_id.to_string(),
            )))
            .build();
        let repos = repositories_with_project_mock(mock);

        let err = call_resolve_project(&repos, proj_id).await.unwrap_err();

        assert!(matches!(err, DbError::NotFound { .. }));
    }

    // -- Happy path -------------------------------------------------------

    #[tokio::test]
    async fn test_empty_request_returns_unchanged_session() {
        let handler = test_handler();

        let (result, mutated) = handler
            .apply_set_context(serde_json::json!({}))
            .await
            .expect(NO_PROTOCOL_ERROR);

        assert!(!mutated);
        assert_eq!(result.is_error, Some(false));

        let structured = result.structured_content.expect(STRUCTURED_CONTENT);
        assert_eq!(structured["principal_key"], "user:test");
        assert!(structured["project"].is_null());
        assert!(structured["actor"]["model"].is_null());
        assert!(structured["actor"]["provider"].is_null());
    }

    #[tokio::test]
    async fn test_set_model_updates_actor() {
        let handler = test_handler();

        let (result, mutated) = handler
            .apply_set_context(serde_json::json!({ "model": "claude-opus-4-6" }))
            .await
            .expect(NO_PROTOCOL_ERROR);

        assert!(mutated);
        assert_eq!(result.is_error, Some(false));

        let structured = result.structured_content.expect(STRUCTURED_CONTENT);
        assert_eq!(structured["actor"]["model"], "claude-opus-4-6");
        assert!(structured["actor"]["provider"].is_null());

        let guard = handler.session.read().await;
        assert_eq!(guard.actor.model.as_deref(), Some("claude-opus-4-6"));
    }

    #[tokio::test]
    async fn test_set_provider_updates_actor() {
        let handler = test_handler();

        let (result, mutated) = handler
            .apply_set_context(serde_json::json!({ "provider": "anthropic" }))
            .await
            .expect(NO_PROTOCOL_ERROR);

        assert!(mutated);
        assert_eq!(result.is_error, Some(false));

        let structured = result.structured_content.expect(STRUCTURED_CONTENT);
        assert_eq!(structured["actor"]["provider"], "anthropic");

        let guard = handler.session.read().await;
        assert_eq!(guard.actor.provider.as_deref(), Some("anthropic"));
    }

    #[tokio::test]
    async fn test_set_model_and_provider() {
        let handler = test_handler();

        let (result, mutated) = handler
            .apply_set_context(serde_json::json!({
                "model": "claude-opus-4-6",
                "provider": "anthropic",
            }))
            .await
            .expect(NO_PROTOCOL_ERROR);

        assert!(mutated);
        assert_eq!(result.is_error, Some(false));

        let structured = result.structured_content.expect(STRUCTURED_CONTENT);
        assert_eq!(structured["actor"]["model"], "claude-opus-4-6");
        assert_eq!(structured["actor"]["provider"], "anthropic");
    }

    /// Uses a dedicated pool (not `lazy_pool`) because the project_id
    /// path calls `pool.acquire()` internally. A per-test pool avoids
    /// contention with the shared pool under concurrent test execution.
    #[tokio::test]
    async fn test_set_project_id_updates_session_and_response() {
        let ctx = test_context().await;
        let pool = ctx.create_pool().await.expect("pool");
        let project = a_project().build();
        let mock = MockProjectRepository::builder()
            .on_find_by_id(project.clone(), None)
            .build();
        let repos = repositories_with_project_mock(mock);
        let handler = test_handler_with_pool_and_repos(pool, repos);

        let (result, mutated) = handler
            .apply_set_context(serde_json::json!({ "project_id": project.id().to_string() }))
            .await
            .expect(NO_PROTOCOL_ERROR);

        assert!(mutated);
        assert_eq!(result.is_error, Some(false));

        let structured = result.structured_content.expect(STRUCTURED_CONTENT);
        assert_eq!(structured["project"]["id"], project.id().to_string());
        assert_eq!(structured["project"]["name"], project.name());
        assert_eq!(structured["project"]["git_remote"], project.git_remote());

        let guard = handler.session.read().await;
        let session_project = guard.project.as_ref().expect("project must be set");
        assert_eq!(session_project.id, project.id());
        assert_eq!(session_project.name, project.name());
        assert_eq!(session_project.git_remote, project.git_remote());
    }

    #[tokio::test]
    async fn test_partial_updates_are_additive() {
        let handler = test_handler();

        handler
            .apply_set_context(serde_json::json!({ "model": "claude-opus-4-6" }))
            .await
            .expect(NO_PROTOCOL_ERROR);

        let (result, _) = handler
            .apply_set_context(serde_json::json!({ "provider": "anthropic" }))
            .await
            .expect(NO_PROTOCOL_ERROR);

        let structured = result.structured_content.expect(STRUCTURED_CONTENT);
        assert_eq!(structured["actor"]["model"], "claude-opus-4-6");
        assert_eq!(structured["actor"]["provider"], "anthropic");
    }

    #[tokio::test]
    async fn test_idempotent_same_values() {
        let handler = test_handler();
        let params = serde_json::json!({ "model": "claude-opus-4-6" });

        let (result1, first_mutated) = handler
            .apply_set_context(params.clone())
            .await
            .expect(NO_PROTOCOL_ERROR);

        let (result2, second_mutated) = handler
            .apply_set_context(params)
            .await
            .expect(NO_PROTOCOL_ERROR);

        assert!(first_mutated);
        assert!(!second_mutated);

        let s1 = result1.structured_content.expect(STRUCTURED_CONTENT);
        let s2 = result2.structured_content.expect(STRUCTURED_CONTENT);
        assert_eq!(s1, s2);
    }

    // -- Error paths -------------------------------------------------------

    #[tokio::test]
    async fn test_invalid_project_id_prefix() {
        let handler = test_handler();
        let wrong_type_id = KnowledgeItemId::new().to_string();

        let (result, mutated) = handler
            .apply_set_context(serde_json::json!({ "project_id": wrong_type_id }))
            .await
            .expect("should return Ok with error result, not Err");

        assert!(!mutated);
        assert_eq!(result.is_error, Some(true));

        let structured = result.structured_content.expect(STRUCTURED_CONTENT);
        assert_eq!(structured["code"], "invalid_argument");
    }

    #[tokio::test]
    async fn test_invalid_project_id_uuid() {
        let handler = test_handler();

        let (result, mutated) = handler
            .apply_set_context(serde_json::json!({ "project_id": "proj_not-a-uuid" }))
            .await
            .expect("should return Ok with error result, not Err");

        assert!(!mutated);
        assert_eq!(result.is_error, Some(true));

        let structured = result.structured_content.expect(STRUCTURED_CONTENT);
        assert_eq!(structured["code"], "invalid_argument");
    }

    #[tokio::test]
    async fn test_malformed_json_params() {
        let handler = test_handler();

        let err = handler
            .apply_set_context(serde_json::json!({ "project_id": 123 }))
            .await
            .expect_err("should return Err(McpError) for malformed params");

        assert_eq!(err.code, ErrorCode::INVALID_PARAMS);
    }

    // -- Response shape ----------------------------------------------------

    #[tokio::test]
    async fn test_response_always_has_principal_key_and_actor() {
        let handler = test_handler();

        let (result, _) = handler
            .apply_set_context(serde_json::json!({}))
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
        let handler = test_handler();

        let (result, _) = handler
            .apply_set_context(serde_json::json!({}))
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
