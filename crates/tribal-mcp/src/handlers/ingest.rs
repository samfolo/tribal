//! Handler for `tribal_ingest` — the write-path entry point for the
//! knowledge extraction pipeline.
//!
//! Accepts raw text, creates a job and its initial extraction task within
//! a single transaction, derives source context from session state, and
//! returns the prefixed job ID.

use std::str::FromStr;

use rmcp::{
    model::{CallToolResult, ErrorData as McpError},
    service::{RequestContext, RoleServer},
};
use sqlx::{PgConnection, PgPool};
use tokio::sync::RwLock;
use tribal_db::{DbError, NewJob, NewTask};
use tribal_domain::{JobId, McpErrorCode, ProjectId, TaskType};

use crate::{
    error::{IntoCallToolResult, IntoMcpError, McpToolError, invalid_argument},
    mapping::{McpIngestRequest, McpIngestResponse},
    server_handler::{
        ActivePromptVersions, ConnectionRepositories, POOL_NAME, TribalServerHandler,
    },
    session::SessionContext,
};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const NO_PROJECT: &str =
    "no project_id in request and no project set in session — call tribal_set_context first";

// ---------------------------------------------------------------------------
// Service types
// ---------------------------------------------------------------------------

/// Domain-level parameters for the ingest service function.
struct IngestParams {
    project_id: ProjectId,
    principal_key: String,
    source_context: serde_json::Value,
    content: String,
    active_prompts: ActivePromptVersions,
}

/// Domain-level result from the ingest service function.
struct IngestResult {
    job_id: JobId,
}

/// Errors that can occur during ingest execution.
#[derive(Debug, thiserror::Error)]
enum IngestError {
    #[error(transparent)]
    Db(#[from] DbError),

    #[error("principal not found for key: {principal_key}")]
    PrincipalNotFound { principal_key: String },
}

impl IntoMcpError for IngestError {
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
    pub(crate) async fn handle_ingest(
        &self,
        params: serde_json::Value,
        _context: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, McpError> {
        Self::apply_ingest(
            &self.pool,
            &self.repositories,
            &self.session,
            &self.active_prompt_versions,
            params,
        )
        .await
    }

    async fn apply_ingest(
        pool: &PgPool,
        repositories: &ConnectionRepositories,
        session: &RwLock<SessionContext>,
        active_prompt_versions: &ActivePromptVersions,
        params: serde_json::Value,
    ) -> Result<CallToolResult, McpError> {
        let request: McpIngestRequest =
            serde_json::from_value(params).map_err(|e| invalid_argument(e.to_string()))?;

        let (session_project_id, principal_key, actor_provider, actor_model) = {
            let guard = session.read().await;
            (
                guard.project.as_ref().map(|p| p.id),
                guard.principal_key.clone(),
                guard.actor.provider.clone(),
                guard.actor.model.clone(),
            )
        };

        let project_id = match request.project_id {
            Some(raw_id) => match ProjectId::from_str(&raw_id) {
                Ok(id) => id,
                Err(e) => return Ok(e.into_mcp_error().into_call_tool_result()),
            },
            None => match session_project_id {
                Some(id) => id,
                None => {
                    return Ok(McpToolError {
                        code: McpErrorCode::FailedPrecondition,
                        message: NO_PROJECT.into(),
                        details: serde_json::json!({}),
                    }
                    .into_call_tool_result());
                }
            },
        };

        let source_context =
            build_source_context(actor_provider.as_deref(), actor_model.as_deref());

        let ingest_params = IngestParams {
            project_id,
            principal_key,
            source_context,
            content: request.content,
            active_prompts: active_prompt_versions.clone(),
        };

        let mut tx = match pool.begin().await {
            Ok(tx) => tx,
            Err(sqlx::Error::PoolTimedOut) => {
                let db_err = DbError::PoolExhausted {
                    pool_name: POOL_NAME,
                };
                return Ok(db_err.into_mcp_error().into_call_tool_result());
            }
            Err(other) => {
                let db_err = DbError::QueryFailed {
                    context: "beginning transaction".into(),
                    source: other,
                };
                return Ok(db_err.into_mcp_error().into_call_tool_result());
            }
        };

        let result = match execute_ingest(&mut tx, repositories, ingest_params).await {
            Ok(r) => r,
            Err(e) => return Ok(e.into_mcp_error().into_call_tool_result()),
        };

        if let Err(e) = tx.commit().await {
            let db_err = DbError::QueryFailed {
                context: "committing ingest transaction".into(),
                source: e,
            };
            return Ok(db_err.into_mcp_error().into_call_tool_result());
        }

        Ok(McpIngestResponse::from(result.job_id).into_call_tool_result())
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Builds the source context JSONB value from session actor fields.
///
/// When `provider` is set, constructs an `AgentMediated` source context.
/// When absent, constructs a `ManualCapture` source context with
/// `capture_method: "mcp"`.
fn build_source_context(provider: Option<&str>, model: Option<&str>) -> serde_json::Value {
    if let Some(provider) = provider {
        serde_json::json!({
            "type": "AgentMediated",
            "provider": provider,
            "model": model.unwrap_or_default()
        })
    } else {
        serde_json::json!({
            "type": "ManualCapture",
            "capture_method": "mcp"
        })
    }
}

// ---------------------------------------------------------------------------
// Service function
// ---------------------------------------------------------------------------

/// Executes the ingest operation: verifies the project, resolves the
/// principal, creates a job and its initial extraction task.
///
/// All inputs and outputs are domain types — no MCP types cross this
/// boundary.
async fn execute_ingest(
    conn: &mut PgConnection,
    repositories: &ConnectionRepositories,
    params: IngestParams,
) -> Result<IngestResult, IngestError> {
    repositories
        .project
        .find_by_id(conn, params.project_id)
        .await?;

    let principal = repositories
        .principal
        .find_by_key(conn, &params.principal_key)
        .await?
        .ok_or_else(|| IngestError::PrincipalNotFound {
            principal_key: params.principal_key.clone(),
        })?;

    let new_job = NewJob::builder()
        .project_id(params.project_id)
        .principal_id(principal.id())
        .source_context(params.source_context)
        .raw_input(params.content)
        .extraction_system_prompt_version_id(
            params.active_prompts.extraction_system_prompt_version_id,
        )
        .extraction_user_prompt_version_id(
            params.active_prompts.extraction_user_prompt_version_id,
        )
        .triage_system_prompt_version_id(params.active_prompts.triage_system_prompt_version_id)
        .triage_user_prompt_version_id(params.active_prompts.triage_user_prompt_version_id)
        .relation_system_prompt_version_id(
            params.active_prompts.relation_system_prompt_version_id,
        )
        .relation_user_prompt_version_id(params.active_prompts.relation_user_prompt_version_id)
        .build();

    let job = repositories.job.insert(conn, &new_job).await?;

    let new_task = NewTask::builder()
        .job_id(job.id())
        .task_type(TaskType::Extraction)
        .build();

    repositories.task.insert(conn, &new_task).await?;

    Ok(IngestResult { job_id: job.id() })
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use rmcp::model::ErrorCode;
    use tokio::sync::RwLock;
    use tribal_domain::{KnowledgeItemId, PrincipalId, ProjectId, PromptVersionId};
    use tribal_test_utils::{
        MockJobRepository, MockPrincipalRepository, MockProjectRepository, MockTaskRepository,
        a_job, a_principal, a_project, a_task, lazy_pool, test_context,
    };

    use super::*;
    use crate::{session::SessionProject, test_utils::test_repositories};

    // -- Constants ----------------------------------------------------------

    const NO_PROTOCOL_ERROR: &str = "should not return a protocol error";
    const STRUCTURED_CONTENT: &str = "structured content must be present";

    // -- Helpers ------------------------------------------------------------

    fn test_active_prompt_versions() -> ActivePromptVersions {
        ActivePromptVersions {
            extraction_system_prompt_version_id: PromptVersionId::new(),
            extraction_user_prompt_version_id: PromptVersionId::new(),
            triage_system_prompt_version_id: PromptVersionId::new(),
            triage_user_prompt_version_id: PromptVersionId::new(),
            relation_system_prompt_version_id: PromptVersionId::new(),
            relation_user_prompt_version_id: PromptVersionId::new(),
        }
    }

    fn session_with_project() -> Arc<RwLock<SessionContext>> {
        let project = SessionProject {
            id: ProjectId::new(),
            name: "tribal".into(),
            git_remote: "git@github.com:user/tribal.git".into(),
        };
        Arc::new(RwLock::new(SessionContext::new(
            Some(project),
            "user:test".into(),
        )))
    }

    fn session_without_project() -> Arc<RwLock<SessionContext>> {
        Arc::new(RwLock::new(SessionContext::new(None, "user:test".into())))
    }

    async fn call_execute(
        repos: &ConnectionRepositories,
        params: IngestParams,
    ) -> Result<IngestResult, IngestError> {
        let ctx = test_context().await;
        let mut tx = ctx.begin_test().await.expect("begin");
        execute_ingest(&mut tx, repos, params).await
    }

    fn default_ingest_params() -> IngestParams {
        IngestParams {
            project_id: ProjectId::new(),
            principal_key: "user:test".into(),
            source_context: serde_json::json!({"type": "ManualCapture", "capture_method": "mcp"}),
            content: "some knowledge".into(),
            active_prompts: test_active_prompt_versions(),
        }
    }

    /// Prompt version IDs as a list, matching the order on [`NewJob`].
    fn prompt_version_ids(prompts: &ActivePromptVersions) -> Vec<PromptVersionId> {
        vec![
            prompts.extraction_system_prompt_version_id,
            prompts.extraction_user_prompt_version_id,
            prompts.triage_system_prompt_version_id,
            prompts.triage_user_prompt_version_id,
            prompts.relation_system_prompt_version_id,
            prompts.relation_user_prompt_version_id,
        ]
    }

    /// Extracts the six prompt version IDs from a [`NewJob`] in the same
    /// order as [`prompt_version_ids`].
    fn new_job_prompt_ids(new_job: &NewJob) -> Vec<PromptVersionId> {
        vec![
            new_job.extraction_system_prompt_version_id,
            new_job.extraction_user_prompt_version_id,
            new_job.triage_system_prompt_version_id,
            new_job.triage_user_prompt_version_id,
            new_job.relation_system_prompt_version_id,
            new_job.relation_user_prompt_version_id,
        ]
    }

    // -- apply_ingest: pre-transaction errors --------------------------------

    #[tokio::test]
    async fn test_apply_ingest_malformed_json_returns_protocol_error() {
        let pool = lazy_pool();
        let repos = test_repositories();
        let session = session_without_project();
        let prompts = test_active_prompt_versions();

        let err = TribalServerHandler::apply_ingest(
            &pool,
            &repos,
            &session,
            &prompts,
            serde_json::json!({"content": 123}),
        )
        .await
        .expect_err("should return Err(McpError) for malformed params");

        assert_eq!(err.code, ErrorCode::INVALID_PARAMS);
    }

    #[tokio::test]
    async fn test_apply_ingest_no_project_returns_failed_precondition() {
        let pool = lazy_pool();
        let repos = test_repositories();
        let session = session_without_project();
        let prompts = test_active_prompt_versions();

        let result = TribalServerHandler::apply_ingest(
            &pool,
            &repos,
            &session,
            &prompts,
            serde_json::json!({"content": "some knowledge"}),
        )
        .await
        .expect(NO_PROTOCOL_ERROR);

        assert_eq!(result.is_error, Some(true));
        let structured = result.structured_content.expect(STRUCTURED_CONTENT);
        assert_eq!(structured["code"], "failed_precondition");
    }

    #[tokio::test]
    async fn test_apply_ingest_invalid_project_prefix_returns_application_error() {
        let pool = lazy_pool();
        let repos = test_repositories();
        let session = session_without_project();
        let prompts = test_active_prompt_versions();

        let wrong_prefix_id = KnowledgeItemId::new().to_string();
        let result = TribalServerHandler::apply_ingest(
            &pool,
            &repos,
            &session,
            &prompts,
            serde_json::json!({"content": "some knowledge", "project_id": wrong_prefix_id}),
        )
        .await
        .expect(NO_PROTOCOL_ERROR);

        assert_eq!(result.is_error, Some(true));
        let structured = result.structured_content.expect(STRUCTURED_CONTENT);
        assert_eq!(structured["code"], "invalid_argument");
    }

    // -- execute_ingest: service function tests ------------------------------

    #[tokio::test]
    async fn test_execute_ingest_creates_job_and_task() {
        let proj_id = ProjectId::new();
        let prin_id = PrincipalId::new();
        let project = a_project().id(proj_id).build();
        let principal = a_principal().id(prin_id).build();
        let job = a_job().project_id(proj_id).principal_id(prin_id).build();
        let task = a_task().job_id(job.id()).build();

        let job_mock = Arc::new(
            MockJobRepository::builder()
                .on_insert(job.clone(), None)
                .build(),
        );
        let task_mock = Arc::new(
            MockTaskRepository::builder()
                .on_insert(task, None)
                .build(),
        );

        let mut repos = test_repositories();
        repos.project = Arc::new(
            MockProjectRepository::builder()
                .on_find_by_id(project, None)
                .build(),
        );
        repos.principal = Arc::new(
            MockPrincipalRepository::builder()
                .on_find_by_key(Some(principal), None)
                .build(),
        );
        repos.job = Arc::clone(&job_mock);
        repos.task = Arc::clone(&task_mock);

        let params = IngestParams {
            project_id: proj_id,
            principal_key: "user:test".into(),
            source_context: serde_json::json!({"type": "ManualCapture", "capture_method": "mcp"}),
            content: "learned something".into(),
            active_prompts: test_active_prompt_versions(),
        };

        let result = call_execute(&repos, params).await.expect("should succeed");
        assert_eq!(result.job_id, job.id());

        let job_history = job_mock.insert_history();
        assert_eq!(job_history.len(), 1);
        assert_eq!(job_history[0].raw_input, "learned something");

        let task_history = task_mock.insert_history();
        assert_eq!(task_history.len(), 1);
        assert_eq!(task_history[0].task_type, TaskType::Extraction);
        assert_eq!(task_history[0].job_id, job.id());
    }

    #[tokio::test]
    async fn test_execute_ingest_passes_prompt_versions() {
        let proj_id = ProjectId::new();
        let prin_id = PrincipalId::new();
        let project = a_project().id(proj_id).build();
        let principal = a_principal().id(prin_id).build();
        let job = a_job().project_id(proj_id).principal_id(prin_id).build();
        let task = a_task().job_id(job.id()).build();

        let job_mock = Arc::new(
            MockJobRepository::builder()
                .on_insert(job, None)
                .build(),
        );

        let mut repos = test_repositories();
        repos.project = Arc::new(
            MockProjectRepository::builder()
                .on_find_by_id(project, None)
                .build(),
        );
        repos.principal = Arc::new(
            MockPrincipalRepository::builder()
                .on_find_by_key(Some(principal), None)
                .build(),
        );
        repos.job = Arc::clone(&job_mock);
        repos.task = Arc::new(
            MockTaskRepository::builder()
                .on_insert(task, None)
                .build(),
        );

        let prompts = test_active_prompt_versions();
        let expected_ids = prompt_version_ids(&prompts);

        let params = IngestParams {
            project_id: proj_id,
            principal_key: "user:test".into(),
            source_context: serde_json::json!({}),
            content: "test content".into(),
            active_prompts: prompts,
        };

        let _ = call_execute(&repos, params).await.expect("should succeed");

        let job_history = job_mock.insert_history();
        let actual_ids = new_job_prompt_ids(&job_history[0]);

        for (actual, expected) in actual_ids.iter().zip(&expected_ids) {
            assert_eq!(actual, expected);
        }
    }

    #[tokio::test]
    async fn test_execute_ingest_source_context_passed_through() {
        let proj_id = ProjectId::new();
        let prin_id = PrincipalId::new();
        let project = a_project().id(proj_id).build();
        let principal = a_principal().id(prin_id).build();
        let job = a_job().project_id(proj_id).principal_id(prin_id).build();
        let task = a_task().job_id(job.id()).build();

        let job_mock = Arc::new(
            MockJobRepository::builder()
                .on_insert(job, None)
                .build(),
        );

        let mut repos = test_repositories();
        repos.project = Arc::new(
            MockProjectRepository::builder()
                .on_find_by_id(project, None)
                .build(),
        );
        repos.principal = Arc::new(
            MockPrincipalRepository::builder()
                .on_find_by_key(Some(principal), None)
                .build(),
        );
        repos.job = Arc::clone(&job_mock);
        repos.task = Arc::new(
            MockTaskRepository::builder()
                .on_insert(task, None)
                .build(),
        );

        let source_ctx = serde_json::json!({
            "type": "AgentMediated",
            "provider": "anthropic",
            "model": "claude-opus-4-6"
        });

        let params = IngestParams {
            project_id: proj_id,
            principal_key: "user:test".into(),
            source_context: source_ctx.clone(),
            content: "test content".into(),
            active_prompts: test_active_prompt_versions(),
        };

        let _ = call_execute(&repos, params).await.expect("should succeed");

        let job_history = job_mock.insert_history();
        assert_eq!(job_history[0].source_context, source_ctx);
    }

    #[tokio::test]
    async fn test_execute_ingest_project_not_found() {
        let project_mock = MockProjectRepository::builder()
            .on_find_by_id_error(
                || DbError::NotFound {
                    entity: "project".into(),
                    id: "missing".into(),
                },
                None,
            )
            .build();

        let mut repos = test_repositories();
        repos.project = Arc::new(project_mock);

        let params = default_ingest_params();
        let err = call_execute(&repos, params).await.expect_err("should fail");

        assert!(
            matches!(&err, IngestError::Db(DbError::NotFound { entity, .. }) if entity == "project")
        );
    }

    #[tokio::test]
    async fn test_execute_ingest_principal_not_found() {
        let proj_id = ProjectId::new();
        let project = a_project().id(proj_id).build();

        let mut repos = test_repositories();
        repos.project = Arc::new(
            MockProjectRepository::builder()
                .on_find_by_id(project, None)
                .build(),
        );
        repos.principal = Arc::new(
            MockPrincipalRepository::builder()
                .on_find_by_key(None, None)
                .build(),
        );

        let params = IngestParams {
            project_id: proj_id,
            principal_key: "user:unknown".into(),
            ..default_ingest_params()
        };

        let err = call_execute(&repos, params).await.expect_err("should fail");

        assert!(
            matches!(&err, IngestError::PrincipalNotFound { principal_key } if principal_key == "user:unknown")
        );
    }

    // -- build_source_context: pure helper tests ----------------------------

    #[test]
    fn test_build_source_context_agent_mediated() {
        let ctx = build_source_context(Some("anthropic"), Some("claude-opus-4-6"));

        assert_eq!(ctx["type"], "AgentMediated");
        assert_eq!(ctx["provider"], "anthropic");
        assert_eq!(ctx["model"], "claude-opus-4-6");
    }

    #[test]
    fn test_build_source_context_agent_mediated_no_model() {
        let ctx = build_source_context(Some("anthropic"), None);

        assert_eq!(ctx["type"], "AgentMediated");
        assert_eq!(ctx["provider"], "anthropic");
        assert_eq!(ctx["model"], "");
    }

    #[test]
    fn test_build_source_context_manual_capture() {
        let ctx = build_source_context(None, None);

        assert_eq!(ctx["type"], "ManualCapture");
        assert_eq!(ctx["capture_method"], "mcp");
    }
}
