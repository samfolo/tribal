//! Handler for `tribal_ingest` — job and extraction task creation.

use std::str::FromStr;

use rmcp::{
    model::{CallToolResult, ErrorData as McpError},
    service::{RequestContext, RoleServer},
};
use sqlx::PgConnection;
use tokio::sync::watch;
use tracing::Instrument;
use tribal_common::JobWatchEntry;
use tribal_db::{DbError, NewJob, NewTask};
use tribal_domain::{JobId, JobState, McpErrorCode, PrincipalId, ProjectId, TaskType, span_attrs};

use super::common::begin_transaction;
use crate::{
    error::{IntoCallToolResult, IntoMcpError, McpToolError, invalid_argument},
    mapping::{McpIngestRequest, McpIngestResponse},
    server_handler::{ActivePromptVersions, ConnectionRepositories, TribalServerHandler},
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
    principal_id: PrincipalId,
    source_context: serde_json::Value,
    content: String,
    active_prompts: ActivePromptVersions,
}

/// Domain-level result from the ingest service function.
#[derive(Debug)]
struct IngestResult {
    job_id: JobId,
}

/// Errors that can occur during ingest execution.
#[derive(Debug, thiserror::Error)]
enum IngestError {
    #[error(transparent)]
    Db(#[from] DbError),
}

impl IntoMcpError for IngestError {
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
    /// Handles the `tribal_ingest` tool call.
    pub(crate) async fn handle_ingest(
        &self,
        params: serde_json::Value,
        context: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, McpError> {
        let principal = self.resolve_principal(&context)?;
        let span = tracing::info_span!(
            parent: None,
            "tribal.ingest",
            { span_attrs::PRINCIPAL_KEY } = principal.principal_key(),
            { span_attrs::TRANSPORT } = self.transport_name.as_str(),
            { span_attrs::PROJECT_ID } = tracing::field::Empty,
        );
        self.apply_ingest(params, principal.principal_id())
            .instrument(span)
            .await
    }

    /// Core logic for `tribal_ingest`, separated from the outer handler
    /// so it can be tested without a `Peer<RoleServer>`.
    ///
    /// Parses the request, reads session state (project, actor fields),
    /// resolves a project ID, builds source context, then
    /// opens a transaction and delegates to [`execute_ingest`] for all
    /// domain logic. Domain errors are returned as error `CallToolResult`
    /// values via `IntoMcpError` / `IntoCallToolResult`. Only
    /// protocol-level errors (malformed JSON) return `Err(McpError)`.
    ///
    /// Submission ordering: the DB transaction is committed before the
    /// watch channel entry is inserted. If the process crashes between
    /// the two, subsequent `wait_seconds` requests find no entry and
    /// fall through to DB polling.
    async fn apply_ingest(
        &self,
        params: serde_json::Value,
        principal_id: PrincipalId,
    ) -> Result<CallToolResult, McpError> {
        let request: McpIngestRequest =
            serde_json::from_value(params).map_err(|e| invalid_argument(e.to_string()))?;

        let (session_project_id, actor_provider, actor_model) = {
            let guard = self.session.read().await;
            (
                guard.project.as_ref().map(|p| p.id),
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

        tracing::Span::current()
            .record(span_attrs::PROJECT_ID, tracing::field::display(&project_id));

        let source_context =
            build_source_context(actor_provider.as_deref(), actor_model.as_deref());

        let active_prompts = self.state.active_prompt_versions.read().await.clone();

        let ingest_params = IngestParams {
            project_id,
            principal_id,
            source_context,
            content: request.content,
            active_prompts,
        };

        let mut tx = match begin_transaction(&self.state.pool_mcp, self.config.pool_name).await {
            Ok(tx) => tx,
            Err(call_result) => return Ok(call_result),
        };

        let result = match execute_ingest(&mut tx, &self.repositories, ingest_params).await {
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

        let (watch_tx, keepalive_rx) = watch::channel(JobState::Queued);
        self.state
            .job_state_txs
            .insert(result.job_id, JobWatchEntry::new(watch_tx, keepalive_rx));

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

/// Executes the ingest operation: verifies the project, creates a job
/// and its initial extraction task.
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

    let trace_context = tribal_telemetry::current_trace_context();

    let new_job = NewJob::builder()
        .project_id(params.project_id)
        .principal_id(params.principal_id)
        .source_context(params.source_context)
        .raw_input(params.content)
        .extraction_system_prompt_version_id(
            params.active_prompts.extraction_system_prompt_version_id,
        )
        .extraction_user_prompt_version_id(params.active_prompts.extraction_user_prompt_version_id)
        .triage_system_prompt_version_id(params.active_prompts.triage_system_prompt_version_id)
        .triage_user_prompt_version_id(params.active_prompts.triage_user_prompt_version_id)
        .relation_system_prompt_version_id(params.active_prompts.relation_system_prompt_version_id)
        .relation_user_prompt_version_id(params.active_prompts.relation_user_prompt_version_id)
        .trace_context(trace_context)
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
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };

    use opentelemetry::trace::TracerProvider;
    use opentelemetry_sdk::trace::SdkTracerProvider;
    use rmcp::model::ErrorCode;
    use tracing::Instrument;
    use tracing_subscriber::layer::SubscriberExt;
    use tribal_domain::{KnowledgeItemId, PrincipalId, ProjectId, PromptVersionId};
    use tribal_test_utils::{
        MockJobRepository, MockProjectRepository, MockTaskRepository, a_job, a_project, a_task,
        test_context,
    };

    use super::*;
    use crate::test_utils::{TestHandler, session_with_project, test_repositories};

    // -- Constants ---------------------------------------------------------

    const STRUCTURED_CONTENT: &str = "structured_content must be present";
    const NO_PROTOCOL_ERROR: &str = "should not return a protocol error";

    // -- Helpers -----------------------------------------------------------

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

    async fn call_execute(
        repos: &ConnectionRepositories,
        params: IngestParams,
    ) -> Result<IngestResult, IngestError> {
        let ctx = test_context().await;
        let mut tx = ctx.begin_test().await.expect("begin");
        execute_ingest(&mut tx, repos, params).await
    }

    fn default_params() -> IngestParams {
        IngestParams {
            project_id: ProjectId::new(),
            principal_id: PrincipalId::new(),
            source_context: serde_json::json!({"type": "ManualCapture", "capture_method": "mcp"}),
            content: "some knowledge".into(),
            active_prompts: test_active_prompt_versions(),
        }
    }

    // -- Adapter: validation -----------------------------------------------

    #[tokio::test]
    async fn test_apply_ingest_malformed_json_returns_protocol_error() {
        let handler = TestHandler::builder().build();

        let err = handler
            .apply_ingest(serde_json::json!({"content": 123}), PrincipalId::new())
            .await
            .expect_err("should return Err(McpError) for malformed params");

        assert_eq!(err.code, ErrorCode::INVALID_PARAMS);
    }

    #[tokio::test]
    async fn test_apply_ingest_no_project_returns_failed_precondition() {
        let handler = TestHandler::builder().build();

        let result = handler
            .apply_ingest(
                serde_json::json!({"content": "some knowledge"}),
                PrincipalId::new(),
            )
            .await
            .expect(NO_PROTOCOL_ERROR);

        assert_eq!(result.is_error, Some(true));
        let structured = result.structured_content.expect(STRUCTURED_CONTENT);
        assert_eq!(structured["code"], "failed_precondition");
    }

    #[tokio::test]
    async fn test_apply_ingest_invalid_project_prefix_returns_application_error() {
        let handler = TestHandler::builder().build();

        let wrong_prefix_id = KnowledgeItemId::new().to_string();
        let result = handler
            .apply_ingest(
                serde_json::json!({"content": "some knowledge", "project_id": wrong_prefix_id}),
                PrincipalId::new(),
            )
            .await
            .expect(NO_PROTOCOL_ERROR);

        assert_eq!(result.is_error, Some(true));
        let structured = result.structured_content.expect(STRUCTURED_CONTENT);
        assert_eq!(structured["code"], "invalid_argument");
    }

    /// With a session project set and no `project_id` in the request, the
    /// handler should resolve the session project and continue past the
    /// precondition check. `lazy_pool` cannot open connections, so the
    /// call fails at the pool phase — we assert the error is NOT
    /// `failed_precondition` to confirm project resolution succeeded.
    #[tokio::test]
    async fn test_apply_ingest_uses_session_project_when_request_omits_it() {
        let handler = TestHandler::builder()
            .session(session_with_project())
            .build();

        let result = handler
            .apply_ingest(
                serde_json::json!({"content": "some knowledge"}),
                PrincipalId::new(),
            )
            .await
            .expect(NO_PROTOCOL_ERROR);

        assert_eq!(result.is_error, Some(true));
        let structured = result.structured_content.expect(STRUCTURED_CONTENT);
        assert_ne!(
            structured["code"], "failed_precondition",
            "session project should be used when request omits project_id",
        );
    }

    // -- Service: happy path -----------------------------------------------

    #[tokio::test]
    async fn test_execute_ingest_creates_job_and_task() {
        let proj_id = ProjectId::new();
        let prin_id = PrincipalId::new();
        let project = a_project().id(proj_id).build();
        let job = a_job().project_id(proj_id).principal_id(prin_id).build();
        let task = a_task().job_id(job.id()).build();
        let expected_job_id = job.id();

        let mut repos = test_repositories();
        repos.project = Arc::new(
            MockProjectRepository::builder()
                .on_find_by_id(project, None)
                .build(),
        );
        repos.job = Arc::new(
            MockJobRepository::builder()
                .when_insert(move |new_job| new_job.principal_id == prin_id)
                .respond_with(job.clone(), None)
                .build(),
        );
        repos.task = Arc::new(MockTaskRepository::builder().on_insert(task, None).build());

        let params = IngestParams {
            project_id: proj_id,
            principal_id: prin_id,
            source_context: serde_json::json!({"type": "ManualCapture", "capture_method": "mcp"}),
            content: "learned something".into(),
            active_prompts: test_active_prompt_versions(),
        };

        let result = call_execute(&repos, params).await.expect("should succeed");
        assert_eq!(result.job_id, expected_job_id);
    }

    #[tokio::test]
    async fn test_execute_ingest_passes_prompt_versions() {
        let proj_id = ProjectId::new();
        let prin_id = PrincipalId::new();
        let project = a_project().id(proj_id).build();
        let job = a_job().project_id(proj_id).principal_id(prin_id).build();
        let task = a_task().job_id(job.id()).build();

        let prompts = test_active_prompt_versions();
        let expected_ids = [
            prompts.extraction_system_prompt_version_id,
            prompts.extraction_user_prompt_version_id,
            prompts.triage_system_prompt_version_id,
            prompts.triage_user_prompt_version_id,
            prompts.relation_system_prompt_version_id,
            prompts.relation_user_prompt_version_id,
        ];

        let job_mock = MockJobRepository::builder()
            .when_insert(move |new_job| {
                let actual_ids = [
                    new_job.extraction_system_prompt_version_id,
                    new_job.extraction_user_prompt_version_id,
                    new_job.triage_system_prompt_version_id,
                    new_job.triage_user_prompt_version_id,
                    new_job.relation_system_prompt_version_id,
                    new_job.relation_user_prompt_version_id,
                ];
                actual_ids.iter().zip(&expected_ids).all(|(a, e)| a == e)
            })
            .respond_with(job.clone(), None)
            .build();

        let mut repos = test_repositories();
        repos.project = Arc::new(
            MockProjectRepository::builder()
                .on_find_by_id(project, None)
                .build(),
        );
        repos.job = Arc::new(job_mock);
        repos.task = Arc::new(MockTaskRepository::builder().on_insert(task, None).build());

        let params = IngestParams {
            project_id: proj_id,
            principal_id: prin_id,
            source_context: serde_json::json!({}),
            content: "test content".into(),
            active_prompts: prompts,
        };

        let result = call_execute(&repos, params).await.expect("should succeed");
        assert_eq!(result.job_id, job.id());
    }

    #[tokio::test]
    async fn test_execute_ingest_source_context_passed_through() {
        let proj_id = ProjectId::new();
        let prin_id = PrincipalId::new();
        let project = a_project().id(proj_id).build();
        let job = a_job().project_id(proj_id).principal_id(prin_id).build();
        let task = a_task().job_id(job.id()).build();

        let source_ctx = serde_json::json!({
            "type": "AgentMediated",
            "provider": "anthropic",
            "model": "claude-opus-4-6"
        });
        let expected_ctx = source_ctx.clone();

        let job_mock = MockJobRepository::builder()
            .when_insert(move |new_job| new_job.source_context == expected_ctx)
            .respond_with(job.clone(), None)
            .build();

        let mut repos = test_repositories();
        repos.project = Arc::new(
            MockProjectRepository::builder()
                .on_find_by_id(project, None)
                .build(),
        );
        repos.job = Arc::new(job_mock);
        repos.task = Arc::new(MockTaskRepository::builder().on_insert(task, None).build());

        let params = IngestParams {
            project_id: proj_id,
            principal_id: prin_id,
            source_context: source_ctx,
            content: "test content".into(),
            active_prompts: test_active_prompt_versions(),
        };

        let result = call_execute(&repos, params).await.expect("should succeed");
        assert_eq!(result.job_id, job.id());
    }

    // -- Service: error paths ----------------------------------------------

    #[tokio::test]
    async fn test_execute_ingest_project_not_found() {
        let mut repos = test_repositories();
        repos.project = Arc::new(
            MockProjectRepository::builder()
                .on_find_by_id_error(
                    || DbError::NotFound {
                        entity: "project",
                        id: "missing".into(),
                    },
                    None,
                )
                .build(),
        );

        let params = default_params();
        let err = call_execute(&repos, params).await.expect_err("should fail");

        assert!(
            matches!(err, IngestError::Db(DbError::NotFound { entity, .. }) if entity == "project")
        );
    }

    // -- Helper: source context --------------------------------------------

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

    // -- Trace context --------------------------------------------------------

    /// Verifies that `execute_ingest` populates `trace_context` on the
    /// `NewJob` when a valid OpenTelemetry context is active.
    #[tokio::test]
    async fn test_execute_ingest_captures_trace_context() {
        let provider = SdkTracerProvider::builder().build();
        let otel_layer = tracing_opentelemetry::layer().with_tracer(provider.tracer("test"));
        let subscriber = tracing_subscriber::registry().with(otel_layer);

        let saw_trace_context = Arc::new(AtomicBool::new(false));
        let saw_clone = Arc::clone(&saw_trace_context);

        let proj_id = ProjectId::new();
        let prin_id = PrincipalId::new();
        let project = a_project().id(proj_id).build();
        let job = a_job().project_id(proj_id).principal_id(prin_id).build();
        let task = a_task().job_id(job.id()).build();

        let job_mock = MockJobRepository::builder()
            .when_insert(move |new_job| {
                if new_job.trace_context.is_some() {
                    saw_clone.store(true, Ordering::SeqCst);
                }
                true
            })
            .respond_with(job.clone(), None)
            .build();

        let mut repos = test_repositories();
        repos.project = Arc::new(
            MockProjectRepository::builder()
                .on_find_by_id(project, None)
                .build(),
        );
        repos.job = Arc::new(job_mock);
        repos.task = Arc::new(MockTaskRepository::builder().on_insert(task, None).build());

        let params = IngestParams {
            project_id: proj_id,
            principal_id: prin_id,
            source_context: serde_json::json!({}),
            content: "trace test".into(),
            active_prompts: test_active_prompt_versions(),
        };

        let _guard = tracing::subscriber::set_default(subscriber);
        let span = tracing::info_span!("test_trace_capture");

        async {
            let ctx = test_context().await;
            let mut tx = ctx.begin_test().await.expect("begin");
            let _ = execute_ingest(&mut tx, &repos, params).await;
        }
        .instrument(span)
        .await;

        assert!(
            saw_trace_context.load(Ordering::SeqCst),
            "NewJob should have a non-None trace_context with an OTel subscriber",
        );
    }
}
