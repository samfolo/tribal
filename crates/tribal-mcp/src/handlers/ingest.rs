//! Handler for `tribal_ingest` — job and extraction task creation.

use std::{str::FromStr, sync::Arc};

use rmcp::{
    model::{CallToolResult, ErrorData as McpError},
    service::{RequestContext, RoleServer},
};
use sqlx::PgConnection;
use tokio::sync::watch;
use tracing::Instrument;
use tribal_common::JobWatchEntry;
use tribal_db::{DbError, IngestInsertOutcome, NewJob, NewTask};
use tribal_domain::{
    JobId, JobState, McpErrorCode, PrincipalId, ProjectId, SourceContextV1, TaskType, span_attrs,
};

use super::common::begin_transaction;
use crate::{
    error::{IntoCallToolResult, IntoMcpError, McpToolError, invalid_argument},
    fingerprint::{FingerprintError, FingerprintInputs, compute_and_upsert_fingerprint},
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
    idempotency_key: Option<uuid::Uuid>,
    active_prompts: ActivePromptVersions,
    build_version: Arc<str>,
    fingerprint_inputs: FingerprintInputs,
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

    #[error(transparent)]
    Fingerprint(#[from] FingerprintError),

    /// The idempotency key names a job whose project or content differ.
    #[error("idempotency key reused with a different project or content")]
    IdempotencyConflict,
}

impl IntoMcpError for IngestError {
    fn into_mcp_error(self) -> McpToolError {
        match self {
            Self::Db(e) => e.into_mcp_error(),
            Self::Fingerprint(e) => e.into_mcp_error(),
            Self::IdempotencyConflict => McpToolError {
                code: McpErrorCode::Conflict,
                message: "idempotency key reused with a different project or content".to_owned(),
                details: serde_json::json!({}),
            },
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
            { span_attrs::TRANSPORT } = self.transport_name(),
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

        let (session_project_id, claimed_actor) = {
            let guard = self.session.read().await;
            (guard.project.as_ref().map(|p| p.id), guard.actor.claimed())
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

        let context = SourceContextV1::from_claims(self.channel, claimed_actor);
        let source_context = match serde_json::to_value(&context) {
            Ok(value) => value,
            Err(e) => {
                return Ok(McpToolError {
                    code: McpErrorCode::Internal,
                    message: format!("serialising source context: {e}"),
                    details: serde_json::json!({}),
                }
                .into_call_tool_result());
            }
        };

        let active_prompts = self.state.active_prompt_versions.read().await.clone();

        let fingerprint_inputs = FingerprintInputs {
            specs: self.state.stage_specs.clone(),
            embedding: self.state.embedding_identity.clone(),
            embedding_dimensions: self.state.embedding_dimensions,
            pipeline: self.state.pipeline_parameters.clone(),
            agents: self.state.agents_config.clone(),
        };

        let ingest_params = IngestParams {
            project_id,
            principal_id,
            source_context,
            content: request.content,
            idempotency_key: request.idempotency_key,
            active_prompts,
            build_version: Arc::clone(&self.state.build_version),
            fingerprint_inputs,
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

    // -- System fingerprint ---------------------------------------------------

    let fingerprint_hash = compute_and_upsert_fingerprint(
        conn,
        repositories,
        &params.active_prompts,
        &params.build_version,
        &params.fingerprint_inputs,
    )
    .await?;

    // -- Job and task ---------------------------------------------------------

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
        .system_fingerprint_hash(fingerprint_hash)
        .trace_context(trace_context)
        .ingest_idempotency_key(params.idempotency_key)
        .build();

    // The extraction task exists only for a job this call created: a
    // recovered job already has its task, and a conflict creates nothing.
    let job = match repositories
        .job
        .insert_or_resolve_idempotency(conn, &new_job)
        .await?
    {
        IngestInsertOutcome::Inserted(job) => {
            let new_task = NewTask::builder()
                .job_id(job.id())
                .task_type(TaskType::Extraction)
                .build();
            repositories.task.insert(conn, &new_task).await?;
            job
        }
        IngestInsertOutcome::Existing(job) => job,
        IngestInsertOutcome::Conflict => return Err(IngestError::IdempotencyConflict),
    };

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
    use tokio::sync::RwLock;
    use tracing::Instrument;
    use tracing_subscriber::layer::SubscriberExt;
    use tribal_db::{
        PgPrincipalRepository, PgProjectRepository, PgTaskRepository, PrincipalRepository,
        ProjectRepository, TaskRepository,
    };
    use tribal_domain::{GitRemote, IngestChannel, KnowledgeItemId, PrincipalId, ProjectId};
    use tribal_test_utils::{
        MockIngestJobRepository, MockProjectRepository, MockPromptVersionRepository,
        MockTaskRepository, TestDb, a_job, a_new_principal, a_new_project, a_new_prompt_version,
        a_project, a_prompt_version, a_task, insert_prompt_version,
    };

    use super::*;
    use crate::{
        error::ERROR_META_KEY,
        session::SessionActor,
        test_utils::{
            NO_STRUCTURED_CONTENT, TestHandler, configure_fingerprint_mocks, first_text_content,
            session_with_project, test_active_prompt_versions, test_fingerprint_inputs,
            test_repositories,
        },
    };

    // -- Constants ---------------------------------------------------------

    const NO_PROTOCOL_ERROR: &str = "should not return a protocol error";
    const STRUCTURED_CONTENT: &str = "structured_content must be present";

    // -- Helpers -----------------------------------------------------------

    async fn call_execute(
        repos: &ConnectionRepositories,
        params: IngestParams,
    ) -> Result<IngestResult, IngestError> {
        let ctx = TestDb::new().await;
        let mut tx = ctx.begin().await.expect("begin");
        execute_ingest(&mut tx, repos, params).await
    }

    fn default_params() -> IngestParams {
        IngestParams {
            project_id: ProjectId::new(),
            principal_id: PrincipalId::new(),
            source_context: serde_json::json!({"type": "ManualCapture", "capture_method": "mcp"}),
            content: "some knowledge".into(),
            idempotency_key: None,
            active_prompts: test_active_prompt_versions(),
            build_version: Arc::from("test-build"),
            fingerprint_inputs: test_fingerprint_inputs(),
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
        assert!(
            result.structured_content.is_none(),
            "{NO_STRUCTURED_CONTENT}"
        );
        assert!(first_text_content(&result).contains(NO_PROJECT));
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
        assert!(
            result.structured_content.is_none(),
            "{NO_STRUCTURED_CONTENT}"
        );
        assert!(first_text_content(&result).contains("expected prefix"));
    }

    /// With a session project set and no `project_id` in the request, the
    /// handler should resolve the session project and continue past the
    /// precondition check. `lazy_pool` cannot open connections, so the
    /// call fails at the pool phase — we assert the error message is NOT the
    /// missing-project precondition message to confirm project resolution
    /// succeeded and the failure originates downstream at the pool.
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
        assert!(
            result.structured_content.is_none(),
            "{NO_STRUCTURED_CONTENT}"
        );

        let message = first_text_content(&result);
        assert!(
            !message.contains(NO_PROJECT),
            "session project should be used when request omits project_id: {message}",
        );
        // The failure must come from the pool layer downstream of resolution.
        assert!(
            message.contains("pool") || message.contains("query failed"),
            "error should originate from the pool, not the precondition check: {message}",
        );
    }

    // -- Adapter: idempotency arbitration through a real database ----------

    /// Inserts a principal, project, and prompt version via the real
    /// repositories, returning IDs a real ingest call can bind against.
    /// The same prompt version id is reused for all six active-prompt
    /// slots, as the arbitration suite in `tribal-db` does.
    async fn setup_real_ingest_prerequisites(
        conn: &mut PgConnection,
        suffix: &str,
    ) -> (PrincipalId, ProjectId, ActivePromptVersions) {
        let principal = PgPrincipalRepository
            .insert(
                conn,
                &a_new_principal()
                    .principal_key(format!("user:ingest-real-{suffix}"))
                    .build(),
            )
            .await
            .expect("insert principal");
        let project = PgProjectRepository
            .insert(
                conn,
                &a_new_project()
                    .git_remote(GitRemote::from_parts(
                        "github.com",
                        &format!("test/ingest-real-{suffix}"),
                        None,
                    ))
                    .build(),
            )
            .await
            .expect("insert project");
        let pv_id = insert_prompt_version(conn, &a_new_prompt_version().build()).await;
        (
            principal.id(),
            project.id(),
            ActivePromptVersions::new(pv_id, pv_id, pv_id, pv_id, pv_id, pv_id),
        )
    }

    /// Builds a handler wired to a real, migrated database through the
    /// production repositories, with the active prompt versions the caller
    /// resolved against that same database.
    fn real_ingest_handler(
        pool: sqlx::PgPool,
        active_prompts: ActivePromptVersions,
    ) -> TribalServerHandler {
        TestHandler::builder()
            .pool(pool)
            .repositories(ConnectionRepositories::new())
            .active_prompt_versions(Arc::new(RwLock::new(active_prompts)))
            .build()
    }

    #[tokio::test]
    async fn test_a_retried_ingest_call_converges_on_one_job_and_one_extraction_task() {
        let ctx = TestDb::new().await;
        let mut conn = ctx.raw_connection().await.expect("conn");
        let (principal_id, project_id, active_prompts) =
            setup_real_ingest_prerequisites(&mut conn, "retry").await;
        drop(conn);

        let handler = real_ingest_handler(ctx.pool().clone(), active_prompts);
        let key = uuid::Uuid::new_v4();
        let params = serde_json::json!({
            "project_id": project_id.to_string(),
            "content": "shared knowledge",
            "idempotency_key": key,
        });

        let first = handler
            .apply_ingest(params.clone(), principal_id)
            .await
            .expect(NO_PROTOCOL_ERROR);
        let retry = handler
            .apply_ingest(params, principal_id)
            .await
            .expect(NO_PROTOCOL_ERROR);

        assert_eq!(first.is_error, Some(false));
        assert_eq!(retry.is_error, Some(false));
        let first_structured = first.structured_content.expect(STRUCTURED_CONTENT);
        let retry_structured = retry.structured_content.expect(STRUCTURED_CONTENT);
        assert_eq!(
            first_structured["job_id"], retry_structured["job_id"],
            "a retried key must resolve to the same job",
        );

        let job_id: JobId = first_structured["job_id"]
            .as_str()
            .expect("job_id is a string")
            .parse()
            .expect("job id parses");
        let mut conn = ctx.raw_connection().await.expect("conn");
        let tasks = PgTaskRepository
            .find_by_job_id(&mut conn, job_id)
            .await
            .expect("find tasks");
        assert_eq!(
            tasks.len(),
            1,
            "a retry must not create a second extraction task",
        );
        assert_eq!(tasks[0].task_type(), TaskType::Extraction);
    }

    #[tokio::test]
    async fn test_a_reused_key_with_different_content_is_refused_as_conflict_without_echoing_it() {
        let ctx = TestDb::new().await;
        let mut conn = ctx.raw_connection().await.expect("conn");
        let (principal_id, project_id, active_prompts) =
            setup_real_ingest_prerequisites(&mut conn, "conflict").await;
        drop(conn);

        let handler = real_ingest_handler(ctx.pool().clone(), active_prompts);
        let key = uuid::Uuid::new_v4();
        let original_content = "the original secret note";

        let first = handler
            .apply_ingest(
                serde_json::json!({
                    "project_id": project_id.to_string(),
                    "content": original_content,
                    "idempotency_key": key,
                }),
                principal_id,
            )
            .await
            .expect(NO_PROTOCOL_ERROR);
        assert_eq!(first.is_error, Some(false));

        let conflicting = handler
            .apply_ingest(
                serde_json::json!({
                    "project_id": project_id.to_string(),
                    "content": "a completely different note",
                    "idempotency_key": key,
                }),
                principal_id,
            )
            .await
            .expect(NO_PROTOCOL_ERROR);

        assert_eq!(conflicting.is_error, Some(true));
        let message = first_text_content(&conflicting).to_owned();
        assert!(
            !message.contains(original_content),
            "a conflict must not echo the original content: {message}",
        );

        let meta = conflicting.meta.expect("error results carry meta");
        let typed = meta.get(ERROR_META_KEY).expect("the typed error member");
        assert_eq!(typed["code"], "conflict");
    }

    #[tokio::test]
    async fn test_a_keyless_ingest_call_twice_creates_two_jobs() {
        let ctx = TestDb::new().await;
        let mut conn = ctx.raw_connection().await.expect("conn");
        let (principal_id, project_id, active_prompts) =
            setup_real_ingest_prerequisites(&mut conn, "keyless").await;
        drop(conn);

        let handler = real_ingest_handler(ctx.pool().clone(), active_prompts);
        let params = serde_json::json!({
            "project_id": project_id.to_string(),
            "content": "a keyless note",
        });

        let first = handler
            .apply_ingest(params.clone(), principal_id)
            .await
            .expect(NO_PROTOCOL_ERROR);
        let second = handler
            .apply_ingest(params, principal_id)
            .await
            .expect(NO_PROTOCOL_ERROR);

        assert_eq!(first.is_error, Some(false));
        assert_eq!(second.is_error, Some(false));
        let first_structured = first.structured_content.expect(STRUCTURED_CONTENT);
        let second_structured = second.structured_content.expect(STRUCTURED_CONTENT);
        assert_ne!(
            first_structured["job_id"], second_structured["job_id"],
            "a keyless call inserts a job on every call",
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
        let active_prompts = test_active_prompt_versions();

        let mut repos = test_repositories();
        repos.project = Arc::new(
            MockProjectRepository::builder()
                .on_find_by_id(project, None)
                .build(),
        );
        repos.job = Arc::new(
            MockIngestJobRepository::builder()
                .when_insert_or_resolve_idempotency(move |new_job| new_job.principal_id == prin_id)
                .respond_with(IngestInsertOutcome::Inserted(job.clone()), None)
                .build(),
        );
        repos.task = Arc::new(MockTaskRepository::builder().on_insert(task, None).build());
        configure_fingerprint_mocks(&mut repos, &active_prompts);

        let params = IngestParams {
            project_id: proj_id,
            principal_id: prin_id,
            source_context: serde_json::json!({"type": "ManualCapture", "capture_method": "mcp"}),
            content: "learned something".into(),
            idempotency_key: None,
            active_prompts,
            build_version: Arc::from("test-build"),
            fingerprint_inputs: test_fingerprint_inputs(),
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

        let job_mock = MockIngestJobRepository::builder()
            .when_insert_or_resolve_idempotency(move |new_job| {
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
            .respond_with(IngestInsertOutcome::Inserted(job.clone()), None)
            .build();

        let mut repos = test_repositories();
        repos.project = Arc::new(
            MockProjectRepository::builder()
                .on_find_by_id(project, None)
                .build(),
        );
        repos.job = Arc::new(job_mock);
        repos.task = Arc::new(MockTaskRepository::builder().on_insert(task, None).build());
        configure_fingerprint_mocks(&mut repos, &prompts);

        let params = IngestParams {
            project_id: proj_id,
            principal_id: prin_id,
            source_context: serde_json::json!({}),
            content: "test content".into(),
            idempotency_key: None,
            active_prompts: prompts,
            build_version: Arc::from("test-build"),
            fingerprint_inputs: test_fingerprint_inputs(),
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
        let active_prompts = test_active_prompt_versions();

        let source_ctx = serde_json::json!({
            "type": "AgentMediated",
            "provider": "anthropic",
            "model": "claude-opus-4-6"
        });
        let expected_ctx = source_ctx.clone();

        let job_mock = MockIngestJobRepository::builder()
            .when_insert_or_resolve_idempotency(move |new_job| new_job.source_context == expected_ctx)
            .respond_with(IngestInsertOutcome::Inserted(job.clone()), None)
            .build();

        let mut repos = test_repositories();
        repos.project = Arc::new(
            MockProjectRepository::builder()
                .on_find_by_id(project, None)
                .build(),
        );
        repos.job = Arc::new(job_mock);
        repos.task = Arc::new(MockTaskRepository::builder().on_insert(task, None).build());
        configure_fingerprint_mocks(&mut repos, &active_prompts);

        let params = IngestParams {
            project_id: proj_id,
            principal_id: prin_id,
            source_context: source_ctx,
            content: "test content".into(),
            idempotency_key: None,
            active_prompts,
            build_version: Arc::from("test-build"),
            fingerprint_inputs: test_fingerprint_inputs(),
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

    #[tokio::test]
    async fn test_execute_ingest_missing_prompt_versions_returns_error() {
        let proj_id = ProjectId::new();
        let project = a_project().id(proj_id).build();

        let mut repos = test_repositories();
        repos.project = Arc::new(
            MockProjectRepository::builder()
                .on_find_by_id(project, None)
                .build(),
        );
        repos.prompt_version = Arc::new(
            MockPromptVersionRepository::builder()
                .on_find_by_ids(vec![], None)
                .build(),
        );

        let params = IngestParams {
            project_id: proj_id,
            ..default_params()
        };
        let err = call_execute(&repos, params).await.expect_err("should fail");

        assert!(matches!(
            err,
            IngestError::Fingerprint(FingerprintError::MissingPromptVersions { .. })
        ));
    }

    /// When `find_by_ids` returns fewer than 6 prompt versions (some
    /// present, some missing), the fingerprint computation still fails.
    #[tokio::test]
    async fn test_execute_ingest_partial_prompt_versions_returns_error() {
        let proj_id = ProjectId::new();
        let project = a_project().id(proj_id).build();

        let prompts = test_active_prompt_versions();
        let ids = prompts.version_ids();

        // Return versions for only the first 3 of 6 required IDs.
        let partial: Vec<_> = ids[..3]
            .iter()
            .map(|&id| a_prompt_version().id(id).build())
            .collect();

        let mut repos = test_repositories();
        repos.project = Arc::new(
            MockProjectRepository::builder()
                .on_find_by_id(project, None)
                .build(),
        );
        repos.prompt_version = Arc::new(
            MockPromptVersionRepository::builder()
                .on_find_by_ids(partial, None)
                .build(),
        );

        let params = IngestParams {
            project_id: proj_id,
            active_prompts: prompts,
            ..default_params()
        };
        let err = call_execute(&repos, params).await.expect_err("should fail");

        assert!(matches!(
            err,
            IngestError::Fingerprint(FingerprintError::MissingPromptVersions { .. })
        ));
    }

    // -- Helper: source context --------------------------------------------

    #[test]
    fn test_a_provider_claim_writes_an_agent_mediated_v1_context() {
        let actor = SessionActor {
            client_name: Some("tribal-mac".into()),
            client_version: Some("1.2.0".into()),
            provider: Some("anthropic".into()),
            model: Some("claude-opus-4-6".into()),
        };

        let context = SourceContextV1::from_claims(IngestChannel::McpHttp, actor.claimed());
        let value = serde_json::to_value(&context).expect("context serialises");

        assert_eq!(value["version"], 1);
        assert_eq!(value["type"], "agent_mediated");
        assert_eq!(value["channel"], "mcp_http");
        assert_eq!(value["claimed_actor"]["client"]["name"], "tribal-mac");
        assert_eq!(value["claimed_actor"]["inference"]["provider"], "anthropic");
        assert_eq!(value.get("extraction"), None);
    }

    #[test]
    fn test_a_bare_session_writes_a_manual_capture_v1_context() {
        let actor = SessionActor {
            client_name: None,
            client_version: None,
            provider: None,
            model: None,
        };

        let context = SourceContextV1::from_claims(IngestChannel::McpStdio, actor.claimed());
        let value = serde_json::to_value(&context).expect("context serialises");

        assert_eq!(value["version"], 1);
        assert_eq!(value["type"], "manual_capture");
        assert_eq!(value["channel"], "mcp_stdio");
        assert_eq!(value.get("claimed_actor"), None);
    }

    // -- Trace context --------------------------------------------------------

    /// Verifies that `execute_ingest` populates `trace_context` on the
    /// `NewJob` when a valid OpenTelemetry context is active.
    #[tokio::test(flavor = "current_thread")]
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
        let active_prompts = test_active_prompt_versions();

        let job_mock = MockIngestJobRepository::builder()
            .when_insert_or_resolve_idempotency(move |new_job| {
                if let Some(tc) = &new_job.trace_context {
                    let valid = tribal_telemetry::trace_id_from_traceparent(tc).is_some();
                    saw_clone.store(valid, Ordering::SeqCst);
                }
                true
            })
            .respond_with(IngestInsertOutcome::Inserted(job.clone()), None)
            .build();

        let mut repos = test_repositories();
        repos.project = Arc::new(
            MockProjectRepository::builder()
                .on_find_by_id(project, None)
                .build(),
        );
        repos.job = Arc::new(job_mock);
        repos.task = Arc::new(MockTaskRepository::builder().on_insert(task, None).build());
        configure_fingerprint_mocks(&mut repos, &active_prompts);

        let params = IngestParams {
            project_id: proj_id,
            principal_id: prin_id,
            source_context: serde_json::json!({}),
            content: "trace test".into(),
            idempotency_key: None,
            active_prompts,
            build_version: Arc::from("test-build"),
            fingerprint_inputs: test_fingerprint_inputs(),
        };

        let _guard = tracing::subscriber::set_default(subscriber);
        let span = tracing::info_span!("test_trace_capture");

        async {
            let ctx = TestDb::new().await;
            let mut tx = ctx.begin().await.expect("begin");
            execute_ingest(&mut tx, &repos, params)
                .await
                .expect("execute_ingest should succeed in trace context test");
        }
        .instrument(span)
        .await;

        assert!(
            saw_trace_context.load(Ordering::SeqCst),
            "NewJob should have a valid W3C traceparent with an OTel subscriber",
        );
    }
}
