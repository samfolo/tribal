//! Handler for `tribal_job_status` — job progress and outcome lookup.

use std::{str::FromStr, time::Duration};

use rmcp::{
    model::{CallToolResult, ErrorData as McpError},
    service::{RequestContext, RoleServer},
};
use tribal_db::DbError;
use tribal_domain::{Job, JobId, McpErrorCode, TaskStatus, TaskType, TriageOutcome};

use super::common::acquire_connection;
use crate::{
    error::{IntoCallToolResult, IntoMcpError, McpToolError, invalid_argument},
    mapping::{McpJobStatusRequest, McpJobStatusResponse},
    polling::{PollScheduler, TickSource, TimedPollScheduler},
    server_handler::{ConnectionRepositories, TribalServerHandler},
};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const MAX_WAIT_SECONDS: u32 = 30;
const POLL_INTERVAL: Duration = Duration::from_secs(1);

// ---------------------------------------------------------------------------
// Service types
// ---------------------------------------------------------------------------

/// Domain-level parameters for the job status service function.
struct JobStatusParams {
    job_id: JobId,
}

/// Domain-level result from the job status service function.
#[derive(Debug)]
struct JobStatusResult {
    job: Job,
    tasks_completed: u32,
    tasks_failed: u32,
    items_created: u32,
    observations_created: u32,
}

/// Errors that can occur during job status execution.
#[derive(Debug, thiserror::Error)]
enum JobStatusError {
    #[error(transparent)]
    Db(#[from] DbError),
}

impl IntoMcpError for JobStatusError {
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
    /// Handles the `tribal_job_status` tool call.
    pub(crate) async fn handle_job_status(
        &self,
        params: serde_json::Value,
        _context: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, McpError> {
        let scheduler = TimedPollScheduler {
            interval: POLL_INTERVAL,
        };
        self.apply_job_status(params, &scheduler).await
    }

    /// Core logic for `tribal_job_status`, separated from the outer handler
    /// so it can be tested without a `Peer<RoleServer>`.
    ///
    /// Parses the request, validates the job ID prefix and `wait_seconds`
    /// range, then queries the database for the job and its aggregate
    /// counts. When `wait_seconds > 0` and the job is not yet terminal,
    /// polls the database using the provided [`PollScheduler`] until the
    /// job reaches a terminal state or the scheduler's deadline expires.
    async fn apply_job_status<S: PollScheduler>(
        &self,
        params: serde_json::Value,
        scheduler: &S,
    ) -> Result<CallToolResult, McpError> {
        let request: McpJobStatusRequest =
            serde_json::from_value(params).map_err(|e| invalid_argument(e.to_string()))?;

        let job_id = match JobId::from_str(&request.job_id) {
            Ok(id) => id,
            Err(e) => return Ok(e.into_mcp_error().into_call_tool_result()),
        };

        if let Some(wait) = request.wait_seconds.filter(|&w| w > MAX_WAIT_SECONDS) {
            return Ok(McpToolError {
                code: McpErrorCode::InvalidArgument,
                message: format!(
                    "wait_seconds must be between 0 and {MAX_WAIT_SECONDS}, got {wait}"
                ),
                details: serde_json::json!({ "wait_seconds": wait }),
            }
            .into_call_tool_result());
        }

        let status_params = JobStatusParams { job_id };

        let mut result = {
            let mut conn = match acquire_connection(&self.pool).await {
                Ok(c) => c,
                Err(call_result) => return Ok(call_result),
            };
            match execute_job_status(&mut conn, &self.repositories, &status_params).await {
                Ok(r) => r,
                Err(e) => return Ok(e.into_mcp_error().into_call_tool_result()),
            }
        };

        if let Some(wait) = request
            .wait_seconds
            .filter(|&w| w > 0)
            .filter(|_| !result.job.status().is_terminal())
        {
            let ticker = scheduler.create_ticker(Duration::from_secs(u64::from(wait)));

            loop {
                if !ticker.tick().await {
                    break;
                }

                result = {
                    let mut conn = match acquire_connection(&self.pool).await {
                        Ok(c) => c,
                        Err(call_result) => return Ok(call_result),
                    };
                    match execute_job_status(&mut conn, &self.repositories, &status_params).await {
                        Ok(r) => r,
                        Err(e) => return Ok(e.into_mcp_error().into_call_tool_result()),
                    }
                };

                if result.job.status().is_terminal() {
                    break;
                }
            }
        }

        let response = McpJobStatusResponse::from_domain(
            &result.job,
            result.tasks_completed,
            result.tasks_failed,
            result.items_created,
            result.observations_created,
        );

        Ok(response.into_call_tool_result())
    }
}

// ---------------------------------------------------------------------------
// Service function
// ---------------------------------------------------------------------------

/// Queries the job, its triage tasks, and triage results, then computes
/// aggregate counts.
///
/// All inputs and outputs are domain types — no MCP types cross this
/// boundary.
async fn execute_job_status(
    conn: &mut sqlx::PgConnection,
    repositories: &ConnectionRepositories,
    params: &JobStatusParams,
) -> Result<JobStatusResult, JobStatusError> {
    let job = repositories.job.find_by_id(conn, params.job_id).await?;

    let tasks = repositories
        .task
        .find_by_job_id(conn, params.job_id)
        .await?;

    let triage_results = repositories
        .triage_result
        .find_by_job_id(conn, params.job_id)
        .await?;

    // Only triage tasks contribute to task counts — extraction and relation
    // tasks are excluded.
    let (mut tasks_completed, mut tasks_failed) = (0u32, 0u32);
    for task in tasks.iter().filter(|t| t.task_type() == TaskType::Triage) {
        match task.status() {
            TaskStatus::Completed => tasks_completed += 1,
            TaskStatus::DeadLetter => tasks_failed += 1,
            _ => {}
        }
    }

    // TriageOutcome::Failed results contribute to neither items_created nor
    // observations_created — task-level failure is captured by tasks_failed.
    let (mut items_created, mut observations_created) = (0u32, 0u32);
    for result in &triage_results {
        match result.outcome() {
            TriageOutcome::Created { .. } => items_created += 1,
            TriageOutcome::Duplicate { .. } => observations_created += 1,
            TriageOutcome::Failed { .. } => {}
        }
    }

    Ok(JobStatusResult {
        job,
        tasks_completed,
        tasks_failed,
        items_created,
        observations_created,
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
    use tribal_domain::{JobId, JobOutcome, JobStatus, KnowledgeItemId, PromptVersionId, TaskType};
    use tribal_test_utils::{
        MockEmbeddingProvider, MockJobRepository, MockTaskRepository, MockTriageResultRepository,
        a_job, a_task, a_triage_result_created, a_triage_result_duplicate, a_triage_result_failed,
        lazy_pool, test_context,
    };

    use super::*;
    use crate::{
        config::HandlerConfig,
        polling::ImmediatePollScheduler,
        server_handler::{ActivePromptVersions, TribalServerHandler},
        session::SessionContext,
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

    async fn call_execute(
        repos: &ConnectionRepositories,
        params: &JobStatusParams,
    ) -> Result<JobStatusResult, JobStatusError> {
        let ctx = test_context().await;
        let mut tx = ctx.begin_test().await.expect("begin");
        execute_job_status(&mut tx, repos, params).await
    }

    fn repos_for_job_status(
        job: Job,
        tasks: Vec<tribal_domain::Task>,
        triage_results: Vec<tribal_domain::TriageResult>,
    ) -> ConnectionRepositories {
        let mut repos = test_repositories();
        repos.job = Arc::new(
            MockJobRepository::builder()
                .on_find_by_id(job, None)
                .build(),
        );
        repos.task = Arc::new(
            MockTaskRepository::builder()
                .on_find_by_job_id(tasks, None)
                .build(),
        );
        repos.triage_result = Arc::new(
            MockTriageResultRepository::builder()
                .on_find_by_job_id(triage_results, None)
                .build(),
        );
        repos
    }

    // -- Adapter: validation -----------------------------------------------

    #[tokio::test]
    async fn test_apply_job_status_malformed_json_returns_protocol_error() {
        let handler = test_handler_with_repos(test_repositories());

        let err = handler
            .apply_job_status(serde_json::json!({"job_id": 123}), &ImmediatePollScheduler)
            .await
            .expect_err("should return Err(McpError) for malformed params");

        assert_eq!(err.code, ErrorCode::INVALID_PARAMS);
    }

    #[tokio::test]
    async fn test_apply_job_status_invalid_job_prefix_returns_application_error() {
        let handler = test_handler_with_repos(test_repositories());

        let wrong_prefix_id = KnowledgeItemId::new().to_string();
        let result = handler
            .apply_job_status(
                serde_json::json!({"job_id": wrong_prefix_id}),
                &ImmediatePollScheduler,
            )
            .await
            .expect(NO_PROTOCOL_ERROR);

        assert_eq!(result.is_error, Some(true));
        let structured = result.structured_content.expect(STRUCTURED_CONTENT);
        assert_eq!(structured["code"], "invalid_argument");
    }

    #[tokio::test]
    async fn test_apply_job_status_wait_seconds_over_30_returns_application_error() {
        let handler = test_handler_with_repos(test_repositories());

        let result = handler
            .apply_job_status(
                serde_json::json!({"job_id": JobId::new().to_string(), "wait_seconds": 31}),
                &ImmediatePollScheduler,
            )
            .await
            .expect(NO_PROTOCOL_ERROR);

        assert_eq!(result.is_error, Some(true));
        let structured = result.structured_content.expect(STRUCTURED_CONTENT);
        assert_eq!(structured["code"], "invalid_argument");
    }

    /// With `wait_seconds=30` (the upper boundary), the handler should
    /// pass validation and proceed to the pool phase. `lazy_pool` cannot
    /// open connections, so the call fails at pool acquisition — we assert
    /// the error is NOT `invalid_argument` to confirm the boundary is
    /// accepted.
    #[tokio::test]
    async fn test_apply_job_status_wait_seconds_at_boundary_30_accepted() {
        let handler = test_handler_with_repos(test_repositories());

        let result = handler
            .apply_job_status(
                serde_json::json!({"job_id": JobId::new().to_string(), "wait_seconds": 30}),
                &ImmediatePollScheduler,
            )
            .await
            .expect(NO_PROTOCOL_ERROR);

        assert_eq!(result.is_error, Some(true));
        let structured = result.structured_content.expect(STRUCTURED_CONTENT);
        assert_ne!(
            structured["code"], "invalid_argument",
            "wait_seconds=30 should be accepted",
        );
    }

    /// With `wait_seconds=0`, the handler should pass validation and
    /// proceed to the pool phase without polling.
    #[tokio::test]
    async fn test_apply_job_status_wait_seconds_zero_accepted() {
        let handler = test_handler_with_repos(test_repositories());

        let result = handler
            .apply_job_status(
                serde_json::json!({"job_id": JobId::new().to_string(), "wait_seconds": 0}),
                &ImmediatePollScheduler,
            )
            .await
            .expect(NO_PROTOCOL_ERROR);

        assert_eq!(result.is_error, Some(true));
        let structured = result.structured_content.expect(STRUCTURED_CONTENT);
        assert_ne!(
            structured["code"], "invalid_argument",
            "wait_seconds=0 should be accepted",
        );
    }

    /// With `wait_seconds` absent from the request, the handler should
    /// pass validation and proceed to the pool phase without polling.
    #[tokio::test]
    async fn test_apply_job_status_wait_seconds_absent_accepted() {
        let handler = test_handler_with_repos(test_repositories());

        let result = handler
            .apply_job_status(
                serde_json::json!({"job_id": JobId::new().to_string()}),
                &ImmediatePollScheduler,
            )
            .await
            .expect(NO_PROTOCOL_ERROR);

        assert_eq!(result.is_error, Some(true));
        let structured = result.structured_content.expect(STRUCTURED_CONTENT);
        assert_ne!(
            structured["code"], "invalid_argument",
            "absent wait_seconds should be accepted",
        );
    }

    // -- Service: happy paths ----------------------------------------------

    #[tokio::test]
    async fn test_execute_job_status_completed_job_with_counts() {
        let job_id = JobId::new();
        let job = a_job()
            .id(job_id)
            .status(JobStatus::Completed)
            .outcome(Some(JobOutcome::Success))
            .build();

        let tasks = vec![
            a_task()
                .job_id(job_id)
                .task_type(TaskType::Triage)
                .status(TaskStatus::Completed)
                .build(),
            a_task()
                .job_id(job_id)
                .task_type(TaskType::Triage)
                .status(TaskStatus::Completed)
                .build(),
            a_task()
                .job_id(job_id)
                .task_type(TaskType::Triage)
                .status(TaskStatus::DeadLetter)
                .build(),
        ];

        let triage_results = vec![
            a_triage_result_created()
                .job_id(job_id)
                .batch_index(0)
                .build(),
            a_triage_result_duplicate()
                .job_id(job_id)
                .batch_index(1)
                .build(),
            a_triage_result_failed()
                .job_id(job_id)
                .batch_index(2)
                .build(),
        ];

        let repos = repos_for_job_status(job, tasks, triage_results);
        let params = JobStatusParams { job_id };
        let result = call_execute(&repos, &params).await.expect("should succeed");

        assert_eq!(result.job.id(), job_id);
        assert_eq!(result.job.status(), JobStatus::Completed);
        assert_eq!(result.job.outcome(), Some(JobOutcome::Success));
        assert_eq!(result.tasks_completed, 2);
        assert_eq!(result.tasks_failed, 1);
        assert_eq!(result.items_created, 1);
        assert_eq!(result.observations_created, 1);
    }

    #[tokio::test]
    async fn test_execute_job_status_queued_job_all_zeroes() {
        let job_id = JobId::new();
        let job = a_job().id(job_id).build();

        let repos = repos_for_job_status(job, vec![], vec![]);
        let params = JobStatusParams { job_id };
        let result = call_execute(&repos, &params).await.expect("should succeed");

        assert_eq!(result.job.id(), job_id);
        assert_eq!(result.job.status(), JobStatus::Queued);
        assert!(result.job.outcome().is_none());
        assert!(result.job.batch_size().is_none());
        assert_eq!(result.tasks_completed, 0);
        assert_eq!(result.tasks_failed, 0);
        assert_eq!(result.items_created, 0);
        assert_eq!(result.observations_created, 0);
    }

    #[tokio::test]
    async fn test_execute_job_status_failed_job() {
        let job_id = JobId::new();
        let job = a_job()
            .id(job_id)
            .status(JobStatus::Failed)
            .outcome(Some(JobOutcome::Failure))
            .build();

        let repos = repos_for_job_status(job, vec![], vec![]);
        let params = JobStatusParams { job_id };
        let result = call_execute(&repos, &params).await.expect("should succeed");

        assert_eq!(result.job.id(), job_id);
        assert_eq!(result.job.status(), JobStatus::Failed);
        assert_eq!(result.job.outcome(), Some(JobOutcome::Failure));
    }

    #[tokio::test]
    async fn test_execute_job_status_in_progress_no_outcome() {
        let job_id = JobId::new();
        let job = a_job()
            .id(job_id)
            .status(JobStatus::Triaging)
            .batch_size(Some(3))
            .build();

        let repos = repos_for_job_status(job, vec![], vec![]);
        let params = JobStatusParams { job_id };
        let result = call_execute(&repos, &params).await.expect("should succeed");

        assert_eq!(result.job.id(), job_id);
        assert_eq!(result.job.status(), JobStatus::Triaging);
        assert!(result.job.outcome().is_none());
        assert!(result.job.batch_size().is_some());
    }

    // -- Service: count filtering ------------------------------------------

    #[tokio::test]
    async fn test_execute_job_status_only_counts_triage_tasks() {
        let job_id = JobId::new();
        let job = a_job()
            .id(job_id)
            .status(JobStatus::Completed)
            .outcome(Some(JobOutcome::Success))
            .build();

        let tasks = vec![
            a_task()
                .job_id(job_id)
                .task_type(TaskType::Extraction)
                .status(TaskStatus::Completed)
                .build(),
            a_task()
                .job_id(job_id)
                .task_type(TaskType::Triage)
                .status(TaskStatus::Completed)
                .build(),
            a_task()
                .job_id(job_id)
                .task_type(TaskType::Relation)
                .status(TaskStatus::Completed)
                .build(),
        ];

        let repos = repos_for_job_status(job, tasks, vec![]);
        let params = JobStatusParams { job_id };
        let result = call_execute(&repos, &params).await.expect("should succeed");

        assert_eq!(
            result.tasks_completed, 1,
            "only the triage task should be counted",
        );
        assert_eq!(result.tasks_failed, 0);
    }

    // -- Service: error paths ----------------------------------------------

    #[tokio::test]
    async fn test_execute_job_status_job_not_found() {
        let mut repos = test_repositories();
        repos.job = Arc::new(
            MockJobRepository::builder()
                .on_find_by_id_error(
                    || DbError::NotFound {
                        entity: "job",
                        id: "missing".into(),
                    },
                    None,
                )
                .build(),
        );

        let job_id = JobId::new();
        let params = JobStatusParams { job_id };
        let err = call_execute(&repos, &params)
            .await
            .expect_err("should fail");

        assert!(
            matches!(err, JobStatusError::Db(DbError::NotFound { entity, .. }) if entity == "job")
        );
    }

    // -- Adapter: error paths ----------------------------------------------

    #[tokio::test]
    async fn test_apply_job_status_job_not_found_returns_application_error() {
        let ctx = test_context().await;
        let pool = ctx.create_pool().await.expect("pool");

        let job_id = JobId::new();
        let mut repos = test_repositories();
        repos.job = Arc::new(
            MockJobRepository::builder()
                .on_find_by_id_error(
                    || DbError::NotFound {
                        entity: "job",
                        id: "missing".into(),
                    },
                    None,
                )
                .build(),
        );

        let handler = test_handler_with_pool_and_repos(pool, repos);
        let result = handler
            .apply_job_status(
                serde_json::json!({"job_id": job_id.to_string()}),
                &ImmediatePollScheduler,
            )
            .await
            .expect(NO_PROTOCOL_ERROR);

        assert_eq!(result.is_error, Some(true));
        let structured = result.structured_content.expect(STRUCTURED_CONTENT);
        assert_eq!(structured["code"], "not_found");
    }

    // -- Polling behaviour -------------------------------------------------

    /// When the initial query returns a terminal state, the handler should
    /// return immediately without entering the polling loop, even with a
    /// non-zero `wait_seconds`.
    #[tokio::test]
    async fn test_apply_job_status_returns_immediately_when_terminal() {
        let ctx = test_context().await;
        let pool = ctx.create_pool().await.expect("pool");

        let job_id = JobId::new();
        let job = a_job()
            .id(job_id)
            .status(JobStatus::Completed)
            .outcome(Some(JobOutcome::Success))
            .build();

        let job_mock = Arc::new(
            MockJobRepository::builder()
                .on_find_by_id(job, None)
                .build(),
        );
        let job_mock_ref = Arc::clone(&job_mock);

        let mut repos = test_repositories();
        repos.job = job_mock;
        repos.task = Arc::new(
            MockTaskRepository::builder()
                .on_find_by_job_id(vec![], None)
                .build(),
        );
        repos.triage_result = Arc::new(
            MockTriageResultRepository::builder()
                .on_find_by_job_id(vec![], None)
                .build(),
        );

        let handler = test_handler_with_pool_and_repos(pool, repos);
        let result = handler
            .apply_job_status(
                serde_json::json!({
                    "job_id": job_id.to_string(),
                    "wait_seconds": 5,
                }),
                &ImmediatePollScheduler,
            )
            .await
            .expect(NO_PROTOCOL_ERROR);

        assert_eq!(result.is_error, Some(false));
        assert_eq!(
            job_mock_ref.find_by_id_call_count(),
            1,
            "should not poll when already terminal",
        );
    }

    /// When the initial query returns a non-terminal state, and
    /// `wait_seconds > 0`, the handler should poll until the job becomes
    /// terminal.
    #[tokio::test]
    async fn test_apply_job_status_polls_until_terminal() {
        let ctx = test_context().await;
        let pool = ctx.create_pool().await.expect("pool");

        let job_id = JobId::new();
        let triaging_job = a_job().id(job_id).status(JobStatus::Triaging).build();
        let completed_job = a_job()
            .id(job_id)
            .status(JobStatus::Completed)
            .outcome(Some(JobOutcome::Success))
            .batch_size(Some(1))
            .build();

        let completed_task = a_task()
            .job_id(job_id)
            .task_type(TaskType::Triage)
            .status(TaskStatus::Completed)
            .build();
        let created_result = a_triage_result_created().job_id(job_id).build();

        let job_mock = Arc::new(
            MockJobRepository::builder()
                .on_find_by_id(triaging_job, None)
                .on_find_by_id(completed_job, None)
                .build(),
        );
        let job_mock_ref = Arc::clone(&job_mock);

        let mut repos = test_repositories();
        repos.job = job_mock;
        repos.task = Arc::new(
            MockTaskRepository::builder()
                .on_find_by_job_id(vec![], None)
                .on_find_by_job_id(vec![completed_task], None)
                .build(),
        );
        repos.triage_result = Arc::new(
            MockTriageResultRepository::builder()
                .on_find_by_job_id(vec![], None)
                .on_find_by_job_id(vec![created_result], None)
                .build(),
        );

        let handler = test_handler_with_pool_and_repos(pool, repos);
        let result = handler
            .apply_job_status(
                serde_json::json!({
                    "job_id": job_id.to_string(),
                    "wait_seconds": 5,
                }),
                &ImmediatePollScheduler,
            )
            .await
            .expect(NO_PROTOCOL_ERROR);

        assert_eq!(result.is_error, Some(false));
        assert_eq!(
            job_mock_ref.find_by_id_call_count(),
            2,
            "should poll once after initial query",
        );

        let structured = result.structured_content.expect(STRUCTURED_CONTENT);
        assert_eq!(structured["job_id"], job_id.to_string());
        assert_eq!(structured["status"], "completed");
        assert_eq!(structured["outcome"], "success");
        assert_eq!(structured["tasks_completed"], 1);
        assert_eq!(structured["items_created"], 1);
    }
}
