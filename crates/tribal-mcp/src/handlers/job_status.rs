//! Handler for `tribal_job_status` — job progress and outcome lookup.

use std::{str::FromStr, time::Duration};

use rmcp::{
    model::{CallToolResult, ErrorData as McpError},
    service::{RequestContext, RoleServer},
};
use tokio::sync::watch;
use tracing::Instrument;
use tribal_db::DbError;
use tribal_domain::{
    Job, JobId, JobState, McpErrorCode, TaskStatus, TaskType, TriageOutcome, span_attrs,
};

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
        context: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, McpError> {
        let principal = self.resolve_principal(&context)?;
        let span = tracing::info_span!(
            parent: None,
            "tribal.job_status",
            { span_attrs::PRINCIPAL_KEY } = principal.principal_key(),
            { span_attrs::TRANSPORT } = self.transport_name,
            { span_attrs::PROJECT_ID } = tracing::field::Empty,
        );
        let scheduler = TimedPollScheduler {
            interval: POLL_INTERVAL,
        };
        self.apply_job_status(params, &scheduler)
            .instrument(span)
            .await
    }

    /// Core logic for `tribal_job_status`, separated from the outer handler
    /// so it can be tested without a `Peer<RoleServer>`.
    ///
    /// 1. Parse + validate the request.
    /// 2. Query the database for the current job state (first DB read).
    /// 3. If the caller asked to wait and the job is not yet terminal:
    ///    - **Watch path** (fast): subscribe to the in-memory watch
    ///      channel, sleep until a state change / timeout / cancellation,
    ///      then do one final DB read. Total: exactly 2 DB reads.
    ///    - **Poll path** (fallback): if no watch entry exists (e.g.
    ///      after a restart), poll the database on a timer until
    ///      terminal or timeout.
    /// 4. Build and return the response.
    async fn apply_job_status<S: PollScheduler>(
        &self,
        params: serde_json::Value,
        scheduler: &S,
    ) -> Result<CallToolResult, McpError> {
        // -- 1. Parse + validate ---------------------------------------------

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

        // -- 2. Initial DB read ----------------------------------------------

        let mut result = match self.query_job_status(&status_params).await {
            Ok(r) => r,
            Err(call_result) => return Ok(call_result),
        };

        tracing::Span::current().record(
            span_attrs::PROJECT_ID,
            tracing::field::display(result.job.project_id()),
        );

        // -- 3. Wait (if requested and not already terminal) -----------------

        if let Some(wait) = request
            .wait_seconds
            .filter(|&w| w > 0)
            .filter(|_| !result.job.status().is_terminal())
        {
            let wait_duration = Duration::from_secs(u64::from(wait));

            let watch_rx = self
                .state
                .job_state_txs
                .get(&job_id)
                .map(|entry| entry.sender.subscribe());

            match watch_rx {
                Some(rx) => {
                    self.wait_via_watch(rx, wait_duration).await;
                    result = match self.query_job_status(&status_params).await {
                        Ok(r) => r,
                        Err(call_result) => return Ok(call_result),
                    };
                }
                None => {
                    if let Err(call_result) = self
                        .wait_via_poll(scheduler, &status_params, wait_duration, &mut result)
                        .await
                    {
                        return Ok(call_result);
                    }
                }
            }
        }

        // -- 4. Response -----------------------------------------------------

        let response = McpJobStatusResponse::from_domain(
            &result.job,
            result.tasks_completed,
            result.tasks_failed,
            result.items_created,
            result.observations_created,
        );

        Ok(response.into_call_tool_result())
    }

    /// Acquires a connection and queries the database for the current
    /// job status and aggregate counts. Returns a `CallToolResult` on
    /// error (pool or query failure) so callers can forward it directly.
    async fn query_job_status(
        &self,
        params: &JobStatusParams,
    ) -> Result<JobStatusResult, CallToolResult> {
        let mut conn = acquire_connection(
            &self.state.pool_mcp,
            self.config.pool_name,
            &self.state.metrics,
        )
        .await?;
        execute_job_status(&mut conn, &self.repositories, params)
            .await
            .map_err(|e| e.into_mcp_error().into_call_tool_result())
    }

    /// Watch path: wait until the watch channel reaches a terminal state,
    /// the deadline expires, or the server is shutting down — whichever
    /// comes first.
    ///
    /// Uses `wait_for(is_terminal)` rather than `changed()` so that
    /// intermediate transitions (e.g. queued → extracting) do not cause
    /// an early return with a non-terminal status. This keeps the watch
    /// path semantically aligned with the poll path.
    ///
    /// After this method returns, the caller does one final DB read.
    async fn wait_via_watch(&self, mut rx: watch::Receiver<JobState>, deadline: Duration) {
        // A terminal transition may have arrived between the initial DB
        // read and subscription. If so, skip the wait entirely.
        if rx.borrow().is_terminal() {
            return;
        }

        let cancel = self.state.cancellation_token.cancelled();
        tokio::select! {
            _ = rx.wait_for(|s| s.is_terminal()) => {}
            () = tokio::time::sleep(deadline) => {}
            () = cancel => {}
        }
    }

    /// Poll path: repeatedly query the database on a timer until the
    /// job reaches a terminal state, the deadline expires, or the server
    /// is shutting down.
    ///
    /// Used as a fallback when no watch channel entry exists for the job
    /// (e.g. after a server restart). If a DB query fails, the error is
    /// returned immediately — no retry inside the wait loop.
    async fn wait_via_poll<S: PollScheduler>(
        &self,
        scheduler: &S,
        params: &JobStatusParams,
        wait_duration: Duration,
        result: &mut JobStatusResult,
    ) -> Result<(), CallToolResult> {
        let ticker = scheduler.create_ticker(wait_duration);

        loop {
            // The cancellation arm always breaks. ticker.tick() is not
            // assumed cancel-safe — we never re-enter it after cancel.
            let should_continue = tokio::select! {
                cont = ticker.tick() => cont,
                () = self.state.cancellation_token.cancelled() => false,
            };
            if !should_continue {
                break;
            }

            *result = self.query_job_status(params).await?;

            if result.job.status().is_terminal() {
                break;
            }
        }

        Ok(())
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
            // In-flight statuses count as neither: a blocked task is a
            // live sibling, exactly like a queued or claimed one.
            TaskStatus::Queued | TaskStatus::Claimed | TaskStatus::Blocked => {}
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

    use dashmap::DashMap;
    use rmcp::model::ErrorCode;
    use tokio::sync::watch;
    use tokio_util::sync::CancellationToken;
    use tribal_common::{JobStateTxs, JobWatchEntry};
    use tribal_domain::{JobId, JobOutcome, JobState, JobStatus, KnowledgeItemId, TaskType};
    use tribal_test_utils::{
        MockJobRepository, MockTaskRepository, MockTriageResultRepository, a_job, a_task,
        a_triage_result_created, a_triage_result_duplicate, a_triage_result_failed, test_context,
    };

    use super::*;
    use crate::{
        polling::{ImmediatePollScheduler, YieldingPollScheduler},
        test_utils::{NO_STRUCTURED_CONTENT, TestHandler, first_text_content, test_repositories},
    };

    // -- Constants ---------------------------------------------------------

    const STRUCTURED_CONTENT: &str = "structured_content must be present";
    const NO_PROTOCOL_ERROR: &str = "should not return a protocol error";

    // -- Helpers -----------------------------------------------------------

    /// Asserts that a `wait_seconds` value was accepted: the error result must
    /// come from the downstream pool layer (`lazy_pool` cannot connect), not
    /// from the `wait_seconds` validation check.
    fn assert_accepted_wait_seconds(result: &CallToolResult, message: &str) {
        let text = first_text_content(result);
        assert!(
            !text.contains("wait_seconds must be between"),
            "{message}: {text}",
        );
        assert!(
            text.contains("pool") || text.contains("query failed"),
            "error should originate from the pool, not wait_seconds validation: {text}",
        );
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
        let handler = TestHandler::builder().build();

        let err = handler
            .apply_job_status(serde_json::json!({"job_id": 123}), &ImmediatePollScheduler)
            .await
            .expect_err("should return Err(McpError) for malformed params");

        assert_eq!(err.code, ErrorCode::INVALID_PARAMS);
    }

    #[tokio::test]
    async fn test_apply_job_status_invalid_job_prefix_returns_application_error() {
        let handler = TestHandler::builder().build();

        let wrong_prefix_id = KnowledgeItemId::new().to_string();
        let result = handler
            .apply_job_status(
                serde_json::json!({"job_id": wrong_prefix_id}),
                &ImmediatePollScheduler,
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
    async fn test_apply_job_status_wait_seconds_over_30_returns_application_error() {
        let handler = TestHandler::builder().build();

        let result = handler
            .apply_job_status(
                serde_json::json!({"job_id": JobId::new().to_string(), "wait_seconds": 31}),
                &ImmediatePollScheduler,
            )
            .await
            .expect(NO_PROTOCOL_ERROR);

        assert_eq!(result.is_error, Some(true));
        assert!(
            result.structured_content.is_none(),
            "{NO_STRUCTURED_CONTENT}"
        );
        assert!(first_text_content(&result).contains("wait_seconds must be between"));
    }

    /// With `wait_seconds=30` (the upper boundary), the handler should
    /// pass validation and proceed to the pool phase. `lazy_pool` cannot
    /// open connections, so the call fails at pool acquisition — we assert
    /// the error message is NOT the `wait_seconds` validation message to confirm
    /// the boundary is accepted and the failure originates at the pool.
    #[tokio::test]
    async fn test_apply_job_status_wait_seconds_at_boundary_30_accepted() {
        let handler = TestHandler::builder().build();

        let result = handler
            .apply_job_status(
                serde_json::json!({"job_id": JobId::new().to_string(), "wait_seconds": 30}),
                &ImmediatePollScheduler,
            )
            .await
            .expect(NO_PROTOCOL_ERROR);

        assert_eq!(result.is_error, Some(true));
        assert!(
            result.structured_content.is_none(),
            "{NO_STRUCTURED_CONTENT}"
        );
        assert_accepted_wait_seconds(&result, "wait_seconds=30 should be accepted");
    }

    /// With `wait_seconds=0`, the handler should pass validation and
    /// proceed to the pool phase without polling.
    #[tokio::test]
    async fn test_apply_job_status_wait_seconds_zero_accepted() {
        let handler = TestHandler::builder().build();

        let result = handler
            .apply_job_status(
                serde_json::json!({"job_id": JobId::new().to_string(), "wait_seconds": 0}),
                &ImmediatePollScheduler,
            )
            .await
            .expect(NO_PROTOCOL_ERROR);

        assert_eq!(result.is_error, Some(true));
        assert!(
            result.structured_content.is_none(),
            "{NO_STRUCTURED_CONTENT}"
        );
        assert_accepted_wait_seconds(&result, "wait_seconds=0 should be accepted");
    }

    /// With `wait_seconds` absent from the request, the handler should
    /// pass validation and proceed to the pool phase without polling.
    #[tokio::test]
    async fn test_apply_job_status_wait_seconds_absent_accepted() {
        let handler = TestHandler::builder().build();

        let result = handler
            .apply_job_status(
                serde_json::json!({"job_id": JobId::new().to_string()}),
                &ImmediatePollScheduler,
            )
            .await
            .expect(NO_PROTOCOL_ERROR);

        assert_eq!(result.is_error, Some(true));
        assert!(
            result.structured_content.is_none(),
            "{NO_STRUCTURED_CONTENT}"
        );
        assert_accepted_wait_seconds(&result, "absent wait_seconds should be accepted");
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

        let handler = TestHandler::builder()
            .pool(pool)
            .repositories(repos)
            .build();
        let result = handler
            .apply_job_status(
                serde_json::json!({"job_id": job_id.to_string()}),
                &ImmediatePollScheduler,
            )
            .await
            .expect(NO_PROTOCOL_ERROR);

        assert_eq!(result.is_error, Some(true));
        assert!(
            result.structured_content.is_none(),
            "{NO_STRUCTURED_CONTENT}"
        );
        assert!(first_text_content(&result).contains("job not found"));
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

        let handler = TestHandler::builder()
            .pool(pool)
            .repositories(repos)
            .build();
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

        let handler = TestHandler::builder()
            .pool(pool)
            .repositories(repos)
            .build();
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

    // -- Wait-path helpers -------------------------------------------------

    /// Creates a `JobStateTxs` map with a single watch entry for the
    /// given job ID. Returns the map and the sender so tests can send
    /// state transitions.
    fn watch_entry_for(job_id: JobId) -> (JobStateTxs, watch::Sender<JobState>) {
        let (tx, rx) = watch::channel(JobState::Queued);
        let txs: JobStateTxs = Arc::new(DashMap::new());
        txs.insert(job_id, JobWatchEntry::new(tx.clone(), rx));
        (txs, tx)
    }

    /// Builds a `TestHandler` with a real pool and mocks returning
    /// `first_job` on the initial query and `second_job` on the second.
    /// Returns the handler and a reference to the job mock for asserting
    /// call counts.
    ///
    /// Used by both watch-path and poll-fallback tests. Pool creation
    /// must happen before `tokio::time::pause()` because sqlx uses
    /// internal timers that would auto-advance to zero under paused
    /// time.
    async fn wait_path_handler(
        first_job: Job,
        second_job: Job,
        job_state_txs: JobStateTxs,
        cancellation_token: CancellationToken,
    ) -> (TribalServerHandler, Arc<MockJobRepository>) {
        let ctx = test_context().await;
        let pool = ctx.create_pool().await.expect("pool");

        let job_mock = Arc::new(
            MockJobRepository::builder()
                .on_find_by_id(first_job, None)
                .on_find_by_id(second_job, None)
                .build(),
        );
        let job_mock_ref = Arc::clone(&job_mock);

        let mut repos = test_repositories();
        repos.job = job_mock;
        repos.task = Arc::new(
            MockTaskRepository::builder()
                .on_find_by_job_id(vec![], None)
                .on_find_by_job_id(vec![], None)
                .build(),
        );
        repos.triage_result = Arc::new(
            MockTriageResultRepository::builder()
                .on_find_by_job_id(vec![], None)
                .on_find_by_job_id(vec![], None)
                .build(),
        );

        let handler = TestHandler::builder()
            .pool(pool)
            .repositories(repos)
            .job_state_txs(job_state_txs)
            .cancellation_token(cancellation_token)
            .build();

        (handler, job_mock_ref)
    }

    // -- Watch path behaviour ----------------------------------------------
    //
    // Signal and cancellation tests resolve via `rx.changed()` or
    // `cancel` — the sleep arm never wins — so no paused time is needed
    // and they complete instantly. The timeout test uses `wait_seconds=1`
    // (real wall-clock time) because `tokio::time::pause()` also affects
    // the pool's internal timers, breaking the post-wait DB read.

    /// When a watch entry exists and a terminal state is sent while the
    /// handler is waiting, it wakes up and does a final DB read.
    /// Total DB reads: exactly 2 (initial + final).
    #[tokio::test]
    async fn test_apply_job_status_watch_path_wakes_on_state_change() {
        let job_id = JobId::new();
        let (txs, watch_tx) = watch_entry_for(job_id);

        // Yield once so the handler enters the select! first, then send.
        tokio::spawn(async move {
            tokio::task::yield_now().await;
            let _ = watch_tx.send(JobState::Completed);
        });

        let first = a_job().id(job_id).status(JobStatus::Triaging).build();
        let second = a_job()
            .id(job_id)
            .status(JobStatus::Completed)
            .outcome(Some(JobOutcome::Success))
            .build();
        let (handler, job_mock) =
            wait_path_handler(first, second, txs, CancellationToken::new()).await;

        let result = handler
            .apply_job_status(
                serde_json::json!({
                    "job_id": job_id.to_string(),
                    "wait_seconds": 10,
                }),
                &ImmediatePollScheduler,
            )
            .await
            .expect(NO_PROTOCOL_ERROR);

        assert_eq!(result.is_error, Some(false));
        assert_eq!(
            job_mock.find_by_id_call_count(),
            2,
            "watch path should make exactly 2 DB reads",
        );
    }

    /// When the watch channel already holds a terminal state before the
    /// handler subscribes, the race-elimination check skips the wait
    /// and proceeds directly to the final DB read.
    #[tokio::test]
    async fn test_apply_job_status_watch_path_terminal_before_subscribe() {
        let job_id = JobId::new();
        let (txs, watch_tx) = watch_entry_for(job_id);
        let _ = watch_tx.send(JobState::Completed);

        let first = a_job().id(job_id).status(JobStatus::Triaging).build();
        let second = a_job()
            .id(job_id)
            .status(JobStatus::Completed)
            .outcome(Some(JobOutcome::Success))
            .build();
        let (handler, job_mock) =
            wait_path_handler(first, second, txs, CancellationToken::new()).await;

        let result = handler
            .apply_job_status(
                serde_json::json!({
                    "job_id": job_id.to_string(),
                    "wait_seconds": 10,
                }),
                &ImmediatePollScheduler,
            )
            .await
            .expect(NO_PROTOCOL_ERROR);

        assert_eq!(result.is_error, Some(false));
        assert_eq!(
            job_mock.find_by_id_call_count(),
            2,
            "race-elimination should skip wait, still 2 DB reads",
        );
    }

    /// When no state change arrives within the wait period, the handler
    /// returns after the timeout with exactly 2 DB reads.
    #[tokio::test]
    async fn test_apply_job_status_watch_path_timeout() {
        let job_id = JobId::new();
        let (txs, _watch_tx) = watch_entry_for(job_id);

        let first = a_job().id(job_id).status(JobStatus::Triaging).build();
        let second = a_job().id(job_id).status(JobStatus::Triaging).build();
        let (handler, job_mock) =
            wait_path_handler(first, second, txs, CancellationToken::new()).await;

        let result = handler
            .apply_job_status(
                serde_json::json!({
                    "job_id": job_id.to_string(),
                    "wait_seconds": 1,
                }),
                &ImmediatePollScheduler,
            )
            .await
            .expect(NO_PROTOCOL_ERROR);

        assert_eq!(result.is_error, Some(false));
        assert_eq!(
            job_mock.find_by_id_call_count(),
            2,
            "watch timeout should still make exactly 2 DB reads",
        );
        let structured = result.structured_content.expect(STRUCTURED_CONTENT);
        assert_eq!(structured["status"], "triaging");
    }

    /// When the cancellation token fires while the handler is waiting
    /// on the watch path, it wakes up and does a final DB read.
    #[tokio::test]
    async fn test_apply_job_status_watch_path_cancellation() {
        let job_id = JobId::new();
        let (txs, _watch_tx) = watch_entry_for(job_id);
        let cancel_token = CancellationToken::new();

        // Yield once so the handler enters the select! first, then cancel.
        let cancel_clone = cancel_token.clone();
        tokio::spawn(async move {
            tokio::task::yield_now().await;
            cancel_clone.cancel();
        });

        let first = a_job().id(job_id).status(JobStatus::Triaging).build();
        let second = a_job().id(job_id).status(JobStatus::Triaging).build();
        let (handler, job_mock) = wait_path_handler(first, second, txs, cancel_token).await;

        let result = handler
            .apply_job_status(
                serde_json::json!({
                    "job_id": job_id.to_string(),
                    "wait_seconds": 30,
                }),
                &ImmediatePollScheduler,
            )
            .await
            .expect(NO_PROTOCOL_ERROR);

        assert_eq!(result.is_error, Some(false));
        assert_eq!(
            job_mock.find_by_id_call_count(),
            2,
            "cancellation should wake, then do 1 final DB read",
        );
        let structured = result.structured_content.expect(STRUCTURED_CONTENT);
        assert_eq!(structured["status"], "triaging");
    }

    // -- Poll fallback behaviour -------------------------------------------

    /// When no watch entry exists (e.g. after a restart), the handler
    /// falls back to the poll path. With `ImmediatePollScheduler` the
    /// poll resolves instantly once the mock returns a terminal state.
    #[tokio::test]
    async fn test_apply_job_status_poll_fallback_when_no_watch_entry() {
        let job_id = JobId::new();
        let empty_txs: JobStateTxs = Arc::new(DashMap::new());

        let first = a_job().id(job_id).status(JobStatus::Triaging).build();
        let second = a_job()
            .id(job_id)
            .status(JobStatus::Completed)
            .outcome(Some(JobOutcome::Success))
            .build();
        let (handler, job_mock) =
            wait_path_handler(first, second, empty_txs, CancellationToken::new()).await;

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
            job_mock.find_by_id_call_count(),
            2,
            "poll fallback should query until terminal",
        );
        let structured = result.structured_content.expect(STRUCTURED_CONTENT);
        assert_eq!(structured["status"], "completed");
    }

    /// When the cancellation token fires during the poll path, the
    /// handler exits the loop and returns the last-known result.
    /// [`YieldingPollScheduler`] creates a scheduling point between
    /// ticks so the spawned cancellation task can fire.
    #[tokio::test]
    async fn test_apply_job_status_poll_path_cancellation() {
        let job_id = JobId::new();
        let empty_txs: JobStateTxs = Arc::new(DashMap::new());
        let cancel_token = CancellationToken::new();

        let cancel_clone = cancel_token.clone();
        tokio::spawn(async move {
            tokio::task::yield_now().await;
            cancel_clone.cancel();
        });

        // Second response is never consumed — cancellation fires before
        // the first poll iteration.
        let first = a_job().id(job_id).status(JobStatus::Triaging).build();
        let second = a_job().id(job_id).status(JobStatus::Triaging).build();
        let (handler, job_mock) = wait_path_handler(first, second, empty_txs, cancel_token).await;

        let result = handler
            .apply_job_status(
                serde_json::json!({
                    "job_id": job_id.to_string(),
                    "wait_seconds": 30,
                }),
                &YieldingPollScheduler,
            )
            .await
            .expect(NO_PROTOCOL_ERROR);

        assert_eq!(result.is_error, Some(false));
        assert_eq!(
            job_mock.find_by_id_call_count(),
            1,
            "cancellation should exit before any poll iteration",
        );
        let structured = result.structured_content.expect(STRUCTURED_CONTENT);
        assert_eq!(structured["status"], "triaging");
    }
}
