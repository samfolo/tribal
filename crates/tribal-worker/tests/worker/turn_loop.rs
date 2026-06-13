//! Integration tests for the agentic turn loop, driven directly through
//! the runtime against a live job — the same harness shape as the
//! suspend/resolve tests. The worker's dispatch does not route to the
//! loop yet; these pin the loop contract the stage wiring will inherit.

use std::sync::{
    Arc,
    atomic::{AtomicU32, Ordering},
};

use async_trait::async_trait;
use sqlx::PgConnection;
use tribal_agent_runtime::{
    AcceptedSubmission, Admission, BUDGET_RECHECK_CAUSE, BudgetFailure, HeartbeatPump, LoopOutcome,
    RecheckPolicy, SUBMIT_RESULT_TOOL, SeenCorpus, StageTool, SubmissionOutcome,
    SubmissionPipeline, ToolOutcome, ToolRegistry, TurnLoopDeps, admit_inference,
    commit_loop_terminal, resolve_stage_thread, run_turn_loop,
};
use tribal_db::{
    AgentThreadRecordRepository, AgentThreadRepository, PgAgentThreadRecordRepository,
    PgAgentThreadRepository,
};
use tribal_domain::{
    AgentThread, AgentThreadRecordKind, AgentThreadStatus, AgentThreadSuspension, ExecutionBudgets,
    ExecutionSpend, RecoverableToolFailure, ToolDescriptor, ToolExecutionMode, ToolFailure,
    ToolSafetyTier, UsageOwner,
};
use tribal_inference::UsageAttribution;
use tribal_test_utils::a_tool_call_response;

use super::common::*;

/// An item id the opening conversation shows the model.
const OPENING_ITEM_ID: &str = "ki_00000000-0000-0000-0000-0000000000aa";
/// An item id only the lookup tool's result shows the model.
const TOOL_ITEM_ID: &str = "ki_00000000-0000-0000-0000-0000000000bb";
/// An item id nothing ever shows the model.
const UNSEEN_ITEM_ID: &str = "ki_00000000-0000-0000-0000-0000000000ff";

const RECHECK: RecheckPolicy = RecheckPolicy {
    delay_seconds: 300,
    bound: 3,
};

// ---------------------------------------------------------------------------
// Test doubles
// ---------------------------------------------------------------------------

/// Records the loop's liveness calls.
#[derive(Default)]
struct TestPump {
    started: AtomicU32,
    deltas: AtomicU32,
    finished: AtomicU32,
    aborts: AtomicU32,
}

impl HeartbeatPump for TestPump {
    fn inference_started(&self) {
        self.started
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }
    fn stream_delta(&self) {
        self.deltas
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }
    fn inference_finished(&self) {
        self.finished
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }
    fn abort(&self) {
        self.aborts
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }
}

fn a_descriptor(name: &str) -> ToolDescriptor {
    ToolDescriptor::builder()
        .name(name.to_owned())
        .description("a test tool".to_owned())
        .input_schema(serde_json::json!({"type": "object"}))
        .response_size_bound(4_096)
        .safety_tier(ToolSafetyTier::Pure)
        .execution_mode(ToolExecutionMode::Immediate)
        .build()
}

/// A read tool whose result shows the model [`TOOL_ITEM_ID`].
struct LookupTool {
    descriptor: ToolDescriptor,
}

impl LookupTool {
    fn new() -> Self {
        Self {
            descriptor: a_descriptor("lookup_note"),
        }
    }
}

#[async_trait]
impl StageTool for LookupTool {
    fn descriptor(&self) -> &ToolDescriptor {
        &self.descriptor
    }
    async fn execute(
        &self,
        _conn: &mut PgConnection,
        _prepared: serde_json::Value,
        _arguments: &serde_json::Value,
    ) -> Result<ToolOutcome, ToolFailure> {
        Ok(ToolOutcome {
            content: format!("the note references {TOOL_ITEM_ID}"),
        })
    }
}

/// A tool that fails on demand, in either failure class and phase.
struct FailingTool {
    descriptor: ToolDescriptor,
    prepare_system: bool,
}

#[async_trait]
impl StageTool for FailingTool {
    fn descriptor(&self) -> &ToolDescriptor {
        &self.descriptor
    }
    async fn prepare(
        &self,
        _arguments: &serde_json::Value,
    ) -> Result<serde_json::Value, ToolFailure> {
        if self.prepare_system {
            return Err(ToolFailure::System {
                context: "the provider behind the tool collapsed".to_owned(),
            });
        }
        Ok(serde_json::Value::Null)
    }
    async fn execute(
        &self,
        _conn: &mut PgConnection,
        _prepared: serde_json::Value,
        _arguments: &serde_json::Value,
    ) -> Result<ToolOutcome, ToolFailure> {
        Err(ToolFailure::Recoverable(
            RecoverableToolFailure::EmptyResult {
                tool: self.descriptor.name.clone(),
                detail: "nothing matched the lookup".to_owned(),
            },
        ))
    }
}

/// Accepts a submission whose `claim_id` the model was actually shown;
/// bounces everything else with the membership diagnostic.
struct MembershipPipeline;

#[async_trait]
impl SubmissionPipeline for MembershipPipeline {
    async fn evaluate(
        &self,
        _conn: &mut PgConnection,
        _thread: &AgentThread,
        corpus: &SeenCorpus,
        arguments: &serde_json::Value,
    ) -> Result<SubmissionOutcome, ToolFailure> {
        let Some(claim_id) = arguments
            .get("claim_id")
            .and_then(serde_json::Value::as_str)
        else {
            return Ok(SubmissionOutcome::Bounced {
                diagnostics: "the submission needs a 'claim_id' string".to_owned(),
            });
        };
        if corpus.contains(claim_id) {
            Ok(SubmissionOutcome::Accepted {
                payload: arguments.clone(),
            })
        } else {
            Ok(SubmissionOutcome::Bounced {
                diagnostics: format!(
                    "'{claim_id}' does not appear in anything you were shown; copy ids exactly"
                ),
            })
        }
    }
}

// ---------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------

/// One claimed stage execution with everything the loop needs.
struct LoopHarness {
    pool: sqlx::PgPool,
    gateway: Arc<InferenceGateway>,
    provider: Arc<MockInferenceProvider>,
    task: tribal_domain::Task,
    thread: AgentThread,
    registry: ToolRegistry,
    submit_descriptor: ToolDescriptor,
    pump: TestPump,
    job_id: tribal_domain::JobId,
}

impl LoopHarness {
    fn deps<'a>(
        &'a self,
        rendered: tribal_agent_runtime::RenderedConversation,
        attribution: &'a UsageAttribution,
        pipeline: &'a dyn SubmissionPipeline,
        budgets: ExecutionBudgets,
    ) -> TurnLoopDeps<'a> {
        TurnLoopDeps {
            pool: &self.pool,
            gateway: &self.gateway,
            stage: TaskType::Extraction,
            thread: &self.thread,
            task_id: self.task.id(),
            claim_token: self.task.claim_token().expect("claimed"),
            attempt: 0,
            attribution,
            registry: &self.registry,
            submit_descriptor: &self.submit_descriptor,
            pipeline,
            pump: &self.pump,
            rendered,
            budgets,
            recheck: RECHECK,
            temperature: None,
            max_tokens: None,
            permit_deadline: tokio::time::Instant::now() + Duration::from_secs(30),
        }
    }

    fn attribution(&self) -> UsageAttribution {
        UsageAttribution {
            owner: UsageOwner::Pipeline {
                job_id: self.job_id,
                task_id: self.task.id(),
                thread_id: self.thread.id(),
                record_id: None,
                attempt: 0,
            },
            system_prompt_version_id: None,
            user_prompt_version_id: None,
            trace_id: None,
        }
    }
}

/// The opening conversation: shows the model [`OPENING_ITEM_ID`].
fn an_opening() -> tribal_agent_runtime::RenderedConversation {
    tribal_agent_runtime::RenderedConversation {
        system: Some("triage the candidate against the claims".to_owned()),
        messages: vec![tribal_agent_runtime::RecordedMessage {
            role: "user".to_owned(),
            content: format!("similar claims:\n- {OPENING_ITEM_ID}: an existing claim"),
        }],
        system_prompt_version_id: None,
        user_prompt_version_id: None,
        resolution_context: None,
    }
}

/// Seeds a job, claims its extraction task, establishes its thread, and
/// builds a loop-ready harness over the queued mock responses.
async fn loop_harness(
    ctx: &'static TestContext,
    suffix: &str,
    responses: Vec<tribal_domain::CompletionResponse>,
) -> LoopHarness {
    let pool = ctx.create_pool().await.expect("create pool");
    let (principal_id, project_id, system_pv_id, user_pv_id) =
        setup_prerequisites(ctx, suffix).await;
    let mut conn = raw_conn(ctx).await;
    let (job_id, _task_id) = seed_extraction_job(
        &mut conn,
        principal_id,
        project_id,
        system_pv_id,
        user_pv_id,
    )
    .await;

    let claimed = PgTaskRepository
        .claim(&mut conn, 1, "loop-test")
        .await
        .expect("claim");
    let task = claimed.first().expect("the seeded task claims").clone();
    let job = PgJobRepository
        .find_by_id(&mut conn, job_id)
        .await
        .expect("find job");
    let binding = tribal_agent_runtime::resolve_binding(
        &mut conn,
        &tribal_test_utils::an_agent_definition().build(),
    )
    .await
    .expect("binding");
    let stage_thread = tribal_agent_runtime::ensure_stage_thread(
        &mut conn,
        &job,
        &task,
        task.claim_token().expect("token"),
        &binding,
    )
    .await
    .expect("thread");
    drop(conn);

    let mut builder = MockInferenceProvider::builder();
    for response in responses {
        builder = builder.on_complete(response, None);
    }
    let provider = Arc::new(builder.build());

    let key = |class| {
        ProviderKey::new("mock", "http://localhost:9999", class).expect("valid provider key")
    };
    let limits = || ProviderLimits {
        max_in_flight: 10,
        request_timeout: Duration::from_secs(30),
    };
    let registry = ProviderRegistry::new(vec![
        (key(RequestClass::Inference), limits()),
        (key(RequestClass::Embedding), limits()),
    ])
    .expect("valid registry");
    let sink = Arc::new(PgLedgerSink::new(pool.clone(), noop_recorder()));
    let gateway = Arc::new(InferenceGateway::with_providers(
        InjectedProviders::uniform(
            registry,
            Arc::clone(&provider) as Arc<dyn InferenceProvider>,
            key(RequestClass::Inference),
            vec![],
            sink,
        ),
    ));

    let mut tools = ToolRegistry::new();
    tools
        .register(Arc::new(LookupTool::new()))
        .expect("lookup registers");

    LoopHarness {
        pool,
        gateway,
        provider,
        task,
        thread: stage_thread.thread,
        registry: tools,
        submit_descriptor: ToolDescriptor::builder()
            .name(SUBMIT_RESULT_TOOL.to_owned())
            .description("submit the triage decision".to_owned())
            .input_schema(serde_json::json!({"type": "object"}))
            .response_size_bound(4_096)
            .safety_tier(ToolSafetyTier::InternalTransactional)
            .execution_mode(ToolExecutionMode::Immediate)
            .build(),
        pump: TestPump::default(),
        job_id,
    }
}

/// A submit call's response, referencing the given claim id.
fn submit_response(call_id: &str, claim_id: &str) -> tribal_domain::CompletionResponse {
    a_tool_call_response(&[(
        call_id,
        SUBMIT_RESULT_TOOL,
        serde_json::json!({"claim_id": claim_id, "decision": "duplicate"}),
    )])
}

/// Composes the stage terminal the way a stage commit will: task
/// completion and the loop terminal in one transaction.
async fn compose_terminal(ctx: &TestContext, harness: &LoopHarness, accepted: &AcceptedSubmission) {
    let mut conn = raw_conn(ctx).await;
    let mut txn = sqlx::Connection::begin(&mut conn).await.expect("begin");
    let rows = PgTaskRepository
        .complete(
            &mut txn,
            harness.task.id(),
            harness.task.claim_token().expect("token"),
        )
        .await
        .expect("complete task");
    assert_eq!(rows, 1);
    commit_loop_terminal(&mut txn, &harness.thread, accepted)
        .await
        .expect("loop terminal");
    txn.commit().await.expect("commit");
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// The loop completes only through `submit_result`: a tool turn, then an
/// accepted submission, with per-turn attribution, the spend projection
/// accumulating cache classes separately, the progress reset at the task
/// row, and the composed terminal leaving the full log.
#[tokio::test]
async fn test_loop_completes_through_submit_with_per_turn_attribution() {
    let _guard = serial_lock().await;
    let ctx = test_context().await;

    let mut second = submit_response("call_1", TOOL_ITEM_ID);
    second.usage.cache_read_tokens = 7;
    second.usage.cache_write_tokens = 3;
    let harness = loop_harness(
        ctx,
        "loop-submit",
        vec![
            a_tool_call_response(&[("call_0", "lookup_note", serde_json::json!({}))]),
            second,
        ],
    )
    .await;
    {
        let mut conn = raw_conn(ctx).await;
        set_retry_count(&mut conn, harness.task.id(), 3).await;
    }

    let attribution = harness.attribution();
    let outcome = run_turn_loop(harness.deps(
        an_opening(),
        &attribution,
        &MembershipPipeline,
        ExecutionBudgets::default(),
    ))
    .await
    .expect("the loop runs");
    let LoopOutcome::Submitted(accepted) = outcome else {
        panic!("the loop completes through submit_result, got {outcome:?}");
    };
    assert_eq!(accepted.payload["claim_id"], TOOL_ITEM_ID);
    assert_eq!(accepted.tool_call_id, "call_1");

    compose_terminal(ctx, &harness, &accepted).await;

    let mut conn = raw_conn(ctx).await;
    let thread = PgAgentThreadRepository
        .find_by_id(&mut conn, harness.thread.id())
        .await
        .expect("find thread")
        .expect("present");
    assert_eq!(thread.status(), AgentThreadStatus::Completed);

    // The committed-spend projection counted both turns, cache classes
    // separately.
    let spend: ExecutionSpend =
        serde_json::from_value(thread.execution_spend().expect("projected").clone())
            .expect("spend parses");
    assert_eq!(spend.turns, 2);
    assert_eq!(spend.input_tokens, 200);
    assert_eq!(spend.output_tokens, 100);
    assert_eq!(spend.cache_read_tokens, 7);
    assert_eq!(spend.cache_write_tokens, 3);

    // The log: opening, tool turn, its result, submit turn, submission.
    let records = PgAgentThreadRecordRepository
        .find_by_thread(&mut conn, thread.id())
        .await
        .expect("log");
    let kinds: Vec<_> = records.iter().map(|r| r.kind()).collect();
    assert_eq!(
        kinds,
        vec![
            AgentThreadRecordKind::Input,
            AgentThreadRecordKind::AssistantMessage,
            AgentThreadRecordKind::ToolResult,
            AgentThreadRecordKind::AssistantMessage,
            AgentThreadRecordKind::Submission,
        ],
    );
    assert_eq!(records[2].tool_call_id(), Some("call_0"));
    assert_eq!(records[2].requesting_seq(), Some(records[1].seq()));
    assert_eq!(records[4].content()["payload"]["claim_id"], TOOL_ITEM_ID);
    assert_eq!(records[4].content()["tool_call_id"], "call_1");

    // Each turn's ledger row linked to its own assistant record.
    let usage_rows = PgTokenUsageRepository
        .find_by_job_id(&mut conn, harness.job_id)
        .await
        .expect("ledger rows");
    let linked: Vec<_> = usage_rows
        .iter()
        .filter_map(|row| row.agent_thread_record_id())
        .collect();
    assert_eq!(
        linked,
        vec![records[1].id(), records[3].id()],
        "per-turn attribution links each call to its own turn's record",
    );

    // The progress reset rode the record commits.
    let task = PgTaskRepository
        .find_by_id(&mut conn, harness.task.id())
        .await
        .expect("task");
    assert_eq!(task.retry_count(), 0, "record commits reset progress");

    // The pump saw both calls and was never aborted.
    assert_eq!(harness.pump.started.load(Ordering::SeqCst), 2);
    assert_eq!(harness.pump.finished.load(Ordering::SeqCst), 2);
    assert!(harness.pump.deltas.load(Ordering::SeqCst) >= 2);
    assert_eq!(harness.pump.aborts.load(Ordering::SeqCst), 0);

    teardown(ctx).await;
}

/// A submission referencing an id the model was never shown bounces as
/// an error-shaped tool result, the diagnostics reach the next turn's
/// conversation, and the corrected submission completes the loop.
#[tokio::test]
async fn test_bounced_submission_returns_in_band_and_the_loop_continues() {
    let _guard = serial_lock().await;
    let ctx = test_context().await;
    let harness = loop_harness(
        ctx,
        "loop-bounce",
        vec![
            submit_response("call_0", UNSEEN_ITEM_ID),
            submit_response("call_1", OPENING_ITEM_ID),
        ],
    )
    .await;

    let attribution = harness.attribution();
    let outcome = run_turn_loop(harness.deps(
        an_opening(),
        &attribution,
        &MembershipPipeline,
        ExecutionBudgets::default(),
    ))
    .await
    .expect("the loop runs");
    assert!(matches!(outcome, LoopOutcome::Submitted(_)));

    let mut conn = raw_conn(ctx).await;
    let records = PgAgentThreadRecordRepository
        .find_by_thread(&mut conn, harness.thread.id())
        .await
        .expect("log");
    let bounce = records
        .iter()
        .find(|r| r.kind() == AgentThreadRecordKind::ToolResult)
        .expect("the bounce committed a result");
    assert_eq!(bounce.content()["is_error"], true);
    assert!(
        bounce.content()["output"]
            .as_str()
            .expect("output text")
            .contains(UNSEEN_ITEM_ID),
        "the diagnostics name the refused id",
    );

    // The second turn's conversation carries the diagnostics in-band.
    let requests = harness.provider.completion_history();
    assert_eq!(requests.len(), 2);
    let last = requests[1].messages.last().expect("messages");
    assert!(
        matches!(last, tribal_inference::Message::Tool { content, .. } if content.contains(UNSEEN_ITEM_ID)),
        "the bounce reaches the model as a tool result",
    );

    teardown(ctx).await;
}

/// A model-recoverable tool failure returns in-band as an error-shaped
/// result and the loop continues to a successful submission.
#[tokio::test]
async fn test_recoverable_tool_failure_returns_in_band() {
    let _guard = serial_lock().await;
    let ctx = test_context().await;
    let mut harness = loop_harness(
        ctx,
        "loop-recoverable",
        vec![
            a_tool_call_response(&[("call_0", "flaky_lookup", serde_json::json!({}))]),
            submit_response("call_1", OPENING_ITEM_ID),
        ],
    )
    .await;
    harness
        .registry
        .register(Arc::new(FailingTool {
            descriptor: a_descriptor("flaky_lookup"),
            prepare_system: false,
        }))
        .expect("registers");

    let attribution = harness.attribution();
    let outcome = run_turn_loop(harness.deps(
        an_opening(),
        &attribution,
        &MembershipPipeline,
        ExecutionBudgets::default(),
    ))
    .await
    .expect("the loop runs");
    assert!(matches!(outcome, LoopOutcome::Submitted(_)));

    let mut conn = raw_conn(ctx).await;
    let records = PgAgentThreadRecordRepository
        .find_by_thread(&mut conn, harness.thread.id())
        .await
        .expect("log");
    let result = records
        .iter()
        .find(|r| r.kind() == AgentThreadRecordKind::ToolResult)
        .expect("the failure committed a result");
    assert_eq!(result.content()["is_error"], true);
    assert!(
        result.content()["output"]
            .as_str()
            .expect("output")
            .contains("nothing matched"),
        "the rendered diagnostic is the model-facing output",
    );

    teardown(ctx).await;
}

/// A tool's system failure aborts the turn into the stage-error path:
/// no result record commits, the conversation never sees it, and the
/// thread stays live for the retry.
#[tokio::test]
async fn test_system_tool_failure_routes_to_the_stage_error_path() {
    let _guard = serial_lock().await;
    let ctx = test_context().await;
    let mut harness = loop_harness(
        ctx,
        "loop-system",
        vec![a_tool_call_response(&[(
            "call_0",
            "broken_lookup",
            serde_json::json!({}),
        )])],
    )
    .await;
    harness
        .registry
        .register(Arc::new(FailingTool {
            descriptor: a_descriptor("broken_lookup"),
            prepare_system: true,
        }))
        .expect("registers");

    let attribution = harness.attribution();
    let err = run_turn_loop(harness.deps(
        an_opening(),
        &attribution,
        &MembershipPipeline,
        ExecutionBudgets::default(),
    ))
    .await
    .expect_err("a system failure aborts the turn");
    assert!(matches!(
        err,
        tribal_agent_runtime::AgentRuntimeError::ToolExecution { .. }
    ));

    let mut conn = raw_conn(ctx).await;
    let records = PgAgentThreadRecordRepository
        .find_by_thread(&mut conn, harness.thread.id())
        .await
        .expect("log");
    assert!(
        records
            .iter()
            .all(|r| r.kind() != AgentThreadRecordKind::ToolResult),
        "a system failure never becomes a conversation record",
    );
    let thread = PgAgentThreadRepository
        .find_by_id(&mut conn, harness.thread.id())
        .await
        .expect("find")
        .expect("present");
    assert_eq!(
        thread.status(),
        AgentThreadStatus::Running,
        "the stage-error path owns the disposition",
    );

    teardown(ctx).await;
}

/// Re-entry after a mid-turn crash executes exactly the unanswered tool
/// calls under the fence and never re-issues the turn's inference; the
/// answered call is not executed twice.
#[tokio::test]
async fn test_mid_turn_crash_resume_executes_pending_calls_only() {
    let _guard = serial_lock().await;
    let ctx = test_context().await;
    // Only the post-resume submit turn is queued: any re-issued
    // inference for the crashed turn would consume it early and panic
    // the mock on exhaustion.
    let harness = loop_harness(
        ctx,
        "loop-crash-resume",
        vec![submit_response("call_2", TOOL_ITEM_ID)],
    )
    .await;

    // Build the crash state by hand: the committed input, an assistant
    // turn with two calls, and exactly one answered.
    {
        let mut conn = raw_conn(ctx).await;
        tribal_agent_runtime::begin_turn(
            &mut conn,
            &harness.thread,
            tribal_agent_runtime::DrivingClaim::stage(
                harness.task.id(),
                harness.task.claim_token().expect("token"),
            ),
            None,
            an_opening(),
        )
        .await
        .expect("input commits");
        let seq = PgAgentThreadRecordRepository
            .next_seq(&mut conn, harness.thread.id())
            .await
            .expect("seq");
        PgAgentThreadRecordRepository
            .append(
                &mut conn,
                &tribal_db::NewAgentThreadRecord::builder()
                    .thread_id(harness.thread.id())
                    .seq(seq)
                    .kind(AgentThreadRecordKind::AssistantMessage)
                    .content(serde_json::json!({
                        "text": "",
                        "tool_calls": [
                            {"id": "call_0", "name": "lookup_note", "arguments": {}},
                            {"id": "call_1", "name": "lookup_note", "arguments": {}},
                        ],
                    }))
                    .build(),
            )
            .await
            .expect("assistant record");
        let result_seq = PgAgentThreadRecordRepository
            .next_seq(&mut conn, harness.thread.id())
            .await
            .expect("seq");
        PgAgentThreadRecordRepository
            .append(
                &mut conn,
                &tribal_db::NewAgentThreadRecord::builder()
                    .thread_id(harness.thread.id())
                    .seq(result_seq)
                    .kind(AgentThreadRecordKind::ToolResult)
                    .requesting_seq(Some(seq))
                    .tool_call_id(Some("call_0".to_owned()))
                    .content(serde_json::json!({"output": "already answered", "is_error": false}))
                    .build(),
            )
            .await
            .expect("first result");
    }

    let attribution = harness.attribution();
    let outcome = run_turn_loop(harness.deps(
        an_opening(),
        &attribution,
        &MembershipPipeline,
        ExecutionBudgets::default(),
    ))
    .await
    .expect("the loop resumes");
    assert!(matches!(outcome, LoopOutcome::Submitted(_)));

    assert_eq!(
        harness.provider.call_count(),
        1,
        "re-entry never re-issues the crashed turn's inference",
    );

    let mut conn = raw_conn(ctx).await;
    let records = PgAgentThreadRecordRepository
        .find_by_thread(&mut conn, harness.thread.id())
        .await
        .expect("log");
    let answers: Vec<_> = records
        .iter()
        .filter(|r| r.kind() == AgentThreadRecordKind::ToolResult)
        .map(|r| r.tool_call_id().expect("call id").to_owned())
        .collect();
    assert_eq!(
        answers,
        vec!["call_0".to_owned(), "call_1".to_owned()],
        "exactly the unanswered call executed; the answered one did not run twice",
    );

    teardown(ctx).await;
}

/// The second turn's request repeats the first turn's conversation as an
/// exact prefix — the cache-discipline contract.
#[tokio::test]
async fn test_second_turn_request_prefix_is_byte_identical() {
    let _guard = serial_lock().await;
    let ctx = test_context().await;
    let harness = loop_harness(
        ctx,
        "loop-prefix",
        vec![
            a_tool_call_response(&[("call_0", "lookup_note", serde_json::json!({}))]),
            submit_response("call_1", TOOL_ITEM_ID),
        ],
    )
    .await;

    let attribution = harness.attribution();
    let outcome = run_turn_loop(harness.deps(
        an_opening(),
        &attribution,
        &MembershipPipeline,
        ExecutionBudgets::default(),
    ))
    .await
    .expect("the loop runs");
    assert!(matches!(outcome, LoopOutcome::Submitted(_)));

    let requests = harness.provider.completion_history();
    assert_eq!(requests.len(), 2);
    assert_eq!(requests[0].system, requests[1].system);
    assert_eq!(
        requests[1].messages[..requests[0].messages.len()],
        requests[0].messages[..],
        "the prior turn's conversation is an exact prefix of the next request",
    );
    assert_eq!(requests[0].tools, requests[1].tools);

    teardown(ctx).await;
}

/// The turn cap fails the thread as an outcome the caller owns: the
/// loop commits nothing further and reports the cap.
#[tokio::test]
async fn test_turn_cap_returns_a_budget_failure() {
    let _guard = serial_lock().await;
    let ctx = test_context().await;
    let harness = loop_harness(
        ctx,
        "loop-turn-cap",
        vec![a_completion_response("thinking, no tools")],
    )
    .await;

    let attribution = harness.attribution();
    let outcome = run_turn_loop(harness.deps(
        an_opening(),
        &attribution,
        &MembershipPipeline,
        ExecutionBudgets::builder().max_turns(Some(1)).build(),
    ))
    .await
    .expect("the loop runs");
    assert!(
        matches!(
            outcome,
            LoopOutcome::BudgetFailed {
                cause: BudgetFailure::TurnCap { turns: 1, cap: 1 },
            }
        ),
        "got {outcome:?}",
    );

    let mut conn = raw_conn(ctx).await;
    let thread = PgAgentThreadRepository
        .find_by_id(&mut conn, harness.thread.id())
        .await
        .expect("find")
        .expect("present");
    assert_eq!(
        thread.status(),
        AgentThreadStatus::Running,
        "the failure's commit belongs to the caller",
    );

    teardown(ctx).await;
}

/// Ledger-side spend exhaustion suspends the thread with the durable
/// re-check count, blocks the task, and aborts the pump before the
/// suspension commits.
#[tokio::test]
async fn test_spend_exhaustion_suspends_with_a_durable_recheck_count() {
    let _guard = serial_lock().await;
    let ctx = test_context().await;
    let harness = loop_harness(
        ctx,
        "loop-spend",
        vec![a_completion_response("an expensive thought")],
    )
    .await;

    let attribution = harness.attribution();
    let outcome = run_turn_loop(
        harness.deps(
            an_opening(),
            &attribution,
            &MembershipPipeline,
            ExecutionBudgets::builder()
                .max_total_tokens(Some(1))
                .build(),
        ),
    )
    .await
    .expect("the loop runs");
    assert!(matches!(outcome, LoopOutcome::Suspended), "got {outcome:?}");

    let mut conn = raw_conn(ctx).await;
    let thread = PgAgentThreadRepository
        .find_by_id(&mut conn, harness.thread.id())
        .await
        .expect("find")
        .expect("present");
    assert_eq!(thread.status(), AgentThreadStatus::Suspended);
    assert_eq!(
        thread.suspension(),
        Some(&AgentThreadSuspension::BudgetExhaustion {
            unchanged_rechecks: 0,
        }),
    );
    assert!(thread.wake_at().is_some(), "the re-check wake is scheduled");

    let task = PgTaskRepository
        .find_by_id(&mut conn, harness.task.id())
        .await
        .expect("task");
    assert_eq!(task.status(), TaskStatus::Blocked);
    assert_eq!(
        harness.pump.aborts.load(Ordering::SeqCst),
        1,
        "the pump stops before the suspension clears the claim",
    );

    teardown(ctx).await;
}

/// A budget wake whose admission finds headroom returned resumes the
/// loop into a normal turn; budgets are the resumption's, not the
/// suspension's.
#[tokio::test]
async fn test_headroom_returning_resumes_the_loop_to_completion() {
    let _guard = serial_lock().await;
    let ctx = test_context().await;
    let harness = loop_harness(
        ctx,
        "loop-headroom",
        vec![
            a_completion_response("an expensive thought"),
            submit_response("call_0", OPENING_ITEM_ID),
        ],
    )
    .await;

    let attribution = harness.attribution();
    let outcome = run_turn_loop(
        harness.deps(
            an_opening(),
            &attribution,
            &MembershipPipeline,
            ExecutionBudgets::builder()
                .max_total_tokens(Some(1))
                .build(),
        ),
    )
    .await
    .expect("the loop runs");
    assert!(matches!(outcome, LoopOutcome::Suspended));

    // The wake: the sweep's resolution shape, then the task re-queues
    // and a fresh claim re-enters the loop.
    let mut conn = raw_conn(ctx).await;
    let resolution = serde_json::json!({
        "cause": BUDGET_RECHECK_CAUSE,
        "fired_at": chrono::Utc::now(),
        "unchanged_rechecks": 0,
    });
    let resolved = resolve_stage_thread(&mut conn, harness.thread.id(), &resolution)
        .await
        .expect("resolve");
    assert!(matches!(
        resolved,
        tribal_agent_runtime::ResolveOutcome::Woken
    ));
    let reclaimed = PgTaskRepository
        .claim(&mut conn, 1, "loop-test-resume")
        .await
        .expect("re-claim");
    let task = reclaimed.first().expect("the woken task claims").clone();
    let thread = PgAgentThreadRepository
        .find_by_id(&mut conn, harness.thread.id())
        .await
        .expect("find")
        .expect("present");
    drop(conn);

    // Budgets re-resolve at resume: the raised cap is this claim's
    // admission basis, so the wake's re-check passes.
    let resumed = LoopHarness {
        task,
        thread,
        ..harness
    };
    let attribution = resumed.attribution();
    let outcome = run_turn_loop(
        resumed.deps(
            an_opening(),
            &attribution,
            &MembershipPipeline,
            ExecutionBudgets::builder()
                .max_total_tokens(Some(1_000_000))
                .build(),
        ),
    )
    .await
    .expect("the loop resumes");
    assert!(
        matches!(outcome, LoopOutcome::Submitted(_)),
        "got {outcome:?}"
    );

    teardown(ctx).await;
}

/// A wake whose re-check still finds the budget exhausted re-suspends
/// with the count advanced; at the bound the loop reports the terminal
/// budget failure instead.
#[tokio::test]
async fn test_unchanged_rechecks_advance_and_run_dry_at_the_bound() {
    let _guard = serial_lock().await;
    let ctx = test_context().await;
    let harness = loop_harness(
        ctx,
        "loop-recheck",
        vec![a_completion_response("an expensive thought")],
    )
    .await;
    let tight = ExecutionBudgets::builder()
        .max_total_tokens(Some(1))
        .build();

    let attribution = harness.attribution();
    let outcome =
        run_turn_loop(harness.deps(an_opening(), &attribution, &MembershipPipeline, tight))
            .await
            .expect("the loop runs");
    assert!(matches!(outcome, LoopOutcome::Suspended));

    // First wake: still exhausted — the suspension's count advances.
    let mut conn = raw_conn(ctx).await;
    let wake = |count: u32| {
        serde_json::json!({
            "cause": BUDGET_RECHECK_CAUSE,
            "fired_at": chrono::Utc::now(),
            "unchanged_rechecks": count,
        })
    };
    resolve_stage_thread(&mut conn, harness.thread.id(), &wake(0))
        .await
        .expect("resolve");
    let reclaimed = PgTaskRepository
        .claim(&mut conn, 1, "loop-test-recheck")
        .await
        .expect("re-claim");
    let task = reclaimed.first().expect("claims").clone();
    let thread = PgAgentThreadRepository
        .find_by_id(&mut conn, harness.thread.id())
        .await
        .expect("find")
        .expect("present");
    drop(conn);

    let resumed = LoopHarness {
        task,
        thread,
        ..harness
    };
    let attribution = resumed.attribution();
    let outcome =
        run_turn_loop(resumed.deps(an_opening(), &attribution, &MembershipPipeline, tight))
            .await
            .expect("the loop re-checks");
    assert!(matches!(outcome, LoopOutcome::Suspended));
    {
        let mut conn = raw_conn(ctx).await;
        let thread = PgAgentThreadRepository
            .find_by_id(&mut conn, resumed.thread.id())
            .await
            .expect("find")
            .expect("present");
        assert_eq!(
            thread.suspension(),
            Some(&AgentThreadSuspension::BudgetExhaustion {
                unchanged_rechecks: 1,
            }),
            "the unchanged re-check advanced the durable count",
        );
    }

    // A wake carrying the bound's predecessor runs the re-checks dry.
    let mut conn = raw_conn(ctx).await;
    resolve_stage_thread(&mut conn, resumed.thread.id(), &wake(RECHECK.bound - 1))
        .await
        .expect("resolve");
    let reclaimed = PgTaskRepository
        .claim(&mut conn, 1, "loop-test-dry")
        .await
        .expect("re-claim");
    let task = reclaimed.first().expect("claims").clone();
    let thread = PgAgentThreadRepository
        .find_by_id(&mut conn, resumed.thread.id())
        .await
        .expect("find")
        .expect("present");
    drop(conn);

    let dry = LoopHarness {
        task,
        thread,
        ..resumed
    };
    let attribution = dry.attribution();
    let outcome = run_turn_loop(dry.deps(an_opening(), &attribution, &MembershipPipeline, tight))
        .await
        .expect("the loop re-checks");
    assert!(
        matches!(
            outcome,
            LoopOutcome::BudgetFailed {
                cause: BudgetFailure::RecheckExhausted { rechecks, .. },
            } if rechecks == RECHECK.bound
        ),
        "got {outcome:?}",
    );

    teardown(ctx).await;
}

/// A durable cancellation intent stops the loop at the boundary before
/// any inference.
#[tokio::test]
async fn test_cancel_intent_stops_the_loop_before_inference() {
    let _guard = serial_lock().await;
    let ctx = test_context().await;
    // No responses queued: any inference call would panic the mock.
    let harness = loop_harness(ctx, "loop-cancel", vec![]).await;
    {
        let mut conn = raw_conn(ctx).await;
        PgAgentThreadRepository
            .record_cancel_intent(&mut conn, harness.thread.id(), "operator:test")
            .await
            .expect("intent");
    }

    let attribution = harness.attribution();
    let outcome = run_turn_loop(harness.deps(
        an_opening(),
        &attribution,
        &MembershipPipeline,
        ExecutionBudgets::default(),
    ))
    .await
    .expect("the loop runs");
    assert!(matches!(outcome, LoopOutcome::CancelIntent));
    assert_eq!(harness.provider.call_count(), 0);

    teardown(ctx).await;
}

/// The admission check under empty caps issues no query at all: it
/// succeeds even inside an aborted transaction, where any query errors.
#[tokio::test]
async fn test_admission_with_empty_caps_issues_no_query() {
    let _guard = serial_lock().await;
    let ctx = test_context().await;
    let thread = tribal_test_utils::an_agent_thread().build();

    let mut conn = raw_conn(ctx).await;
    let mut txn = sqlx::Connection::begin(&mut conn).await.expect("begin");
    let _ = sqlx::query("SELECT this_is_not_sql")
        .execute(&mut *txn)
        .await;

    let admitted = admit_inference(&mut txn, &thread, &ExecutionBudgets::default())
        .await
        .expect("no caps must touch nothing");
    assert_eq!(admitted, Admission::Admitted);

    let err = admit_inference(
        &mut txn,
        &thread,
        &ExecutionBudgets::builder()
            .max_total_tokens(Some(1))
            .build(),
    )
    .await
    .expect_err("a capped admission queries, and the aborted transaction proves it");
    assert!(matches!(
        err,
        tribal_agent_runtime::AgentRuntimeError::Database { .. }
    ));
}
