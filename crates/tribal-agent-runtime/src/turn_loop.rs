//! The turn loop: the agentic generalisation of the one-shot bracket.
//!
//! At every entry the loop derives its position from the committed tail:
//! fresh, mid-turn crash, tool-batch crash, and budget wake all
//! resolve to a position before anything executes. The conversation an
//! inference call receives is the recorded projection plus in-memory
//! appends, never a re-rendering, so the byte-stable prefix and the
//! pinned nonce ride the stored input record exactly as one-shot resume
//! does. Per turn, in order, each numbered write its own committed
//! transaction: boundary checks (the durable cancel intent, the turn
//! cap, the execution deadline, ledger-side token admission), the
//! streamed inference call with no transaction held across it, the
//! assistant record commit (with its per-turn ledger link, spend
//! projection, and the driving task's progress reset), then each tool
//! call sequentially under the two-phase contract and the result fence.
//!
//! Completion happens only through the distinguished `submit_result`
//! entry: an accepted submission exits to the caller, which composes the
//! stage terminal with [`commit_loop_terminal`]; a bounced one returns
//! in-band as an error-shaped tool result and the loop continues.

use std::{collections::HashSet, sync::Arc};

use async_trait::async_trait;
use chrono::Utc;
use futures_util::StreamExt;
use sqlx::PgConnection;
use tracing::Instrument;
use tribal_db::{
    AgentThreadRecordRepository, AgentThreadRepository, DbError, NewAgentThreadRecord,
    PgAgentThreadRecordRepository, PgAgentThreadRepository, PgTaskRepository,
    PgTokenUsageRepository, TaskRepository, TokenUsageRepository,
};
use tribal_domain::{
    AgentThread, AgentThreadRecord, AgentThreadRecordKind, AgentThreadRecordSeq, AgentThreadStatus,
    AgentThreadSuspension, AgentThreadTerminal, CompletionResponse, ExecutionBudgets,
    ExecutionSpend, InferenceEvent, StageExecutorKind, TaskId, TaskType, ToolDescriptor,
    ToolFailure, gen_ai, span_attrs,
};
use tribal_inference::{
    CompletionRequest, InferenceGateway, Message, PermitWait, ToolWireDefinition, UsageAttribution,
};
use tribal_telemetry::{MetricsRecorder, current_span_id, current_trace_id};

use crate::{
    AgentRuntimeError, SuspendOutcome, ToolRegistry,
    driver::{ChildLaunch, SuspendWithChildOutcome, suspend_with_child},
    suspend_stage_thread,
    turn::{
        AssistantContent, DrivingClaim, RecordedToolCall, RecordedUsage, RenderedConversation,
        begin_turn, is_rendered_conversation,
    },
    txn::{begin, commit},
};

/// The distinguished completion tool: calling it is the only way an
/// agentic stage completes.
pub const SUBMIT_RESULT_TOOL: &str = "submit_result";

/// The bookkeeping cause a budget-exhaustion wake's resolution record
/// carries, discriminating it from a plain timer wake.
pub const BUDGET_RECHECK_CAUSE: &str = "budget_recheck";

/// The `content_kind` tag of an injected conversational message: a
/// system-authored user turn the model reads (post-acceptance drift
/// diagnostics, a human resolution). Distinct from the rendered opening
/// and from bookkeeping resolutions.
pub(crate) const INJECTED_MESSAGE_KIND: &str = "injected_message";

// ---------------------------------------------------------------------------
// Record content shapes
// ---------------------------------------------------------------------------

/// A tool-result record's content: the model-facing output and the
/// explicit error marker the contract requires.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ToolResultContent {
    /// What the model reads: the serialised result, or the rendered
    /// recoverable diagnostic when `is_error` is set.
    pub output: String,
    /// Whether this result is an error-shaped diagnostic.
    pub is_error: bool,
}

/// A system-authored user turn the model reads: post-acceptance drift
/// diagnostics, or a human resolution. Tagged so the projection renders
/// it as a conversation message rather than treating it as bookkeeping.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "content_kind", rename = "injected_message")]
struct InjectedMessage {
    content: String,
}

/// A submission record's content: the accepted, validated payload with
/// the call it answers as provenance.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct SubmissionContent {
    /// The validated payload as submitted.
    pub payload: serde_json::Value,
    /// The assistant message whose `submit_result` call this accepts.
    pub requesting_seq: AgentThreadRecordSeq,
    /// The accepted call's identifier.
    pub tool_call_id: String,
}

// ---------------------------------------------------------------------------
// The heartbeat pump
// ---------------------------------------------------------------------------

/// How the loop keeps the driving task's lease alive, phase-aware.
///
/// The worker's handle implements this over its heartbeat machinery;
/// the loop only names the moments: streamed deltas during inference
/// (the timer suppressed), the return to timer-driven beating between
/// calls, and the stop before a suspension commits. A cleared claim
/// must never race a spurious ownership-lost signal from a beat in
/// flight.
pub trait HeartbeatPump: Send + Sync {
    /// An inference call is starting; deltas will carry liveness.
    fn inference_started(&self);
    /// A streamed event arrived; the implementation debounces.
    fn stream_delta(&self);
    /// The inference call finished; timer-driven beating resumes.
    fn inference_finished(&self);
    /// Stop beating entirely: a suspension is about to commit.
    fn abort(&self);
}

// ---------------------------------------------------------------------------
// The submission seam
// ---------------------------------------------------------------------------

/// Everything the model was actually shown as graph content: the
/// opening conversation's rendered messages and every non-error tool
/// result. The ID-membership validation resolves references against
/// this corpus: what the model saw, not what the system could derive.
///
/// Containment is substring containment: identifiers are prefixed
/// UUIDs, unmistakable in any rendering, so no per-stage extraction
/// schema is needed. Error-shaped results are excluded by construction:
/// a diagnostic that echoes an unknown or out-of-scope id must not
/// launder it into the seen set.
#[derive(Debug, Default)]
pub struct SeenCorpus {
    texts: Vec<String>,
}

impl SeenCorpus {
    /// Records one piece of model-shown content.
    pub fn push(&mut self, text: impl Into<String>) {
        self.texts.push(text.into());
    }

    /// Whether the id appears in anything the model was shown.
    #[must_use]
    pub fn contains(&self, id: &str) -> bool {
        self.texts.iter().any(|text| text.contains(id))
    }
}

/// What one `submit_result` evaluation concluded.
#[derive(Debug, Clone, PartialEq)]
pub enum SubmissionOutcome {
    /// The submission parsed and passed every validator.
    Accepted {
        /// The validated payload.
        payload: serde_json::Value,
    },
    /// A validator rejected it; the diagnostics return in-band and the
    /// loop continues.
    Bounced {
        /// The model-facing rejection, held to the prompt bar.
        diagnostics: String,
    },
}

impl SubmissionOutcome {
    /// The telemetry label for a submission evaluation, content-free.
    fn label(&self) -> &'static str {
        match self {
            Self::Accepted { .. } => span_attrs::SUBMISSION_OUTCOME_ACCEPTED,
            Self::Bounced { .. } => span_attrs::SUBMISSION_OUTCOME_BOUNCED,
        }
    }
}

/// The stage's submission pipeline: schema parse, then the deterministic
/// validators, in order, never a model call.
///
/// Implementations resolve ID references against the corpus (what the
/// model actually saw) and re-check graph preconditions (the matched
/// item still exists in the stage's scope; its supersedence state is
/// unchanged) against the live connection. Model-addressable rejections
/// are [`SubmissionOutcome::Bounced`]; a recoverable failure renders
/// in-band; a system failure routes to the stage-error path.
#[async_trait]
pub trait SubmissionPipeline: Send + Sync {
    /// Evaluates one `submit_result` call's arguments.
    ///
    /// # Errors
    ///
    /// Returns [`ToolFailure`] per the two-class routing contract.
    async fn evaluate(
        &self,
        conn: &mut PgConnection,
        thread: &AgentThread,
        corpus: &SeenCorpus,
        arguments: &serde_json::Value,
    ) -> Result<SubmissionOutcome, ToolFailure>;

    /// Decides whether a validators-accepted submission is verified by a
    /// child execution, and if so renders that child's opening.
    ///
    /// `None` commits the submission directly: the default, and the only
    /// behaviour for a binding with no verifier. `Some` launches the
    /// verifier as a fresh-context child whose verdict the loop awaits.
    ///
    /// # Errors
    ///
    /// Returns [`ToolFailure`] per the two-class routing contract.
    async fn verifier_launch(
        &self,
        _conn: &mut PgConnection,
        _thread: &AgentThread,
        _payload: &serde_json::Value,
    ) -> Result<Option<VerifierLaunch>, ToolFailure> {
        Ok(None)
    }
}

/// The verifier child a validators-accepted submission launches: its
/// binding, its stage, and the opening the verifier reads: the
/// submission and the rubric, never the submitting thread's reasoning.
#[derive(Debug, Clone)]
pub struct VerifierLaunch {
    /// The content-addressed binding the verifier child runs under.
    pub binding_version_id: tribal_domain::AgentBindingVersionId,
    /// The stage the verifier child executes (its parent's).
    pub pipeline_stage: TaskType,
    /// The verifier child's rendered opening.
    pub child_input: RenderedConversation,
}

/// A verifier's verdict, the envelope every verifier binding produces and
/// the loop reads: acceptance commits the submission, rejection returns
/// the critique to the submitting loop, which continues.
#[derive(
    Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize, schemars::JsonSchema,
)]
#[schemars(
    description = "Your verdict on the submission. Accept a sound submission; reject an unsound \
    one with a critique the submitting agent can act on."
)]
pub struct VerdictContent {
    /// Whether the submission is sound.
    #[schemars(description = "Whether the submission is sound and should be committed.")]
    pub accepted: bool,
    /// The rejection's reasoning, returned to the loop as context.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(
        description = "When rejecting, the specific defect the submitting agent must address. \
        Omit when accepting."
    )]
    pub critique: Option<String>,
}

/// The JSON Schema a verifier child's structured output is constrained
/// to: a [`VerdictContent`]. A binding-hash input for a verifier
/// definition, and the response-format schema the child's call carries.
///
/// # Panics
///
/// Panics only if the derived schema cannot serialise to JSON, which the
/// `schemars` derive makes impossible.
#[must_use]
pub fn verdict_schema() -> serde_json::Value {
    serde_json::to_value(schemars::schema_for!(VerdictContent))
        .expect("schema_for! produces serialisable output")
}

/// An accepted submission: what the caller's stage terminal commits.
#[derive(Debug, Clone, PartialEq)]
pub struct AcceptedSubmission {
    /// The validated payload.
    pub payload: serde_json::Value,
    /// The assistant message whose call was accepted.
    pub requesting_seq: AgentThreadRecordSeq,
    /// The accepted call's identifier.
    pub tool_call_id: String,
}

// ---------------------------------------------------------------------------
// Budgets
// ---------------------------------------------------------------------------

/// What the ledger-side admission check concluded.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Admission {
    /// Headroom exists (or no cap applies).
    Admitted,
    /// The thread's ledger spend reached its cap.
    Exhausted {
        /// Tokens the ledger accounts to the thread.
        spent: u64,
        /// The binding's cap.
        cap: u64,
    },
}

/// Checks the thread's ledger spend against its token cap before an
/// inference call: the admission contract both executors share.
///
/// An absent cap admits without touching the database at all: the
/// default path gains no new failure surface.
///
/// # Errors
///
/// Returns [`AgentRuntimeError::Database`] on database errors.
pub async fn admit_inference(
    conn: &mut PgConnection,
    thread: &AgentThread,
    budgets: &ExecutionBudgets,
) -> Result<Admission, AgentRuntimeError> {
    let Some(cap) = budgets.max_total_tokens else {
        return Ok(Admission::Admitted);
    };
    let spent = PgTokenUsageRepository
        .sum_thread_spend(conn, thread.id())
        .await
        .map_err(|source| AgentRuntimeError::database("summing the thread's spend", source))?;
    if spent >= cap {
        Ok(Admission::Exhausted { spent, cap })
    } else {
        Ok(Admission::Admitted)
    }
}

/// How budget-exhaustion suspensions re-check: the wake delay and the
/// bound on consecutive unchanged re-checks before the thread fails.
#[derive(Debug, Clone, Copy)]
pub struct RecheckPolicy {
    /// Seconds until a budget suspension's wake.
    pub delay_seconds: u32,
    /// Consecutive unchanged re-checks that fail the thread.
    pub bound: u32,
}

/// What the pre-call budget enforcement decided.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdmissionDecision {
    /// Headroom exists; the call proceeds.
    Proceed,
    /// The budget is exhausted; the caller commits the suspension with
    /// this re-check count.
    Suspend {
        /// Consecutive wakes whose re-check found the budget unchanged.
        unchanged_rechecks: u32,
    },
    /// The bounded re-checks ran dry; the caller fails the thread.
    Exhausted(BudgetFailure),
}

impl AdmissionDecision {
    /// The telemetry label for a pre-call admission decision, shared by the
    /// loop and the one-shot bracket so both count the same wire strings.
    #[must_use]
    pub fn label(&self) -> &'static str {
        match self {
            Self::Proceed => span_attrs::BUDGET_DECISION_ADMITTED,
            Self::Suspend { .. } => span_attrs::BUDGET_DECISION_SUSPEND,
            Self::Exhausted(_) => span_attrs::BUDGET_DECISION_FAILED,
        }
    }
}

/// Runs the token admission and folds in the bounded-recheck
/// accounting: the decision both executors share before an inference
/// call. The caller commits the suspension (or the failure), so each
/// path keeps its own heartbeat discipline around the commit.
///
/// # Errors
///
/// Returns [`AgentRuntimeError::Database`] on database errors.
pub async fn decide_admission(
    conn: &mut PgConnection,
    thread: &AgentThread,
    budgets: &ExecutionBudgets,
    recheck: RecheckPolicy,
    carried_rechecks: Option<u32>,
) -> Result<AdmissionDecision, AgentRuntimeError> {
    match admit_inference(conn, thread, budgets).await? {
        Admission::Admitted => Ok(AdmissionDecision::Proceed),
        Admission::Exhausted { spent, cap } => match carried_rechecks {
            Some(count) => {
                let unchanged = count.saturating_add(1);
                if unchanged >= recheck.bound {
                    Ok(AdmissionDecision::Exhausted(
                        BudgetFailure::RecheckExhausted {
                            spent,
                            cap,
                            rechecks: unchanged,
                        },
                    ))
                } else {
                    Ok(AdmissionDecision::Suspend {
                        unchanged_rechecks: unchanged,
                    })
                }
            }
            None => Ok(AdmissionDecision::Suspend {
                unchanged_rechecks: 0,
            }),
        },
    }
}

/// Reads the re-check count a trailing budget wake carries, for callers
/// without a projection in hand (the one-shot bracket). Only consulted
/// when a token cap exists, so the default path issues no query.
///
/// # Errors
///
/// Returns [`AgentRuntimeError::Database`] on database errors.
pub async fn carried_rechecks(
    conn: &mut PgConnection,
    thread: &AgentThread,
) -> Result<Option<u32>, AgentRuntimeError> {
    let last = PgAgentThreadRecordRepository
        .last_record(conn, thread.id())
        .await
        .map_err(|source| AgentRuntimeError::database("reading the log's tail", source))?;
    Ok(last.as_ref().and_then(wake_count))
}

/// The wake-count discrimination: a budget-recheck resolution record's
/// carried count, `None` for everything else.
fn wake_count(record: &AgentThreadRecord) -> Option<u32> {
    if record.kind() != AgentThreadRecordKind::Input {
        return None;
    }
    if record
        .content()
        .get("cause")
        .and_then(serde_json::Value::as_str)
        != Some(BUDGET_RECHECK_CAUSE)
    {
        return None;
    }
    record
        .content()
        .get("unchanged_rechecks")
        .and_then(serde_json::Value::as_u64)
        .and_then(|count| u32::try_from(count).ok())
}

// ---------------------------------------------------------------------------
// Outcomes
// ---------------------------------------------------------------------------

/// What the loop concluded for this claim.
#[derive(Debug)]
pub enum LoopOutcome {
    /// An accepted submission: the caller composes the stage terminal
    /// (its domain effects plus [`commit_loop_terminal`]) in one
    /// transaction.
    Submitted(AcceptedSubmission),
    /// A budget suspension committed; no terminal this claim.
    Suspended,
    /// A durable cancellation intent was observed at a turn boundary;
    /// nothing committed. The caller performs the cancel transaction.
    CancelIntent,
    /// A budget exhausted in a way that fails the thread; nothing
    /// committed. The caller fails the thread with its task
    /// disposition and job coupling.
    BudgetFailed {
        /// Which budget, with its numbers.
        cause: BudgetFailure,
    },
}

impl LoopOutcome {
    /// The telemetry label for the loop's concluding outcome.
    fn label(&self) -> &'static str {
        match self {
            Self::Submitted(_) => span_attrs::LOOP_OUTCOME_SUBMITTED,
            Self::Suspended => span_attrs::LOOP_OUTCOME_SUSPENDED,
            Self::CancelIntent => span_attrs::LOOP_OUTCOME_CANCEL_INTENT,
            Self::BudgetFailed { .. } => span_attrs::LOOP_OUTCOME_BUDGET_FAILED,
        }
    }
}

/// The budget exhaustions that fail a thread rather than suspend it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BudgetFailure {
    /// The committed-turn count reached the cap.
    TurnCap {
        /// Committed assistant turns.
        turns: u32,
        /// The binding's cap.
        cap: u32,
    },
    /// The thread outlived its execution deadline.
    Deadline {
        /// The binding's bound, in seconds from thread creation.
        seconds: u32,
    },
    /// The bounded budget re-checks ran dry without headroom returning.
    RecheckExhausted {
        /// Tokens the ledger accounts to the thread.
        spent: u64,
        /// The binding's cap.
        cap: u64,
        /// Consecutive unchanged re-checks performed.
        rechecks: u32,
    },
}

impl std::fmt::Display for BudgetFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::TurnCap { turns, cap } => {
                write!(
                    f,
                    "the turn cap: {turns} committed turns against a cap of {cap}"
                )
            }
            Self::Deadline { seconds } => {
                write!(
                    f,
                    "the execution deadline: the thread outlived its {seconds}s bound"
                )
            }
            Self::RecheckExhausted {
                spent,
                cap,
                rechecks,
            } => write!(
                f,
                "the token budget: {spent} spent against a cap of {cap} after {rechecks} \
                 unchanged re-checks",
            ),
        }
    }
}

// ---------------------------------------------------------------------------
// Dependencies
// ---------------------------------------------------------------------------

/// Everything one loop execution runs against, captured at claim.
pub struct TurnLoopDependencies<'a> {
    /// The pool the loop's per-step transactions draw from.
    pub pool: &'a sqlx::PgPool,
    /// The gateway the loop's streamed calls route through.
    pub gateway: &'a InferenceGateway,
    /// The stage whose endpoint binding the gateway resolves.
    pub stage: TaskType,
    /// The thread, as claimed.
    pub thread: &'a AgentThread,
    /// The driving stage task.
    pub task_id: TaskId,
    /// The claim every guarded write verifies.
    pub claim_token: uuid::Uuid,
    /// The claim's attempt number, scoping the per-turn ledger link.
    pub attempt: i32,
    /// The attribution every inference call carries.
    pub attribution: &'a UsageAttribution,
    /// The stage's ordinary tools.
    pub registry: &'a ToolRegistry,
    /// The distinguished completion tool's model-facing contract.
    pub submit_descriptor: &'a ToolDescriptor,
    /// The stage's submission pipeline.
    pub pipeline: &'a dyn SubmissionPipeline,
    /// The lease-liveness seam.
    pub pump: &'a dyn HeartbeatPump,
    /// This attempt's rendering, committed only when the log holds no
    /// rendered conversation yet: the log, not caller bookkeeping,
    /// decides.
    pub rendered: RenderedConversation,
    /// The binding's execution caps.
    pub budgets: ExecutionBudgets,
    /// The budget-exhaustion re-check policy.
    pub recheck: RecheckPolicy,
    /// The binding's sampling temperature, pre-narrowed by the caller
    /// so the loop and the one-shot path share one mapping.
    pub temperature: Option<f32>,
    /// The binding's output-token cap per call.
    pub max_tokens: Option<u32>,
    /// The claim's outer deadline, bounding provider-permit waits.
    pub permit_deadline: tokio::time::Instant,
    /// The sink for the loop's lifecycle metrics.
    pub recorder: &'a dyn MetricsRecorder,
}

// ---------------------------------------------------------------------------
// The driver
// ---------------------------------------------------------------------------

/// Runs the turn loop for one claim, from position derivation to an
/// outcome.
///
/// # Errors
///
/// Returns [`AgentRuntimeError::LeaseLost`] when a claim guard misses,
/// [`AgentRuntimeError::Inference`] on provider faults, and
/// [`AgentRuntimeError::ToolExecution`] on a tool's system failure
/// (all of which route to the stage-error path, never the conversation),
/// plus the serialisation and database errors of the parts.
pub async fn run_turn_loop(
    deps: TurnLoopDependencies<'_>,
) -> Result<LoopOutcome, AgentRuntimeError> {
    // The runtime root for one thread-advancing execution: the per-claim
    // stage span (created in the worker) encloses this, and the inference,
    // tool, and embedding spans nest under it. The thread id rides
    // `gen_ai.conversation.id`, stitching a thread's fresh-per-claim traces.
    let span = tracing::info_span!(
        "invoke_agent",
        { gen_ai::OPERATION_NAME } = gen_ai::OPERATION_INVOKE_AGENT,
        { gen_ai::CONVERSATION_ID } = %deps.thread.id(),
        { span_attrs::STAGE } = deps.stage.as_str(),
        { span_attrs::TASK_ID } = %deps.task_id,
        { span_attrs::RETRY_COUNT } = deps.attempt,
        { span_attrs::EXECUTOR } = StageExecutorKind::BuiltInLoop.as_str(),
        { span_attrs::THREAD_RESUMED } = tracing::field::Empty,
    );
    run_turn_loop_inner(deps).instrument(span).await
}

async fn run_turn_loop_inner(
    deps: TurnLoopDependencies<'_>,
) -> Result<LoopOutcome, AgentRuntimeError> {
    let mut conn = acquire(deps.pool).await?;

    // The committed log decides whether this attempt renders the
    // opening: only a log with no rendered conversation commits one, so
    // a resumed entry can never write a duplicate whatever the caller
    // believes about prior attempts.
    let mut records = read_log(&mut conn, deps.thread).await?;
    let resumed = records
        .iter()
        .any(|record| is_rendered_conversation(record.content()));
    tracing::Span::current().record(span_attrs::THREAD_RESUMED, resumed);
    if resumed {
        // The committed log is a re-derivation, not a re-execution, so the
        // replay is one event at the boundary, never one per replayed turn.
        tracing::info!(
            { span_attrs::RESUMED_AT_SEQ } =
                records.last().map_or(0, |record| record.seq().inner()),
            { span_attrs::TURNS_REPLAYED } = records
                .iter()
                .filter(|record| record.kind() == AgentThreadRecordKind::AssistantMessage)
                .count(),
            "agentic thread resumed from its committed log",
        );
        deps.recorder.record_agent_thread_resume();
    } else {
        begin_turn(
            &mut conn,
            deps.thread,
            DrivingClaim::stage(deps.task_id, deps.claim_token),
            None,
            deps.rendered.clone(),
        )
        .await?;
        records = read_log(&mut conn, deps.thread).await?;
    }
    let mut projection = project(deps.thread, &records)?;

    let outcome = loop {
        // A returned verifier verdict resolves before the unanswered tail
        // and before any boundary: an accepted-and-still-valid submission
        // terminates without a further call, and any call the model issued
        // alongside `submit_result` is dropped rather than executed after
        // acceptance. A rejection or post-acceptance drift continues the
        // loop with the critique or the diagnostics already in context,
        // leaving those leftover calls for the tail to answer on the way
        // to the next turn. Dispositioning the verdict first is what keeps
        // a sibling tool call from pre-empting (or, on failure, sinking)
        // the verified submission.
        if let Some(resolved) = projection.resolved_submission.take() {
            if let Some(accepted) =
                resolve_verdict(&mut conn, &deps, &mut projection, resolved).await?
            {
                break LoopOutcome::Submitted(accepted);
            }
            continue;
        }

        if let Some(batch) = projection.tail.take() {
            match execute_batch(&mut conn, &deps, &mut projection, batch).await? {
                BatchOutcome::Completed => {}
                BatchOutcome::Submitted(accepted) => break LoopOutcome::Submitted(accepted),
                BatchOutcome::Suspended => break LoopOutcome::Suspended,
                BatchOutcome::CancelIntervened => break LoopOutcome::CancelIntent,
                BatchOutcome::Reproject => {
                    projection = read_projection(&mut conn, deps.thread).await?;
                }
            }
            continue;
        }

        let carried_rechecks = projection.carried_rechecks.take();

        // 1. The durable cancellation intent, observed at every turn
        //    boundary.
        let current = PgAgentThreadRepository
            .find_by_id(&mut conn, deps.thread.id())
            .await
            .map_err(|source| {
                AgentRuntimeError::database("re-reading the thread at a boundary", source)
            })?
            .ok_or(AgentRuntimeError::ThreadMissing {
                task_id: deps.task_id,
            })?;
        if current.cancel_requested_at().is_some() {
            break LoopOutcome::CancelIntent;
        }

        // 2. The turn cap, against committed turns.
        if let Some(cap) = deps.budgets.max_turns
            && projection.assistant_turns >= cap
        {
            break LoopOutcome::BudgetFailed {
                cause: BudgetFailure::TurnCap {
                    turns: projection.assistant_turns,
                    cap,
                },
            };
        }

        // 3. The execution deadline, against thread age.
        if let Some(seconds) = deps.budgets.execution_deadline_seconds
            && Utc::now() - deps.thread.created_at() > chrono::Duration::seconds(i64::from(seconds))
        {
            break LoopOutcome::BudgetFailed {
                cause: BudgetFailure::Deadline { seconds },
            };
        }

        // 4. Ledger-side token admission, with the bounded re-check
        //    accounting.
        let decision = decide_admission(
            &mut conn,
            deps.thread,
            &deps.budgets,
            deps.recheck,
            carried_rechecks,
        )
        .await?;
        emit_budget_admission(decision);
        deps.recorder
            .record_agent_budget_admission(decision.label());
        match decision {
            AdmissionDecision::Proceed => {}
            AdmissionDecision::Exhausted(cause) => {
                break LoopOutcome::BudgetFailed { cause };
            }
            AdmissionDecision::Suspend { unchanged_rechecks } => {
                deps.pump.abort();
                let wake_at =
                    Utc::now() + chrono::Duration::seconds(i64::from(deps.recheck.delay_seconds));
                let suspension = AgentThreadSuspension::BudgetExhaustion { unchanged_rechecks };
                break match suspend_stage_thread(
                    &mut conn,
                    deps.thread,
                    deps.task_id,
                    deps.claim_token,
                    &suspension,
                    Some(wake_at),
                )
                .await?
                {
                    SuspendOutcome::Suspended => {
                        emit_suspension(&suspension, Some(wake_at), Some(unchanged_rechecks));
                        deps.recorder
                            .record_agent_suspension(suspension.reason_label());
                        LoopOutcome::Suspended
                    }
                    SuspendOutcome::CancelIntervened => LoopOutcome::CancelIntent,
                };
            }
        }

        // 5. The streamed inference call, no transaction held.
        let request = projection.build_request(&deps);
        let response = stream_to_terminal(&deps, request).await?;

        // 6. The assistant record commit, with its per-turn attribution,
        //    spend projection, and progress reset.
        let (record, calls) = commit_assistant_turn(&mut conn, &deps, &response).await?;
        projection.append_assistant(&response, record.seq(), calls);
        tracing::info!(
            { span_attrs::TURN_INDEX } = record.seq().inner(),
            { span_attrs::ASSISTANT_TURNS } = projection.assistant_turns,
            { gen_ai::USAGE_INPUT_TOKENS } = response.usage.input_tokens,
            { gen_ai::USAGE_OUTPUT_TOKENS } = response.usage.output_tokens,
            "agentic turn committed",
        );
    };

    tracing::info!(
        { span_attrs::LOOP_OUTCOME } = outcome.label(),
        "agentic loop reached an outcome",
    );
    Ok(outcome)
}

/// Emits the pre-call budget admission event, carrying the decision and
/// its numbers (never content).
fn emit_budget_admission(decision: AdmissionDecision) {
    match decision {
        AdmissionDecision::Proceed => {
            tracing::debug!(
                { span_attrs::BUDGET_DECISION } = decision.label(),
                "budget admitted"
            );
        }
        AdmissionDecision::Suspend { unchanged_rechecks } => {
            tracing::info!(
                { span_attrs::BUDGET_DECISION } = decision.label(),
                { span_attrs::UNCHANGED_RECHECKS } = unchanged_rechecks,
                "budget exhausted; suspending",
            );
        }
        AdmissionDecision::Exhausted(BudgetFailure::RecheckExhausted {
            spent,
            cap,
            rechecks,
        }) => {
            tracing::warn!(
                { span_attrs::BUDGET_DECISION } = decision.label(),
                { span_attrs::BUDGET_SPENT } = spent,
                { span_attrs::BUDGET_CAP } = cap,
                { span_attrs::UNCHANGED_RECHECKS } = rechecks,
                "budget re-checks ran dry; failing the thread",
            );
        }
        AdmissionDecision::Exhausted(_) => {
            tracing::warn!(
                { span_attrs::BUDGET_DECISION } = decision.label(),
                "budget exhausted; failing the thread",
            );
        }
    }
}

/// Emits a suspension-committed event with the typed reason and, for a
/// budget wake, its wake instant and re-check count.
fn emit_suspension(
    suspension: &AgentThreadSuspension,
    wake_at: Option<chrono::DateTime<Utc>>,
    unchanged_rechecks: Option<u32>,
) {
    tracing::info!(
        { span_attrs::SUSPENSION_REASON } = suspension.reason_label(),
        { span_attrs::WAKE_AT } = wake_at.map(|at| at.to_rfc3339()),
        { span_attrs::UNCHANGED_RECHECKS } = unchanged_rechecks,
        "agentic thread suspended",
    );
}

/// Dispositions a returned verifier verdict. `Some` terminates the stage;
/// `None` continues the loop. A rejection's critique already rides the
/// conversation, and a drift's diagnostics are committed as a fresh
/// conversation-bearing input here.
async fn resolve_verdict(
    conn: &mut PgConnection,
    deps: &TurnLoopDependencies<'_>,
    projection: &mut Projection,
    resolved: ResolvedSubmission,
) -> Result<Option<AcceptedSubmission>, AgentRuntimeError> {
    if !resolved.verdict.accepted {
        // The verifier rejected: its critique is already the tool result
        // in the conversation, so the loop continues and the model
        // re-decides against it.
        return Ok(None);
    }

    // Acceptance is not yet a commit: the validators re-run against the
    // current graph, since the verifier's latency opened a window for
    // drift the optimistic re-check at submit could not foresee.
    match deps
        .pipeline
        .evaluate(conn, deps.thread, &projection.corpus, &resolved.arguments)
        .await
    {
        Ok(SubmissionOutcome::Accepted { payload }) => Ok(Some(AcceptedSubmission {
            payload,
            requesting_seq: resolved.requesting_seq,
            tool_call_id: resolved.tool_call_id,
        })),
        Ok(SubmissionOutcome::Bounced { diagnostics }) => {
            // Post-acceptance drift: the graph changed between
            // verification and this commit. A second tool result for the
            // same call is fenced out, so the diagnostics cross as a
            // conversation-bearing input and the model re-decides against
            // the changed graph rather than the thread dying on a
            // deterministic wall.
            commit_drift_input(conn, deps, projection, &diagnostics).await?;
            Ok(None)
        }
        Err(ToolFailure::Recoverable(recoverable)) => {
            commit_drift_input(conn, deps, projection, &recoverable.render()).await?;
            Ok(None)
        }
        Err(ToolFailure::System { context }) => Err(AgentRuntimeError::ToolExecution { context }),
    }
}

/// Commits a drift-diagnostics input record (the injected-message shape)
/// and mirrors it into the projection, so the model's next turn reads why
/// its verified submission could not commit.
async fn commit_drift_input(
    conn: &mut PgConnection,
    deps: &TurnLoopDependencies<'_>,
    projection: &mut Projection,
    diagnostics: &str,
) -> Result<(), AgentRuntimeError> {
    let message = format!(
        "Your submission was verified, but the graph changed before it could be committed: \
         {diagnostics} Re-examine and submit again."
    );
    let content = serde_json::to_value(InjectedMessage {
        content: message.clone(),
    })
    .map_err(|source| serialisation(deps.thread, "the drift diagnostics", source))?;

    let mut txn = begin(conn, "beginning the drift-input transaction").await?;
    guard_claim(&mut txn, deps).await?;
    PgAgentThreadRepository
        .lock(&mut txn, deps.thread.id())
        .await
        .map_err(|source| {
            AgentRuntimeError::database("locking the thread for the drift input", source)
        })?;
    let seq = next_seq(&mut txn, deps.thread).await?;
    PgAgentThreadRecordRepository
        .append(
            &mut txn,
            &NewAgentThreadRecord::builder()
                .thread_id(deps.thread.id())
                .seq(seq)
                .kind(AgentThreadRecordKind::Input)
                .content(content)
                .trace_id(current_trace_id())
                .span_id(current_span_id())
                .build(),
        )
        .await
        .map_err(|source| AgentRuntimeError::database("committing the drift input", source))?;
    reset_progress(&mut txn, deps).await?;
    commit(txn, "committing the drift-input transaction").await?;

    projection.messages.push(Message::User { content: message });
    Ok(())
}

// ---------------------------------------------------------------------------
// Projection
// ---------------------------------------------------------------------------

/// The committed log's model-facing reading, kept current in memory as
/// the loop commits: never re-rendered, never re-derived mid-claim
/// except when a fence conflict proves the memory stale.
#[derive(Debug)]
struct Projection {
    system: Option<String>,
    messages: Vec<Message>,
    corpus: SeenCorpus,
    assistant_turns: u32,
    /// The unanswered tail batch, when the last assistant message has
    /// calls outstanding. `None` is a clean turn boundary.
    tail: Option<PendingBatch>,
    /// The wake count carried by a trailing budget-recheck record.
    carried_rechecks: Option<u32>,
    /// A submit-result call whose verifier verdict has just returned: the
    /// last assistant turn's submission, answered by a non-error tool
    /// result and not yet superseded by a later turn. The loop re-derives
    /// its disposition from this rather than issuing fresh inference.
    resolved_submission: Option<ResolvedSubmission>,
    /// Child threads this thread has launched: the count of
    /// suspend-with-child suspension records in the log, the
    /// committed-record measure the verify budget (`max_child_launches`)
    /// bounds. A launch always suspends and re-enters, so this is
    /// recomputed fresh at every entry rather than maintained.
    child_launches: u32,
}

/// A returned verifier verdict awaiting the loop's disposition.
#[derive(Debug)]
struct ResolvedSubmission {
    /// The submitted arguments, re-validated against the current graph.
    arguments: serde_json::Value,
    /// The assistant message bearing the submit call.
    requesting_seq: AgentThreadRecordSeq,
    /// The submit call's id, for the accepted terminal.
    tool_call_id: String,
    /// The verifier's verdict.
    verdict: VerdictContent,
}

/// The last assistant message's outstanding calls.
#[derive(Debug)]
struct PendingBatch {
    requesting_seq: AgentThreadRecordSeq,
    calls: Vec<RecordedToolCall>,
    answered: HashSet<String>,
}

async fn read_projection(
    conn: &mut PgConnection,
    thread: &AgentThread,
) -> Result<Projection, AgentRuntimeError> {
    let records = read_log(conn, thread).await?;
    project(thread, &records)
}

async fn read_log(
    conn: &mut PgConnection,
    thread: &AgentThread,
) -> Result<Vec<AgentThreadRecord>, AgentRuntimeError> {
    PgAgentThreadRecordRepository
        .find_by_thread_id(conn, thread.id())
        .await
        .map_err(|source| AgentRuntimeError::database("reading the thread's log", source))
}

fn project(
    thread: &AgentThread,
    records: &[AgentThreadRecord],
) -> Result<Projection, AgentRuntimeError> {
    let mut projection = Projection {
        system: None,
        messages: Vec::new(),
        corpus: SeenCorpus::default(),
        assistant_turns: 0,
        tail: None,
        carried_rechecks: None,
        resolved_submission: None,
        child_launches: 0,
    };
    let mut saw_rendered = false;
    let mut pending: Option<PendingBatch> = None;
    let mut resolved: Option<ResolvedSubmission> = None;

    for record in records {
        projection.carried_rechecks = None;
        match record.kind() {
            AgentThreadRecordKind::Input => {
                if is_rendered_conversation(record.content()) {
                    if saw_rendered {
                        return Err(log_fault(thread, "a second rendered conversation"));
                    }
                    saw_rendered = true;
                    let rendered: RenderedConversation =
                        parse_content(thread, record, "the rendered conversation")?;
                    projection.system = rendered.system;
                    for message in rendered.messages {
                        projection.corpus.push(message.content.clone());
                        projection.messages.push(recorded_to_wire(message));
                    }
                } else if let Some(message) = injected_message(record.content()) {
                    // A system-authored user turn the model reads (drift
                    // diagnostics, a human resolution): conversation-
                    // bearing, but not graph data, so it joins the
                    // messages without entering the membership corpus. It
                    // also supersedes a pending verdict (the injection is
                    // that verdict's disposition), so a crash between the
                    // injection and the next turn never re-resolves it.
                    projection.messages.push(Message::User { content: message });
                    resolved = None;
                } else {
                    // A bookkeeping resolution (a timer or budget wake):
                    // never model-facing. A budget wake carries the
                    // unchanged-recheck count forward when it is the
                    // log's trailing record.
                    projection.carried_rechecks = wake_count(record);
                }
            }
            AgentThreadRecordKind::AssistantMessage => {
                let content: AssistantContent =
                    parse_content(thread, record, "an assistant message")?;
                projection.assistant_turns += 1;
                projection.messages.push(Message::Assistant {
                    content: content.text,
                    tool_calls: content
                        .tool_calls
                        .iter()
                        .map(|call| tribal_domain::ToolCall {
                            id: call.id.clone(),
                            name: call.name.clone(),
                            arguments: call.arguments.clone(),
                        })
                        .collect(),
                });
                // A new turn supersedes any verdict that has not yet been
                // dispositioned: only the trailing submission is resolved.
                resolved = None;
                pending = if content.tool_calls.is_empty() {
                    None
                } else {
                    Some(PendingBatch {
                        requesting_seq: record.seq(),
                        calls: content.tool_calls,
                        answered: HashSet::new(),
                    })
                };
            }
            AgentThreadRecordKind::ToolResult => {
                let content: ToolResultContent = parse_content(thread, record, "a tool result")?;
                let call_id = record
                    .tool_call_id()
                    .ok_or_else(|| log_fault(thread, "a tool result without its call id"))?;
                projection.messages.push(Message::Tool {
                    tool_call_id: call_id.to_owned(),
                    content: content.output.clone(),
                });
                if !content.is_error {
                    projection.corpus.push(content.output.clone());
                }
                if let Some(batch) = &mut pending
                    && record.requesting_seq() == Some(batch.requesting_seq)
                {
                    batch.answered.insert(call_id.to_owned());
                    // A non-error result answering the trailing turn's
                    // submit call is a returned verifier verdict: the
                    // suspension between them cleared no record, so this
                    // is the verdict's arrival, never a bounce (a bounce
                    // is an error result, and the loop runs on past it).
                    if !content.is_error
                        && let Some(submit) = batch
                            .calls
                            .iter()
                            .find(|c| c.id == call_id && c.name == SUBMIT_RESULT_TOOL)
                        && let Ok(verdict) = serde_json::from_str::<VerdictContent>(&content.output)
                    {
                        resolved = Some(ResolvedSubmission {
                            arguments: submit.arguments.clone(),
                            requesting_seq: batch.requesting_seq,
                            tool_call_id: call_id.to_owned(),
                            verdict,
                        });
                    }
                }
            }
            // A suspension that launched a child counts against the
            // verify budget: one suspend-with-child is one launch,
            // counted whether its child later completed or died, so the
            // budget bounds fan-out rather than only successful rounds. A
            // budget-exhaustion suspension launches nothing and is skipped.
            AgentThreadRecordKind::Suspension => {
                if let Ok(AgentThreadSuspension::DeferredToolResults { .. }) =
                    serde_json::from_value::<AgentThreadSuspension>(record.content().clone())
                {
                    projection.child_launches += 1;
                }
            }
            // Control, terminal, and product records are never model-facing:
            // the observed kind belongs to the external executor, and an
            // appended artifact is a durable output, not a conversation turn.
            AgentThreadRecordKind::Cancellation
            | AgentThreadRecordKind::Submission
            | AgentThreadRecordKind::ObservedToolEvent
            | AgentThreadRecordKind::AppendArtifact => {}
        }
    }

    if !saw_rendered {
        return Err(log_fault(thread, "no rendered conversation"));
    }
    projection.resolved_submission = resolved;
    projection.tail = pending.filter(|batch| {
        !batch
            .calls
            .iter()
            .all(|call| batch.answered.contains(&call.id))
    });
    Ok(projection)
}

/// The content of an injected conversational message, when the record is
/// one: a system-authored user turn the model reads.
fn injected_message(content: &serde_json::Value) -> Option<String> {
    if content
        .get("content_kind")
        .and_then(serde_json::Value::as_str)
        != Some(INJECTED_MESSAGE_KIND)
    {
        return None;
    }
    content
        .get("content")
        .and_then(serde_json::Value::as_str)
        .map(str::to_owned)
}

impl Projection {
    fn build_request(&self, deps: &TurnLoopDependencies<'_>) -> CompletionRequest {
        let mut tools: Vec<ToolWireDefinition> = deps
            .registry
            .descriptors()
            .iter()
            .map(wire_definition)
            .collect();
        tools.push(wire_definition(deps.submit_descriptor));
        CompletionRequest {
            // The system prompt is byte-stable across a thread's turns: the
            // loop's request prefix must not churn, or every turn forfeits the
            // provider cache discount for the whole history. The turn cap is a
            // silent runaway guard; the loop prompts carry the static budget
            // guidance the model paces against.
            system: self.system.clone(),
            messages: self.messages.clone(),
            tools,
            temperature: deps.temperature,
            max_tokens: deps.max_tokens,
            response_format: None,
        }
    }

    /// Mirrors a freshly committed assistant record into the in-memory
    /// reading.
    fn append_assistant(
        &mut self,
        response: &CompletionResponse,
        seq: AgentThreadRecordSeq,
        calls: Vec<RecordedToolCall>,
    ) {
        self.assistant_turns += 1;
        self.messages.push(Message::Assistant {
            content: response.text.clone(),
            tool_calls: response.tool_calls.clone(),
        });
        self.tail = if calls.is_empty() {
            None
        } else {
            Some(PendingBatch {
                requesting_seq: seq,
                calls,
                answered: HashSet::new(),
            })
        };
    }

    /// Mirrors a freshly committed tool result.
    fn append_tool_result(&mut self, call_id: &str, content: &ToolResultContent) {
        self.messages.push(Message::Tool {
            tool_call_id: call_id.to_owned(),
            content: content.output.clone(),
        });
        if !content.is_error {
            self.corpus.push(content.output.clone());
        }
    }
}

fn recorded_to_wire(message: crate::turn::RecordedMessage) -> Message {
    // The loop records the two conversational roles in the rendered
    // opening; an unrecognised string (a future format's role)
    // downgrades to User rather than dropping the message, keeping
    // resume total. Tool turns never appear in a rendered opening.
    if message.role == "assistant" {
        Message::Assistant {
            content: message.content,
            tool_calls: vec![],
        }
    } else {
        Message::User {
            content: message.content,
        }
    }
}

fn wire_definition(descriptor: &ToolDescriptor) -> ToolWireDefinition {
    ToolWireDefinition {
        name: descriptor.name.clone(),
        description: descriptor.description.clone(),
        input_schema: descriptor.input_schema.clone(),
    }
}

// ---------------------------------------------------------------------------
// Inference
// ---------------------------------------------------------------------------

async fn stream_to_terminal(
    deps: &TurnLoopDependencies<'_>,
    request: CompletionRequest,
) -> Result<CompletionResponse, AgentRuntimeError> {
    let remaining = deps
        .permit_deadline
        .saturating_duration_since(tokio::time::Instant::now());
    deps.pump.inference_started();
    let outcome = async {
        let mut stream = deps
            .gateway
            .complete_stream(
                deps.stage,
                request,
                PermitWait::Bounded { limit: remaining },
                deps.attribution,
            )
            .await
            .map_err(|source| AgentRuntimeError::Inference {
                context: "opening the loop's streamed call".to_owned(),
                source: Box::new(source),
            })?;
        let mut terminal = None;
        while let Some(event) = stream.next().await {
            let event = event.map_err(|source| AgentRuntimeError::Inference {
                context: "consuming the loop's streamed call".to_owned(),
                source: Box::new(source),
            })?;
            deps.pump.stream_delta();
            if let InferenceEvent::Completed { response } = event {
                terminal = Some(response);
            }
        }
        terminal.ok_or_else(|| AgentRuntimeError::Inference {
            context: "consuming the loop's streamed call".to_owned(),
            source: Box::new(tribal_inference::InferenceError::ResponseParseFailed {
                expected_shape: "a terminal Completed event".to_owned(),
                actual: "stream ended without one".to_owned(),
            }),
        })
    }
    .await;
    deps.pump.inference_finished();
    outcome
}

// ---------------------------------------------------------------------------
// The per-turn commits
// ---------------------------------------------------------------------------

/// Commits one turn's assistant record: the record with its usage, the
/// per-turn ledger link (constrained to the single most-recent matching
/// row), the spend projection, and the driving task's progress reset:
/// one transaction.
async fn commit_assistant_turn(
    conn: &mut PgConnection,
    deps: &TurnLoopDependencies<'_>,
    response: &CompletionResponse,
) -> Result<(AgentThreadRecord, Vec<RecordedToolCall>), AgentRuntimeError> {
    let calls: Vec<RecordedToolCall> = response
        .tool_calls
        .iter()
        .map(|call| RecordedToolCall {
            id: call.id.clone(),
            name: call.name.clone(),
            arguments: call.arguments.clone(),
        })
        .collect();
    let content = serde_json::to_value(AssistantContent {
        text: response.text.clone(),
        tool_calls: calls.clone(),
    })
    .map_err(|source| serialisation(deps.thread, "the assistant content", source))?;
    let usage = serde_json::to_value(RecordedUsage::from(response))
        .map_err(|source| serialisation(deps.thread, "the turn's usage", source))?;

    let mut txn = begin(conn, "beginning the assistant-turn transaction").await?;
    guard_claim(&mut txn, deps).await?;
    let current = PgAgentThreadRepository
        .lock(&mut txn, deps.thread.id())
        .await
        .map_err(|source| AgentRuntimeError::database("locking the thread for the turn", source))?
        .ok_or(AgentRuntimeError::ThreadMissing {
            task_id: deps.task_id,
        })?;
    let seq = next_seq(&mut txn, deps.thread).await?;
    let record = PgAgentThreadRecordRepository
        .append(
            &mut txn,
            &NewAgentThreadRecord::builder()
                .thread_id(deps.thread.id())
                .seq(seq)
                .kind(AgentThreadRecordKind::AssistantMessage)
                .content(content)
                .usage(Some(usage))
                .trace_id(current_trace_id())
                .span_id(current_span_id())
                .build(),
        )
        .await
        .map_err(|source| AgentRuntimeError::database("committing the assistant record", source))?;

    PgTokenUsageRepository
        .link_completion_to_record(
            &mut txn,
            deps.thread.id(),
            deps.task_id,
            deps.attempt,
            record.id(),
        )
        .await
        .map_err(|source| {
            AgentRuntimeError::database("linking the turn's spend to its record", source)
        })?;

    let mut spend: ExecutionSpend = match current.execution_spend() {
        Some(value) => serde_json::from_value(value.clone()).map_err(|source| {
            serialisation(deps.thread, "the recorded spend projection", source)
        })?,
        None => ExecutionSpend::default(),
    };
    spend.input_tokens += u64::from(response.usage.input_tokens);
    spend.output_tokens += u64::from(response.usage.output_tokens);
    spend.cache_read_tokens += u64::from(response.usage.cache_read_tokens);
    spend.cache_write_tokens += u64::from(response.usage.cache_write_tokens);
    spend.turns += 1;
    PgAgentThreadRepository
        .set_execution_spend(&mut txn, deps.thread.id(), &spend)
        .await
        .map_err(|source| AgentRuntimeError::database("projecting the turn's spend", source))?;

    reset_progress(&mut txn, deps).await?;
    commit(txn, "committing the assistant-turn transaction").await?;
    Ok((record, calls))
}

/// What a fenced tool-result commit concluded.
#[derive(Clone, Copy)]
enum ResultCommit {
    Committed,
    /// The fence refused the append: another actor answered this call.
    /// The in-memory reading is stale; the caller re-projects.
    FenceConflict,
}

/// Commits one tool-result record under the fence in its own
/// transaction: the error-shaped and bounced paths, where no tool side
/// effect shares the commit.
async fn commit_result_record(
    conn: &mut PgConnection,
    deps: &TurnLoopDependencies<'_>,
    requesting_seq: AgentThreadRecordSeq,
    call_id: &str,
    content: &ToolResultContent,
) -> Result<ResultCommit, AgentRuntimeError> {
    let value = serde_json::to_value(content)
        .map_err(|source| serialisation(deps.thread, "a tool result", source))?;
    let mut txn = begin(conn, "beginning the tool-result transaction").await?;
    guard_claim(&mut txn, deps).await?;
    PgAgentThreadRepository
        .lock(&mut txn, deps.thread.id())
        .await
        .map_err(|source| {
            AgentRuntimeError::database("locking the thread for a tool result", source)
        })?;
    let seq = next_seq(&mut txn, deps.thread).await?;
    let appended = PgAgentThreadRecordRepository
        .append(
            &mut txn,
            &NewAgentThreadRecord::builder()
                .thread_id(deps.thread.id())
                .seq(seq)
                .kind(AgentThreadRecordKind::ToolResult)
                .requesting_seq(Some(requesting_seq))
                .tool_call_id(Some(call_id.to_owned()))
                .content(value)
                .trace_id(current_trace_id())
                .span_id(current_span_id())
                .build(),
        )
        .await;
    match appended {
        Ok(_) => {}
        Err(DbError::UniqueViolation { .. }) => return Ok(ResultCommit::FenceConflict),
        Err(source) => {
            return Err(AgentRuntimeError::database(
                "committing a tool result",
                source,
            ));
        }
    }
    reset_progress(&mut txn, deps).await?;
    commit(txn, "committing the tool-result transaction").await?;
    Ok(ResultCommit::Committed)
}

/// Commits an [`AppendArtifact`] record — a job's typed product — to the thread's
/// log, guarding the driving claim so a stale runner never writes one: lock,
/// next seq, append, commit. The artifact is durable the moment this returns,
/// before any read surface exists to serve it.
///
/// [`AppendArtifact`]: AgentThreadRecordKind::AppendArtifact
///
/// # Errors
///
/// Returns [`AgentRuntimeError::LeaseLost`] when the claim guard misses (this is
/// the whole transaction, rolled back on the error), plus the serialisation and
/// database errors of the parts.
pub async fn commit_artifact_record(
    conn: &mut PgConnection,
    thread: &AgentThread,
    claim: DrivingClaim,
    artifact: &serde_json::Value,
) -> Result<AgentThreadRecordSeq, AgentRuntimeError> {
    let mut txn = begin(conn, "beginning the artifact transaction").await?;
    claim.require(&mut txn).await?;
    PgAgentThreadRepository
        .lock(&mut txn, thread.id())
        .await
        .map_err(|source| {
            AgentRuntimeError::database("locking the thread for an artifact", source)
        })?;
    let seq = next_seq(&mut txn, thread).await?;
    PgAgentThreadRecordRepository
        .append(
            &mut txn,
            &NewAgentThreadRecord::builder()
                .thread_id(thread.id())
                .seq(seq)
                .kind(AgentThreadRecordKind::AppendArtifact)
                .content(artifact.clone())
                .trace_id(current_trace_id())
                .span_id(current_span_id())
                .build(),
        )
        .await
        .map_err(|source| AgentRuntimeError::database("committing an artifact", source))?;
    commit(txn, "committing the artifact transaction").await?;
    Ok(seq)
}

/// Commits the loop's terminal contribution inside the caller's
/// transaction: the typed submission record and the thread's completed
/// status. The spend projection is maintained per turn, so it is
/// already final. The submission itself makes no call.
///
/// # Errors
///
/// Returns [`AgentRuntimeError::StatusCasMissed`] when the thread is no
/// longer `running` (the caller rolls back), plus the serialisation and
/// database errors of the parts.
pub async fn commit_loop_terminal(
    txn: &mut PgConnection,
    thread: &AgentThread,
    accepted: &AcceptedSubmission,
) -> Result<AgentThreadRecord, AgentRuntimeError> {
    let content = serde_json::to_value(SubmissionContent {
        payload: accepted.payload.clone(),
        requesting_seq: accepted.requesting_seq,
        tool_call_id: accepted.tool_call_id.clone(),
    })
    .map_err(|source| serialisation(thread, "the submission content", source))?;

    PgAgentThreadRepository
        .lock(txn, thread.id())
        .await
        .map_err(|source| {
            AgentRuntimeError::database("locking the thread for the terminal", source)
        })?;
    let seq = next_seq(txn, thread).await?;
    let record = PgAgentThreadRecordRepository
        .append(
            txn,
            &NewAgentThreadRecord::builder()
                .thread_id(thread.id())
                .seq(seq)
                .kind(AgentThreadRecordKind::Submission)
                .content(content)
                .trace_id(current_trace_id())
                .span_id(current_span_id())
                .build(),
        )
        .await
        .map_err(|source| {
            AgentRuntimeError::database("committing the submission record", source)
        })?;

    let moved = PgAgentThreadRepository
        .complete(
            txn,
            thread.id(),
            AgentThreadTerminal::Completed,
            AgentThreadStatus::Running,
        )
        .await
        .map_err(|source| AgentRuntimeError::database("completing the thread", source))?;
    if moved == 0 {
        return Err(AgentRuntimeError::StatusCasMissed {
            thread_id: thread.id(),
            expected: AgentThreadStatus::Running,
        });
    }
    Ok(record)
}

// ---------------------------------------------------------------------------
// Tool execution
// ---------------------------------------------------------------------------

enum BatchOutcome {
    /// Every call answered; the tail is a clean boundary.
    Completed,
    /// `submit_result` was accepted with no verifier; the loop exits to
    /// the caller's terminal.
    Submitted(AcceptedSubmission),
    /// `submit_result` was accepted and the binding verifies it: the
    /// thread suspended on the verifier child.
    Suspended,
    /// A durable cancellation intent intervened at the verifier suspend.
    CancelIntervened,
    /// A fence conflict proved the in-memory reading stale.
    Reproject,
}

/// What handling one call concluded, before the batch maps it to its own
/// outcome.
enum CallStep {
    /// A result committed (ordinary, bounce, or error); the batch continues.
    Committed,
    /// The fence refused the append; the in-memory reading is stale.
    FenceConflict,
    /// `submit_result` was accepted with no verifier; exit to the terminal.
    Submitted(AcceptedSubmission),
    /// `submit_result` was accepted and a verifier launched; suspended.
    Suspended,
    /// A durable cancellation intent intervened at the verifier suspend.
    CancelIntervened,
}

impl CallStep {
    /// The telemetry label for an `execute_tool` span's outcome.
    fn label(&self) -> &'static str {
        match self {
            Self::Committed => span_attrs::TOOL_OUTCOME_COMMITTED,
            Self::FenceConflict => span_attrs::TOOL_OUTCOME_FENCE_CONFLICT,
            Self::Submitted(_) => span_attrs::TOOL_OUTCOME_SUBMITTED,
            Self::Suspended => span_attrs::TOOL_OUTCOME_SUSPENDED,
            Self::CancelIntervened => span_attrs::TOOL_OUTCOME_CANCEL_INTERVENED,
        }
    }
}

/// Executes a batch's unanswered calls sequentially, in call order, each
/// under its own `execute_tool` span.
///
/// A bounced submission commits its diagnostics and the batch continues;
/// an accepted submission stops the batch, so any calls the model issued
/// alongside `submit_result` are left for the next entry to project. The
/// batch returns at that point to either the stage terminal (no verifier
/// or budget spent) or the verifier suspension.
async fn execute_batch(
    conn: &mut PgConnection,
    deps: &TurnLoopDependencies<'_>,
    projection: &mut Projection,
    batch: PendingBatch,
) -> Result<BatchOutcome, AgentRuntimeError> {
    for call in &batch.calls {
        if batch.answered.contains(&call.id) {
            continue;
        }
        let is_submit = call.name == SUBMIT_RESULT_TOOL;
        // The model authors the call name, so an unrecognised one is
        // arbitrary text that must never reach telemetry. Record the fixed
        // submit name, a matched registered name (a fixed identifier), or a
        // constant `unknown` classification, never the raw name.
        let tool_label = if is_submit {
            SUBMIT_RESULT_TOOL
        } else if deps.registry.lookup(&call.name).is_ok() {
            call.name.as_str()
        } else {
            "unknown"
        };
        let span = tracing::info_span!(
            "execute_tool",
            { gen_ai::OPERATION_NAME } = gen_ai::OPERATION_EXECUTE_TOOL,
            { span_attrs::OTEL_NAME } = %format!("execute_tool {tool_label}"),
            { span_attrs::TOOL_NAME } = tool_label,
            { span_attrs::TOOL_CALL_ID } = %call.id,
            { span_attrs::TOOL_IS_SUBMIT } = is_submit,
            { span_attrs::TOOL_OUTCOME } = tracing::field::Empty,
        );
        let step = handle_call(conn, deps, projection, &batch, call)
            .instrument(span)
            .await?;
        match step {
            CallStep::Committed => {}
            CallStep::FenceConflict => {
                tracing::warn!(
                    thread_id = %deps.thread.id(),
                    call_id = %call.id,
                    "the tool-result fence refused an append; re-reading the log",
                );
                return Ok(BatchOutcome::Reproject);
            }
            CallStep::Submitted(accepted) => return Ok(BatchOutcome::Submitted(accepted)),
            CallStep::Suspended => return Ok(BatchOutcome::Suspended),
            CallStep::CancelIntervened => return Ok(BatchOutcome::CancelIntervened),
        }
    }
    projection.tail = None;
    Ok(BatchOutcome::Completed)
}

/// Handles one unanswered call, recording its outcome on the enclosing
/// `execute_tool` span: the submission pipeline for `submit_result`, the
/// two-phase ordinary path otherwise.
async fn handle_call(
    conn: &mut PgConnection,
    deps: &TurnLoopDependencies<'_>,
    projection: &mut Projection,
    batch: &PendingBatch,
    call: &RecordedToolCall,
) -> Result<CallStep, AgentRuntimeError> {
    let step = if call.name == SUBMIT_RESULT_TOOL {
        handle_submission(conn, deps, projection, batch, call).await?
    } else {
        commit_to_step(execute_ordinary(conn, deps, projection, batch.requesting_seq, call).await?)
    };
    tracing::Span::current().record(span_attrs::TOOL_OUTCOME, step.label());
    Ok(step)
}

/// Evaluates a `submit_result` call through the submission pipeline, then
/// either commits, launches a verifier child, or returns the diagnostics
/// in-band. Emits the submission-evaluated event at the pipeline boundary.
async fn handle_submission(
    conn: &mut PgConnection,
    deps: &TurnLoopDependencies<'_>,
    projection: &mut Projection,
    batch: &PendingBatch,
    call: &RecordedToolCall,
) -> Result<CallStep, AgentRuntimeError> {
    let evaluation = deps
        .pipeline
        .evaluate(conn, deps.thread, &projection.corpus, &call.arguments)
        .await;
    if let Ok(outcome) = &evaluation {
        tracing::info!(
            { span_attrs::SUBMISSION_OUTCOME } = outcome.label(),
            "submission evaluated",
        );
    }
    match evaluation {
        Ok(SubmissionOutcome::Accepted { payload }) => {
            // Validators passed; the binding decides whether a verifier
            // child must agree before the commit.
            match deps
                .pipeline
                .verifier_launch(conn, deps.thread, &payload)
                .await
            {
                Ok(None) => Ok(CallStep::Submitted(AcceptedSubmission {
                    payload,
                    requesting_seq: batch.requesting_seq,
                    tool_call_id: call.id.clone(),
                })),
                Ok(Some(launch)) => {
                    launch_verifier(conn, deps, projection, batch, call, payload, launch).await
                }
                Err(ToolFailure::Recoverable(recoverable)) => Ok(commit_to_step(
                    error_result(
                        conn,
                        deps,
                        projection,
                        batch.requesting_seq,
                        call,
                        recoverable.render(),
                    )
                    .await?,
                )),
                Err(ToolFailure::System { context }) => {
                    Err(AgentRuntimeError::ToolExecution { context })
                }
            }
        }
        Ok(SubmissionOutcome::Bounced { diagnostics }) => Ok(commit_to_step(
            error_result(
                conn,
                deps,
                projection,
                batch.requesting_seq,
                call,
                diagnostics,
            )
            .await?,
        )),
        Err(ToolFailure::Recoverable(recoverable)) => Ok(commit_to_step(
            error_result(
                conn,
                deps,
                projection,
                batch.requesting_seq,
                call,
                recoverable.render(),
            )
            .await?,
        )),
        Err(ToolFailure::System { context }) => Err(AgentRuntimeError::ToolExecution { context }),
    }
}

/// Launches a validators-accepted submission's verifier child, or commits
/// the validated `payload` directly when the verify budget is spent (the
/// validators are the correctness gate; the verifier is the quality lever).
// The launch's full pipeline context; a params struct would only move the
// same arguments behind a name.
#[allow(clippy::too_many_arguments)]
async fn launch_verifier(
    conn: &mut PgConnection,
    deps: &TurnLoopDependencies<'_>,
    projection: &Projection,
    batch: &PendingBatch,
    call: &RecordedToolCall,
    payload: serde_json::Value,
    launch: VerifierLaunch,
) -> Result<CallStep, AgentRuntimeError> {
    if deps
        .budgets
        .max_child_launches
        .is_some_and(|cap| projection.child_launches >= cap)
    {
        return Ok(CallStep::Submitted(AcceptedSubmission {
            payload,
            requesting_seq: batch.requesting_seq,
            tool_call_id: call.id.clone(),
        }));
    }
    // The heartbeat stops before the suspension clears the claim, lest a
    // beat in flight race a spurious ownership-lost signal.
    deps.pump.abort();
    let child = ChildLaunch {
        pipeline_stage: launch.pipeline_stage,
        binding_version_id: launch.binding_version_id,
        principal_id: deps.thread.principal_id(),
        // The parent's job, so the child's spend meters to it without a
        // lineage walk.
        job_id: deps.attribution.owner.job_id(),
        format_version: deps.thread.format_version(),
    };
    match suspend_with_child(
        conn,
        deps.thread,
        deps.task_id,
        deps.claim_token,
        batch.requesting_seq,
        &call.id,
        child,
        &launch.child_input,
    )
    .await?
    {
        SuspendWithChildOutcome::Launched(_) => {
            deps.recorder.record_agent_child_launch();
            deps.recorder
                .record_agent_suspension(span_attrs::SUSPENSION_REASON_DEFERRED);
            Ok(CallStep::Suspended)
        }
        SuspendWithChildOutcome::CancelIntervened => Ok(CallStep::CancelIntervened),
    }
}

/// Maps a fenced result commit onto the batch's call step.
fn commit_to_step(commit: ResultCommit) -> CallStep {
    match commit {
        ResultCommit::Committed => CallStep::Committed,
        ResultCommit::FenceConflict => CallStep::FenceConflict,
    }
}

/// Executes one ordinary call under the two-phase contract: provider
/// work first with no transaction held, then the fenced transaction
/// committing the result record (plus any internal side effect) and the
/// progress reset.
async fn execute_ordinary(
    conn: &mut PgConnection,
    deps: &TurnLoopDependencies<'_>,
    projection: &mut Projection,
    requesting_seq: AgentThreadRecordSeq,
    call: &RecordedToolCall,
) -> Result<ResultCommit, AgentRuntimeError> {
    let tool = match deps.registry.lookup(&call.name) {
        Ok(tool) => Arc::clone(tool),
        Err(unknown) => {
            return error_result(
                conn,
                deps,
                projection,
                requesting_seq,
                call,
                unknown.render(),
            )
            .await;
        }
    };

    let prepared = match tool.prepare(&call.arguments).await {
        Ok(prepared) => prepared,
        Err(ToolFailure::Recoverable(recoverable)) => {
            return error_result(
                conn,
                deps,
                projection,
                requesting_seq,
                call,
                recoverable.render(),
            )
            .await;
        }
        Err(ToolFailure::System { context }) => {
            return Err(AgentRuntimeError::ToolExecution { context });
        }
    };

    let mut txn = begin(conn, "beginning the tool transaction").await?;
    guard_claim(&mut txn, deps).await?;
    PgAgentThreadRepository
        .lock(&mut txn, deps.thread.id())
        .await
        .map_err(|source| AgentRuntimeError::database("locking the thread for a tool", source))?;
    let seq = next_seq(&mut txn, deps.thread).await?;

    match tool.execute(&mut txn, prepared, &call.arguments).await {
        Ok(outcome) => {
            // An over-bound result is a recoverable failure, not a
            // silent success: a harvester then reads only whole results.
            let (output, oversized) =
                ToolRegistry::bound_result(tool.descriptor(), outcome.content);
            let content = ToolResultContent {
                output,
                is_error: oversized,
            };
            let value = serde_json::to_value(&content)
                .map_err(|source| serialisation(deps.thread, "a tool result", source))?;
            let appended = PgAgentThreadRecordRepository
                .append(
                    &mut txn,
                    &NewAgentThreadRecord::builder()
                        .thread_id(deps.thread.id())
                        .seq(seq)
                        .kind(AgentThreadRecordKind::ToolResult)
                        .requesting_seq(Some(requesting_seq))
                        .tool_call_id(Some(call.id.clone()))
                        .content(value)
                        .trace_id(current_trace_id())
                        .span_id(current_span_id())
                        .build(),
                )
                .await;
            match appended {
                Ok(_) => {}
                Err(DbError::UniqueViolation { .. }) => return Ok(ResultCommit::FenceConflict),
                Err(source) => {
                    return Err(AgentRuntimeError::database(
                        "committing a tool result",
                        source,
                    ));
                }
            }
            reset_progress(&mut txn, deps).await?;
            commit(txn, "committing the tool transaction").await?;
            projection.append_tool_result(&call.id, &content);
            Ok(ResultCommit::Committed)
        }
        Err(ToolFailure::Recoverable(recoverable)) => {
            // Any partial side effect rolls back with this transaction;
            // the diagnostic commits on its own.
            drop(txn);
            error_result(
                conn,
                deps,
                projection,
                requesting_seq,
                call,
                recoverable.render(),
            )
            .await
        }
        Err(ToolFailure::System { context }) => {
            drop(txn);
            Err(AgentRuntimeError::ToolExecution { context })
        }
    }
}

/// Commits an error-shaped result and mirrors it into the projection.
async fn error_result(
    conn: &mut PgConnection,
    deps: &TurnLoopDependencies<'_>,
    projection: &mut Projection,
    requesting_seq: AgentThreadRecordSeq,
    call: &RecordedToolCall,
    diagnostics: String,
) -> Result<ResultCommit, AgentRuntimeError> {
    let content = ToolResultContent {
        output: diagnostics,
        is_error: true,
    };
    let committed = commit_result_record(conn, deps, requesting_seq, &call.id, &content).await?;
    if matches!(committed, ResultCommit::Committed) {
        projection.append_tool_result(&call.id, &content);
    }
    Ok(committed)
}

// ---------------------------------------------------------------------------
// Shared parts
// ---------------------------------------------------------------------------

async fn acquire(
    pool: &sqlx::PgPool,
) -> Result<sqlx::pool::PoolConnection<sqlx::Postgres>, AgentRuntimeError> {
    pool.acquire().await.map_err(|source| {
        AgentRuntimeError::database(
            "acquiring a loop connection",
            DbError::QueryFailed {
                context: "pool acquire".to_owned(),
                source,
            },
        )
    })
}

async fn guard_claim(
    txn: &mut PgConnection,
    deps: &TurnLoopDependencies<'_>,
) -> Result<(), AgentRuntimeError> {
    // The triage loop is stage-driven; the shared guard verifies its
    // stage lease.
    DrivingClaim::stage(deps.task_id, deps.claim_token)
        .require(txn)
        .await
}

async fn reset_progress(
    txn: &mut PgConnection,
    deps: &TurnLoopDependencies<'_>,
) -> Result<(), AgentRuntimeError> {
    PgTaskRepository
        .reset_retry_count(txn, deps.task_id, deps.claim_token)
        .await
        .map_err(|source| AgentRuntimeError::database("resetting the task's progress", source))?;
    Ok(())
}

async fn next_seq(
    txn: &mut PgConnection,
    thread: &AgentThread,
) -> Result<AgentThreadRecordSeq, AgentRuntimeError> {
    PgAgentThreadRecordRepository
        .next_seq(txn, thread.id())
        .await
        .map_err(|source| AgentRuntimeError::database("deriving the next seq", source))
}

fn parse_content<T: serde::de::DeserializeOwned>(
    thread: &AgentThread,
    record: &AgentThreadRecord,
    what: &str,
) -> Result<T, AgentRuntimeError> {
    serde_json::from_value(record.content().clone()).map_err(|source| {
        AgentRuntimeError::ContentSerialisation {
            context: format!(
                "re-reading {what} at seq {} of thread {} (format_version {})",
                record.seq(),
                thread.id(),
                thread.format_version(),
            ),
            source,
        }
    })
}

fn log_fault(thread: &AgentThread, what: &str) -> AgentRuntimeError {
    AgentRuntimeError::LogProjection {
        context: format!("thread {} holds {what}", thread.id()),
    }
}

fn serialisation(thread: &AgentThread, what: &str, source: serde_json::Error) -> AgentRuntimeError {
    AgentRuntimeError::ContentSerialisation {
        context: format!("serialising {what} of thread {}", thread.id()),
        source,
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use tribal_test_utils::{an_agent_thread, an_agent_thread_record};

    use super::*;
    use crate::turn::RecordedMessage;

    fn rendered_input(thread: &AgentThread, opening: &str) -> AgentThreadRecord {
        let rendered = RenderedConversation {
            system: Some("system".to_owned()),
            messages: vec![RecordedMessage {
                role: "user".to_owned(),
                content: opening.to_owned(),
            }],
            system_prompt_version_id: None,
            user_prompt_version_id: None,
            resolution_context: None,
            response_schema: None,
        };
        an_agent_thread_record()
            .thread_id(thread.id())
            .seq(AgentThreadRecordSeq::new(0))
            .kind(AgentThreadRecordKind::Input)
            .content(serde_json::to_value(rendered).expect("serialises"))
            .build()
    }

    fn assistant_record(
        thread: &AgentThread,
        seq: i64,
        text: &str,
        calls: Vec<RecordedToolCall>,
    ) -> AgentThreadRecord {
        an_agent_thread_record()
            .thread_id(thread.id())
            .seq(AgentThreadRecordSeq::new(seq))
            .kind(AgentThreadRecordKind::AssistantMessage)
            .content(
                serde_json::to_value(AssistantContent {
                    text: text.to_owned(),
                    tool_calls: calls,
                })
                .expect("serialises"),
            )
            .build()
    }

    fn tool_result_record(
        thread: &AgentThread,
        seq: i64,
        requesting_seq: i64,
        call_id: &str,
        output: &str,
        is_error: bool,
    ) -> AgentThreadRecord {
        an_agent_thread_record()
            .thread_id(thread.id())
            .seq(AgentThreadRecordSeq::new(seq))
            .kind(AgentThreadRecordKind::ToolResult)
            .requesting_seq(Some(AgentThreadRecordSeq::new(requesting_seq)))
            .tool_call_id(Some(call_id.to_owned()))
            .content(
                serde_json::to_value(ToolResultContent {
                    output: output.to_owned(),
                    is_error,
                })
                .expect("serialises"),
            )
            .build()
    }

    fn a_call(id: &str, name: &str) -> RecordedToolCall {
        RecordedToolCall {
            id: id.to_owned(),
            name: name.to_owned(),
            arguments: serde_json::json!({}),
        }
    }

    #[test]
    fn test_projection_derives_a_clean_boundary_from_a_completed_turn() {
        let thread = an_agent_thread().build();
        let records = vec![
            rendered_input(&thread, "the opening"),
            assistant_record(&thread, 1, "done thinking", vec![]),
        ];

        let projection = project(&thread, &records).expect("projects");
        assert!(projection.tail.is_none(), "no calls means a turn boundary");
        assert_eq!(projection.assistant_turns, 1);
        assert_eq!(projection.system.as_deref(), Some("system"));
        assert_eq!(projection.messages.len(), 2);
        assert!(matches!(
            &projection.messages[1],
            Message::Assistant { content, .. } if content == "done thinking"
        ));
    }

    #[test]
    fn test_projection_routes_an_unanswered_batch_to_tool_execution() {
        // A trailing assistant message with one of two calls answered
        // must resume at tool execution for exactly the unanswered call,
        // never at a fresh inference call.
        let thread = an_agent_thread().build();
        let records = vec![
            rendered_input(&thread, "the opening"),
            assistant_record(
                &thread,
                1,
                "",
                vec![a_call("call_0", "search"), a_call("call_1", "read")],
            ),
            tool_result_record(&thread, 2, 1, "call_0", "the committed answer", false),
        ];

        let projection = project(&thread, &records).expect("projects");
        let batch = projection
            .tail
            .expect("an unanswered call routes to execution");
        assert_eq!(batch.requesting_seq, AgentThreadRecordSeq::new(1));
        assert!(batch.answered.contains("call_0"));
        assert!(!batch.answered.contains("call_1"));
        assert!(
            projection.corpus.contains("the committed answer"),
            "a successful result joins the seen corpus",
        );
    }

    #[test]
    fn test_projection_treats_a_fully_answered_batch_as_a_boundary() {
        let thread = an_agent_thread().build();
        let records = vec![
            rendered_input(&thread, "the opening"),
            assistant_record(&thread, 1, "", vec![a_call("call_0", "search")]),
            tool_result_record(&thread, 2, 1, "call_0", "answered", false),
        ];

        let projection = project(&thread, &records).expect("projects");
        assert!(projection.tail.is_none());
    }

    #[test]
    fn test_projection_excludes_bookkeeping_and_carries_a_trailing_wake_count() {
        let thread = an_agent_thread().build();
        let wake = an_agent_thread_record()
            .thread_id(thread.id())
            .seq(AgentThreadRecordSeq::new(2))
            .kind(AgentThreadRecordKind::Input)
            .content(serde_json::json!({
                "cause": BUDGET_RECHECK_CAUSE,
                "unchanged_rechecks": 1,
            }))
            .build();
        let records = vec![
            rendered_input(&thread, "the opening"),
            assistant_record(&thread, 1, "thinking", vec![]),
            wake,
        ];

        let projection = project(&thread, &records).expect("projects");
        assert_eq!(
            projection.carried_rechecks,
            Some(1),
            "a trailing budget wake carries its count into the boundary",
        );
        assert_eq!(
            projection.messages.len(),
            2,
            "the wake record never enters the model-facing conversation",
        );

        // A non-trailing wake carries nothing: the count is consumed by
        // the boundary that follows it, never a later one.
        let records = vec![
            rendered_input(&thread, "the opening"),
            an_agent_thread_record()
                .thread_id(thread.id())
                .seq(AgentThreadRecordSeq::new(1))
                .kind(AgentThreadRecordKind::Input)
                .content(serde_json::json!({
                    "cause": BUDGET_RECHECK_CAUSE,
                    "unchanged_rechecks": 2,
                }))
                .build(),
            assistant_record(&thread, 2, "a full turn ran", vec![]),
        ];
        let projection = project(&thread, &records).expect("projects");
        assert_eq!(projection.carried_rechecks, None);
    }

    #[test]
    fn test_projection_keeps_error_results_out_of_the_seen_corpus() {
        // A diagnostic that echoes an id must not launder it into the
        // membership corpus; only content the system showed as graph
        // data counts.
        let thread = an_agent_thread().build();
        let records = vec![
            rendered_input(
                &thread,
                "the opening shows ki_00000000-0000-0000-0000-000000000001",
            ),
            assistant_record(
                &thread,
                1,
                "",
                vec![a_call("call_0", "read"), a_call("call_1", "read")],
            ),
            tool_result_record(
                &thread,
                2,
                1,
                "call_0",
                "no item with id ki_00000000-0000-0000-0000-00000000dead exists",
                true,
            ),
            tool_result_record(
                &thread,
                3,
                1,
                "call_1",
                "item ki_00000000-0000-0000-0000-000000000002 reads fine",
                false,
            ),
        ];

        let projection = project(&thread, &records).expect("projects");
        assert!(
            projection
                .corpus
                .contains("ki_00000000-0000-0000-0000-000000000001")
        );
        assert!(
            projection
                .corpus
                .contains("ki_00000000-0000-0000-0000-000000000002")
        );
        assert!(
            !projection
                .corpus
                .contains("ki_00000000-0000-0000-0000-00000000dead"),
            "an error-shaped result must not admit the id it complains about",
        );
        assert_eq!(
            projection.messages.len(),
            4,
            "error results still reach the model as conversation",
        );
    }

    #[test]
    fn test_projection_requires_the_rendered_conversation() {
        let thread = an_agent_thread().build();
        let records = vec![assistant_record(&thread, 0, "orphaned", vec![])];
        let err = project(&thread, &records).expect_err("an input-less log is a fault");
        assert!(matches!(err, AgentRuntimeError::LogProjection { .. }));
    }

    /// A non-error tool result answering the trailing turn's submit call
    /// is a returned verifier verdict. The loop must re-derive its
    /// disposition, not issue fresh inference.
    #[test]
    fn test_projection_detects_a_returned_verifier_verdict() {
        let thread = an_agent_thread().build();
        let records = vec![
            rendered_input(&thread, "triage this"),
            assistant_record(
                &thread,
                1,
                "",
                vec![a_call("call_submit", SUBMIT_RESULT_TOOL)],
            ),
            tool_result_record(
                &thread,
                2,
                1,
                "call_submit",
                r#"{"accepted": false, "critique": "missed a duplicate"}"#,
                false,
            ),
        ];

        let projection = project(&thread, &records).expect("projects");
        let resolved = projection
            .resolved_submission
            .expect("a verdict answering the submit call is a resolved submission");
        assert!(!resolved.verdict.accepted);
        assert_eq!(
            resolved.verdict.critique.as_deref(),
            Some("missed a duplicate"),
        );
        assert!(
            projection.tail.is_none(),
            "the verdict answers the only call, so the batch is no tail",
        );
    }

    /// A bounced submission (an error result answering the submit call)
    /// is not a verdict: the loop has already run on past it, so the
    /// projection leaves no resolved submission to re-derive.
    #[test]
    fn test_projection_does_not_mistake_a_bounce_for_a_verdict() {
        let thread = an_agent_thread().build();
        let records = vec![
            rendered_input(&thread, "triage this"),
            assistant_record(
                &thread,
                1,
                "",
                vec![a_call("call_submit", SUBMIT_RESULT_TOOL)],
            ),
            tool_result_record(
                &thread,
                2,
                1,
                "call_submit",
                "bounced: copy ids exactly",
                true,
            ),
        ];

        let projection = project(&thread, &records).expect("projects");
        assert!(
            projection.resolved_submission.is_none(),
            "an error-shaped result is a bounce, not a verdict",
        );
    }

    /// A later assistant turn supersedes a returned verdict: only the
    /// trailing submission resolves.
    #[test]
    fn test_projection_supersedes_a_verdict_with_a_later_turn() {
        let thread = an_agent_thread().build();
        let records = vec![
            rendered_input(&thread, "triage this"),
            assistant_record(
                &thread,
                1,
                "",
                vec![a_call("call_submit", SUBMIT_RESULT_TOOL)],
            ),
            tool_result_record(&thread, 2, 1, "call_submit", r#"{"accepted": true}"#, false),
            assistant_record(&thread, 3, "thinking again", vec![]),
        ];

        let projection = project(&thread, &records).expect("projects");
        assert!(
            projection.resolved_submission.is_none(),
            "a later turn means the verdict was already dispositioned",
        );
    }

    /// A post-acceptance drift injection dispositions the pending verdict:
    /// a log ending in an injected message after an accepted verdict
    /// re-projects to no resolved submission, so a crash between the
    /// injection and the next turn never re-resolves an already-handled
    /// verdict.
    #[test]
    fn test_projection_clears_a_verdict_superseded_by_an_injected_message() {
        let thread = an_agent_thread().build();
        let injected = an_agent_thread_record()
            .thread_id(thread.id())
            .seq(AgentThreadRecordSeq::new(3))
            .kind(AgentThreadRecordKind::Input)
            .content(
                serde_json::to_value(InjectedMessage {
                    content: "the graph changed since acceptance; re-decide".to_owned(),
                })
                .expect("serialises"),
            )
            .build();
        let records = vec![
            rendered_input(&thread, "triage this"),
            assistant_record(
                &thread,
                1,
                "",
                vec![a_call("call_submit", SUBMIT_RESULT_TOOL)],
            ),
            tool_result_record(&thread, 2, 1, "call_submit", r#"{"accepted": true}"#, false),
            injected,
        ];

        let projection = project(&thread, &records).expect("projects");
        assert!(
            projection.resolved_submission.is_none(),
            "an injected message after a verdict is that verdict's disposition",
        );
        assert!(
            matches!(
                projection.messages.last(),
                Some(Message::User { content }) if content.contains("re-decide"),
            ),
            "the drift diagnostics reach the model as the next user turn",
        );
    }

    /// Child launches are counted from the suspend-with-child suspension
    /// records across the whole log: the measure the verify budget
    /// bounds. A launch counts whether its verdict accepts, rejects, or
    /// the child dies; a budget-exhaustion suspension launches nothing and
    /// is not counted.
    #[test]
    fn test_projection_counts_child_launches_from_suspensions() {
        let thread = an_agent_thread().build();
        let launch = |seq: i64, call: &str| {
            an_agent_thread_record()
                .thread_id(thread.id())
                .seq(AgentThreadRecordSeq::new(seq))
                .kind(AgentThreadRecordKind::Suspension)
                .content(
                    serde_json::to_value(AgentThreadSuspension::DeferredToolResults {
                        requesting_seq: AgentThreadRecordSeq::new(seq - 1),
                        pending_tool_call_ids: vec![call.to_owned()],
                    })
                    .expect("serialises"),
                )
                .build()
        };
        let records = vec![
            rendered_input(&thread, "triage this"),
            // Launch one: submit, suspend on the verifier, verdict rejects.
            assistant_record(&thread, 1, "", vec![a_call("call_1", SUBMIT_RESULT_TOOL)]),
            launch(2, "call_1"),
            tool_result_record(&thread, 3, 1, "call_1", r#"{"accepted": false}"#, false),
            // Launch two: re-submit, suspend, verdict accepts.
            assistant_record(&thread, 4, "", vec![a_call("call_2", SUBMIT_RESULT_TOOL)]),
            launch(5, "call_2"),
            tool_result_record(&thread, 6, 4, "call_2", r#"{"accepted": true}"#, false),
            // A budget-exhaustion suspension is not a child launch.
            an_agent_thread_record()
                .thread_id(thread.id())
                .seq(AgentThreadRecordSeq::new(7))
                .kind(AgentThreadRecordKind::Suspension)
                .content(
                    serde_json::to_value(AgentThreadSuspension::BudgetExhaustion {
                        unchanged_rechecks: 0,
                    })
                    .expect("serialises"),
                )
                .build(),
        ];

        let projection = project(&thread, &records).expect("projects");
        assert_eq!(
            projection.child_launches, 2,
            "two suspend-with-child launches counted; the budget-exhaustion suspension is \
             not a launch",
        );
    }

    /// An injected message is a system-authored user turn the model
    /// reads, not bookkeeping. It joins the conversation but not the
    /// membership corpus.
    #[test]
    fn test_projection_renders_an_injected_message_as_a_user_turn() {
        let thread = an_agent_thread().build();
        let injected = an_agent_thread_record()
            .thread_id(thread.id())
            .seq(AgentThreadRecordSeq::new(1))
            .kind(AgentThreadRecordKind::Input)
            .content(
                serde_json::to_value(InjectedMessage {
                    content: "the graph changed; re-decide ki_00000000-0000-0000-0000-0000000000aa"
                        .to_owned(),
                })
                .expect("serialises"),
            )
            .build();
        let records = vec![rendered_input(&thread, "triage this"), injected];

        let projection = project(&thread, &records).expect("projects");
        assert!(
            matches!(
                projection.messages.last(),
                Some(Message::User { content }) if content.contains("the graph changed")
            ),
            "the injected message reaches the model as a user turn",
        );
        assert!(
            !projection
                .corpus
                .contains("ki_00000000-0000-0000-0000-0000000000aa"),
            "a system-authored message is not graph data for membership",
        );
    }
}
