//! The turn loop: the agentic generalisation of the one-shot bracket.
//!
//! At every entry the loop derives its position from the committed tail
//! — fresh, mid-turn crash, tool-batch crash, and budget wake all
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
use tribal_db::{
    AgentThreadRecordRepository, AgentThreadRepository, DbError, NewAgentThreadRecord,
    PgAgentThreadRecordRepository, PgAgentThreadRepository, PgTaskRepository,
    PgTokenUsageRepository, TaskRepository, TokenUsageRepository,
};
use tribal_domain::{
    AgentThread, AgentThreadRecord, AgentThreadRecordKind, AgentThreadRecordSeq, AgentThreadStatus,
    AgentThreadSuspension, AgentThreadTerminal, CompletionResponse, ExecutionBudgets,
    ExecutionSpend, InferenceEvent, TaskId, TaskType, ToolDescriptor, ToolFailure,
};
use tribal_inference::{
    CompletionRequest, InferenceGateway, Message, PermitWait, ToolWireDefinition, UsageAttribution,
};
use tribal_telemetry::{current_span_id, current_trace_id};

use crate::{
    AgentRuntimeError, SuspendOutcome, ToolRegistry, suspend_stage_thread,
    turn::{
        AssistantContent, RecordedToolCall, RecordedUsage, RenderedConversation, begin_turn,
        is_rendered_conversation,
    },
    txn::{begin, commit},
};

/// The distinguished completion tool: calling it is the only way an
/// agentic stage completes.
pub const SUBMIT_RESULT_TOOL: &str = "submit_result";

/// The bookkeeping cause a budget-exhaustion wake's resolution record
/// carries, discriminating it from a plain timer wake.
pub const BUDGET_RECHECK_CAUSE: &str = "budget_recheck";

// ---------------------------------------------------------------------------
// Record content shapes
// ---------------------------------------------------------------------------

/// A tool-result record's content: the model-facing output and the
/// explicit error marker the contract requires.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ToolResultContent {
    /// What the model reads — the serialised result, or the rendered
    /// recoverable diagnostic when `is_error` is set.
    pub output: String,
    /// Whether this result is an error-shaped diagnostic.
    pub is_error: bool,
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
/// calls, and the stop before a suspension commits — a cleared claim
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
/// this corpus — what the model saw, not what the system could derive.
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
    fn push(&mut self, text: impl Into<String>) {
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
/// inference call — the admission contract both executors share.
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
    /// nothing committed — the caller performs the cancel transaction.
    CancelIntent,
    /// A budget exhausted in a way that fails the thread; nothing
    /// committed — the caller fails the thread with its task
    /// disposition and job coupling.
    BudgetFailed {
        /// Which budget, with its numbers.
        cause: BudgetFailure,
    },
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

// ---------------------------------------------------------------------------
// Dependencies
// ---------------------------------------------------------------------------

/// Everything one loop execution runs against, captured at claim.
pub struct TurnLoopDeps<'a> {
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
    /// The committed input record, when a prior attempt rendered it.
    pub committed_input: Option<&'a AgentThreadRecord>,
    /// This attempt's rendering, committed only when no input exists.
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
/// [`AgentRuntimeError::ToolExecution`] on a tool's system failure —
/// all of which route to the stage-error path, never the conversation —
/// plus the serialisation and database errors of the parts.
pub async fn run_turn_loop(deps: TurnLoopDeps<'_>) -> Result<LoopOutcome, AgentRuntimeError> {
    let mut conn = acquire(deps.pool).await?;

    begin_turn(
        &mut conn,
        deps.thread,
        deps.task_id,
        deps.claim_token,
        deps.committed_input,
        deps.rendered.clone(),
    )
    .await?;

    let mut projection = read_projection(&mut conn, deps.thread).await?;

    loop {
        if let Some(batch) = projection.tail.take() {
            match execute_batch(&mut conn, &deps, &mut projection, batch).await? {
                BatchOutcome::Completed => {}
                BatchOutcome::Submitted(accepted) => return Ok(LoopOutcome::Submitted(accepted)),
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
            return Ok(LoopOutcome::CancelIntent);
        }

        // 2. The turn cap, against committed turns.
        if let Some(cap) = deps.budgets.max_turns
            && projection.assistant_turns >= cap
        {
            return Ok(LoopOutcome::BudgetFailed {
                cause: BudgetFailure::TurnCap {
                    turns: projection.assistant_turns,
                    cap,
                },
            });
        }

        // 3. The execution deadline, against thread age.
        if let Some(seconds) = deps.budgets.execution_deadline_seconds
            && Utc::now() - deps.thread.created_at() > chrono::Duration::seconds(i64::from(seconds))
        {
            return Ok(LoopOutcome::BudgetFailed {
                cause: BudgetFailure::Deadline { seconds },
            });
        }

        // 4. Ledger-side token admission.
        if let Admission::Exhausted { spent, cap } =
            admit_inference(&mut conn, deps.thread, &deps.budgets).await?
        {
            let unchanged_rechecks = match carried_rechecks {
                Some(count) => {
                    let unchanged = count.saturating_add(1);
                    if unchanged >= deps.recheck.bound {
                        return Ok(LoopOutcome::BudgetFailed {
                            cause: BudgetFailure::RecheckExhausted {
                                spent,
                                cap,
                                rechecks: unchanged,
                            },
                        });
                    }
                    unchanged
                }
                None => 0,
            };
            deps.pump.abort();
            let wake_at =
                Utc::now() + chrono::Duration::seconds(i64::from(deps.recheck.delay_seconds));
            return match suspend_stage_thread(
                &mut conn,
                deps.thread,
                deps.task_id,
                deps.claim_token,
                &AgentThreadSuspension::BudgetExhaustion { unchanged_rechecks },
                Some(wake_at),
            )
            .await?
            {
                SuspendOutcome::Suspended => Ok(LoopOutcome::Suspended),
                SuspendOutcome::CancelIntervened => Ok(LoopOutcome::CancelIntent),
            };
        }

        // 5. The streamed inference call, no transaction held.
        let request = projection.build_request(&deps);
        let response = stream_to_terminal(&deps, request).await?;

        // 6. The assistant record commit, with its per-turn attribution,
        //    spend projection, and progress reset.
        let (record, calls) = commit_assistant_turn(&mut conn, &deps, &response).await?;
        projection.append_assistant(&response, record.seq(), calls);
    }
}

// ---------------------------------------------------------------------------
// Projection
// ---------------------------------------------------------------------------

/// The committed log's model-facing reading, kept current in memory as
/// the loop commits — never re-rendered, never re-derived mid-claim
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
    let records = PgAgentThreadRecordRepository
        .find_by_thread(conn, thread.id())
        .await
        .map_err(|source| AgentRuntimeError::database("reading the thread's log", source))?;
    project(thread, &records)
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
    };
    let mut saw_rendered = false;
    let mut pending: Option<PendingBatch> = None;

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
                } else {
                    // A bookkeeping resolution (a timer or budget wake):
                    // never model-facing. A budget wake carries the
                    // unchanged-recheck count forward when it is the
                    // log's trailing record.
                    if record
                        .content()
                        .get("cause")
                        .and_then(serde_json::Value::as_str)
                        == Some(BUDGET_RECHECK_CAUSE)
                    {
                        projection.carried_rechecks = record
                            .content()
                            .get("unchanged_rechecks")
                            .and_then(serde_json::Value::as_u64)
                            .and_then(|count| u32::try_from(count).ok());
                    }
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
                    projection.corpus.push(content.output);
                }
                if let Some(batch) = &mut pending
                    && record.requesting_seq() == Some(batch.requesting_seq)
                {
                    batch.answered.insert(call_id.to_owned());
                }
            }
            // Control and terminal records are never model-facing; the
            // observed kind belongs to the external executor.
            AgentThreadRecordKind::Suspension
            | AgentThreadRecordKind::Cancellation
            | AgentThreadRecordKind::Submission
            | AgentThreadRecordKind::ObservedToolEvent => {}
        }
    }

    if !saw_rendered {
        return Err(log_fault(thread, "no rendered conversation"));
    }
    projection.tail = pending.filter(|batch| {
        !batch
            .calls
            .iter()
            .all(|call| batch.answered.contains(&call.id))
    });
    Ok(projection)
}

impl Projection {
    fn build_request(&self, deps: &TurnLoopDeps<'_>) -> CompletionRequest {
        let mut tools: Vec<ToolWireDefinition> = deps
            .registry
            .descriptors()
            .iter()
            .map(wire_definition)
            .collect();
        tools.push(wire_definition(deps.submit_descriptor));
        CompletionRequest {
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
    deps: &TurnLoopDeps<'_>,
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
/// row), the spend projection, and the driving task's progress reset —
/// one transaction.
async fn commit_assistant_turn(
    conn: &mut PgConnection,
    deps: &TurnLoopDeps<'_>,
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
enum ResultCommit {
    Committed,
    /// The fence refused the append: another actor answered this call.
    /// The in-memory reading is stale; the caller re-projects.
    FenceConflict,
}

/// Commits one tool-result record under the fence in its own
/// transaction — the error-shaped and bounced paths, where no tool side
/// effect shares the commit.
async fn commit_result_record(
    conn: &mut PgConnection,
    deps: &TurnLoopDeps<'_>,
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

/// Commits the loop's terminal contribution inside the caller's
/// transaction: the typed submission record and the thread's completed
/// status. The spend projection is maintained per turn, so it is
/// already final — the submission itself makes no call.
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
    /// `submit_result` was accepted; the loop exits to the caller.
    Submitted(AcceptedSubmission),
    /// A fence conflict proved the in-memory reading stale.
    Reproject,
}

/// Executes a batch's unanswered calls sequentially, in call order.
///
/// A bounced submission commits its diagnostics and the batch
/// continues; an accepted one stops it — the calls after an accepted
/// submit are never executed, and no model awaits their answers,
/// because the thread terminates.
async fn execute_batch(
    conn: &mut PgConnection,
    deps: &TurnLoopDeps<'_>,
    projection: &mut Projection,
    batch: PendingBatch,
) -> Result<BatchOutcome, AgentRuntimeError> {
    for call in &batch.calls {
        if batch.answered.contains(&call.id) {
            continue;
        }
        let commit = if call.name == SUBMIT_RESULT_TOOL {
            match deps
                .pipeline
                .evaluate(conn, deps.thread, &projection.corpus, &call.arguments)
                .await
            {
                Ok(SubmissionOutcome::Accepted { payload }) => {
                    return Ok(BatchOutcome::Submitted(AcceptedSubmission {
                        payload,
                        requesting_seq: batch.requesting_seq,
                        tool_call_id: call.id.clone(),
                    }));
                }
                Ok(SubmissionOutcome::Bounced { diagnostics }) => {
                    error_result(
                        conn,
                        deps,
                        projection,
                        batch.requesting_seq,
                        call,
                        diagnostics,
                    )
                    .await?
                }
                Err(ToolFailure::Recoverable(recoverable)) => {
                    error_result(
                        conn,
                        deps,
                        projection,
                        batch.requesting_seq,
                        call,
                        recoverable.render(),
                    )
                    .await?
                }
                Err(ToolFailure::System { context }) => {
                    return Err(AgentRuntimeError::ToolExecution { context });
                }
            }
        } else {
            execute_ordinary(conn, deps, projection, batch.requesting_seq, call).await?
        };
        if matches!(commit, ResultCommit::FenceConflict) {
            tracing::warn!(
                thread_id = %deps.thread.id(),
                call_id = %call.id,
                "the tool-result fence refused an append; re-reading the log",
            );
            return Ok(BatchOutcome::Reproject);
        }
    }
    projection.tail = None;
    Ok(BatchOutcome::Completed)
}

/// Executes one ordinary call under the two-phase contract: provider
/// work first with no transaction held, then the fenced transaction
/// committing the result record (plus any internal side effect) and the
/// progress reset.
async fn execute_ordinary(
    conn: &mut PgConnection,
    deps: &TurnLoopDeps<'_>,
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
            let content = ToolResultContent {
                output: ToolRegistry::trim_to_bound(tool.descriptor(), outcome.content),
                is_error: false,
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
    deps: &TurnLoopDeps<'_>,
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
    deps: &TurnLoopDeps<'_>,
) -> Result<(), AgentRuntimeError> {
    let held = PgTaskRepository
        .holds_claim(txn, deps.task_id, deps.claim_token)
        .await
        .map_err(|source| AgentRuntimeError::database("verifying the lease", source))?;
    if held {
        Ok(())
    } else {
        Err(AgentRuntimeError::LeaseLost {
            task_id: deps.task_id,
        })
    }
}

async fn reset_progress(
    txn: &mut PgConnection,
    deps: &TurnLoopDeps<'_>,
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
        // must resume at tool execution for exactly the unanswered call
        // — never at a fresh inference call.
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
}
