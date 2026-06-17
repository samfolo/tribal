//! The one-shot turn: the degenerate case of the turn loop.
//!
//! Render prompt, single structured-output inference call, parse,
//! reconcile: exactly the launched behaviour, bracketed by the thread
//! log. The bracket commits two records: `input` (the rendered
//! conversation as sent, so resume and evaluation read what the model
//! read without re-rendering) and `assistant_message` (the response with
//! its usage). The input record commits before the call in its own small
//! transaction; the terminal record commits inside the caller's domain
//! transaction, after the wire call returns. No transaction is ever held
//! across inference. The loop's structure commits the model's response
//! before any tool-dispatch seam executes; one-shot has zero tools, so
//! the ordering is unobservable here, but the shape is the loop's.

use serde::{Deserialize, Serialize};
use sqlx::{Connection, PgConnection};
use tribal_db::{
    AgentDriverTaskRepository, AgentThreadRecordRepository, AgentThreadRepository, DrivingTaskRef,
    NewAgentThreadRecord, PgAgentDriverTaskRepository, PgAgentThreadRecordRepository,
    PgAgentThreadRepository, PgTaskRepository, TaskRepository,
};
use tribal_domain::{
    AgentThread, AgentThreadRecord, AgentThreadRecordKind, AgentThreadStatus, AgentThreadTerminal,
    CompletionResponse, ExecutionSpend, PromptVersionId,
};
use tribal_telemetry::{current_span_id, current_trace_id};

use crate::AgentRuntimeError;

/// A driving task's lease, verified before a guarded write.
///
/// Pairs the driving task (a launched stage task or a driver-family
/// row) with the claim token the holder presents. Both families verify
/// through their repository's shared-lock check, so the turn machinery
/// guards a stage thread and a driver-driven child through one type.
#[derive(Debug, Clone, Copy)]
pub struct DrivingClaim {
    /// Which driving task holds the lease.
    pub driving_task: DrivingTaskRef,
    /// The token the holder presents.
    pub claim_token: uuid::Uuid,
}

impl DrivingClaim {
    /// A stage task's claim.
    #[must_use]
    pub fn stage(task_id: tribal_domain::TaskId, claim_token: uuid::Uuid) -> Self {
        Self {
            driving_task: DrivingTaskRef::Stage(task_id),
            claim_token,
        }
    }

    /// A driver-family task's claim.
    #[must_use]
    pub fn driver(
        driver_task_id: tribal_domain::AgentDriverTaskId,
        claim_token: uuid::Uuid,
    ) -> Self {
        Self {
            driving_task: DrivingTaskRef::Driver(driver_task_id),
            claim_token,
        }
    }

    /// Verifies the lease with a shared row lock, holding it for the rest
    /// of the enclosing transaction. The `Err` arm is a database fault;
    /// the `Ok(false)` arm is the deterministic lost-ownership signal.
    async fn holds(&self, conn: &mut PgConnection) -> Result<bool, AgentRuntimeError> {
        match self.driving_task {
            DrivingTaskRef::Stage(task_id) => PgTaskRepository
                .holds_claim(conn, task_id, self.claim_token)
                .await
                .map_err(|source| AgentRuntimeError::database("verifying the stage lease", source)),
            DrivingTaskRef::Driver(driver_task_id) => PgAgentDriverTaskRepository
                .holds_claim(conn, driver_task_id, self.claim_token)
                .await
                .map_err(|source| {
                    AgentRuntimeError::database("verifying the driver lease", source)
                }),
        }
    }

    /// Verifies the lease, returning [`AgentRuntimeError::LeaseLost`] on a
    /// miss: the guarded-write helper.
    pub(crate) async fn require(&self, conn: &mut PgConnection) -> Result<(), AgentRuntimeError> {
        if self.holds(conn).await? {
            Ok(())
        } else {
            Err(AgentRuntimeError::LeaseLost {
                driving_task: self.driving_task,
            })
        }
    }
}

/// The `content_kind` tag value identifying a rendered-conversation
/// input record.
pub(crate) const RENDERED_CONVERSATION_KIND: &str = "rendered_conversation";

/// Returns `true` when an input record's content is a rendered
/// conversation: the record resume re-sends.
pub(crate) fn is_rendered_conversation(content: &serde_json::Value) -> bool {
    content
        .get("content_kind")
        .and_then(serde_json::Value::as_str)
        == Some(RENDERED_CONVERSATION_KIND)
}

// ---------------------------------------------------------------------------
// Record content shapes
// ---------------------------------------------------------------------------

/// One message of the conversation as sent, in the thread's serialisation
/// format (deliberately independent of any wire type, so provider-layer
/// changes never reshape committed records).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecordedMessage {
    /// The conversational role (`user`, `assistant`).
    pub role: String,
    /// The message content verbatim.
    pub content: String,
}

/// The rendered conversation a turn sends: the input record's content.
///
/// Stores what was sent (the conversation an inference call receives is
/// a pure projection of the log) with the prompt versions as provenance
/// metadata. The `content_kind` tag discriminates this shape from other
/// input-record payloads (a timer resolution, a human reply), so resume
/// selects the right record structurally rather than by position.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "content_kind", rename = "rendered_conversation")]
pub struct RenderedConversation {
    /// The system prompt as sent, when the stage uses one.
    pub system: Option<String>,
    /// The conversation messages as sent.
    pub messages: Vec<RecordedMessage>,
    /// The system prompt version rendered from.
    pub system_prompt_version_id: Option<PromptVersionId>,
    /// The user prompt version rendered from.
    pub user_prompt_version_id: Option<PromptVersionId>,
    /// Stage-opaque context the model's positional references resolve
    /// against: recorded with the conversation so a resumed attempt
    /// resolves the answer it gets against what this attempt rendered,
    /// never against re-derived state that may have drifted. Defaulted
    /// on read so records written before the field existed deserialise.
    #[serde(default)]
    pub resolution_context: Option<serde_json::Value>,
    /// The JSON Schema this turn's response is constrained to, when it
    /// is: a driver child's structured output (a verifier's verdict).
    /// Held as the schema value, not a wire type, so the record stays
    /// provider-independent; the executor builds the response format from
    /// it. Defaulted on read for records written before the field
    /// existed.
    #[serde(default)]
    pub response_schema: Option<serde_json::Value>,
}

/// One tool call as the assistant message requested it, in the thread's
/// serialisation format (independent of the wire type, so provider-layer
/// changes never reshape committed records).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecordedToolCall {
    /// The call identifier its result must answer.
    pub id: String,
    /// The tool the model called.
    pub name: String,
    /// The call's JSON arguments.
    pub arguments: serde_json::Value,
}

/// The assistant message's content: the response text and any tool
/// calls it carries. Usage rides the record's dedicated column, not the
/// content. The `tool_calls` field is additive: one-shot records omit
/// it (empty serialises to nothing), so their bytes are unchanged and
/// old records deserialise with the default.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct AssistantContent {
    pub(crate) text: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(crate) tool_calls: Vec<RecordedToolCall>,
}

/// The usage column's shape: token counts and wire latency, independent
/// of the inference layer's in-memory type so committed records never
/// reshape with it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct RecordedUsage {
    pub(crate) input_tokens: u32,
    pub(crate) output_tokens: u32,
    pub(crate) cache_read_tokens: u32,
    pub(crate) cache_write_tokens: u32,
    pub(crate) total_tokens: u32,
    pub(crate) latency_ms: u64,
}

impl From<&CompletionResponse> for RecordedUsage {
    fn from(response: &CompletionResponse) -> Self {
        Self {
            input_tokens: response.usage.input_tokens,
            output_tokens: response.usage.output_tokens,
            cache_read_tokens: response.usage.cache_read_tokens,
            cache_write_tokens: response.usage.cache_write_tokens,
            total_tokens: response.usage.total_tokens,
            latency_ms: u64::try_from(response.usage.latency.as_millis()).unwrap_or(u64::MAX),
        }
    }
}

// ---------------------------------------------------------------------------
// The pre-call half
// ---------------------------------------------------------------------------

/// A turn ready to call: the conversation to send, durably committed.
#[derive(Debug)]
pub struct BegunTurn {
    /// The conversation to send: the committed input verbatim, whether
    /// this attempt rendered it or a prior one did.
    pub conversation: RenderedConversation,
}

/// Commits the input record (first attempt) or adopts the committed one
/// (resume), and returns the conversation the call must send. Both
/// executors begin here: the one-shot bracket and the turn loop share
/// this entry, so the byte-stable opening rides one mechanism.
///
/// The input commits in its own transaction before any wire call: what
/// was sent is durable even if the process dies mid-call, and at-least-
/// once re-execution sends byte-identical content rather than
/// re-rendering against drifted sources. The transaction carries the
/// driving task's claim guard (a shared lock on the lease), so a zombie
/// worker's write fails on ownership, deterministically, never by the
/// luck of a seq collision, which the locked tail-append makes
/// impossible anyway.
///
/// # Errors
///
/// Returns [`AgentRuntimeError::LeaseLost`] when the claim guard misses
/// (nothing committed), [`AgentRuntimeError::ContentSerialisation`] on a
/// format fault, and [`AgentRuntimeError::Database`] on database errors.
pub async fn begin_turn(
    conn: &mut PgConnection,
    thread: &AgentThread,
    claim: DrivingClaim,
    committed_input: Option<&AgentThreadRecord>,
    rendered: RenderedConversation,
) -> Result<BegunTurn, AgentRuntimeError> {
    if let Some(record) = committed_input {
        let conversation = serde_json::from_value(record.content().clone()).map_err(|source| {
            AgentRuntimeError::ContentSerialisation {
                context: format!(
                    "re-reading the input record of thread {} (format_version {})",
                    thread.id(),
                    thread.format_version(),
                ),
                source,
            }
        })?;
        return Ok(BegunTurn { conversation });
    }

    let content = serde_json::to_value(&rendered).map_err(|source| {
        AgentRuntimeError::ContentSerialisation {
            context: format!("serialising the input record of thread {}", thread.id()),
            source,
        }
    })?;

    let mut txn = conn
        .begin()
        .await
        .map_err(|source| db_failure("beginning the input-record transaction", source))?;

    // Lock order: the driving task's lease first (the guard), then the
    // thread row for the seq derivation.
    claim.require(&mut txn).await?;

    PgAgentThreadRepository
        .lock(&mut txn, thread.id())
        .await
        .map_err(|source| AgentRuntimeError::database("locking the thread for input", source))?;
    let seq = PgAgentThreadRecordRepository
        .next_seq(&mut txn, thread.id())
        .await
        .map_err(|source| AgentRuntimeError::database("deriving the input seq", source))?;
    PgAgentThreadRecordRepository
        .append(
            &mut txn,
            &NewAgentThreadRecord::builder()
                .thread_id(thread.id())
                .seq(seq)
                .kind(AgentThreadRecordKind::Input)
                .content(content)
                .trace_id(current_trace_id())
                .span_id(current_span_id())
                .build(),
        )
        .await
        .map_err(|source| AgentRuntimeError::database("committing the input record", source))?;
    txn.commit()
        .await
        .map_err(|source| db_failure("committing the input-record transaction", source))?;

    Ok(BegunTurn {
        conversation: rendered,
    })
}

/// Reads a thread's committed rendered conversation for re-sending
/// verbatim.
///
/// A driver-family child is born with its input record (a verifier's
/// rubric and the submission, committed in the suspend-with-child
/// transaction), so the driver adopts that conversation rather than
/// rendering its own: the one-shot bracket's no-re-render contract. The
/// child has not begun a turn, so the rendered conversation is the only
/// input record; its absence is a consistency fault, never a first-claim
/// case.
///
/// # Errors
///
/// Returns [`AgentRuntimeError::Database`] on read failure,
/// [`AgentRuntimeError::ContentSerialisation`] when the record will not
/// deserialise, and a database not-found fault when no rendered
/// conversation is committed.
pub async fn adopt_conversation(
    conn: &mut PgConnection,
    thread: &AgentThread,
) -> Result<RenderedConversation, AgentRuntimeError> {
    recorded_conversation(conn, thread).await?.ok_or_else(|| {
        AgentRuntimeError::database(
            "adopting the thread's conversation",
            tribal_db::DbError::NotFound {
                entity: "agent_thread_record",
                id: format!("rendered conversation for thread {}", thread.id()),
            },
        )
    })
}

/// Returns the thread's recorded rendered conversation, or `None` when the
/// thread has no recorded rendered conversation yet (a fresh claim, whose
/// first turn has not committed one).
///
/// A resuming caller reuses the recorded conversation, and the
/// stage-opaque resolution context it carries, rather than re-deriving it.
///
/// # Errors
///
/// Returns [`AgentRuntimeError::Database`] on read failure and
/// [`AgentRuntimeError::ContentSerialisation`] when a present input record
/// will not deserialise.
pub async fn recorded_conversation(
    conn: &mut PgConnection,
    thread: &AgentThread,
) -> Result<Option<RenderedConversation>, AgentRuntimeError> {
    let records = PgAgentThreadRecordRepository
        .find_by_thread_id(conn, thread.id())
        .await
        .map_err(|source| AgentRuntimeError::database("reading the thread's input", source))?;
    let Some(input) = records.iter().find(|record| {
        record.kind() == AgentThreadRecordKind::Input && is_rendered_conversation(record.content())
    }) else {
        return Ok(None);
    };
    serde_json::from_value(input.content().clone())
        .map(Some)
        .map_err(|source| AgentRuntimeError::ContentSerialisation {
            context: format!(
                "adopting the conversation of thread {} (format_version {})",
                thread.id(),
                thread.format_version(),
            ),
            source,
        })
}

// ---------------------------------------------------------------------------
// The terminal half
// ---------------------------------------------------------------------------

/// The committed terminal: what the caller's transaction now carries.
#[derive(Debug)]
pub struct OneShotOutcome {
    /// The committed assistant-message record.
    pub assistant_record: AgentThreadRecord,
}

/// Commits the one-shot terminal inside the caller's transaction: the
/// assistant-message record, the thread's completed status, and the
/// spend projection, composed with the caller's claim-guarded task
/// completion, domain effects, and job coupling so the whole turn
/// outcome is one atomic commit.
///
/// Locks the thread row before deriving the seq (the categorical rule);
/// the caller must have already taken its driving-task claim guard, so
/// the lock order is driving task, then thread, then job.
///
/// # Errors
///
/// Returns [`AgentRuntimeError::StatusCasMissed`] when the thread is no
/// longer `running` (a sweep moved it; the caller rolls back), plus the
/// serialisation and database errors of the parts.
pub async fn commit_one_shot_terminal(
    txn: &mut PgConnection,
    thread: &AgentThread,
    response: &CompletionResponse,
) -> Result<OneShotOutcome, AgentRuntimeError> {
    let content = serde_json::to_value(AssistantContent {
        text: response.text.clone(),
        tool_calls: vec![],
    })
    .map_err(|source| AgentRuntimeError::ContentSerialisation {
        context: format!("serialising the assistant record of thread {}", thread.id()),
        source,
    })?;
    let usage = serde_json::to_value(RecordedUsage::from(response)).map_err(|source| {
        AgentRuntimeError::ContentSerialisation {
            context: format!("serialising usage for thread {}", thread.id()),
            source,
        }
    })?;

    PgAgentThreadRepository
        .lock(txn, thread.id())
        .await
        .map_err(|source| AgentRuntimeError::database("locking the thread for terminal", source))?;
    let seq = PgAgentThreadRecordRepository
        .next_seq(txn, thread.id())
        .await
        .map_err(|source| AgentRuntimeError::database("deriving the terminal seq", source))?;
    let assistant_record = PgAgentThreadRecordRepository
        .append(
            txn,
            &NewAgentThreadRecord::builder()
                .thread_id(thread.id())
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

    let spend = ExecutionSpend {
        input_tokens: u64::from(response.usage.input_tokens),
        output_tokens: u64::from(response.usage.output_tokens),
        cache_read_tokens: u64::from(response.usage.cache_read_tokens),
        cache_write_tokens: u64::from(response.usage.cache_write_tokens),
        turns: 1,
    };
    PgAgentThreadRepository
        .set_execution_spend(txn, thread.id(), &spend)
        .await
        .map_err(|source| AgentRuntimeError::database("recording the spend projection", source))?;

    Ok(OneShotOutcome { assistant_record })
}

/// Completes a thread whose attempt did no model work (an idempotency
/// no-op: the stage's result already existed). The log honestly carries
/// no assistant record for the attempt; the thread still reaches its
/// terminal inside the caller's transaction.
///
/// # Errors
///
/// Returns [`AgentRuntimeError::StatusCasMissed`] when the thread is no
/// longer `running`, or [`AgentRuntimeError::Database`] on database
/// errors.
pub async fn commit_noop_terminal(
    txn: &mut PgConnection,
    thread: &AgentThread,
) -> Result<(), AgentRuntimeError> {
    let moved = PgAgentThreadRepository
        .complete(
            txn,
            thread.id(),
            AgentThreadTerminal::Completed,
            AgentThreadStatus::Running,
        )
        .await
        .map_err(|source| AgentRuntimeError::database("completing the no-op thread", source))?;
    if moved == 0 {
        return Err(AgentRuntimeError::StatusCasMissed {
            thread_id: thread.id(),
            expected: AgentThreadStatus::Running,
        });
    }
    Ok(())
}

fn db_failure(context: &str, source: sqlx::Error) -> AgentRuntimeError {
    AgentRuntimeError::database(
        context,
        tribal_db::DbError::QueryFailed {
            context: context.to_owned(),
            source,
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rendered_conversation_carries_its_content_kind_tag() {
        // The serde tag and the discriminator constant are two spellings
        // of one string (attribute macros cannot reference consts); this
        // pins them together.
        let rendered = RenderedConversation {
            system: None,
            messages: vec![],
            system_prompt_version_id: None,
            user_prompt_version_id: None,
            resolution_context: None,
            response_schema: None,
        };
        let value = serde_json::to_value(&rendered).expect("serialises");
        assert_eq!(
            value
                .get("content_kind")
                .and_then(serde_json::Value::as_str),
            Some(RENDERED_CONVERSATION_KIND),
        );
        assert!(is_rendered_conversation(&value));
        assert!(!is_rendered_conversation(
            &serde_json::json!({"cause": "timer"})
        ));
    }
}
