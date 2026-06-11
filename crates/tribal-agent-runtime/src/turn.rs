//! The one-shot turn: the degenerate case of the turn loop.
//!
//! Render prompt, single structured-output inference call, parse,
//! reconcile — exactly the launched behaviour, bracketed by the thread
//! log. The bracket commits two records: `input` (the rendered
//! conversation as sent, so resume and evaluation read what the model
//! read without re-rendering) and `assistant_message` (the response with
//! its usage). The input record commits before the call in its own small
//! transaction; the terminal record commits inside the caller's domain
//! transaction, after the wire call returns — no transaction is ever held
//! across inference. The loop's structure commits the model's response
//! before any tool-dispatch seam executes; one-shot has zero tools, so
//! the ordering is unobservable here, but the shape is the loop's.

use serde::{Deserialize, Serialize};
use sqlx::{Connection, PgConnection};
use tribal_db::{
    AgentThreadRecordRepository, AgentThreadRepository, NewAgentThreadRecord,
    PgAgentThreadRecordRepository, PgAgentThreadRepository,
};
use tribal_domain::{
    AgentThread, AgentThreadRecord, AgentThreadRecordKind, AgentThreadStatus, AgentThreadTerminal,
    CompletionResponse, ExecutionSpend, PromptVersionId,
};
use tribal_telemetry::{current_span_id, current_trace_id};

use crate::AgentRuntimeError;

/// The `content_kind` tag value identifying a rendered-conversation
/// input record.
pub(crate) const RENDERED_CONVERSATION_KIND: &str = "rendered_conversation";

/// Returns `true` when an input record's content is a rendered
/// conversation — the record resume re-sends.
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
/// Stores what was sent — the conversation an inference call receives is
/// a pure projection of the log — with the prompt versions as provenance
/// metadata. The `content_kind` tag discriminates this shape from other
/// input-record payloads (a timer resolution, a human reply), so resume
/// selects the right record structurally rather than by position.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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
}

/// The assistant message's content: the response text. Usage rides the
/// record's dedicated column, not the content.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct AssistantContent {
    text: String,
}

/// The usage column's shape: token counts and wire latency, independent
/// of the inference layer's in-memory type so committed records never
/// reshape with it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
struct RecordedUsage {
    input_tokens: u32,
    output_tokens: u32,
    cache_read_tokens: u32,
    cache_write_tokens: u32,
    total_tokens: u32,
    latency_ms: u64,
}

// ---------------------------------------------------------------------------
// The pre-call half
// ---------------------------------------------------------------------------

/// A turn ready to call: the conversation to send, durably committed.
pub struct BegunTurn {
    /// The conversation to send — the committed input verbatim, whether
    /// this attempt rendered it or a prior one did.
    pub conversation: RenderedConversation,
}

/// Commits the input record (first attempt) or re-reads it (resume), and
/// returns the conversation the call must send.
///
/// The input commits in its own transaction before any wire call: what
/// was sent is durable even if the process dies mid-call, and at-least-
/// once re-execution sends byte-identical content rather than
/// re-rendering against drifted sources.
///
/// # Errors
///
/// Returns [`AgentRuntimeError::ContentSerialisation`] on a format fault
/// and [`AgentRuntimeError::Database`] on database errors.
pub async fn begin_one_shot(
    conn: &mut PgConnection,
    thread: &AgentThread,
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

    // Lock before seq derivation; for a stage thread the claim already
    // excludes rival writers, so this is the categorical rule, not a
    // contended path.
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

// ---------------------------------------------------------------------------
// The terminal half
// ---------------------------------------------------------------------------

/// The committed terminal: what the caller's transaction now carries.
pub struct OneShotOutcome {
    /// The committed assistant-message record.
    pub assistant_record: AgentThreadRecord,
}

/// Commits the one-shot terminal inside the caller's transaction: the
/// assistant-message record, the thread's completed status, and the
/// spend projection — composed with the caller's claim-guarded task
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
    })
    .map_err(|source| AgentRuntimeError::ContentSerialisation {
        context: format!("serialising the assistant record of thread {}", thread.id()),
        source,
    })?;
    let usage = serde_json::to_value(RecordedUsage {
        input_tokens: response.usage.input_tokens,
        output_tokens: response.usage.output_tokens,
        cache_read_tokens: response.usage.cache_read_tokens,
        cache_write_tokens: response.usage.cache_write_tokens,
        total_tokens: response.usage.total_tokens,
        latency_ms: u64::try_from(response.usage.latency.as_millis()).unwrap_or(u64::MAX),
    })
    .map_err(|source| AgentRuntimeError::ContentSerialisation {
        context: format!("serialising usage for thread {}", thread.id()),
        source,
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
