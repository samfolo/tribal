//! Span attribute constants for consistent tracing instrumentation.
//!
//! These constants define the field names used in `tracing` spans across
//! the Tribal workspace.  Using constants rather than string literals
//! prevents typos and ensures consistent naming in structured log output
//! and OpenTelemetry export.
//!
//! # Usage
//!
//! ```ignore
//! use tribal_domain::span_attrs;
//!
//! let span = tracing::info_span!(
//!     "process_job",
//!     { span_attrs::PROJECT_ID } = %project_id,
//!     { span_attrs::JOB_ID } = %job_id,
//! );
//! ```

// ---------------------------------------------------------------------------
// OpenTelemetry plumbing
// ---------------------------------------------------------------------------

/// Span field name that `tracing-opentelemetry` maps onto the exported span
/// name. `tracing` span names are static strings, so a dynamic span name
/// (such as the `GenAI` conventions' `{operation} {model}` form) is recorded
/// through this field instead.
pub const OTEL_NAME: &str = "otel.name";

// ---------------------------------------------------------------------------
// Core identifiers
// ---------------------------------------------------------------------------

/// Span field name for the project identifier.
pub const PROJECT_ID: &str = "tribal.project_id";

/// Span field name for the principal (user or agent) key.
pub const PRINCIPAL_KEY: &str = "tribal.principal_key";

/// Span field name for the job identifier.
pub const JOB_ID: &str = "tribal.job_id";

/// Span field name for the task identifier.
pub const TASK_ID: &str = "tribal.task_id";

/// Span field name for the episode identifier.
pub const EPISODE_ID: &str = "tribal.episode_id";

/// Span field name for the transport type (e.g. `"stdio"`, `"http"`, `"sse"`).
pub const TRANSPORT: &str = "tribal.transport";

// ---------------------------------------------------------------------------
// Embedding spans
// ---------------------------------------------------------------------------

/// Span field name for the embedding model identifier.
pub const EMBEDDING_MODEL: &str = "tribal.embedding.model";

/// Span field name for embedding vector dimensions.
pub const EMBEDDING_DIMENSIONS: &str = "tribal.embedding.dimensions";

/// Span field name for the embedding purpose: an
/// [`EmbeddingPurpose`](crate::EmbeddingPurpose) wire string.
pub const EMBEDDING_PURPOSE: &str = "tribal.embedding.purpose";

// ---------------------------------------------------------------------------
// Reindex spans
// ---------------------------------------------------------------------------

/// Span field name for the reindex run identifier. The target model and
/// dimensions ride [`EMBEDDING_MODEL`] and [`EMBEDDING_DIMENSIONS`].
pub const REINDEX_RUN_ID: &str = "tribal.reindex.run_id";

// ---------------------------------------------------------------------------
// Pipeline attribution
// ---------------------------------------------------------------------------

/// Span field name for the pipeline stage a call is attributed to.
pub const STAGE: &str = "tribal.stage";

/// Span field name for the system prompt version used in an inference call.
pub const SYSTEM_PROMPT_VERSION_ID: &str = "tribal.prompt.system_version_id";

/// Span field name for the user prompt version used in an inference call.
pub const USER_PROMPT_VERSION_ID: &str = "tribal.prompt.user_version_id";

// ---------------------------------------------------------------------------
// Worker spans
// ---------------------------------------------------------------------------

/// Span field name for the batch index of a triage task within its extraction.
pub const BATCH_INDEX: &str = "tribal.batch_index";

/// Span field name for the triage batch size (number of candidates).
pub const BATCH_SIZE: &str = "tribal.batch_size";

/// Span field name for the original extraction candidate count (pre-cap).
pub const EXTRACTION_ORIGINAL_COUNT: &str = "tribal.extraction.original_count";

/// Span field name for the triage stage outcome classification.
pub const TRIAGE_OUTCOME: &str = "tribal.triage.outcome";

/// Span field name for the number of relations committed.
pub const RELATIONS_COMMITTED: &str = "tribal.relations.committed";

/// Span field name for the number of relations skipped during normalisation.
pub const RELATIONS_SKIPPED: &str = "tribal.relations.skipped";

/// Span field name for relations dropped by pre-insert endpoint validation.
pub const RELATIONS_VALIDATION_DROPPED: &str = "tribal.relations.validation_dropped";

/// Span field name for the overall job outcome.
pub const JOB_OUTCOME: &str = "tribal.job.outcome";

/// Span field name for the relation batch identifier.
pub const RELATION_BATCH_ID: &str = "tribal.relation_batch_id";

/// Span field name for the number of semantic search results returned.
pub const SEARCH_RESULTS_COUNT: &str = "tribal.search.results_count";

/// Span field name for the semantic search limit (top-K).
pub const SEARCH_LIMIT: &str = "tribal.search.limit";

/// Span field name for the task retry count.
pub const RETRY_COUNT: &str = "tribal.retry_count";

/// Span field name for the worker instance identifier.
pub const WORKER_INSTANCE_ID: &str = "tribal.worker.instance_id";

/// Span field name indicating the trace context was invalid or unparseable.
pub const TRACE_CONTEXT_INVALID: &str = "tribal.trace_context.invalid";

// ---------------------------------------------------------------------------
// Tag resolution spans
// ---------------------------------------------------------------------------

/// Span field name for the number of tags resolved (exact + semantic matches).
pub const TAG_RESOLUTION_RESOLVED: &str = "tribal.tag_resolution.resolved";

/// Span field name for the number of new tags created.
pub const TAG_RESOLUTION_NEW: &str = "tribal.tag_resolution.new";

/// Span field name for the number of tags matched via embedding similarity.
pub const TAG_RESOLUTION_SEMANTIC_MATCHED: &str = "tribal.tag_resolution.semantic_matched";

/// Span field name for the highest similarity score observed during resolution.
pub const TAG_RESOLUTION_BEST_SIMILARITY: &str = "tribal.tag_resolution.best_similarity";

// ---------------------------------------------------------------------------
// Agentic runtime spans and lifecycle events
// ---------------------------------------------------------------------------

/// Span field name for the executor a thread-advancing execution runs
/// under: a `StageExecutorKind` wire string.
pub const EXECUTOR: &str = "tribal.executor";

/// Span field name flagging an `invoke_agent` that resumed a committed log
/// rather than rendering a fresh opening.
pub const THREAD_RESUMED: &str = "tribal.thread.resumed";

/// Span field name for the log seq a resumed execution re-entered at.
pub const RESUMED_AT_SEQ: &str = "tribal.resumed_at_seq";

/// Span field name for the committed assistant turns a resume replayed.
pub const TURNS_REPLAYED: &str = "tribal.turns_replayed";

/// Span field name for a committed turn's index (its record seq).
pub const TURN_INDEX: &str = "tribal.turn.index";

/// Span field name for the running count of committed assistant turns.
pub const ASSISTANT_TURNS: &str = "tribal.assistant_turns";

/// Span field name for a tool call's name on an `execute_tool` span.
pub const TOOL_NAME: &str = "tribal.tool.name";

/// Span field name for the tool call identifier its result answers.
pub const TOOL_CALL_ID: &str = "tribal.tool.call_id";

/// Span field name for an `execute_tool` span's outcome classification.
pub const TOOL_OUTCOME: &str = "tribal.tool.outcome";

/// Span field name flagging the distinguished completion tool.
pub const TOOL_IS_SUBMIT: &str = "tribal.tool.is_submit";

/// Span field name for a submission evaluation's outcome.
pub const SUBMISSION_OUTCOME: &str = "tribal.submission.outcome";

/// [`SUBMISSION_OUTCOME`] value: the submission passed every validator.
pub const SUBMISSION_OUTCOME_ACCEPTED: &str = "accepted";

/// [`SUBMISSION_OUTCOME`] value: a validator rejected it.
pub const SUBMISSION_OUTCOME_BOUNCED: &str = "bounced";

/// [`TOOL_OUTCOME`] value: a result record committed.
pub const TOOL_OUTCOME_COMMITTED: &str = "committed";

/// [`TOOL_OUTCOME`] value: the fence refused the append.
pub const TOOL_OUTCOME_FENCE_CONFLICT: &str = "fence_conflict";

/// [`TOOL_OUTCOME`] value: an accepted submission with no verifier.
pub const TOOL_OUTCOME_SUBMITTED: &str = "submitted";

/// [`TOOL_OUTCOME`] value: an accepted submission launched a verifier.
pub const TOOL_OUTCOME_SUSPENDED: &str = "suspended";

/// [`TOOL_OUTCOME`] value: a cancellation intent intervened at the suspend.
pub const TOOL_OUTCOME_CANCEL_INTERVENED: &str = "cancel_intervened";

/// Span field name for why a thread suspended: an `AgentThreadSuspension`
/// wire string.
pub const SUSPENSION_REASON: &str = "tribal.suspension.reason";

/// [`SUSPENSION_REASON`] value: pending deferred tool results (a child launch).
pub const SUSPENSION_REASON_DEFERRED: &str = "deferred_tool_results";

/// [`SUSPENSION_REASON`] value: sleeping on a wake-at timer.
pub const SUSPENSION_REASON_TIMER: &str = "timer";

/// [`SUSPENSION_REASON`] value: an exhausted execution budget.
pub const SUSPENSION_REASON_BUDGET: &str = "budget_exhausted";

/// [`SUSPENSION_REASON`] value: awaiting human input.
pub const SUSPENSION_REASON_HUMAN: &str = "human_input";

/// [`SUSPENSION_REASON`] value: a durable wait on an opaque signal.
pub const SUSPENSION_REASON_SIGNAL: &str = "signal";

/// Span field name for a suspension's scheduled wake instant.
pub const WAKE_AT: &str = "tribal.wake_at";

/// Span field name for consecutive unchanged budget re-checks.
pub const UNCHANGED_RECHECKS: &str = "tribal.unchanged_rechecks";

/// Span field name for a pre-call budget admission decision
/// (`admitted`/`suspend`/`failed`).
pub const BUDGET_DECISION: &str = "tribal.budget.decision";

/// [`BUDGET_DECISION`] value: headroom exists; the call proceeds.
pub const BUDGET_DECISION_ADMITTED: &str = "admitted";

/// [`BUDGET_DECISION`] value: the budget is exhausted; the thread suspends.
pub const BUDGET_DECISION_SUSPEND: &str = "suspend";

/// [`BUDGET_DECISION`] value: the bounded re-checks ran dry; the thread fails.
pub const BUDGET_DECISION_FAILED: &str = "failed";

/// Span field name for the tokens the ledger accounts to a thread.
pub const BUDGET_SPENT: &str = "tribal.budget.spent";

/// Span field name for a binding's token cap.
pub const BUDGET_CAP: &str = "tribal.budget.cap";

/// Span field name for a launched child's content-addressed binding version.
pub const CHILD_BINDING_VERSION: &str = "tribal.child.binding_version";

/// Span field name for a launched child's pipeline stage.
pub const CHILD_STAGE: &str = "tribal.child.stage";

/// Span field name for the assistant message a deferred result answers.
pub const REQUESTING_SEQ: &str = "tribal.requesting_seq";

/// Span field name for a child terminal's outcome for its parent.
pub const CHILD_TERMINAL_OUTCOME: &str = "tribal.child.terminal_outcome";

/// [`CHILD_TERMINAL_OUTCOME`] value: the verdict woke and re-queued the parent.
pub const CHILD_TERMINAL_HANDED_BACK: &str = "handed_back";

/// [`CHILD_TERMINAL_OUTCOME`] value: the parent was no longer waiting.
pub const CHILD_TERMINAL_PARENT_NOT_WAITING: &str = "parent_not_waiting";

/// Span field name for a child execution's parent thread id.
pub const PARENT_THREAD_ID: &str = "tribal.parent_thread_id";

/// Span field name flagging an error-shaped hand-back (a child death).
pub const TERMINAL_IS_ERROR: &str = "tribal.terminal.is_error";

/// Span field name for the driver attempt a child terminal committed under.
pub const DRIVER_ATTEMPT: &str = "tribal.driver.attempt";

/// Span field name for the in-place conflict retries left when one fires.
pub const CONFLICT_RETRY_REMAINING: &str = "tribal.conflict_retry.remaining";

/// Span field name for the loop's concluding outcome
/// (`submitted`/`suspended`/`cancel_intent`/`budget_failed`).
pub const LOOP_OUTCOME: &str = "tribal.loop.outcome";

/// [`LOOP_OUTCOME`] value: an accepted submission.
pub const LOOP_OUTCOME_SUBMITTED: &str = "submitted";

/// [`LOOP_OUTCOME`] value: a suspension committed.
pub const LOOP_OUTCOME_SUSPENDED: &str = "suspended";

/// [`LOOP_OUTCOME`] value: a durable cancellation intent was observed.
pub const LOOP_OUTCOME_CANCEL_INTENT: &str = "cancel_intent";

/// [`LOOP_OUTCOME`] value: a budget exhaustion failed the thread.
pub const LOOP_OUTCOME_BUDGET_FAILED: &str = "budget_failed";

// ---------------------------------------------------------------------------
// Availability sweep
// ---------------------------------------------------------------------------

/// Span field name for suspended threads woken by an elapsed timer.
pub const SWEEP_TIMER_WAKES: &str = "tribal.sweep.timer_wakes";

/// Span field name for orphaned descendants the cascade janitor marked.
pub const SWEEP_CASCADED: &str = "tribal.sweep.cascaded";

/// Span field name for threads the cancel fallback terminated.
pub const SWEEP_CANCELLED: &str = "tribal.sweep.cancelled";

/// Span field name for stranded relation threads the sweep failed.
pub const SWEEP_STUCK_RELATING: &str = "tribal.sweep.stuck_relating";

/// Span field name for managed runs whose elapsed cap or signal wake the sweep
/// carried to the job plane.
pub const SWEEP_MANAGED_WAKES: &str = "tribal.sweep.managed_wakes";

/// Span field name for suspended managed runs the sweep woke to be torn down on
/// a durable cancel intent.
pub const SWEEP_MANAGED_CANCELS: &str = "tribal.sweep.managed_cancels";

/// Span field name for which sweep predicate a per-action event reports.
pub const SWEEP_ACTION: &str = "tribal.sweep.action";

/// Span field name for the count a sweep action converged.
pub const SWEEP_ACTION_COUNT: &str = "tribal.sweep.action_count";

/// [`SWEEP_ACTION`] value: the timer-wake predicate.
pub const SWEEP_ACTION_TIMER_WAKE: &str = "timer_wake";

/// [`SWEEP_ACTION`] value: the orphan-cascade janitor.
pub const SWEEP_ACTION_ORPHAN_CASCADE: &str = "orphan_cascade";

/// [`SWEEP_ACTION`] value: the cancel-fallback predicate.
pub const SWEEP_ACTION_CANCEL_FALLBACK: &str = "cancel_fallback";

/// [`SWEEP_ACTION`] value: the stuck-relating predicate.
pub const SWEEP_ACTION_STUCK_RELATING: &str = "stuck_relating";

/// [`SWEEP_ACTION`] value: the managed timer-wake predicate.
pub const SWEEP_ACTION_MANAGED_WAKE: &str = "managed_wake";

/// [`SWEEP_ACTION`] value: the cancel-while-suspended predicate.
pub const SWEEP_ACTION_MANAGED_CANCEL: &str = "managed_cancel";
