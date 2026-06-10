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

/// Span field name for the embedding purpose (`"candidate"`, `"query"`,
/// `"tag"`, or `"probe"`).
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
