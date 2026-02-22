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

/// Span field name for the embedding provider name.
pub const EMBEDDING_PROVIDER: &str = "tribal.embedding.provider";

/// Span field name for the embedding model identifier.
pub const EMBEDDING_MODEL: &str = "tribal.embedding.model";

/// Span field name for embedding token count.
pub const EMBEDDING_TOKENS: &str = "tribal.embedding.tokens";

/// Span field name for embedding vector dimensions.
pub const EMBEDDING_DIMENSIONS: &str = "tribal.embedding.dimensions";

/// Span field name for embedding request latency in milliseconds.
pub const EMBEDDING_LATENCY_MS: &str = "tribal.embedding.latency_ms";

/// Span field name for the embedding purpose (`"candidate"` or `"query"`).
pub const EMBEDDING_PURPOSE: &str = "tribal.embedding.purpose";

// ---------------------------------------------------------------------------
// LLM completion spans
// ---------------------------------------------------------------------------

/// Span field name for the LLM provider name.
pub const LLM_PROVIDER: &str = "tribal.llm.provider";

/// Span field name for the LLM model identifier.
pub const LLM_MODEL: &str = "tribal.llm.model";

/// Span field name for the pipeline stage invoking the LLM.
pub const LLM_STAGE: &str = "tribal.llm.stage";

/// Span field name for LLM input token count.
pub const LLM_TOKENS_INPUT: &str = "tribal.llm.tokens.input";

/// Span field name for LLM output token count.
pub const LLM_TOKENS_OUTPUT: &str = "tribal.llm.tokens.output";

/// Span field name for LLM cache-read token count.
pub const LLM_TOKENS_CACHE_READ: &str = "tribal.llm.tokens.cache_read";

/// Span field name for LLM cache-write token count.
pub const LLM_TOKENS_CACHE_WRITE: &str = "tribal.llm.tokens.cache_write";

/// Span field name for LLM total token count.
pub const LLM_TOKENS_TOTAL: &str = "tribal.llm.tokens.total";

/// Span field name for LLM request latency in milliseconds.
pub const LLM_LATENCY_MS: &str = "tribal.llm.latency_ms";

/// Span field name for the sampling temperature.
pub const LLM_TEMPERATURE: &str = "tribal.llm.temperature";
