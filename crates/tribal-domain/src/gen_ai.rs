//! OpenTelemetry `GenAI` semantic-convention names.
//!
//! Every `gen_ai.*` string Tribal emits lives in this module, pinned against
//! the semantic-conventions registry at v1.36.0 (stability: Development).
//! The conventions are still churning, so a registry rename must only ever
//! be a one-file diff here; no other module may spell a `gen_ai.*` string.
//! Deprecated names (`gen_ai.system`, `gen_ai.usage.prompt_tokens`,
//! `gen_ai.usage.completion_tokens`) are never emitted.
//!
//! Span names follow the convention's `{operation} {request model}` rule,
//! for example `chat gpt-4o-mini`. `tracing` span names are static, so the
//! dynamic form is recorded through the `otel.name` field
//! ([`span_attrs::OTEL_NAME`](crate::span_attrs::OTEL_NAME)), which
//! `tracing-opentelemetry` maps onto the exported span name.
//!
//! The value for [`PROVIDER_NAME`] is
//! [`ProviderKind::as_str()`](crate::ProviderKind::as_str): `anthropic` and
//! `openai` are well-known registry values, and `ollama` is the custom value
//! permitted for providers the registry does not list.

// ---------------------------------------------------------------------------
// Attribute keys
// ---------------------------------------------------------------------------

/// Attribute key for the operation name (see the `OPERATION_*` values).
pub const OPERATION_NAME: &str = "gen_ai.operation.name";

/// Attribute key for the provider name.
pub const PROVIDER_NAME: &str = "gen_ai.provider.name";

/// Attribute key for the model named in the request.
pub const REQUEST_MODEL: &str = "gen_ai.request.model";

/// Attribute key correlating spans of one conversation. Carries the agent
/// thread id, so a thread's traces stitch across the fresh root each claim
/// opens.
pub const CONVERSATION_ID: &str = "gen_ai.conversation.id";

/// Attribute key for the sampling temperature sent in the request.
pub const REQUEST_TEMPERATURE: &str = "gen_ai.request.temperature";

/// Attribute key for the input (prompt) token count.
pub const USAGE_INPUT_TOKENS: &str = "gen_ai.usage.input_tokens";

/// Attribute key for the output (completion) token count.
pub const USAGE_OUTPUT_TOKENS: &str = "gen_ai.usage.output_tokens";

/// Attribute key for input tokens served from a provider-managed cache.
pub const USAGE_CACHE_READ_INPUT_TOKENS: &str = "gen_ai.usage.cache_read.input_tokens";

/// Attribute key for input tokens written to a provider-managed cache.
pub const USAGE_CACHE_CREATION_INPUT_TOKENS: &str = "gen_ai.usage.cache_creation.input_tokens";

/// Attribute key for the requested embedding dimension count.
pub const EMBEDDINGS_DIMENSION_COUNT: &str = "gen_ai.embeddings.dimension.count";

/// Attribute key for the token class on the token-usage metric (see the
/// `TOKEN_TYPE_*` values).
pub const TOKEN_TYPE: &str = "gen_ai.token.type";

// ---------------------------------------------------------------------------
// Operation-name values
// ---------------------------------------------------------------------------

/// [`OPERATION_NAME`] value for a chat completion call.
pub const OPERATION_CHAT: &str = "chat";

/// [`OPERATION_NAME`] value for an embedding call.
pub const OPERATION_EMBEDDINGS: &str = "embeddings";

/// [`OPERATION_NAME`] value for a thread-advancing agent execution.
pub const OPERATION_INVOKE_AGENT: &str = "invoke_agent";

/// [`OPERATION_NAME`] value for a single tool call.
pub const OPERATION_EXECUTE_TOOL: &str = "execute_tool";

// ---------------------------------------------------------------------------
// Token-type values
// ---------------------------------------------------------------------------

/// [`TOKEN_TYPE`] value for input (prompt) tokens.
pub const TOKEN_TYPE_INPUT: &str = "input";

/// [`TOKEN_TYPE`] value for output (completion) tokens.
pub const TOKEN_TYPE_OUTPUT: &str = "output";

// ---------------------------------------------------------------------------
// Metric names
// ---------------------------------------------------------------------------

/// Histogram of token counts per request, classed by [`TOKEN_TYPE`].
/// Unit: `{token}`.
pub const CLIENT_TOKEN_USAGE: &str = "gen_ai.client.token.usage";

/// Histogram of client operation duration. Unit: seconds.
pub const CLIENT_OPERATION_DURATION: &str = "gen_ai.client.operation.duration";
