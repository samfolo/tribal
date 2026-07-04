//! The inference bracket: the streamed call a managed run makes, its response
//! stream, and the typed outcomes the gateway can return.
//!
//! The request carries the call and its position key and nothing that asserts
//! tenancy — no account, principal, or rate — so the gateway derives
//! `(account, principal)` from the presented credential and the spend cap can
//! never be made opt-in. A completion pins `max_tokens` in the type because the
//! metered path requires it to size a worst-case hold; an embedding has no such
//! axis and carries none.

use serde::{Deserialize, Serialize};

use crate::gateway::reference::{ModelId, PositionKey};

/// The HTTP header a metered call presents its run-scoped grant set on. The
/// body cannot carry tenancy — a client-asserted account would make the spend
/// cap opt-in — so the grant set rides beside the fleet bearer token, and the
/// gateway derives `(account, principal)` from it. Its value is the JSON of a
/// [`GrantSet`](crate::gateway::GrantSet).
pub const GRANT_SET_HEADER: &str = "x-tribal-grant-set";

// ---------------------------------------------------------------------------
// Conversation
// ---------------------------------------------------------------------------

/// A message in a completion conversation. Each variant carries exactly the
/// fields its role admits, so a user message with tool calls is unrepresentable.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(tag = "role", rename_all = "snake_case")]
pub enum ChatMessage {
    /// A user-authored message.
    User {
        /// The message content.
        content: String,
    },
    /// An assistant turn: text, tool calls, or both.
    Assistant {
        /// The assistant's text, empty when the turn was tool calls alone.
        content: String,
        /// The tool calls this turn requested, in response order.
        tool_calls: Vec<ToolCall>,
    },
    /// A tool result answering an earlier assistant turn's call.
    Tool {
        /// The call this result answers.
        tool_call_id: String,
        /// The serialised result content.
        content: String,
    },
}

/// A tool call the model requested.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct ToolCall {
    /// The identifier a later tool result answers.
    pub id: String,
    /// The tool name called.
    pub name: String,
    /// The call arguments as a JSON value.
    pub arguments: serde_json::Value,
}

/// A tool offered to the model for one call — the wire projection of a tool's
/// registry descriptor: only what the provider needs to offer it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct ToolDefinition {
    /// The tool name the model calls.
    pub name: String,
    /// The model-facing description.
    pub description: String,
    /// The JSON Schema for the tool's arguments.
    pub input_schema: serde_json::Value,
}

/// The response format a completion requests.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ResponseFormat {
    /// A JSON response the provider validates for syntax.
    Json,
    /// A JSON response conforming to a schema.
    JsonSchema {
        /// The JSON Schema the response must conform to.
        schema: serde_json::Value,
    },
}

// ---------------------------------------------------------------------------
// Request
// ---------------------------------------------------------------------------

/// A completion call. `max_tokens` is required, not optional: the metered path
/// sizes a worst-case hold from it, so an envelope that omitted it could not be
/// reserved against.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct CompletionEnvelope {
    /// The model to serve the call.
    pub model: ModelId,
    /// The system prompt prepended to the conversation, when set.
    pub system: Option<String>,
    /// The conversation so far.
    pub messages: Vec<ChatMessage>,
    /// The tools offered to the model.
    pub tools: Vec<ToolDefinition>,
    /// The requested response format, when constrained.
    pub response_format: Option<ResponseFormat>,
    /// The output-token ceiling — the worst-case hold is sized against it.
    pub max_tokens: u32,
    /// The sampling temperature, when overridden.
    pub temperature: Option<f32>,
    /// The nucleus-sampling probability mass, when overridden.
    pub top_p: Option<f32>,
    /// Sequences that end generation, when set.
    pub stop_sequences: Vec<String>,
}

/// A batch embedding call. Sized on input tokens — embeddings have no
/// `max_tokens` axis — so the re-index bulk path meters through the same
/// bracket.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct EmbeddingEnvelope {
    /// The model to embed with.
    pub model: ModelId,
    /// The inputs to embed, one vector per entry.
    pub inputs: Vec<String>,
}

/// The call an inference-bracket request carries.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(tag = "operation", rename_all = "snake_case")]
pub enum InferenceCall {
    /// A streamed completion.
    Completion(CompletionEnvelope),
    /// A batch embedding.
    Embedding(EmbeddingEnvelope),
}

/// A managed run's metered call: the call itself and the position key that
/// scopes it, stamped with the contract version the client speaks.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct InferenceRequest {
    /// The gateway contract version this request is built against.
    pub contract_version: u16,
    /// The call within its run.
    pub position_key: PositionKey,
    /// The call to make.
    pub call: InferenceCall,
}

// ---------------------------------------------------------------------------
// Response stream
// ---------------------------------------------------------------------------

/// One streamed piece of a completion — an incremental text delta.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct CompletionChunk {
    /// The text produced since the previous chunk.
    pub text: String,
}

/// Why a completion stopped.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum FinishReason {
    /// The model ended its turn.
    Stop,
    /// The output-token ceiling was reached.
    Length,
    /// The turn stopped to call tools.
    ToolUse,
}

/// The terminal of a completion stream — the final output the run commits.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct CompletionTerminal {
    /// Why the stream ended.
    pub finish_reason: FinishReason,
    /// The tool calls the turn requested, if any.
    pub tool_calls: Vec<ToolCall>,
}

/// A typed non-delivery outcome, distinct so a client never misreads one for
/// another — an over-cap for a live call, or a transient in-flight for an
/// overdraft.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum GatewayError {
    /// The account's plan does not entitle this capability.
    NotEntitled,
    /// The account has no credit or headroom under its caps.
    OverCap,
    /// The rate card has no rate for this call, so it cannot be priced.
    Unpriceable,
    /// Another presentation owns the live call; retry after the delay.
    InFlight {
        /// How long to wait before retrying, in milliseconds.
        retry_after_ms: u64,
    },
    /// The provider failed the call; its partial cost is already settled.
    Failed,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn a_completion_request() -> InferenceRequest {
        InferenceRequest {
            contract_version: super::super::GATEWAY_CONTRACT_VERSION,
            position_key: PositionKey::new("thread_01:42"),
            call: InferenceCall::Completion(CompletionEnvelope {
                model: ModelId::new("claude-sonnet-5"),
                system: Some("be terse".to_owned()),
                messages: vec![ChatMessage::User {
                    content: "hello".to_owned(),
                }],
                tools: vec![],
                response_format: None,
                max_tokens: 1_024,
                temperature: Some(0.2),
                top_p: None,
                stop_sequences: vec![],
            }),
        }
    }

    #[test]
    fn completion_request_round_trips() {
        let request = a_completion_request();
        let parsed: InferenceRequest =
            serde_json::from_str(&serde_json::to_string(&request).unwrap()).unwrap();
        assert_eq!(parsed, request);
    }

    #[test]
    fn embedding_call_round_trips() {
        let call = InferenceCall::Embedding(EmbeddingEnvelope {
            model: ModelId::new("text-embedding-3"),
            inputs: vec!["a".to_owned(), "b".to_owned()],
        });
        let parsed: InferenceCall =
            serde_json::from_str(&serde_json::to_string(&call).unwrap()).unwrap();
        assert_eq!(parsed, call);
    }

    #[test]
    fn an_in_flight_error_is_distinct_from_over_cap() {
        let in_flight = serde_json::to_string(&GatewayError::InFlight {
            retry_after_ms: 500,
        })
        .expect("serialises");
        let over_cap = serde_json::to_string(&GatewayError::OverCap).expect("serialises");
        assert_ne!(in_flight, over_cap);
        let parsed: GatewayError = serde_json::from_str(&in_flight).unwrap();
        assert_eq!(
            parsed,
            GatewayError::InFlight {
                retry_after_ms: 500
            }
        );
    }
}
