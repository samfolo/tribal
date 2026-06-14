//! Request types for inference provider calls.
//!
//! All types have public fields and derive `Debug, Clone, PartialEq`.
//! Serialisation is intentionally omitted — it is a concern of concrete
//! provider implementations.

use tribal_domain::{EmbeddingPurpose, ToolCall};

/// The role of a message author in a completion conversation.
///
/// New variants may be added in future without a semver-breaking change.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum Role {
    /// A user-authored message.
    User,
    /// An assistant-authored message.
    Assistant,
    /// A tool result answering an assistant's tool call.
    Tool,
}

impl Role {
    /// Returns the conventional wire-format string for this role.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::User => "user",
            Self::Assistant => "assistant",
            Self::Tool => "tool",
        }
    }
}

/// The desired response format for a completion.
///
/// New variants may be added in future without a semver-breaking change.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum ResponseFormat {
    /// Request a JSON response (provider validates JSON syntax).
    Json,
    /// Request a JSON response conforming to a specific schema.
    JsonSchema {
        /// The JSON Schema the response must conform to.
        schema: serde_json::Value,
    },
}

/// A message in a completion conversation.
///
/// The variants carry exactly the fields their role admits, so a user
/// message with tool calls or a tool result without its call identifier
/// is unrepresentable; each provider translates the variants into its
/// own wire shapes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Message {
    /// A user-authored message.
    User {
        /// The message content.
        content: String,
    },
    /// An assistant turn: text, tool calls, or both.
    Assistant {
        /// The assistant's text, empty when the turn was tool calls
        /// alone.
        content: String,
        /// The tool calls this turn requested, in response order.
        tool_calls: Vec<ToolCall>,
    },
    /// A tool result answering one of an earlier assistant turn's calls.
    Tool {
        /// The call this result answers.
        tool_call_id: String,
        /// The serialised result content.
        content: String,
    },
}

impl Message {
    /// Returns the author role of this message.
    #[must_use]
    pub fn role(&self) -> Role {
        match self {
            Self::User { .. } => Role::User,
            Self::Assistant { .. } => Role::Assistant,
            Self::Tool { .. } => Role::Tool,
        }
    }

    /// Returns the message content.
    #[must_use]
    pub fn content(&self) -> &str {
        match self {
            Self::User { content }
            | Self::Assistant { content, .. }
            | Self::Tool { content, .. } => content,
        }
    }
}

/// A tool made available to the model for one completion call.
///
/// The wire projection of a tool's registry descriptor: only what the
/// provider needs to offer the tool. Safety tier, execution mode, and
/// response-size bounds are runtime concerns that never reach the wire.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ToolWireDefinition {
    /// The tool name the model calls.
    pub name: String,
    /// The model-facing description.
    pub description: String,
    /// The JSON Schema for the tool's arguments.
    pub input_schema: serde_json::Value,
}

/// A request for an LLM completion.
#[derive(Debug, Clone, PartialEq)]
pub struct CompletionRequest {
    /// Optional system prompt prepended to the conversation.
    pub system: Option<String>,
    /// The conversation messages.
    pub messages: Vec<Message>,
    /// Tools offered to the model. Empty offers none; tool selection
    /// stays with the model (every provider defaults to automatic
    /// choice).
    pub tools: Vec<ToolWireDefinition>,
    /// Sampling temperature.  `None` uses the provider default.
    pub temperature: Option<f32>,
    /// Maximum tokens to generate.  `None` uses the provider default.
    pub max_tokens: Option<u32>,
    /// Desired response format.  `None` requests free-form text.
    pub response_format: Option<ResponseFormat>,
}

/// A request for an embedding generation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EmbeddingRequest {
    /// The text to embed.
    pub input: String,
    /// Whether this embedding is for indexing a candidate or querying.
    pub purpose: EmbeddingPurpose,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_completion_request_fields_accessible() {
        let request = CompletionRequest {
            system: Some("You are a helpful assistant.".to_owned()),
            messages: vec![Message::User {
                content: "hello".to_owned(),
            }],
            tools: vec![],
            temperature: None,
            max_tokens: Some(100),
            response_format: None,
        };
        let cloned = request.clone();
        assert_eq!(request, cloned);
    }

    #[test]
    fn test_embedding_request_fields_accessible() {
        let request = EmbeddingRequest {
            input: "some text".to_owned(),
            purpose: EmbeddingPurpose::Candidate,
        };
        let cloned = request.clone();
        assert_eq!(request, cloned);
    }

    #[test]
    fn test_role_is_copy() {
        let role = Role::User;
        let copied = role;
        assert_eq!(role, copied);
    }

    #[test]
    fn test_role_as_str() {
        assert_eq!(Role::User.as_str(), "user");
        assert_eq!(Role::Assistant.as_str(), "assistant");
        assert_eq!(Role::Tool.as_str(), "tool");
    }

    #[test]
    fn test_message_role_derivation() {
        let user = Message::User {
            content: "q".to_owned(),
        };
        let assistant = Message::Assistant {
            content: String::new(),
            tool_calls: vec![tribal_domain::ToolCall {
                id: "call_1".to_owned(),
                name: "search".to_owned(),
                arguments: serde_json::json!({}),
            }],
        };
        let tool = Message::Tool {
            tool_call_id: "call_1".to_owned(),
            content: "{}".to_owned(),
        };
        assert_eq!(user.role(), Role::User);
        assert_eq!(assistant.role(), Role::Assistant);
        assert_eq!(tool.role(), Role::Tool);
    }

    #[test]
    fn test_response_format_json_schema_equality() {
        let a = ResponseFormat::JsonSchema {
            schema: serde_json::json!({"type": "object"}),
        };
        let b = ResponseFormat::JsonSchema {
            schema: serde_json::json!({"type": "object"}),
        };
        assert_eq!(a, b);
    }
}
