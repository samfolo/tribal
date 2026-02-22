//! Request types for inference provider calls.
//!
//! All types have public fields and derive `Debug, Clone, PartialEq`.
//! Serialisation is intentionally omitted — it is a concern of concrete
//! provider implementations.

use tribal_domain::EmbeddingPurpose;

/// The role of a message author in a completion conversation.
///
/// New variants may be added in future (e.g. `Tool`) without a
/// semver-breaking change.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum Role {
    /// A user-authored message.
    User,
    /// An assistant-authored message.
    Assistant,
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
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Message {
    /// The role of the message author.
    pub role: Role,
    /// The message content.
    pub content: String,
}

/// A request for an LLM completion.
#[derive(Debug, Clone, PartialEq)]
pub struct CompletionRequest {
    /// Optional system prompt prepended to the conversation.
    pub system: Option<String>,
    /// The conversation messages.
    pub messages: Vec<Message>,
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
            messages: vec![Message {
                role: Role::User,
                content: "hello".to_owned(),
            }],
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
