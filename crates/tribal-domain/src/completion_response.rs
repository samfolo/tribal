//! The completed result of an LLM completion call.

use crate::CompletionUsage;

/// One tool invocation requested by the model.
///
/// Carried on the completed response and echoed back on the assistant
/// message of subsequent conversation turns; the result a tool produces
/// answers it by `id`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ToolCall {
    /// The call identifier the matching tool result must echo.
    pub id: String,
    /// The tool name.
    pub name: String,
    /// The call's arguments, parsed from the model's output.
    pub arguments: serde_json::Value,
}

/// The response from an LLM completion call.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompletionResponse {
    /// The generated text. Empty when the model answered with tool
    /// calls alone.
    pub text: String,
    /// The tool invocations the model requested, in response order.
    /// Empty for a plain text response.
    pub tool_calls: Vec<ToolCall>,
    /// Token usage and latency for this call.
    pub usage: CompletionUsage,
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    fn usage() -> CompletionUsage {
        CompletionUsage {
            provider: "ollama".to_owned(),
            model: "llama3".to_owned(),
            input_tokens: 10,
            output_tokens: 5,
            cache_read_tokens: 0,
            cache_write_tokens: 0,
            total_tokens: 15,
            latency: Duration::from_millis(200),
        }
    }

    #[test]
    fn test_completion_response_equality() {
        let a = CompletionResponse {
            text: "hello".to_owned(),
            tool_calls: vec![],
            usage: usage(),
        };
        let b = a.clone();
        assert_eq!(a, b);
    }

    #[test]
    fn test_tool_call_carries_parsed_arguments() {
        let call = ToolCall {
            id: "call_1".to_owned(),
            name: "search_similar_items".to_owned(),
            arguments: serde_json::json!({"query": "hippocampal indexing"}),
        };
        let response = CompletionResponse {
            text: String::new(),
            tool_calls: vec![call.clone()],
            usage: usage(),
        };
        assert_eq!(response.tool_calls[0], call);
        assert_eq!(
            response.tool_calls[0].arguments["query"],
            "hippocampal indexing"
        );
    }
}
