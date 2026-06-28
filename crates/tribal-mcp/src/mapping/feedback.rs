//! rmcp response glue for `tribal_feedback`.

use rmcp::model::{CallToolResult, Content};
use tribal_wire::mcp::McpFeedbackResponse;

use crate::error::IntoCallToolResult;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const SERIALISE_FEEDBACK_RESPONSE: &str =
    "McpFeedbackResponse should always serialise successfully";

// ---------------------------------------------------------------------------
// Response
// ---------------------------------------------------------------------------

impl IntoCallToolResult for McpFeedbackResponse {
    fn into_call_tool_result(self) -> CallToolResult {
        let text = format!("Feedback recorded: {}", self.feedback_id);
        let structured = serde_json::to_value(&self).expect(SERIALISE_FEEDBACK_RESPONSE);
        let mut result = CallToolResult::success(vec![Content::text(text)]);
        result.structured_content = Some(structured);
        result
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use rmcp::model::RawContent;
    use tribal_test_utils::a_retrieval_feedback;

    use super::*;

    #[test]
    fn test_feedback_response_into_call_tool_result() {
        let feedback = a_retrieval_feedback().build();
        let resp = McpFeedbackResponse::from(&feedback);
        let result = resp.into_call_tool_result();

        assert_eq!(result.is_error, Some(false));
        assert!(result.structured_content.is_some());

        let RawContent::Text(text) = &result.content[0].raw else {
            panic!("expected text content");
        };
        assert!(text.text.contains("fb_"));
    }
}
