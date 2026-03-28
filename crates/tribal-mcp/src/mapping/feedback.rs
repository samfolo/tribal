//! MCP request and response types for `tribal_feedback`.

use rmcp::model::{CallToolResult, Content};
use serde::{Deserialize, Serialize};
use tribal_domain::RetrievalFeedback;

use crate::error::IntoCallToolResult;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const SERIALISE_FEEDBACK_RESPONSE: &str =
    "McpFeedbackResponse should always serialise successfully";

// ---------------------------------------------------------------------------
// Request
// ---------------------------------------------------------------------------

/// Deserialisation target for `tribal_feedback` input.
///
/// The `rating` field is a raw `String` so that invalid values (e.g.
/// `"neutral"`) are caught during explicit validation rather than at
/// the `serde_json::from_value` boundary.
#[derive(Debug, Deserialize)]
pub(crate) struct McpFeedbackRequest {
    pub(crate) trace_id: String,
    pub(crate) query_text: String,
    pub(crate) returned_item_ids: Vec<String>,
    pub(crate) explored_anchor_ids: Option<Vec<String>>,
    pub(crate) rating: String,
    pub(crate) notes: Option<String>,
}

// ---------------------------------------------------------------------------
// Response
// ---------------------------------------------------------------------------

/// Response for `tribal_feedback`.
///
/// The `rating` field is `#[serde(skip)]` — it is not part of the output
/// schema but is needed for the human-readable text summary.
#[derive(Debug, Serialize)]
pub(crate) struct McpFeedbackResponse {
    pub(crate) feedback_id: String,
    #[serde(skip)]
    pub(crate) rating: tribal_domain::FeedbackRating,
}

impl From<&RetrievalFeedback> for McpFeedbackResponse {
    fn from(feedback: &RetrievalFeedback) -> Self {
        Self {
            feedback_id: feedback.id().to_string(),
            rating: feedback.rating(),
        }
    }
}

impl IntoCallToolResult for McpFeedbackResponse {
    fn into_call_tool_result(self) -> CallToolResult {
        let text = format!(
            "Feedback recorded: {} (rating: {})",
            self.feedback_id, self.rating,
        );
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
    use tribal_domain::FeedbackRating;
    use tribal_test_utils::a_retrieval_feedback;

    use super::*;

    #[test]
    fn test_feedback_request_deserialises() {
        let json = serde_json::json!({
            "trace_id": "4bf92f3577b34da6a3ce929d0e0e4736",
            "query_text": "auth patterns",
            "returned_item_ids": ["ki_abc"],
            "rating": "positive",
        });
        let req: McpFeedbackRequest = serde_json::from_value(json).expect("deserialises");
        assert_eq!(req.rating, "positive");
        assert!(req.explored_anchor_ids.is_none());
        assert!(req.notes.is_none());
    }

    #[test]
    fn test_feedback_response_from_domain() {
        let feedback = a_retrieval_feedback().build();
        let resp = McpFeedbackResponse::from(&feedback);

        assert!(resp.feedback_id.starts_with("fb_"));
        assert_eq!(resp.rating, FeedbackRating::Positive);
    }

    #[test]
    fn test_feedback_response_structured_content_omits_rating() {
        let feedback = a_retrieval_feedback().build();
        let resp = McpFeedbackResponse::from(&feedback);
        let json = serde_json::to_value(&resp).expect("serialises");
        assert!(json.get("rating").is_none());
    }

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
        assert!(text.text.contains("positive"));
        assert!(text.text.contains("fb_"));
    }
}
