//! Wire DTOs for `tribal_feedback`.

use serde::{Deserialize, Serialize};
use tribal_domain::RetrievalFeedback;

// ---------------------------------------------------------------------------
// Request
// ---------------------------------------------------------------------------

/// Deserialisation target for `tribal_feedback` input.
///
/// The `rating` field is a raw `String` so that invalid values (e.g.
/// `"neutral"`) are caught during explicit validation rather than at
/// the `serde_json::from_value` boundary.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct McpFeedbackRequest {
    pub trace_id: String,
    pub query_text: String,
    pub returned_item_ids: Vec<String>,
    pub explored_anchor_ids: Option<Vec<String>>,
    pub rating: String,
    pub notes: Option<String>,
    /// The `embedding_profile_id` from the discover response this feedback
    /// rates, carried back so the lineage records the profile that produced
    /// the results. Absent when the client does not echo it.
    #[serde(default)]
    pub embedding_profile_id: Option<String>,
}

// ---------------------------------------------------------------------------
// Response
// ---------------------------------------------------------------------------

/// Response for `tribal_feedback`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct McpFeedbackResponse {
    pub feedback_id: String,
}

impl From<&RetrievalFeedback> for McpFeedbackResponse {
    fn from(feedback: &RetrievalFeedback) -> Self {
        Self {
            feedback_id: feedback.id().to_string(),
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
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
    fn test_feedback_response_round_trips() {
        let resp = McpFeedbackResponse {
            feedback_id: "fb_123".into(),
        };
        let json = serde_json::to_value(&resp).expect("serialises");
        let back: McpFeedbackResponse = serde_json::from_value(json).expect("deserialises");
        assert_eq!(back, resp);
    }
}
