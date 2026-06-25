//! rmcp response glue for `tribal_discover`.

use std::fmt::Write;

use rmcp::model::{CallToolResult, Content};
use tribal_wire::mcp::McpDiscoverResponse;

use crate::{error::IntoCallToolResult, format::truncate_content};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const SERIALISE_DISCOVER_RESPONSE: &str =
    "McpDiscoverResponse should always serialise successfully";

/// Maximum number of characters to include in the content preview
/// within the human-readable text summary.
const CONTENT_PREVIEW_MAX_LENGTH: usize = 100;

// ---------------------------------------------------------------------------
// IntoCallToolResult
// ---------------------------------------------------------------------------

impl IntoCallToolResult for McpDiscoverResponse {
    fn into_call_tool_result(self) -> CallToolResult {
        let count = self.items.len();
        let scope = match self.project_name.as_deref() {
            Some(name) => format!("scoped to project '{name}'"),
            None => "global search".to_owned(),
        };
        let mut text = format!("Found {count} items for '{}' ({scope}).", self.query);

        if let Some(top) = self.items.first() {
            let kind = &top.item.kind;
            let preview = truncate_content(&top.item.content, CONTENT_PREVIEW_MAX_LENGTH);
            let _ = write!(
                text,
                " Top result: [{kind}] {preview} ({:.2} similarity).",
                top.similarity,
            );
        }

        let structured = serde_json::to_value(&self).expect(SERIALISE_DISCOVER_RESPONSE);
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

    use super::*;

    #[test]
    fn test_discover_response_into_call_tool_result() {
        let resp = McpDiscoverResponse {
            items: vec![],
            next_cursor: None,
            applied_project_id: Some("proj_abc".into()),
            embedding_model: "m".into(),
            embedding_profile_id: "eprof_test".into(),
            trace_id: "t".into(),
            exact: true,
            query: "auth patterns".into(),
            project_name: Some("tribal".into()),
        };
        let result = resp.into_call_tool_result();
        assert_eq!(result.is_error, Some(false));
        assert_eq!(result.content.len(), 1);
        assert!(result.structured_content.is_some());
    }

    #[test]
    fn test_text_summary_global_search() {
        let resp = McpDiscoverResponse {
            items: vec![],
            next_cursor: None,
            applied_project_id: None,
            embedding_model: "m".into(),
            embedding_profile_id: "eprof_test".into(),
            trace_id: "t".into(),
            exact: true,
            query: "auth patterns".into(),
            project_name: None,
        };
        let result = resp.into_call_tool_result();
        let RawContent::Text(raw_text) = &result.content[0].raw else {
            panic!("expected text content");
        };
        assert!(raw_text.text.contains("global search"));
        assert!(raw_text.text.contains("auth patterns"));
    }

    #[test]
    fn test_text_summary_scoped_to_project() {
        let resp = McpDiscoverResponse {
            items: vec![],
            next_cursor: None,
            applied_project_id: Some("proj_abc".into()),
            embedding_model: "m".into(),
            embedding_profile_id: "eprof_test".into(),
            trace_id: "t".into(),
            exact: true,
            query: "auth".into(),
            project_name: Some("tribal".into()),
        };
        let result = resp.into_call_tool_result();
        let RawContent::Text(raw_text) = &result.content[0].raw else {
            panic!("expected text content");
        };
        assert!(raw_text.text.contains("scoped to project 'tribal'"));
    }
}
