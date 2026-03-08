//! MCP request and response types for `tribal_discover`.

use std::fmt::Write;

use chrono::{DateTime, Utc};
use rmcp::model::{CallToolResult, Content};
use serde::{Deserialize, Deserializer, Serialize};
use tribal_domain::KnowledgeKind;

use super::common::{McpKnowledgeItem, McpReference, McpStanding};
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
// Request
// ---------------------------------------------------------------------------

/// Deserialisation target for `tribal_discover` input.
#[derive(Debug, Deserialize)]
pub(crate) struct McpDiscoverRequest {
    pub(crate) query: String,
    /// Three-way semantics: absent → use session project; explicit null →
    /// search globally; present → filter to this project.
    #[serde(default, deserialize_with = "deserialise_optional_nullable")]
    #[allow(clippy::option_option)]
    pub(crate) project_id: Option<Option<String>>,
    pub(crate) kinds: Option<Vec<KnowledgeKind>>,
    pub(crate) tags: Option<Vec<String>>,
    pub(crate) time_range: Option<McpTimeRange>,
    pub(crate) include_superseded: Option<bool>,
    pub(crate) include_standing: Option<bool>,
    pub(crate) include_references: Option<bool>,
    pub(crate) limit: Option<u32>,
    pub(crate) cursor: Option<String>,
}

/// Time range filter for discovery queries.
#[derive(Debug, Deserialize)]
pub(crate) struct McpTimeRange {
    pub(crate) from: Option<DateTime<Utc>>,
    pub(crate) to: Option<DateTime<Utc>>,
}

/// Custom deserialiser that distinguishes an absent field (`None`) from an
/// explicit JSON `null` (`Some(None)`) from a present value (`Some(Some(v))`).
///
/// serde's default `Option<Option<T>>` handling does not distinguish absent
/// from null — both produce `None` for the outer option. This deserialiser
/// is used with `#[serde(default, deserialize_with = "...")]` to preserve
/// the distinction.
#[allow(clippy::option_option)]
fn deserialise_optional_nullable<'de, D, T>(deserialiser: D) -> Result<Option<Option<T>>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    // If the field is present in the JSON, this function is called.
    // Deserialise as Option<T>: null → None, value → Some(value).
    Ok(Some(Option::deserialize(deserialiser)?))
}

// ---------------------------------------------------------------------------
// Response
// ---------------------------------------------------------------------------

/// Response for `tribal_discover`.
#[derive(Debug, Serialize)]
pub(crate) struct McpDiscoverResponse {
    pub(crate) items: Vec<McpDiscoveryResult>,
    /// Required + nullable — always present (null when no more results).
    pub(crate) next_cursor: Option<String>,
    /// Required + nullable — always present in serialised JSON.
    pub(crate) applied_project_id: Option<String>,
    pub(crate) embedding_model: String,
    pub(crate) trace_id: String,
    pub(crate) exact: bool,
    /// The original query text — used by `IntoCallToolResult` for the
    /// human-readable summary. Excluded from `structuredContent`.
    #[serde(skip)]
    pub(crate) query: String,
    /// Resolved project name — used by `IntoCallToolResult` for the
    /// human-readable summary. Excluded from `structuredContent`.
    #[serde(skip)]
    pub(crate) project_name: Option<String>,
}

/// A single discovery result with its similarity score.
#[derive(Debug, Serialize)]
pub(crate) struct McpDiscoveryResult {
    pub(crate) item: McpKnowledgeItem,
    pub(crate) similarity: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) standing: Option<McpStanding>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) references: Option<Vec<McpReference>>,
}

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
    fn test_discover_request_deserialises_minimal() {
        let json = serde_json::json!({"query": "authentication patterns"});
        let req: McpDiscoverRequest = serde_json::from_value(json).expect("deserialises");
        assert_eq!(req.query, "authentication patterns");
        // project_id absent → outer None
        assert!(req.project_id.is_none());
        assert!(req.kinds.is_none());
    }

    #[test]
    fn test_discover_request_deserialises_full() {
        let json = serde_json::json!({
            "query": "auth",
            "project_id": null,
            "kinds": ["fact"],
            "tags": ["auth"],
            "time_range": {"from": "2025-01-01T00:00:00Z"},
            "include_superseded": true,
            "include_standing": true,
            "include_references": false,
            "limit": 5,
            "cursor": "abc"
        });
        let req: McpDiscoverRequest = serde_json::from_value(json).expect("deserialises");
        // project_id is explicit null → Some(None)
        assert_eq!(req.project_id, Some(None));
        assert_eq!(req.kinds.as_ref().unwrap().len(), 1);
        assert_eq!(req.limit, Some(5));
    }

    #[test]
    fn test_discover_request_deserialises_present_project_id() {
        let json = serde_json::json!({
            "query": "auth",
            "project_id": "proj_abc",
        });
        let req: McpDiscoverRequest = serde_json::from_value(json).expect("deserialises");
        assert_eq!(req.project_id, Some(Some("proj_abc".to_owned())));
    }

    #[test]
    fn test_discover_response_serialises_required_nullable() {
        let resp = McpDiscoverResponse {
            items: vec![],
            next_cursor: None,
            applied_project_id: None,
            embedding_model: "text-embedding-3-small".into(),
            trace_id: "trace123".into(),
            exact: true,
            query: "test".into(),
            project_name: None,
        };
        let json = serde_json::to_value(&resp).expect("serialises");
        // applied_project_id must be present as null
        assert!(json.get("applied_project_id").is_some());
        assert!(json["applied_project_id"].is_null());
        // next_cursor must be present as null (required field)
        assert!(json.get("next_cursor").is_some());
        assert!(json["next_cursor"].is_null());
        // total_count must not exist
        assert!(json.get("total_count").is_none());
        // serde(skip) fields must not appear
        assert!(json.get("query").is_none());
        assert!(json.get("project_name").is_none());
    }

    #[test]
    fn test_discover_response_into_call_tool_result() {
        let resp = McpDiscoverResponse {
            items: vec![],
            next_cursor: None,
            applied_project_id: Some("proj_abc".into()),
            embedding_model: "m".into(),
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
