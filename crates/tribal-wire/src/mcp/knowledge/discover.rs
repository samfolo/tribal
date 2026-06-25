//! MCP request and response types for `tribal_discover`.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Deserializer, Serialize};
use tribal_domain::KnowledgeKind;

use super::common::{McpKnowledgeItem, McpReference, McpStanding};

// ---------------------------------------------------------------------------
// Request
// ---------------------------------------------------------------------------

/// Deserialisation target for `tribal_discover` input.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct McpDiscoverRequest {
    pub query: String,
    /// Three-way semantics: absent → use session project; explicit null →
    /// search globally; present → filter to this project.
    #[serde(default, deserialize_with = "deserialise_optional_nullable")]
    #[allow(clippy::option_option)]
    pub project_id: Option<Option<String>>,
    pub kinds: Option<Vec<KnowledgeKind>>,
    pub tags: Option<Vec<String>>,
    pub time_range: Option<McpTimeRange>,
    pub include_superseded: Option<bool>,
    pub include_standing: Option<bool>,
    pub include_references: Option<bool>,
    pub limit: Option<u32>,
    pub cursor: Option<String>,
}

/// Time range filter for discovery queries.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct McpTimeRange {
    pub from: Option<DateTime<Utc>>,
    pub to: Option<DateTime<Utc>>,
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
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct McpDiscoverResponse {
    pub items: Vec<McpDiscoveryResult>,
    /// Required + nullable — always present (null when no more results).
    pub next_cursor: Option<String>,
    /// Required + nullable — always present in serialised JSON.
    pub applied_project_id: Option<String>,
    pub embedding_model: String,
    /// The active embedding profile that produced these results. Cursors and
    /// feedback are bound to it; a reindex changes it.
    pub embedding_profile_id: String,
    pub trace_id: String,
    pub exact: bool,
    /// The original query text — used by `IntoCallToolResult` for the
    /// human-readable summary. Excluded from `structuredContent`.
    #[serde(skip)]
    pub query: String,
    /// Resolved project name — used by `IntoCallToolResult` for the
    /// human-readable summary. Excluded from `structuredContent`.
    #[serde(skip)]
    pub project_name: Option<String>,
}

/// A single discovery result with its similarity score.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct McpDiscoveryResult {
    pub item: McpKnowledgeItem,
    pub similarity: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub standing: Option<McpStanding>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub references: Option<Vec<McpReference>>,
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
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
            embedding_profile_id: "eprof_test".into(),
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
}
