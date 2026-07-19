//! Wire DTOs for `tribal_set_context` — the request and response types
//! exchanged over the MCP surface.

use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// Request
// ---------------------------------------------------------------------------

/// Deserialisation target for `tribal_set_context` input.
#[derive(Debug, Default, Clone, Serialize, Deserialize, PartialEq)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct McpSetContextRequest {
    pub project_id: Option<String>,
    pub model: Option<String>,
    pub provider: Option<String>,
}

// ---------------------------------------------------------------------------
// Response types
// ---------------------------------------------------------------------------

/// Project on the MCP `set_context` response surface.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct McpSessionProject {
    pub id: String,
    pub name: String,
    pub git_remote: String,
}

/// Actor metadata on the MCP `set_context` response surface.
///
/// All fields are always present in serialised JSON (null when absent),
/// matching the existing session resource representation.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct McpSessionActor {
    pub client_name: Option<String>,
    pub client_version: Option<String>,
    pub model: Option<String>,
    pub provider: Option<String>,
}

/// Typed response for `tribal_set_context`.
///
/// The `project` field is non-required in the output schema but always
/// included (null when absent) for consistency with the session
/// resource representation.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct McpSetContextResponse {
    pub project: Option<McpSessionProject>,
    pub principal_key: String,
    pub actor: McpSessionActor,
    #[serde(skip)]
    pub mutated: bool,
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // -- McpSetContextRequest ---------------------------------------------

    #[test]
    fn test_set_context_request_deserialises_empty() {
        let json = serde_json::json!({});
        let req: McpSetContextRequest = serde_json::from_value(json).expect("deserialises");
        assert!(req.project_id.is_none());
        assert!(req.model.is_none());
        assert!(req.provider.is_none());
    }

    #[test]
    fn test_set_context_request_round_trip() {
        let req = McpSetContextRequest {
            project_id: Some("proj-1".into()),
            model: Some("claude-opus-4-6".into()),
            provider: Some("anthropic".into()),
        };
        let json = serde_json::to_value(&req).expect("serialises");
        let back: McpSetContextRequest = serde_json::from_value(json).expect("deserialises");
        assert_eq!(req, back);
    }

    // -- McpSetContextResponse --------------------------------------------

    #[test]
    fn test_set_context_response_with_project() {
        let resp = McpSetContextResponse {
            project: Some(McpSessionProject {
                id: "proj-1".into(),
                name: "tribal".into(),
                git_remote: "github.com/user/tribal".into(),
            }),
            principal_key: "user:sam".into(),
            actor: McpSessionActor {
                client_name: None,
                client_version: None,
                model: None,
                provider: None,
            },
            mutated: false,
        };
        let json = serde_json::to_value(&resp).expect("serialises");

        assert_eq!(json["project"]["id"], "proj-1");
        assert_eq!(json["project"]["name"], "tribal");
        assert_eq!(json["principal_key"], "user:sam");
    }

    #[test]
    fn test_set_context_response_without_project() {
        let resp = McpSetContextResponse {
            project: None,
            principal_key: "user:sam".into(),
            actor: McpSessionActor {
                client_name: None,
                client_version: None,
                model: None,
                provider: None,
            },
            mutated: false,
        };
        let json = serde_json::to_value(&resp).expect("serialises");

        // project is always present — null when absent
        assert!(json.get("project").is_some());
        assert!(json["project"].is_null());
        assert_eq!(json["principal_key"], "user:sam");
    }

    #[test]
    fn test_set_context_response_mutated_skipped() {
        let resp = McpSetContextResponse {
            project: None,
            principal_key: "user:sam".into(),
            actor: McpSessionActor {
                client_name: Some("claude-code".into()),
                client_version: None,
                model: Some("claude-opus-4-6".into()),
                provider: Some("anthropic".into()),
            },
            mutated: true,
        };
        let json = serde_json::to_value(&resp).expect("serialises");

        // `mutated` is `#[serde(skip)]` and never appears in the JSON.
        assert!(json.get("mutated").is_none());
        assert_eq!(json["actor"]["client_name"], "claude-code");
        assert!(json["actor"]["client_version"].is_null());
        assert_eq!(json["actor"]["model"], "claude-opus-4-6");
        assert_eq!(json["actor"]["provider"], "anthropic");
    }

    #[test]
    fn test_set_context_response_round_trip() {
        let resp = McpSetContextResponse {
            project: Some(McpSessionProject {
                id: "proj-1".into(),
                name: "tribal".into(),
                git_remote: "github.com/user/tribal".into(),
            }),
            principal_key: "user:sam".into(),
            actor: McpSessionActor {
                client_name: Some("claude-code".into()),
                client_version: Some("1.0".into()),
                model: Some("claude-opus-4-6".into()),
                provider: Some("anthropic".into()),
            },
            mutated: false,
        };
        let json = serde_json::to_value(&resp).expect("serialises");
        let back: McpSetContextResponse = serde_json::from_value(json).expect("deserialises");
        assert_eq!(resp, back);
    }
}
